#![allow(warnings, unused)]

mod command;
mod config;
mod constants;
mod parser;
mod rdb;
mod redis;
mod resp;
mod utils;
mod data_entry;

use crate::{
    command::Cmd,
    config::{Config, ReplicaInfo, Role},
    parser::Parser,
    rdb::{RDBHeader, RDBParser},
    data_entry::{DataEntry, ValueType},
    redis::Redis,
    resp::RESPType,
};
use anyhow::{bail, Context};
use std::{collections::HashMap, env, net::{SocketAddr, Ipv4Addr}, sync::Arc};
use tokio::{
    io::{AsyncWriteExt, Interest},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

async fn act_as_master(stream: TcpStream, redis: Arc<Mutex<Redis>>) -> anyhow::Result<()> {
    println!("[+] Got Connection: {:?}", stream.peer_addr()?);
    let client_ip = stream.peer_addr().map(|socket| socket.ip())?;
    loop {
        let ready = stream
            .ready(Interest::READABLE | Interest::WRITABLE)
            .await?;
        stream.readable().await?;
        let mut buffer = vec![0; 1024];
        let n = match stream.try_read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        };
        let (parsed, _rem) = Parser::parse_resp(&buffer[..n])?;
        let cmd = Cmd::from_resp(parsed)?;
        let resp = redis.lock().await.apply_cmd(client_ip, cmd);
        if ready.is_writable() {
            match stream.try_write(&resp.serialize()) {
                Ok(_n) => (),
                Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e.into()),
            }
        }
        redis.lock().await.apply_pending_updates_per_host(&client_ip).await;
    }
}

async fn act_as_replica(
    service_port: u16,
    master_host: &Ipv4Addr,
    master_port: &u16,
) -> anyhow::Result<()> {
    let stream = TcpStream::connect(format!("{}:{}", master_host, master_port))
        .await
        .context("slave replica can't connect to its master")?;
    let stream = Arc::new(Mutex::new(stream));

    let validate_response = |stream: Arc<Mutex<TcpStream>>, expexted: RESPType| async move {
        let stream = stream.lock().await;
        let mut buffer = vec![0; 1024];
        let n = loop {
            stream.readable().await?;
            match stream.try_read(&mut buffer) {
                Ok(n) => break n,
                Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e.into()),
            };
        };
        let (parsed, _rem) = Parser::parse_resp(&buffer[..n])?;
        match parsed == expexted {
            true => Ok(()),
            false => bail!(format!(
                "slave replica received `{}` but expected response `{}`",
                parsed, expexted
            )),
        }
    };

    stream
        .lock()
        .await
        .write_all(&resp_array_of_bulks!("PING").serialize())
        .await
        .context("slave PING can't reach its master")?;

    validate_response(
        Arc::clone(&stream),
        RESPType::SimpleString("PONG".to_string()),
    )
    .await?;

    stream
        .lock()
        .await
        .write_all(&resp_array_of_bulks!("REPLCONF", "listening-port", service_port).serialize())
        .await
        .context("slave REPLCONF can't reach its master")?;

    validate_response(
        Arc::clone(&stream),
        RESPType::SimpleString("OK".to_string()),
    )
    .await?;

    stream
        .lock()
        .await
        .write_all(&resp_array_of_bulks!("REPLCONF", "capa", "eof", "capa", "psync2").serialize())
        .await
        .context("slave REPLCONF can't reach its master")?;

    validate_response(
        Arc::clone(&stream),
        RESPType::SimpleString("OK".to_string()),
    )
    .await?;

    // TODO: add response validation for PSYNC cmd
    stream
        .lock()
        .await
        .write_all(&resp_array_of_bulks!("PSYNC", "?", "-1").serialize())
        .await
        .context("slave PSYNC can't reach its master")?;
    // let rdb_file = RDBParser::from_rdb(&mut ibytes)?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::try_from(env::args())?;

    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.service_port).as_str())
        .await
        .unwrap();

    match cfg.replica_of.role {
        Role::Slave {
            ref master_host,
            ref master_port,
        } => act_as_replica(cfg.service_port, master_host, master_port).await?,
        _ => (),
    };

    let redis = Arc::new(Mutex::new(Redis::with_config(cfg)));

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let redis = redis.clone();
                tokio::spawn(async move { act_as_master(stream, redis).await });
            }
            Err(e) => {
                eprintln!("ERROR: {}", e);
            }
        };
    }
}
