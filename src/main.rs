#![allow(warnings, unused)]

mod command;
mod config;
mod constants;
mod data_entry;
mod parser;
mod rdb;
mod redis;
mod resp;
mod utils;

use crate::{
    command::Cmd,
    config::{Config, ReplicaInfo, Role},
    data_entry::{DataEntry, ValueType},
    parser::Parser,
    rdb::{RDBHeader, RDBParser},
    redis::Redis,
    resp::RESPType,
};
use anyhow::{bail, Context};
use std::{
    collections::HashMap,
    env,
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
};
use tokio::{
    io::{AsyncWriteExt, Interest},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::{self, Duration},
};

async fn act_as_master(mut stream: TcpStream, redis: Arc<Mutex<Redis>>) -> anyhow::Result<()> {
    println!("[+] Got Connection: {:?}", stream.peer_addr()?);
    let client_ip = stream.peer_addr().map(|socket| socket.ip())?;
    let (rx, wr) = stream.into_split();
    let wr = Arc::new(Mutex::new(wr));
    let mut pending_interval = time::interval(time::Duration::from_millis(500));
    loop {
        tokio::select! {
            Ok(master_sent_buffer) = async {
                let mut redis_guard = redis.lock().await;
                redis_guard.cfg.read_slave_master_connection().await
            }
            => {
                println!("[+] Master Sent Buffer: {:?}", String::from_utf8_lossy(&master_sent_buffer));
                let (parsed, _rem) = Parser::parse_resp(master_sent_buffer.as_slice())?;
                let cmd = Cmd::from_resp(parsed)?;
                let resp = redis
                    .lock()
                    .await
                    .apply_cmd(client_ip, Arc::clone(&wr), cmd);
            }
            Ok(_) = rx.readable() => {
                let wr = Arc::clone(&wr);
                let mut buffer = vec![0; 1024];
                let n = match rx.try_read(&mut buffer) {
                    Ok(0) => break Ok(()),
                    Ok(n) => n,
                    Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(e.into()),
                };
                println!("[+] Got {:?}", String::from_utf8_lossy(&buffer[..n]));
                let (parsed, _rem) = Parser::parse_resp(&buffer[..n])?;
                let cmd = Cmd::from_resp(parsed)?;
                let resp = redis
                    .lock()
                    .await
                    .apply_cmd(client_ip, Arc::clone(&wr), cmd);
                if wr.lock().await.writable().await.is_ok() {
                    match wr.lock().await.try_write(&resp.serialize()) {
                        Ok(_n) => (),
                        Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            _ = pending_interval.tick() => {
                redis.lock().await.apply_pending_updates_per_host(&client_ip).await;
                println!("[+] Updates Propagated");
            }
        }
    }
}

async fn act_as_replica(redis: Arc<Mutex<Redis>>) -> anyhow::Result<()> {
    let stream = match redis.lock().await.cfg.replica_of.role {
        Role::Slave {
            ref master_host,
            ref master_port,
            ref mut master_connection,
        } => {
            let stream = Arc::new(Mutex::new(
                TcpStream::connect(format!("{}:{}", master_host, master_port))
                    .await
                    .context("slave replica can't connect to its master")?,
            ));
            *master_connection = Some(Arc::clone(&stream));
            stream
        }
        _ => return Ok(()),
    };

    let read_response = |stream: Arc<Mutex<TcpStream>>| async move {
        let stream = stream.lock().await;
        let mut buffer = vec![0; 1024];
        let n = loop {
            stream.readable().await?;
            match stream.try_read(&mut buffer) {
                Ok(n) => break n,
                Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err::<Vec<u8>, std::io::Error>(e.into()),
            };
        };
        Ok(buffer[..n].to_vec())
    };
    let validate_response = |actual: Vec<u8>, expexted: RESPType| {
        let (parsed, _rem) = Parser::parse_resp(&actual)?;
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
    let actual = read_response(Arc::clone(&stream)).await?;
    validate_response(actual, RESPType::SimpleString("PONG".to_string()));

    let service_port = redis.lock().await.cfg.service_port;
    stream
        .lock()
        .await
        .write_all(&resp_array_of_bulks!("REPLCONF", "listening-port", service_port).serialize())
        .await
        .context("slave REPLCONF can't reach its master")?;
    let actual = read_response(Arc::clone(&stream)).await?;
    validate_response(actual, RESPType::SimpleString("OK".to_string()));

    stream
        .lock()
        .await
        .write_all(&resp_array_of_bulks!("REPLCONF", "capa", "eof", "capa", "psync2").serialize())
        .await
        .context("slave REPLCONF can't reach its master")?;
    let actual = read_response(Arc::clone(&stream)).await?;
    validate_response(actual, RESPType::SimpleString("OK".to_string()));

    // TODO: add response validation for PSYNC cmd
    stream
        .lock()
        .await
        .write_all(&resp_array_of_bulks!("PSYNC", "?", "-1").serialize())
        .await
        .context("slave PSYNC can't reach its master")?;
    let _actual = read_response(Arc::clone(&stream)).await?;
    // let rdb_file = RDBParser::from_rdb(&mut ibytes)?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::try_from(env::args())?;

    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.service_port).as_str())
        .await
        .unwrap();

    let is_replica = matches!(cfg.replica_of.role, Role::Slave { .. });
    let redis = Arc::new(Mutex::new(Redis::with_config(cfg)));

    if is_replica {
        let redis = Arc::clone(&redis);
        tokio::spawn(async move { act_as_replica(redis).await });
    }

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let redis = Arc::clone(&redis);
                tokio::spawn(async move { act_as_master(stream, redis).await });
            }
            Err(e) => {
                eprintln!("ERROR: {}", e);
            }
        };
    }
}
