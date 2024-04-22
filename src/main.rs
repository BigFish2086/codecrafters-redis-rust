#![allow(warnings, unused)]

mod command;
mod config;
mod parser;
mod rdb;
mod redis;
mod resp;
mod utils;
mod constants;

use crate::{
    command::Cmd,
    config::{Config, Role, ReplicaInfo},
    parser::Parser,
    rdb::{RDBHeader, RDBParser},
    redis::{ValueType, DataEntry, Redis},
    resp::RESPType,
};
use anyhow::{bail, Context};
use std::{env, sync::Arc, collections::HashMap};
use tokio::{
    io::{AsyncWriteExt, Interest},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

async fn handle_client(stream: TcpStream, redis: Arc<Mutex<Redis>>) -> anyhow::Result<()> {
    println!("accepted new connection");
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
        let resp = redis.lock().await.apply_cmd(cmd);
        if ready.is_writable() {
            match stream.try_write(&resp.serialize()) {
                Ok(_n) => (),
                Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Arc::new(Config::try_from(env::args())?);

    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.service_port).as_str())
        .await
        .unwrap();

    let redis = Arc::new(Mutex::new(Redis::with_config(Arc::clone(&cfg))));

    match cfg.replica_of.role {
        Role::Slave { ref host, ref port } => {
            let stream = TcpStream::connect(format!("{}:{}", host, port))
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
                .write_all(
                    &resp_array_of_bulks!("REPLCONF", "listening-port", cfg.service_port)
                        .serialize()
                )
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
                .write_all(
                    &resp_array_of_bulks!("REPLCONF", "capa", "eof", "capa", "psync2")
                        .serialize()
                )
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
                .write_all(
                    &resp_array_of_bulks!("PSYNC", "?", "-1")
                        .serialize()
                )
                .await
                .context("slave PSYNC can't reach its master")?;
        }

        _ => (),
    };

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let redis = redis.clone();
                tokio::spawn(async move { handle_client(stream, redis).await });
            }
            Err(e) => {
                eprintln!("ERROR: {}", e);
            }
        };
    }
}
