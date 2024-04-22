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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use std::fs::File;
    use std::io::{BufReader, Read};
    use std::io::{BufWriter, Write};
    let mut args = std::env::args().skip(1);
    let mut opt = args.next().unwrap();
    if opt == "--read" {
        let mut file_path = args.next().unwrap();

        let mut ibytes = vec![];
        let mut input = BufReader::new(File::open(file_path)?);
        let read_bytes = input.read_to_end(&mut ibytes)?;
        let mut ibytes: &[u8] = &ibytes;

        let rdb_file = RDBParser::from_rdb(&mut ibytes)?;
        println!("{:#?}", rdb_file);

    } else if opt == "--write" {
        let mut file_path = args.next().unwrap();

        let header = RDBHeader {
            magic: String::from("REDIS"),
            rdb_version: 3,
            aux_settings: HashMap::new(),
        };

        let cfg = Arc::new(Config {
            service_port: crate::constants::DEFAULT_PORT,
            replica_of: ReplicaInfo {
                role: Role::Master,
                master_replid: utils::random_string(40),
                master_repl_offset: 0u64,
            },
        });

        let redis = Arc::new(Mutex::new(Redis::with_config(Arc::clone(&cfg))));
        redis.lock().await.dict.insert(
            ValueType::new("a".to_string()),
            DataEntry::new("-500".to_string(), None),
        );
        redis.lock().await.dict.insert(
            ValueType::new("z".to_string()),
            DataEntry::new(utils::random_string(300), None),
        );
        redis.lock().await.dict.insert(
            ValueType::new("b".to_string()),
            DataEntry::new(String::from_utf8(vec![b'X'; 200]).unwrap(), None),
        );
        let output: Vec<u8> = utils::dump_rdb_file(&header, Arc::clone(&redis)).await;
        let mut file_path = BufWriter::new(File::create(file_path)?);
        file_path.write_all(&output[..])?;
        println!("Complete Write!");
    }
    Ok(())
}
// let irand = random_string(40);
// println!("{:?}", irand);
// let mut ibytes = &hex::decode(input)? as &[u8];
// println!("{:?}", ibytes);

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
            match stream.try_write(resp.serialize().as_bytes()) {
                Ok(_n) => (),
                Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }
}

#[tokio::main]
async fn main2() -> anyhow::Result<()> {
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
                .write_all(&resp_array_of_bulks!("PING").serialize().as_bytes())
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
                        .as_bytes(),
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
                        .as_bytes(),
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
                        .as_bytes(),
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
