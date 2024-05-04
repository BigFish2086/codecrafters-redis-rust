#![allow(warnings, unused)]

mod cmd;
mod config;
mod constants;
mod data_entry;
mod parser;
mod rdb;
mod redis;
mod resp;
mod slave_meta;
mod stream_entry;
mod utils;

use crate::{
    cmd::{cmd_builder::CmdBuilder, Cmd},
    config::{Config, Role},
    parser::Parser,
    rdb::RDBParser,
    redis::*,
    resp::RespType,
};
use anyhow::{bail, Context};
use std::{
    collections::HashMap,
    env,
    fs::File,
    io::{BufReader, Read},
    net::SocketAddr,
    path::Path,
    sync::Arc,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time,
};
// pub type RedisDB = HashMap<ValueType, DataEntry>;
// pub type StreamDB = HashMap<ValueType, StreamEntry>;
//
// // TODO: what if entered dict has same key as streams?
// pub type AMConfig = Arc<Mutex<Config>>;
// pub type AMStreams = Arc<Mutex<StreamDB>>;
// pub type AMStreamSenders = Arc<Mutex<HashMap<String, Sender<RespType>>>>;

async fn handle_client(
    stream: TcpStream,
    socket_addr: SocketAddr,
    redis: AMRedisDB,
    config: AMConfig,
    slaves: AMSlaves,
    streams: AMStreams,
    stream_senders: AMStreamSenders,
) -> anyhow::Result<()> {
    println!("[+] Got Connection: {:?}", socket_addr);
    let (rx, wr) = stream.into_split();
    let wr = Arc::new(Mutex::new(wr));
    let mut pending_interval = time::interval(time::Duration::from_millis(700));
    loop {
        tokio::select! {
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
                let mut input = &buffer[..n];
                loop {
                    let (parsed, rem) = Parser::parse_resp(input)?;
                    let mut cmd = CmdBuilder::from_resp(
                        parsed,
                        redis.clone(),
                        config.clone(),
                        slaves.clone(),
                        streams.clone(),
                        stream_senders.clone(),
                        Some(socket_addr),
                        Some(wr.clone())
                    );
                    let resp = cmd.run().await;
                    if wr.lock().await.writable().await.is_ok() {
                        match wr.lock().await.try_write(&resp.serialize()) {
                            Ok(_n) => (),
                            Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                            Err(e) => return Err(e.into()),
                        }
                    }
                    if rem.is_empty() {
                        break;
                    }
                    input = rem;
                };
            }
            _ = pending_interval.tick() => {
                redis::apply_all_pending_updates(slaves.clone()).await;
            }
        }
    }
}

async fn setup_replica(
    mut redis: &mut AMRedisDB,
    mut config: &mut AMConfig,
    slaves: &mut AMSlaves,
    streams: &mut AMStreams,
    stream_senders: &mut AMStreamSenders,
) -> anyhow::Result<Arc<Mutex<TcpStream>>> {
    let mut cfg_guard = config.lock().await;
    let stream = match cfg_guard.replica_of.role {
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
        _ => anyhow::bail!("not replica then it can't send handshake"),
    };
    let service_port = cfg_guard.service_port;
    drop(cfg_guard);

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
    // TODO: if failed, should i quit the handshake ?
    let validate_response = |actual: Vec<u8>, expexted: RespType| {
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
    let _ = validate_response(actual, RespType::SimpleString("PONG".to_string()));

    stream
        .lock()
        .await
        .write_all(&resp_array_of_bulks!("REPLCONF", "listening-port", service_port).serialize())
        .await
        .context("slave REPLCONF can't reach its master")?;
    let actual = read_response(Arc::clone(&stream)).await?;
    let _ = validate_response(actual, RespType::SimpleString("OK".to_string()));

    stream
        .lock()
        .await
        .write_all(&resp_array_of_bulks!("REPLCONF", "capa", "eof", "capa", "psync2").serialize())
        .await
        .context("slave REPLCONF can't reach its master")?;
    let actual = read_response(Arc::clone(&stream)).await?;
    let _ = validate_response(actual, RespType::SimpleString("OK".to_string()));

    // TODO: add response validation for PSYNC cmd
    stream
        .lock()
        .await
        .write_all(&resp_array_of_bulks!("PSYNC", "?", "-1").serialize())
        .await
        .context("slave PSYNC can't reach its master")?;

    let input = read_response(Arc::clone(&stream)).await?;
    println!("[+] PSYNC RESULT: {:?}", String::from_utf8_lossy(&input));

    let (fullsync_resp, rem) = Parser::parse_until_crlf(&input)?;
    println!(
        "[+] FULLSYNC RESULT: {:?}",
        String::from_utf8_lossy(&fullsync_resp)
    );
    let mut input = rem;

    if input.is_empty() {
        // TODO wait until reading rdb
    }
    // TODO: it would be better if the parsers for RESP and RDB have similar API
    // simple and better change, would be if both agree on mutably change `input`
    let (rdb_header, redis_db) = RDBParser::from_rdb_resp(&mut input)?;
    *redis = Arc::new(Mutex::new(redis_db));

    let client_socket_addr = stream.lock().await.peer_addr()?;
    while !input.is_empty() {
        let input_len_before_parsing = input.len();
        let (parsed, rem) = Parser::parse_resp(&input)?;
        let mut cmd = CmdBuilder::from_resp(
            parsed,
            redis.clone(),
            config.clone(),
            slaves.clone(),
            streams.clone(),
            stream_senders.clone(),
            None,
            None,
        );
        let resp = cmd.run().await;
        let replica_need_to_respond = false; // matches!(cmd, Cmd::GetAck);
        redis::incr_master_repl_offset(
            config.clone(),
            (input_len_before_parsing - rem.len()) as u64,
        )
        .await;
        if replica_need_to_respond {
            if stream.lock().await.writable().await.is_ok() {
                match stream.lock().await.try_write(&resp.serialize()) {
                    Ok(_n) => (),
                    Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
        input = rem;
    }

    println!("[+] Replica Completed HandShake");
    Ok(stream)
}

async fn replica_handle_master_connection(
    master_connection: Arc<Mutex<TcpStream>>,
    mut redis: AMRedisDB,
    config: AMConfig,
    slaves: AMSlaves,
    streams: AMStreams,
    stream_senders: AMStreamSenders,
) -> anyhow::Result<()> {
    let client_socket_addr = master_connection.lock().await.peer_addr()?;
    let master_connection_guard = master_connection.lock().await;

    let mut buffer = vec![0; 1024];
    let n = loop {
        match master_connection_guard.try_read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(n) => break n,
            Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        };
    };
    let master_sent_buffer = buffer[..n].to_vec();

    let mut input = master_sent_buffer.as_slice();
    println!(
        "[+] Master Sent Buffer: {:?}",
        String::from_utf8_lossy(&master_sent_buffer)
    );
    loop {
        let input_len_before_parsing = input.len();
        let (parsed, rem) = Parser::parse_resp(input)?;
        let mut cmd = CmdBuilder::from_resp(
            parsed,
            redis.clone(),
            config.clone(),
            slaves.clone(),
            streams.clone(),
            stream_senders.clone(),
            None,
            None,
        );
        let replica_need_to_respond = false; // matches!(cmd, Cmd::GetAck);
        let resp = cmd.run().await;
        redis::incr_master_repl_offset(
            config.clone(),
            (input_len_before_parsing - rem.len()) as u64,
        )
        .await;
        if replica_need_to_respond {
            if master_connection_guard.writable().await.is_ok() {
                match master_connection_guard.try_write(&resp.serialize()) {
                    Ok(_n) => (),
                    Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
        if rem.is_empty() {
            break;
        }
        input = rem;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::try_from(env::args())?;

    let is_replica = matches!(cfg.replica_of.role, Role::Slave { .. });

    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.service_port).as_str())
        .await
        .unwrap();

    let db_filepath = cfg.get_db_filepath();
    let mut redis = if db_filepath.exists() {
        let mut ibytes = vec![];
        let mut input = BufReader::new(File::open(db_filepath)?);
        let _read_bytes = input.read_to_end(&mut ibytes)?;
        let mut ibytes: &[u8] = &ibytes;
        match RDBParser::from_rdb_file(&mut ibytes) {
            Ok((rdb_header, redis_db)) => {
                println!("{:#?}, {:#?}", rdb_header, redis_db);
                Arc::new(Mutex::new(redis_db))
            }
            _ => Arc::new(Mutex::new(RedisDB::default())),
        }
    } else {
        Arc::new(Mutex::new(RedisDB::default()))
    };

    let mut config = Arc::new(Mutex::new(cfg));
    let mut slaves = Arc::new(Mutex::new(HashMap::default()));
    let mut streams = Arc::new(Mutex::new(HashMap::default()));
    let mut stream_senders = Arc::new(Mutex::new(HashMap::default()));

    if !is_replica {
        loop {
            match listener.accept().await {
                Ok((stream, socket_addr)) => {
                    let redis = Arc::clone(&redis);
                    let config = Arc::clone(&config);
                    let slaves = Arc::clone(&slaves);
                    let streams = Arc::clone(&streams);
                    let stream_senders = Arc::clone(&stream_senders);
                    tokio::spawn(async move {
                        handle_client(
                            stream,
                            socket_addr,
                            redis,
                            config,
                            slaves,
                            streams,
                            stream_senders,
                        )
                        .await
                    });
                }
                Err(e) => {
                    eprintln!("ERROR: {}", e);
                }
            };
        }
    } else {
        let master_connection = setup_replica(
            &mut redis,
            &mut config,
            &mut slaves,
            &mut streams,
            &mut stream_senders,
        )
        .await?;
        println!("{:?}", redis.clone().lock().await);
        loop {
            let redis = Arc::clone(&redis);
            let config = Arc::clone(&config);
            let slaves = Arc::clone(&slaves);
            let streams = Arc::clone(&streams);
            let stream_senders = Arc::clone(&stream_senders);
            tokio::select! {
                Ok((stream, socket_addr)) = listener.accept() => {
                    tokio::spawn(async move { handle_client(stream, socket_addr, redis, config, slaves, streams, stream_senders).await });
                }

                Ok(_) = async {
                    master_connection.lock().await.readable().await
                } => {
                    let master_connection = Arc::clone(&master_connection);
                    // XXX that spawns a lot of threads
                    tokio::spawn(async move { replica_handle_master_connection(master_connection, redis, config, slaves, streams, stream_senders).await });
                }
            }
        }
    }
}
