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
    config::{Config, Role},
    parser::Parser,
    rdb::RDBParser,
    redis::Redis,
    resp::RESPType,
};
use anyhow::{bail, Context};
use std::{
    env,
    net::SocketAddr,
    sync::Arc,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::Mutex,
    time::self,
};

async fn act_as_master(stream: TcpStream, socket_addr: SocketAddr, redis: Arc<Mutex<Redis>>) -> anyhow::Result<()> {
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
                    let cmd = Cmd::from_resp(parsed)?;
                    let resp = redis
                        .lock()
                        .await
                        .apply_cmd(socket_addr, Some(Arc::clone(&wr)), cmd).await;
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
                redis.lock().await.apply_all_pending_updates().await;
            }
        }
    }
}

async fn act_as_replica(redis: Arc<Mutex<Redis>>) -> anyhow::Result<Arc<Mutex<TcpStream>>> {
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
        _ => anyhow::bail!("not replica then it can't send handshake"),
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
    // TODO: if failed, should i quit the handshake ?
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

    let input = read_response(Arc::clone(&stream)).await?;
    println!("[+] PSYNC RESULT: {:?}", String::from_utf8_lossy(&input));

    let (fullsync_resp, rem) = Parser::parse_until_crlf(&input)?;
    println!("[+] FULLSYNC RESULT: {:?}", String::from_utf8_lossy(&fullsync_resp));
    let mut input = rem;

    if input.is_empty() {
        // TODO
        // wait until reading rdb
    }
    // TODO: it would be better if the parsers for RESP and RDB have similar API
    // simple and better change, would be if both agree on mutably change `input`
    let (rdb_header, redis_db) = RDBParser::from_rdb(&mut input)?;
    println!("[+] RDB Header: {:?}", rdb_header);
    redis.lock().await.dict = redis_db;
    println!("[+] RDB DataBase Parsed and written to redis: {:?}", redis.lock().await.dict);

    let client_socket_addr = stream.lock().await.peer_addr()?;
    while !input.is_empty() {
        let input_len_before_parsing = input.len();
        let (parsed, rem) = Parser::parse_resp(&input)?;
        if let Ok(cmd) = Cmd::from_resp(parsed) {
            let replica_need_to_respond = matches!(cmd, Cmd::GetAck);
            let resp = redis
                .lock()
                .await
                .apply_cmd(client_socket_addr, None, cmd).await;
            redis
                .lock()
                .await
                .cfg.replica_of.master_repl_offset += (input_len_before_parsing - rem.len()) as u64;
            if replica_need_to_respond {
                if stream.lock().await.writable().await.is_ok() {
                    match stream.lock().await.try_write(&resp.serialize()) {
                        Ok(_n) => (),
                        Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
        input = rem;
    }

    println!("[+] Replica Completed HandShake");
    Ok(stream)

}

async fn replica_handle_master_connection(master_connection: Arc<Mutex<TcpStream>>, redis: Arc<Mutex<Redis>>) -> anyhow::Result<()> {
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
    println!("[+] Master Sent Buffer: {:?}", String::from_utf8_lossy(&master_sent_buffer));
    loop {
        let input_len_before_parsing = input.len();
        let (parsed, rem) = Parser::parse_resp(input)?;
        if let Ok(cmd) = Cmd::from_resp(parsed) {
            let replica_need_to_respond = matches!(cmd, Cmd::GetAck);
            let resp = redis
                .lock()
                .await
                .apply_cmd(client_socket_addr, None, cmd).await;
            redis
                .lock()
                .await
                .cfg.replica_of.master_repl_offset += (input_len_before_parsing - rem.len()) as u64;
            if replica_need_to_respond {
                if master_connection_guard.writable().await.is_ok() {
                    match master_connection_guard.try_write(&resp.serialize()) {
                        Ok(_n) => (),
                        Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
        if rem.is_empty() {
            break;
        }
        input = rem;
    };
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

    if !is_replica {
        loop {
            match listener.accept().await {
                Ok((stream, socket_addr)) => {
                    let redis = Arc::clone(&redis);
                    tokio::spawn(async move { act_as_master(stream, socket_addr, redis).await });
                }
                Err(e) => {
                    eprintln!("ERROR: {}", e);
                }
            };
        }
    } else {
        let master_connection = act_as_replica(Arc::clone(&redis)).await?;
        loop {
            tokio::select! {
                Ok((stream, socket_addr)) = listener.accept() => {
                    let redis = Arc::clone(&redis);
                    let master_connection = Arc::clone(&master_connection);
                    tokio::spawn(async move { act_as_master(stream, socket_addr, redis).await });
                }

                Ok(_) = async {
                    master_connection.lock().await.readable().await
                } => {
                    let redis = Arc::clone(&redis);
                    let master_connection = Arc::clone(&master_connection);
                    // XXX that spawns a lot of threads
                    tokio::spawn(async move { replica_handle_master_connection(master_connection, redis).await });
                }
            }
        }
    }
}
