mod command;
mod config;
mod parser;
mod redis;
mod resp;

use crate::{command::Cmd, config::Config, parser::Parser, redis::Redis};
use std::{env, sync::Arc};
use tokio::{
    io::Interest,
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
            match stream.try_write(resp.serialize().as_bytes()) {
                Ok(n) => println!("write {} bytes", n),
                Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::try_from(env::args())?;

    let listener = TcpListener::bind(format!("127.0.0.1:{}", cfg.service_port).as_str())
        .await
        .unwrap();

    let redis = Arc::new(Mutex::new(Redis::with_config(cfg)));

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
