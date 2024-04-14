mod parser;

use std::env;
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::thread;

use crate::parser::Parser;

fn handle_client(mut stream: TcpStream) {
    println!("accepted new connection");
    let mut buffer = [0u8; 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(x) if x > 0 => stream.write_all(b"+PONG\r\n").unwrap(),
            _ => break,
        };
    }
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let port = match args.len() {
        2 if args[0] == "--port".to_string() => args[1]
            .trim()
            .parse::<u16>()
            .expect("ERROR: expected port to be 0-65535"),
        _ => 6379,
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port).as_str()).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || handle_client(stream));
            }
            Err(e) => {
                eprintln!("ERROR: {}", e);
            }
        };
    }
}
