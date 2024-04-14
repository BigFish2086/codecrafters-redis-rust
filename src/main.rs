mod parser;

use std::env;
use std::net::TcpListener;
use std::io::{Read, Write};

use crate::parser::Parser;

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
            Ok(mut stream) => {
                let mut buffer = [0u8; 1024];
                match stream.read(&mut buffer) {
                    Ok(buffer_len) => {
                        println!("accepted new connection");
                        let content = Parser::parse_resp(&buffer[..buffer_len]).unwrap();
                        dbg!(content);
                        stream.write_all(b"+PONG\r\n").unwrap();
                    },
                    Err(e) => eprintln!("ERROR: {}", e),
                };
            }
            Err(e) => {
                eprintln!("ERROR: {}", e);
            }
        }
    }
}
