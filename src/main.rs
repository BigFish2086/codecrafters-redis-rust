mod parser;

use std::env;
use std::net::TcpListener;

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
            Ok(_stream) => {
                println!("accepted new connection");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
