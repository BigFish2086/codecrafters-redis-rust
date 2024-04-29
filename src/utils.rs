use crate::constants::EOF;
use crate::rdb::RDBHeader;
use crate::redis::Redis;
use rand::{thread_rng, Rng};
use std::sync::Arc;
use tokio::sync::Mutex;

pub fn random_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub fn take_upto<'a, const N: usize>(data: &mut &'a [u8]) -> Option<&'a [u8; N]> {
    match data.split_first_chunk::<N>() {
        Some((left, right)) => {
            *data = right;
            Some(left)
        }
        None => None,
    }
}

pub async fn dump_rdb_file(header: &RDBHeader, redis: Arc<Mutex<Redis>>) -> Vec<u8> {
    let mut out = header.as_rdb();
    out.extend_from_slice(&redis.lock().await.as_rdb()[..]);
    out.push(EOF);
    out
}

#[allow(dead_code)]
async fn main_test_rdb_read_write() -> anyhow::Result<()> {
    use crate::{
        config::{Config, Role, ReplicaInfo},
        rdb::RDBParser,
        data_entry::{ValueType, DataEntry},
    };
    use std::fs::File;
    use std::collections::HashMap;
    use std::io::{BufReader, Read, BufWriter, Write};

    let mut args = std::env::args().skip(1);
    let opt = args.next().unwrap();
    if opt == "--read" {
        let file_path = args.next().unwrap();

        let mut ibytes = vec![];
        let mut input = BufReader::new(File::open(file_path)?);
        let _read_bytes = input.read_to_end(&mut ibytes)?;
        let mut ibytes: &[u8] = &ibytes;

        let rdb_file = RDBParser::from_rdb(&mut ibytes)?;
        println!("{:#?}", rdb_file);

    } else if opt == "--write" {
        let file_path = args.next().unwrap();

        let header = RDBHeader {
            magic: String::from("REDIS"),
            rdb_version: 3,
            aux_settings: HashMap::new(),
        };

        let cfg = Config {
            service_port: crate::constants::DEFAULT_PORT,
            replica_of: ReplicaInfo {
                role: Role::Master,
                master_replid: random_string(40),
                master_repl_offset: 0u64,
            },
            parameters: HashMap::default(),
        };

        let redis = Arc::new(Mutex::new(Redis::with_config(cfg)));
        redis.lock().await.dict.insert(
            ValueType::new("a".to_string()),
            DataEntry::new("-500".to_string(), None),
        );
        redis.lock().await.dict.insert(
            ValueType::new("z".to_string()),
            DataEntry::new(random_string(300), None),
        );
        redis.lock().await.dict.insert(
            ValueType::new("b".to_string()),
            DataEntry::new(String::from_utf8(vec![b'X'; 200]).unwrap(), None),
        );
        let output: Vec<u8> = dump_rdb_file(&header, Arc::clone(&redis)).await;
        let mut file_path = BufWriter::new(File::create(file_path)?);
        file_path.write_all(&output[..])?;
        println!("Complete Write!");
    }
    Ok(())
}

