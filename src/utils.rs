use crate::constants::EOF;
use crate::rdb::RDBHeader;
use crate::redis::Redis;
use rand::{thread_rng, Rng};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn gen_millis() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("Time Went Backwards").as_millis()
}

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

pub async fn dump_rdb_file(header: &RDBHeader, redis: Redis) -> Vec<u8> {
    let mut out = header.as_rdb();
    out.extend_from_slice(&redis.as_rdb().await[..]);
    out.push(EOF);
    out
}
