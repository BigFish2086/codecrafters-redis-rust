use crate::constants::EOF;
use crate::rdb::RDBHeader;
use crate::resp::RespType;
use crate::redis::{AMRedisDB, db_as_rdb};
use crate::cmd::CmdError;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use rand::{thread_rng, Rng};
use tokio::sync::Mutex;

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

pub async fn dump_rdb_file(header: &RDBHeader, redis: AMRedisDB) -> Vec<u8> {
    let mut out = header.as_rdb();
    out.extend_from_slice(&db_as_rdb(redis).await[..]);
    out.push(EOF);
    out
}

pub fn unpack_bulk_string(resp: &RespType) -> Result<String, CmdError> {
    match resp {
        RespType::BulkString(s) => Ok(s.clone()),
        _ => Err(CmdError::InvalidCmdType),
    }
}

