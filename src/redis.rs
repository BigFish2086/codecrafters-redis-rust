use crate::command::Cmd;
use crate::resp::RESPType;
use std::collections::HashMap;

use tokio::time::{Duration, Instant};

#[derive(Debug)]
pub struct DataEntry {
    value: String,
    created_at: Instant,
    expired_millis: Option<Duration>,
}

impl DataEntry {
    pub fn new(value: String, expired_millis: Option<u64>) -> Self {
        Self {
            value,
            expired_millis: expired_millis.map(|x| Duration::from_millis(x)),
            created_at: Instant::now(),
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.expired_millis {
            Some(expiry) => self.created_at.elapsed() > expiry,
            None => false,
        }
    }
}

#[derive(Default, Debug)]
pub struct Redis {
    pub dict: HashMap<String, DataEntry>,
}

impl Redis {
    pub fn apply_cmd(&mut self, cmd: Cmd) -> RESPType {
        use Cmd::*;
        match cmd {
            Ping => RESPType::SimpleString("PONG".to_string()),
            Echo(msg) => RESPType::BulkString(msg),
            Set { key, value, px } => {
                self.dict.insert(key, DataEntry::new(value, px));
                RESPType::SimpleString("OK".to_string())
            }
            Get(key) => match self.dict.get(&key) {
                Some(data) => {
                    if data.is_expired() {
                        self.dict.remove(&key);
                        RESPType::Null
                    } else {
                        RESPType::BulkString(data.value.clone())
                    }
                }
                None => RESPType::Null,
            },
            Info(section) if section.is_none() || section.as_deref().unwrap().to_lowercase() == "replication" => {
                RESPType::BulkString("role:master".to_string())
            },
            _ => unimplemented!("Handle Info Section Better"),
        }
    }
}

