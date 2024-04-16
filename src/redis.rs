use crate::command::Cmd;
use crate::resp::RESPType;
use std::collections::HashMap;

#[derive(Default, Debug)]
pub struct Redis {
    // TODO: add timers for keys experiation dates
    pub dict: HashMap<String, String>,
}

impl Redis {
    pub fn apply_cmd(&mut self, cmd: Cmd) -> RESPType {
        use Cmd::*;
        match cmd {
            Ping => RESPType::SimpleString("PONG".to_string()),
            Echo(msg) => RESPType::BulkString(msg),
            Set(key, value) => {
                self.dict.insert(key, value);
                RESPType::SimpleString("OK".to_string())
            }
            Get(key) => match self.dict.get(&key) {
                Some(value) => RESPType::BulkString(value.clone()),
                None => RESPType::Null,
            },
        }
    }
}
