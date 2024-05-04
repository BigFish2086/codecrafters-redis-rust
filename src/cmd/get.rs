use async_trait::async_trait;
use crate::resp::RespType;

use crate::cmd::{Cmd, CmdError, CmdType};
use crate::redis::AMRedisDB;
use crate::data_entry::ValueType;
use crate::utils::unpack_bulk_string;

pub struct Get {
    pub key: String,
    pub dict: AMRedisDB,
}

#[async_trait]
impl Cmd for Get {
    async fn run(&mut self) -> RespType {
        let key = ValueType::new(self.key.clone());
        let mut dict_guard = self.dict.lock().await;
        match dict_guard.get(&key) {
            Some(data) => {
                if data.is_expired() {
                    dict_guard.remove(&key);
                    RespType::Null
                } else {
                    RespType::BulkString(data.value.as_string())
                }
            }
            None => RespType::Null,
        }
    }

    fn cmd_type(&self) -> CmdType {
        CmdType::GET
    }
}

impl Get {
    pub fn new<'a>(
        mut args_iter: &mut impl Iterator<Item = &'a RespType>,
        dict: AMRedisDB,
    ) -> Result<Self, CmdError> {
        let key = unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        Ok(Self { key, dict })
    }
}
