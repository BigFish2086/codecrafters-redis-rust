use async_trait::async_trait;

use crate::cmd::{Cmd, CmdError};
use crate::resp::RespType;
use crate::redis::AMRedisDB;
use crate::utils::unpack_bulk_string;

pub struct Keys {
    pub pattern: String,
    pub dict: AMRedisDB,
}

#[async_trait]
impl Cmd for Keys {
    async fn run(&mut self) -> RespType {
        // TODO: should match the given pattern instead
        let mut dict_guard = self.dict.lock().await;
        dict_guard.retain(|_, v| !v.is_expired());
        let mut result = Vec::with_capacity(dict_guard.len());
        for (key, _value) in dict_guard.iter() {
            result.push(RespType::BulkString(key.as_string()));
        }
        drop(dict_guard);
        RespType::Array(result)
    }
}

impl Keys {
    pub fn new<'a>(
        mut args_iter: &mut impl Iterator<Item = &'a RespType>,
        dict: AMRedisDB,
    ) -> Result<Self, CmdError> {
        let pattern =
            unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        Ok(Self { pattern, dict })
    }
}
