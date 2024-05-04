use async_trait::async_trait;

use crate::cmd::{Cmd, CmdError};
use crate::resp::RespType;
use crate::redis::{AMRedisDB, AMStreams};
use crate::utils::unpack_bulk_string;
use crate::data_entry::ValueType;

pub struct Type {
    pub key: String,
    pub dict: AMRedisDB,
    pub streams: AMStreams,
}

#[async_trait]
impl Cmd for Type {
    async fn run(&mut self) -> RespType {
        use RespType::SimpleString;
        let key = ValueType::new(self.key.clone());
        let mut dict_guard = self.dict.lock().await;
        if let Some(data) = dict_guard.get(&key) {
            if data.is_expired() {
                dict_guard.remove(&key);
                return SimpleString("none".to_string());
            } else {
                return SimpleString(key.type_as_string());
            }
        }
        drop(dict_guard);

        if let Some(stream) = self.streams.lock().await.get(&key) {
            SimpleString("stream".to_string())
        } else {
            SimpleString("none".to_string())
        }
    }
}

impl Type {
    pub fn new<'a>(
        mut args_iter: &mut impl Iterator<Item = &'a RespType>,
        dict: AMRedisDB,
        streams: AMStreams,
    ) -> Result<Self, CmdError> {
        let key = unpack_bulk_string(
            args_iter.next().ok_or_else(|| CmdError::MissingArgs)?,
        )?;
        Ok(Type {
            key,
            dict,
            streams,
        })
    }
}
