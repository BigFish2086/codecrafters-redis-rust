use async_trait::async_trait;

use crate::resp::RespType;
use crate::resp_array_of_bulks;
use crate::utils::unpack_bulk_string;
use crate::cmd::{Cmd, CmdError};
use crate::redis::{AMRedisDB, AMSlaves, add_pending_update_resp};
use crate::data_entry::{ValueType, DataEntry};

use std::time::Duration;

pub struct Set {
    pub key: String,
    pub value: String,
    pub px: Option<u64>,
    pub dict: AMRedisDB,
    pub slaves: AMSlaves,
}

#[async_trait]
impl Cmd for Set {
    async fn run(&mut self) -> RespType {
        let mut dict_guard = self.dict.lock().await;
        dict_guard.insert(
            ValueType::new(self.key.clone()),
            DataEntry::new(self.value.clone(), self.px),
        );
        drop(dict_guard);
        add_pending_update_resp(self.slaves.clone(), &self.as_resp()).await;
        RespType::SimpleString("OK".to_string())
    }
}

impl Set {
    pub fn new<'a>(
        mut args_iter: &mut impl Iterator<Item = &'a RespType>,
        dict: AMRedisDB,
        slaves: AMSlaves,
    ) -> Result<Self, CmdError> {
        let key =
            unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        let value =
            unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        let px = match args_iter.next() {
            Some(_) => {
                let px_value = unpack_bulk_string(
                    args_iter.next().ok_or_else(|| CmdError::MissingArgs)?,
                )?;
                match px_value.parse::<u64>() {
                    Ok(px_value) => Some(px_value),
                    Err(_) => return Err(CmdError::InvalidArg),
                }
            }
            None => None,
        };
        Ok(Set {
            key: key.to_string(),
            value: value.to_string(),
            px,
            dict,
            slaves,
        })
    }

    // TODO: can be part of the Cmd trait
    pub fn as_resp(&self) -> RespType {
        match self.px {
            Some(millis) => resp_array_of_bulks!("SET", self.key, self.value, "px", millis),
            None => resp_array_of_bulks!("SET", self.key, self.value),
        }
    }
}
