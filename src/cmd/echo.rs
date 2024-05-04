use async_trait::async_trait;
use crate::resp::RespType;

use crate::cmd::{Cmd, CmdError};
use crate::utils::unpack_bulk_string;

pub struct Echo {
    pub msg: String,
}

#[async_trait]
impl Cmd for Echo {
    async fn run(&mut self) -> RespType {
        RespType::BulkString(self.msg.clone())
    }
}

impl Echo {
    pub fn new<'a>(mut args_iter: &mut impl Iterator<Item = &'a RespType>) -> Result<Self, CmdError> {
        let msg = unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        Ok(Self { msg })
    }
}
