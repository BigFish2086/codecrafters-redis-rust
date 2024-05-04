use async_trait::async_trait;

use crate::cmd::{Cmd, CmdError};
use crate::resp_array_of_bulks;
use crate::resp::RespType;
use crate::redis::AMConfig;
use crate::utils::unpack_bulk_string;

pub struct ConfigGet {
    pub param: String,
    pub config: AMConfig,
}

#[async_trait]
impl Cmd for ConfigGet {
    async fn run(&mut self) -> RespType {
        resp_array_of_bulks!(
            &self.param,
            self.config
                .lock()
                .await
                .parameters
                .get(&self.param)
                .unwrap_or(&"-1".to_string())
        )
    }
}

impl ConfigGet {
    pub fn new<'a>(
        mut args_iter: &mut impl Iterator<Item = &'a RespType>,
        config: AMConfig,
    ) -> Result<Self, CmdError> {
        let arg = unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        if arg.to_lowercase().as_str() == "get" {
            let param =
                unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
            return Ok(Self {
                config,
                param: param.trim().to_lowercase(),
            });
        }
        Err(CmdError::NotImplementedCmd)
    }
}
