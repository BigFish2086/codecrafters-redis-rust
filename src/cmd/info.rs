use async_trait::async_trait;
use crate::resp::RespType;

use crate::cmd::{Cmd, CmdError, CmdType};
use crate::redis::AMConfig;
use crate::utils::unpack_bulk_string;

pub struct Info {
    section: Option<String>,
    config: AMConfig,
}

#[async_trait]
impl Cmd for Info {
    async fn run(&mut self) -> RespType {
        RespType::BulkString(self.config.lock().await.get_info(self.section.clone()))
    }

    fn cmd_type(&self) -> CmdType {
        CmdType::INFO
    }
}

impl Info {
    pub fn new<'a>(
        mut args_iter: &mut impl Iterator<Item = &'a RespType>,
        config: AMConfig,
    ) -> Result<Self, CmdError> {
        let section = match args_iter.next() {
            Some(section) => Some(unpack_bulk_string(section)?),
            None => None,
        };
        Ok(Self {
            section,
            config,
        })
    }
}
