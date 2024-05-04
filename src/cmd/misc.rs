use async_trait::async_trait;
use crate::resp::RespType;

use crate::cmd::Cmd;

pub struct Ping;

#[async_trait]
impl Cmd for Ping {
    async fn run(&mut self) -> RespType {
        RespType::SimpleString("PONG".to_string())
    }
}

pub struct ReplConf;

#[async_trait]
impl Cmd for ReplConf {
    async fn run(&mut self) -> RespType {
        RespType::SimpleString("OK".to_string())
    }
}

pub struct ErrCmd {
    pub err_msg: String,
}

#[async_trait]
impl Cmd for ErrCmd {
    async fn run(&mut self) -> RespType {
        RespType::SimpleError(self.err_msg.clone())
    }
}

