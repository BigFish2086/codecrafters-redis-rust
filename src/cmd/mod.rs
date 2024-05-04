pub mod ack;
pub mod info;
pub mod misc;
pub mod echo;
pub mod get;
pub mod set;
pub mod psync;
pub mod keys;
pub mod typ;
pub mod wait;
pub mod xadd;
pub mod xrange;
pub mod xread;
pub mod config_get;
pub mod cmd_builder;

use async_trait::async_trait;
use crate::resp::RespType;

#[async_trait]
pub trait Cmd {
    async fn run(&mut self) -> RespType;

    fn cmd_type(&self) -> CmdType;
}

pub enum CmdType {
    PING,
    ECHO,
    INFO,
    CONFIG_GET,
    ERR_CMD,

    KEYS,
    TYPE,

    SET,
    GET,
    WAIT,

    ACK,
    GETACK,
    REPLCONF,
    PSYNC,

    XADD,
    XRANGE,
    XREAD,
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum CmdError {
    #[error("ERROR: No commands where provided")]
    NoCmdsProvided,
    #[error("ERROR: Invalid command RESP type")]
    InvalidCmdType,
    #[error("ERROR: Missing arguments for current command")]
    MissingArgs,
    #[error("ERROR: Invalid argument type")]
    InvalidArg,
    #[error("ERROR: Provided command is not implemented")]
    NotImplementedCmd,
}

