use crate::cmd::{
    ack::{Ack, GetAck},
    config_get::ConfigGet,
    echo::Echo,
    get::Get,
    info::Info,
    keys::Keys,
    misc::{ErrCmd, Ping, ReplConf},
    psync::Psync,
    set::Set,
    typ::Type,
    wait::Wait,
    xadd::XAdd,
    xrange::XRange,
    xread::XRead,
};
use crate::cmd::{Cmd, CmdError};
use crate::redis::*;
use crate::resp::RespType;
use crate::slave_meta::WriteStream;
use crate::utils::unpack_bulk_string;

use std::net::SocketAddr;

pub struct CmdBuilder;

impl CmdBuilder {
    pub fn from_resp(
        resp: RespType,
        dict: AMRedisDB,
        config: AMConfig,
        slaves: AMSlaves,
        streams: AMStreams,
        stream_senders: AMStreamSenders,
        socket_addr: Option<SocketAddr>,
        wr: Option<WriteStream>,
    ) -> Box<dyn Cmd + Send> {
        if let RespType::Array(array) = resp {
            let mut array_iter = array.iter();
            let cmd_type = Self::cmd_type(&mut array_iter);
            let cmd_type = match cmd_type {
                Ok(cmd_type) => cmd_type,
                Err(err_msg) => {
                    return Box::new(ErrCmd {
                        err_msg: err_msg.to_string(),
                    }) as Box<dyn Cmd + Send>
                }
            };
            let cmd: Result<Box<dyn Cmd + Send>, CmdError> = match cmd_type.to_lowercase().as_str() {
                "ping" => Ok(Box::new(Ping {})),
                "echo" => Echo::new(&mut array_iter).map(|cmd| Box::new(cmd) as Box<dyn Cmd + Send>),
                "set" => {
                    Set::new(&mut array_iter, dict, slaves).map(|cmd| Box::new(cmd) as Box<dyn Cmd + Send>)
                }
                "get" => Get::new(&mut array_iter, dict).map(|cmd| Box::new(cmd) as Box<dyn Cmd + Send>),
                "info" => {
                    Info::new(&mut array_iter, config).map(|cmd| Box::new(cmd) as Box<dyn Cmd + Send>)
                }
                "psync" => Psync::new(&mut array_iter, dict, config, slaves, wr, socket_addr)
                    .map(|cmd| Box::new(cmd) as Box<dyn Cmd + Send>),
                "wait" => {
                    Wait::new(&mut array_iter, slaves).map(|cmd| Box::new(cmd) as Box<dyn Cmd + Send>)
                }
                "config" => {
                    ConfigGet::new(&mut array_iter, config).map(|cmd| Box::new(cmd) as Box<dyn Cmd + Send>)
                }

                "replconf" => Self::replconf_cmd(&mut array_iter, slaves, config),

                "keys" => Keys::new(&mut array_iter, dict).map(|cmd| Box::new(cmd) as Box<dyn Cmd + Send>),
                "type" => Type::new(&mut array_iter, dict, streams)
                    .map(|cmd| Box::new(cmd) as Box<dyn Cmd + Send>),
                "xadd" => XAdd::new(&mut array_iter, streams, stream_senders)
                    .map(|cmd| Box::new(cmd) as Box<dyn Cmd + Send>),
                "xrange" => {
                    XRange::new(&mut array_iter, streams).map(|cmd| Box::new(cmd) as Box<dyn Cmd + Send>)
                }
                "xread" => XRead::new(&mut array_iter, streams, stream_senders)
                    .map(|cmd| Box::new(cmd) as Box<dyn Cmd + Send>),

                _ => Ok(Box::new(ErrCmd {
                    err_msg: CmdError::NotImplementedCmd.to_string(),
                }) as Box<dyn Cmd + Send>),
            };
            return match cmd {
                Ok(cmd) => cmd,
                Err(err_msg) => Box::new(ErrCmd {
                    err_msg: err_msg.to_string(),
                }) as Box<dyn Cmd + Send>,
            };
        }
        Box::new(ErrCmd {
            err_msg: CmdError::InvalidCmdType.to_string(),
        }) as Box<dyn Cmd + Send>
    }

    fn cmd_type<'a>(
        mut args_iter: &mut impl Iterator<Item = &'a RespType>,
    ) -> Result<String, CmdError> {
        unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::NoCmdsProvided)?)
    }

    fn replconf_cmd<'a>(
        mut args_iter: &mut impl Iterator<Item = &'a RespType>,
        slaves: AMSlaves,
        config: AMConfig,
    ) -> Result<Box<dyn Cmd + Send>, CmdError> {
        let arg = unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        let cmd: Box<dyn Cmd + Send> = match arg.to_lowercase().as_str() {
            "getack" => Box::new(GetAck { slaves, config }),
            "ack" => Box::new(Ack {}),
            _ => Box::new(ReplConf {}),
        };
        Ok(cmd)
    }
}
