use crate::resp::RESPType;
use crate::resp_array_of_bulks;
use std::collections::{HashMap, BTreeMap};
use std::fmt;
use tokio::time::Duration;

#[derive(Debug, PartialEq)]
pub enum Cmd {
    Ping,
    Echo(String),
    Set {
        key: String,
        value: String,
        px: Option<u64>,
    },
    Get(String),
    Info(Option<String>),
    Ack,
    GetAck,
    ReplConf(HashMap<String, Vec<String>>),
    Psync {
        replid: String,
        offset: i64,
    },
    Wait {
        num_replicas: u64,
        timeout: Duration,
    },
    ConfigGet(String),
    Keys(String),
    Type(String),
    XAdd {
        stream_key: String,
        stream_id: String,
        stream_data: BTreeMap<String, String>,
    },
    XRange {
        stream_key: String,
        start_id: String,
        end_id: String,
    },
    XRead {
        keys: Vec<String>,
        ids: Vec<String>,
    }
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
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

impl Cmd {
    pub fn from_resp(resp: RESPType) -> Result<Self, CmdError> {
        if let RESPType::Array(array) = resp {
            let cmd_type =
                Self::unpack_bulk_string(array.get(0).ok_or_else(|| CmdError::NoCmdsProvided)?)?;
            match cmd_type.to_lowercase().as_str() {
                "ping" => Ok(Self::Ping),
                "echo" => Self::echo_cmd(array),
                "set" => Self::set_cmd(array),
                "get" => Self::get_cmd(array),
                "info" => Self::info_cmd(array),
                "psync" => Self::psync_cmd(array),
                "wait" => Self::wait_cmd(array),
                "replconf" => {
                    let arg = Self::unpack_bulk_string(
                        array.get(1).ok_or_else(|| CmdError::MissingArgs)?,
                    )?;
                    if arg.to_lowercase().as_str() == "getack" {
                        Ok(Self::GetAck)
                    } else if arg.to_lowercase().as_str() == "ack" {
                        Ok(Self::Ack)
                    } else {
                        Self::replconf_cmd(array)
                    }
                }
                "config" => {
                    let arg = Self::unpack_bulk_string(
                        array.get(1).ok_or_else(|| CmdError::MissingArgs)?,
                    )?;
                    if arg.to_lowercase().as_str() == "get" {
                        let param = Self::unpack_bulk_string(
                            array.get(2).ok_or_else(|| CmdError::MissingArgs)?,
                        )?;
                        return Ok(Self::ConfigGet(param.trim().to_lowercase()));
                    }
                    Err(CmdError::NotImplementedCmd)
                }
                "keys" => {
                    let pattern = Self::unpack_bulk_string(
                        array.get(1).ok_or_else(|| CmdError::MissingArgs)?,
                    )?;
                    Ok(Self::Keys(pattern))
                }
                "type" => {
                    let key = Self::unpack_bulk_string(
                        array.get(1).ok_or_else(|| CmdError::MissingArgs)?,
                    )?;
                    Ok(Self::Type(key))
                }
                "xadd" => {
                    let stream_key = Self::unpack_bulk_string(
                        array.get(1).ok_or_else(|| CmdError::MissingArgs)?,
                    )?;
                    let stream_id = Self::unpack_bulk_string(
                        array.get(2).ok_or_else(|| CmdError::MissingArgs)?,
                    )?;
                    let mut stream_data = BTreeMap::new();
                    for key_value in array[3..].chunks_exact(2) {
                        let key = Self::unpack_bulk_string(&key_value[0])?;
                        let value = Self::unpack_bulk_string(&key_value[1])?;
                        stream_data.insert(key, value);
                    }
                    Ok(Self::XAdd {
                        stream_key,
                        stream_id,
                        stream_data,
                    })
                }
                "xrange" => {
                    let stream_key = Self::unpack_bulk_string(
                        array.get(1).ok_or_else(|| CmdError::MissingArgs)?,
                    )?;
                    let start_id = Self::unpack_bulk_string(
                        array.get(2).ok_or_else(|| CmdError::MissingArgs)?,
                    )?;
                    let end_id = Self::unpack_bulk_string(
                        array.get(3).ok_or_else(|| CmdError::MissingArgs)?,
                    )?;
                    Ok(Self::XRange {
                        stream_key,
                        start_id,
                        end_id,
                    })
                }
                "xread" => {
                    let streams_arg = Self::unpack_bulk_string(
                        array.get(1).ok_or_else(|| CmdError::MissingArgs)?,
                    )?;
                    if !streams_arg.eq("streams") {
                        return Err(CmdError::InvalidArg);
                    }
                    let array = &array[2..];
                    let mut keys = Vec::new();
                    let mut ids = Vec::new();
                    for i in 0..array.len()/2 {
                        keys.push(Self::unpack_bulk_string(&array[i])?);
                        ids.push(Self::unpack_bulk_string(&array[i+array.len()/2])?);
                    }
                    println!("{:?}", keys);
                    println!("{:?}", ids);
                    Ok(Self::XRead { keys, ids })
                }
                _ => Err(CmdError::NotImplementedCmd),
            }
        } else {
            Err(CmdError::InvalidCmdType)
        }
    }

    pub fn to_resp_array_of_bulks(&self) -> RESPType {
        match self {
            Self::Set { key, value, px } => match px {
                Some(millis) => resp_array_of_bulks!("SET", key, value, "px", millis),
                None => resp_array_of_bulks!("SET", key, value),
            },
            _ => todo!("Getting RESPType::Array of Cmd is not fully implemented yet"),
        }
    }

    fn echo_cmd(args: Vec<RESPType>) -> Result<Self, CmdError> {
        let msg = Self::unpack_bulk_string(args.get(1).ok_or_else(|| CmdError::MissingArgs)?)?;
        Ok(Self::Echo(msg.clone()))
    }

    fn set_cmd(args: Vec<RESPType>) -> Result<Self, CmdError> {
        let key = Self::unpack_bulk_string(args.get(1).ok_or_else(|| CmdError::MissingArgs)?)?;
        let value = Self::unpack_bulk_string(args.get(2).ok_or_else(|| CmdError::MissingArgs)?)?;
        let px = match args.get(3) {
            Some(_) => {
                let px_value =
                    Self::unpack_bulk_string(args.get(4).ok_or_else(|| CmdError::MissingArgs)?)?;
                match px_value.parse::<u64>() {
                    Ok(px_value) => Some(px_value),
                    Err(_) => return Err(CmdError::InvalidArg),
                }
            }
            None => None,
        };
        Ok(Self::Set {
            key: key.to_string(),
            value: value.to_string(),
            px,
        })
    }

    fn get_cmd(args: Vec<RESPType>) -> Result<Self, CmdError> {
        let key = Self::unpack_bulk_string(args.get(1).ok_or_else(|| CmdError::MissingArgs)?)?;
        Ok(Self::Get(key.clone()))
    }

    fn info_cmd(args: Vec<RESPType>) -> Result<Self, CmdError> {
        match args.get(1) {
            Some(section) => Ok(Self::Info(Some(Self::unpack_bulk_string(section)?))),
            None => Ok(Self::Info(None)),
        }
    }

    fn replconf_cmd(args: Vec<RESPType>) -> Result<Self, CmdError> {
        let mut replica_config = HashMap::new();
        // TODO: later we can verify each config cmd vs. its args
        for cmd_arg in args[1..].chunks_exact(2) {
            let cmd = Self::unpack_bulk_string(&cmd_arg[0])?;
            let arg = Self::unpack_bulk_string(&cmd_arg[1])?;
            replica_config.entry(cmd).or_insert_with(Vec::new).push(arg);
        }
        Ok(Self::ReplConf(replica_config))
    }

    fn psync_cmd(args: Vec<RESPType>) -> Result<Self, CmdError> {
        let replid = Self::unpack_bulk_string(args.get(1).ok_or_else(|| CmdError::MissingArgs)?)?;
        let offset = Self::unpack_bulk_string(args.get(2).ok_or_else(|| CmdError::MissingArgs)?)?;
        let offset = offset.parse::<i64>().map_err(|_| CmdError::InvalidArg)?;
        Ok(Self::Psync { replid, offset })
    }

    fn wait_cmd(args: Vec<RESPType>) -> Result<Self, CmdError> {
        let num_replicas =
            Self::unpack_bulk_string(args.get(1).ok_or_else(|| CmdError::MissingArgs)?)?;
        let num_replicas = num_replicas
            .parse::<u64>()
            .map_err(|_| CmdError::InvalidArg)?;

        let timeout = Self::unpack_bulk_string(args.get(2).ok_or_else(|| CmdError::MissingArgs)?)?;
        let timeout = timeout.parse::<u64>().map_err(|_| CmdError::InvalidArg)?;
        let timeout = Duration::from_millis(timeout);

        Ok(Self::Wait {
            num_replicas,
            timeout,
        })
    }

    fn unpack_bulk_string(resp: &RESPType) -> Result<String, CmdError> {
        match resp {
            RESPType::BulkString(s) => Ok(s.clone()),
            _ => Err(CmdError::InvalidCmdType),
        }
    }
}
