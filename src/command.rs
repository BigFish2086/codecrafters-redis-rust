use crate::resp::RESPType;
use std::fmt;

#[derive(Debug, PartialEq)]
pub enum Cmd {
    Ping,
    Echo(String),
    Set(String, String),
    Get(String),
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
    #[error("ERROR: Invalid command type, expected type was RESP::`{expected}`,but found RESP::`{found}`")]
    InvalidCmdType { expected: String, found: String },
    #[error("ERROR: Missing `{arg_name}` argument for the command `{cmd_name}`")]
    MissingArgs { arg_name: String, cmd_name: String },
    #[error("ERROR: Provided command is not implemented")]
    NotImplementedCmd,
}

impl Cmd {
    pub fn from_resp(resp: RESPType) -> Result<Self, CmdError> {
        if let RESPType::Array(array) = resp {
            let cmd_type = Self::unpack_bulk_string(array.get(0).ok_or(CmdError::NoCmdsProvided)?)?;
            match cmd_type.to_lowercase().as_str() {
                "ping" => Ok(Self::Ping),
                "echo" => {
                    let msg =
                        Self::unpack_bulk_string(array.get(1).ok_or(CmdError::MissingArgs {
                            arg_name: "message".to_string(),
                            cmd_name: "ECHO".to_string(),
                        })?)?;
                    Ok(Self::Echo(msg.clone()))
                }
                "set" => {
                    let key =
                        Self::unpack_bulk_string(array.get(1).ok_or(CmdError::MissingArgs {
                            arg_name: "Key".to_string(),
                            cmd_name: "SET".to_string(),
                        })?)?;
                    let value =
                        Self::unpack_bulk_string(array.get(2).ok_or(CmdError::MissingArgs {
                            arg_name: "Value".to_string(),
                            cmd_name: "SET".to_string(),
                        })?)?;
                    Ok(Self::Set(key.to_string(), value.to_string()))
                }
                "get" => {
                    let key =
                        Self::unpack_bulk_string(array.get(1).ok_or(CmdError::MissingArgs {
                            arg_name: "Key".to_string(),
                            cmd_name: "GET".to_string(),
                        })?)?;
                    Ok(Self::Get(key.clone()))
                }
                _ => Err(CmdError::NotImplementedCmd),
            }
        } else {
            Err(CmdError::InvalidCmdType {
                expected: "Array".to_string(),
                found: resp.to_string(),
            })
        }
    }

    fn unpack_bulk_string(resp: &RESPType) -> Result<String, CmdError> {
        match resp {
            RESPType::BulkString(s) => Ok(s.clone()),
            _ => Err(CmdError::InvalidCmdType {
                expected: "BulkString".to_string(),
                found: resp.to_string(),
            }),
        }
    }
}
