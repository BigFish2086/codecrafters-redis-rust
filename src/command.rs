use crate::resp::RESPType;

#[derive(Debug, PartialEq)]
pub enum Cmd {
    Ping,
    Echo(RESPType),
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum CmdError {
    #[error("ERROR: Invalid command type, expected type was `{0}`")]
    InvalidCmdType(String),
    #[error("ERROR: No commands where provided")]
    NoCmdsProvided,
    #[error("ERROR: Missing args for the `{0}` command")]
    MissingArgs(String),
    #[error("ERROR: Provided command is not implemented")]
    NotImplementedCmd,
}

impl Cmd {
    pub fn from_resp(resp: RESPType) -> Result<Self, CmdError> {
        if let RESPType::Array(array) = resp {
            let RESPType::BulkString(cmd_type) = array.get(0).ok_or(CmdError::NoCmdsProvided)?
            else {
                return Err(CmdError::InvalidCmdType("BulkString".to_string()));
            };
            match cmd_type.to_lowercase().as_str() {
                "ping" => Ok(Self::Ping),
                "echo" => {
                    let cmd_args = array.get(1).ok_or(CmdError::MissingArgs("ECHO".to_string()))?;
                    Ok(Self::Echo(cmd_args.clone()))
                }
                _ => Err(CmdError::NotImplementedCmd),
            }
        } else {
            Err(CmdError::InvalidCmdType("Array".to_string()))
        }
    }

    pub fn respond(self) -> RESPType {
        match self {
            Self::Ping => RESPType::SimpleString("PONG".to_string()),
            Self::Echo(arg) => arg.clone(),
        }
    }
}
