use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub enum RespType {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RespType>),
    WildCard(Vec<u8>),
    Null,
}

impl fmt::Display for RespType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl RespType {
    pub fn serialize(&self) -> Vec<u8> {
        use RespType::*;
        match self {
            SimpleString(s) => format!("+{}\r\n", s).as_bytes().to_vec(),
            SimpleError(err) => format!("-ERR {}\r\n", err).as_bytes().to_vec(),
            Integer(num) => format!(":{}\r\n", num).as_bytes().to_vec(),
            BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s).as_bytes().to_vec(),
            Array(values) => {
                let mut ret: Vec<u8> = format!("*{}\r\n", values.len()).as_bytes().to_vec();
                for value in values {
                    ret.extend_from_slice(&value.serialize());
                }
                ret
            }
            WildCard(s) => s.to_vec(),
            Null => format!("$-1\r\n").as_bytes().to_vec(),
        }
    }
}

#[macro_export]
macro_rules! resp_array_of_bulks {
    ($($args:expr),*) => {{
        RespType::Array(vec![$(RespType::BulkString($args.to_string())),*])
    }}
}
