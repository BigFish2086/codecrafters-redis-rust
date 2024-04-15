#[derive(Clone, Debug, PartialEq)]
pub enum RESPType {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RESPType>),
    Null,
}

impl RESPType {
    pub fn serialize(self) -> String {
        use RESPType::*;
        match self {
            SimpleString(s) => format!("+{}\r\n", s),
            SimpleError(err) => format!("-{}\r\n", err),
            Integer(num) => format!(":{}\r\n", num),
            BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Array(values) => {
                let mut ret = format!("*{}\r\n", values.len());
                for value in values {
                    ret = format!("{}{}", ret, value.serialize());
                }
                ret
            }
            Null => format!("$-1\r\n"),
        }
    }
}


