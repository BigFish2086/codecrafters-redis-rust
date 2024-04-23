use crate::{
    command::Cmd,
    config::{Config, Role},
    constants::{COMPRESS_AT_LENGTH, EXPIRETIMEMS},
    rdb::RDBHeader,
    resp::RESPType,
};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::{
    io::{AsyncWriteExt, Interest},
    net::{TcpListener, TcpStream},
};

use tokio::time::{Duration, Instant};

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum ValueType {
    I8Int(i8),
    I16Int(i16),
    I32Int(i32),
    IntOrString(String),
    CompressedString {
        real_data_len: usize,
        compressed_data: Vec<u8>,
    },
}

impl ValueType {
    pub fn new(data: String) -> Self {
        if let Ok(as_i8) = data.parse::<i8>() {
            Self::I8Int(as_i8)
        } else if let Ok(as_i16) = data.parse::<i16>() {
            Self::I16Int(as_i16)
        } else if let Ok(as_i32) = data.parse::<i32>() {
            Self::I32Int(as_i32)
        } else if data.len() < COMPRESS_AT_LENGTH as usize {
            Self::IntOrString(data)
        } else {
            match lzf::compress(&data.as_bytes()) {
                Ok(compressed_data) => Self::CompressedString {
                    real_data_len: data.len(),
                    compressed_data,
                },
                Err(lzf::LzfError::NoCompressionPossible) => Self::IntOrString(data),
                _ => unreachable!(),
            }
        }
    }

    pub fn as_string(&self) -> String {
        use ValueType::*;
        match self {
            I8Int(x) => x.to_string(),
            I16Int(x) => x.to_string(),
            I32Int(x) => x.to_string(),
            IntOrString(x) => x.to_string(),
            CompressedString {
                real_data_len,
                compressed_data,
            } => {
                // TODO: handle errors better
                let decompressed = lzf::decompress(&compressed_data, *real_data_len).unwrap();
                String::from(String::from_utf8_lossy(&decompressed))
            }
        }
    }

    fn number_to_length_encoded(number: u32) -> Vec<u8> {
        match number {
            0..=63 => {
                // 0  = 0b0000_0000 => le_bytes [[00]00_0000]
                // 63 = 0b0011_1111 => le_bytes [[00]11_1111]
                // 0b[00][aa_aaaa] => 0b[type=00][num=...]
                vec![number as u8]
            }
            64..=16383 => {
                // 64    = 0b0000_0000_0100_0000 => le_bytes [[01]00_0000] [0000_0000]
                // 16383 = 0b0011_1111_1111_1111 => le_bytes [[01]11_1111] [0011_1111]
                // 0b[01][aa_aaaa][next_byte] =>
                // 0b[type=01][len=u16::from_le_bytes([next_byte, 0b00aa_aaaa])]
                //
                // the parser is using
                // length = ((bytes[0] & 0x3F) << 8) | bytes[1]
                let le_bytes = (number as u16).to_le_bytes();
                let first_byte = ((number >> 8) & 0b00111111) as u8 | 0b01000000;
                let second_byte = (number & 0xFF) as u8;
                vec![first_byte, second_byte]
            }
            16384..=u32::MAX => {
                // 16384    = 0x3FFF
                // u32::MAX = 0xFFFF
                // 0b[10][aa_aaaa][next_4_byte] =>
                // 0b[type=10][len=u32::from_le_bytes(next_4_byte)
                let mut out: Vec<u8> = vec![0b10000000];
                out.extend_from_slice(&number.to_le_bytes());
                out
            }
        }
    }

    fn as_length_encoded_value(&self) -> Vec<u8> {
        use ValueType::*;
        match self {
            I8Int(x) => {
                // value-type = 0x0 i.e string_encoded_value
                // len = 0xc0 = 192 = 0b1100_0000
                // - 0b1100_0000 & 0b1100_0000 = 3 => special format
                // - 0b1100_0000 & 0b0011_1111 = 0 => i8 encoded as string
                vec![0xc0, x.to_le_bytes()[0]]
            }
            I16Int(x) => {
                // value-type = 0x0 i.e string_encoded_value
                // len = 0xc1 = 193 = 0b1100_0000
                // - 0b1100_0001 & 0b1100_0000 = 3 => special format
                // - 0b1100_0001 & 0b0011_1111 = 1 => i16 encoded as string
                let x = x.to_le_bytes();
                vec![0xc1, x[0], x[1]]
            }
            I32Int(x) => {
                // value-type = 0x0 i.e string_encoded_value
                // len = 0xc2 = 194 = 0b1100_0000
                // - 0b1100_0002 & 0b1100_0000 = 3 => special format
                // - 0b1100_0002 & 0b0011_1111 = 2 => i32 encoded as string
                let x = x.to_le_bytes();
                vec![0xc2, x[0], x[1], x[2], x[3]]
            }
            IntOrString(x) => {
                let x_len = x.len() as u32;
                let mut out: Vec<u8> = Self::number_to_length_encoded(x.len() as u32);
                out.extend_from_slice(x.as_bytes());
                out
            }
            _ => panic!("Self Can't be Length Encoded Directly"),
        }
    }

    #[inline]
    pub fn as_rdb_value_type(&self) -> Vec<u8> {
        // TODO: for now all values considered of type String
        // this function is just in case of supporting other types later
        vec![0]
    }

    pub fn as_rdb(&self) -> Vec<u8> {
        use ValueType::*;
        match self {
            I8Int { .. } | I16Int { .. } | I32Int { .. } | IntOrString { .. } => {
                self.as_length_encoded_value()
            }
            CompressedString {
                real_data_len,
                compressed_data,
            } => {
                // 0b1100_0011 => (0b1100_0011 & 0b0000_00011) >> 6 = 3 i.e special_type
                // 0b1100_0011 => (0b1100_0011 & 0b0000_00011)      = 3 i.e compressed_string
                // comp_len as len_encoded
                // real_len as len_encoded
                // comp_data it self
                let mut out: Vec<u8> = vec![0b1100_0011];
                // TODO: I do think this should get recursive, however other parsers, doesn't
                // consider that idea at all. It's also not obivious from the mentioned source how
                // these lengthes should be encoded!
                let comp_len = Self::number_to_length_encoded(compressed_data.len() as u32);
                let real_len = Self::number_to_length_encoded(*real_data_len as u32);
                out.extend_from_slice(&comp_len);
                out.extend_from_slice(&real_len);
                out.extend_from_slice(&compressed_data);
                out
            }
        }
    }
}

#[derive(Debug)]
pub struct DataEntry {
    pub value: ValueType,
    pub created_at: Instant,
    // TODO: add distinction between expiry millis and expiry secs
    pub expired_millis: Option<Duration>,
}

impl DataEntry {
    pub fn new(data: String, expired_millis: Option<u64>) -> Self {
        Self {
            value: ValueType::new(data),
            expired_millis: expired_millis.map(|x| Duration::from_millis(x)),
            created_at: Instant::now(),
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.expired_millis {
            Some(expiry) => self.created_at.elapsed() > expiry,
            None => false,
        }
    }
}

pub fn key_value_as_rdb(key: &ValueType, value: &DataEntry) -> Vec<u8> {
    // expiry_opcode|None expiry_4_bytes(secs)|expiry_8_bytes(millis)|None
    // 1 byte value_type (only support string encoded type for now i.e val_type=0)
    // string_encoded_key
    // encoded_value
    let mut out: Vec<u8> = vec![];
    match value.expired_millis {
        Some(expiry) => {
            out = vec![EXPIRETIMEMS];
            out.extend_from_slice(&(expiry.as_millis() as u64).to_le_bytes());
        }
        None => (),
    };
    out.push(value.value.as_rdb_value_type()[0]);
    out.extend_from_slice(&key.as_rdb()[..]);
    out.extend_from_slice(&value.value.as_rdb()[..]);
    out
}

pub type RedisDB = HashMap<ValueType, DataEntry>;

#[derive(Debug)]
pub struct Redis {
    pub cfg: Config,
    pub dict: RedisDB,
}

impl Redis {
    pub fn with_config(cfg: Config) -> Self {
        Self {
            cfg,
            dict: HashMap::default(),
        }
    }

    pub fn apply_cmd(&mut self, ip: IpAddr, cmd: Cmd) -> RESPType {
        use Cmd::*;
        use RESPType::*;
        match cmd {
            Ping => SimpleString("PONG".to_string()),
            Echo(msg) => BulkString(msg),
            Set { key, value, px } => {
                self.dict
                    .insert(ValueType::new(key), DataEntry::new(value, px));
                SimpleString("OK".to_string())
            }
            Get(key) => {
                let key = ValueType::new(key);
                match self.dict.get(&key) {
                    Some(data) => {
                        if data.is_expired() {
                            self.dict.remove(&key);
                            Null
                        } else {
                            BulkString(data.value.as_string())
                        }
                    }
                    None => Null,
                }
            }
            Info(section) => BulkString(self.cfg.get_info(section)),
            ReplConf(replica_config) => {
                match self.cfg.replica_of.role {
                    Role::Master { ref mut slaves } => {
                        // TODO: for now updating current config will happen if the
                        // slave-repilca-Ip has been seen before, or if the replica-conf command
                        // has the listening-port argument, then add that slave-replica-ip to
                        // config and keep updating since then.
                        if let Some(prev_cfg) = slaves.get_mut(&ip) {
                            // here slave-replica-ip has been inserted before
                            for (cmd, args) in replica_config {
                                for arg in args {
                                    prev_cfg
                                        .entry(cmd.clone())
                                        .or_insert_with(Vec::new)
                                        .push(arg);
                                }
                            }
                        } else if let Some(replica_port) =
                            replica_config.get(&"listening-port".to_string())
                        {
                            // here slave-replica-ip is new to this master
                            slaves.insert(ip, replica_config);
                        } else {
                            // skip other situations mentioned in the todo.
                        }
                    }
                    _ => todo!("Replica of Replica Senario is not implemented yet."),
                }
                SimpleString("OK".to_string())
            }
            Psync { replid, offset: -1 } if replid == "?".to_string() => {
                let rdb_header = RDBHeader {
                    magic: String::from("REDIS"),
                    rdb_version: 3,
                    aux_settings: std::collections::HashMap::new(),
                };
                let mut rdb_content = rdb_header.as_rdb();
                rdb_content.extend_from_slice(&self.as_rdb()[..]);
                rdb_content.push(crate::constants::EOF);

                let mut msg: Vec<u8> =
                    format!("+FULLRESYNC {} 0\r\n", &self.cfg.replica_of.master_replid)
                        .as_bytes()
                        .to_vec();
                msg.extend_from_slice(format!("${}\r\n", rdb_content.len()).as_bytes());
                msg.extend_from_slice(&rdb_content);
                WildCard(msg)
            }
            Psync { .. } => todo!(),
        }
    }

    pub fn as_rdb(&self) -> Vec<u8> {
        let mut out: Vec<u8> = vec![];
        for (key, value) in self.dict.iter() {
            out.extend_from_slice(&key_value_as_rdb(&key, &value)[..]);
        }
        out
    }

    pub fn propagate_cmd(&self, cmd: &Cmd) {
        // TODO: simplifications that can be resolved later:
        // a. in case of not responding replica, just leave it. keep sending to it
        // - can remove it from slaves directly, so it can do another full reysnc later
        // - can buffer commands for it, and test its response multiple times before removing it.
        // b. no buffering for commands, just propagate received command to all replicas on time
        // - I think real buffering should be per replica as it would solve the above problem also
        // c. spawn as many threads as needed per replicas
        // - can have specified number of threads instead and divide work over them.
        // - work division can be done using channels or each thread can have pre-specified tasks

        let prop_cmd = |socket: SocketAddr, cmd_resp: Arc<RESPType>| async move {
            match TcpStream::connect(socket).await {
                Ok(mut stream) => stream.write_all(&cmd_resp.serialize()).await,
                _ => return,
            };
        };

        let cmd_resp = Arc::new(cmd.to_resp_array_of_bulks());

        match &self.cfg.replica_of.role {
            Role::Master { slaves } => {
                for (replica_host, replica_config) in slaves.iter() {
                    for host_ports in replica_config.get("listening-port") {
                        for port in host_ports {
                            match port.parse::<u16>() {
                                Ok(port) => {
                                    let socket = SocketAddr::new(*replica_host, port);
                                    let cmd_resp_clone = Arc::clone(&cmd_resp);
                                    tokio::spawn(async move { prop_cmd(socket, cmd_resp_clone).await });
                                }
                                _ => continue,
                            }
                        }
                    }
                }
            }
            _ => todo!("Replica of Replica Senario is not implemented yet."),
        }
    }
}
