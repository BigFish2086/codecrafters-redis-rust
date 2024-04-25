use crate::{
    command::Cmd,
    config::{Config, Role, SlaveMeta, WriteStream},
    constants::{COMPRESS_AT_LENGTH, EXPIRETIMEMS},
    rdb::RDBHeader,
    resp::RESPType,
    data_entry::{DataEntry, ValueType, key_value_as_rdb},
};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::{
    io::{AsyncWriteExt, Interest},
    net::{TcpListener, TcpStream},
};

use tokio::time::{Duration, Instant};

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

    pub fn apply_cmd(&mut self, ip: IpAddr, wr: WriteStream, cmd: Cmd) -> RESPType {
        use Cmd::*;
        use RESPType::*;
        match cmd {
            Ping => SimpleString("PONG".to_string()),
            Echo(msg) => BulkString(msg),
            Set {
                ref key,
                ref value,
                px,
            } => {
                self.dict.insert(
                    ValueType::new(key.clone()),
                    DataEntry::new(value.clone(), px),
                );
                self.add_pending_update(&cmd);
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
                let slave_meta = self.cfg.slaves.entry(ip).or_insert(SlaveMeta {
                    host_ip: ip,
                    metadata: HashMap::new(),
                    pending_updates: HashMap::new(),
                });
                for (cmd, args) in replica_config {
                    for arg in args {
                        if cmd.as_str() == "listening-port" {
                            match arg.parse::<u16>() {
                                Ok(port) => {
                                    let _ = slave_meta.pending_updates.entry(port).or_insert((wr.clone(), Vec::new()));
                                }
                                _ => continue,
                            }
                        } else {
                            slave_meta
                                .metadata
                                .entry(cmd.clone())
                                .or_insert_with(Vec::new)
                                .push(arg);
                        }
                    }
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

    pub fn add_pending_update(&mut self, cmd: &Cmd) {
        for (_host_ip, slave_meta) in self.cfg.slaves.iter_mut() {
            slave_meta.append_update(&cmd.to_resp_array_of_bulks().serialize());
        }
    }

    pub async fn apply_pending_updates_per_host(&mut self, host_ip: &IpAddr) {
        println!("[+] Redis: Apply Pending Update Per Host");
        match self.cfg.slaves.get_mut(host_ip) {
            Some(ref mut slave_meta) => {
                slave_meta.apply_pending_updates().await;
            }
            None => (),
        }
    }

    pub async fn apply_pending_updates(&mut self) {
        println!("[+] Redis: Apply ALL Pending Update");
        for (_host_ip, slave_meta) in self.cfg.slaves.iter_mut() {
            slave_meta.apply_pending_updates().await;
        }
    }
}
