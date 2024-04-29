use crate::resp_array_of_bulks;
use crate::{
    command::Cmd,
    config::Config,
    data_entry::{key_value_as_rdb, DataEntry, ValueType},
    rdb::RDBHeader,
    resp::RESPType,
};
use futures::stream::{self, StreamExt};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, atomic::AtomicUsize},
};
use tokio::{
    sync::Mutex,
    task::JoinSet,
    net::tcp::OwnedWriteHalf,
};

pub type RedisDB = HashMap<ValueType, DataEntry>;
pub type WriteStream = Arc<Mutex<OwnedWriteHalf>>;

enum UpdateState {
    Success(SocketAddr),
    Failed(SocketAddr),
}

#[derive(Debug, Clone)]
pub struct SlaveMeta {
    pub expected_offset: usize,
    pub actual_offset: usize,
    pub wr: WriteStream,
    pub socket_addr: SocketAddr,
    pub pending_updates: Vec<u8>,
    pub metadata: HashMap<String, Vec<String>>,
}

impl SlaveMeta {
    pub fn append_update(&mut self, cmd: &Vec<u8>) {
        self.pending_updates.extend_from_slice(cmd);
    }

    pub async fn write_cmd(&mut self, cmd: &Vec<u8>) {
        let wr_guard = self.wr.lock().await;
        loop {
            if wr_guard.writable().await.is_ok() {
                match wr_guard.try_write(cmd) {
                    Ok(n) => {
                        self.actual_offset += n;
                        break;
                    }
                    Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                    Err(_e) => break,
                }
            }
        }
        self.expected_offset += cmd.len();
    }

    pub async fn apply_pending_updates(&mut self) -> UpdateState {
        if self.pending_updates.is_empty() {
            return UpdateState::Success(self.socket_addr);
        }
        let wr_guard = self.wr.lock().await;
        self.expected_offset += self.pending_updates.len();
        loop {
            if wr_guard.writable().await.is_ok() {
                match wr_guard.try_write(&self.pending_updates) {
                    Ok(n) => {
                        println!(
                            "[+] Success: Write_ALL({:?})",
                            String::from_utf8_lossy(&self.pending_updates)
                        );
                        self.actual_offset += n;
                        return UpdateState::Success(self.socket_addr);
                    }
                    Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                    Err(e) => {
                        println!(
                            "[+] Failed: Write_ALL({:?}) / {:?}",
                            String::from_utf8_lossy(&self.pending_updates),
                            e
                        );
                        return UpdateState::Failed(self.socket_addr);
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Redis {
    pub cfg: Config,
    pub dict: RedisDB,
    pub slaves: HashMap<SocketAddr, SlaveMeta>,
}

impl Redis {
    pub fn with_config(cfg: Config) -> Self {
        Self {
            cfg,
            dict: HashMap::default(),
            slaves: HashMap::default(),
        }
    }

    pub async fn apply_cmd(
        &mut self,
        socket_addr: SocketAddr,
        wr: Option<WriteStream>,
        cmd: Cmd,
    ) -> RESPType {
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
                self.add_pending_update_cmd(&cmd);
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
            GetAck => {
                self.add_pending_update_resp(&resp_array_of_bulks!("REPLCONF", "GETACK", "*"));
                resp_array_of_bulks!("REPLCONF", "ACK", self.cfg.replica_of.master_repl_offset)
            }
            Wait { .. } => {
                Integer(self.slaves.len() as i64)
            }
            ReplConf(replica_config) => {
                match wr {
                    Some(wr) => {
                        let mut slave_meta = self.slaves.entry(socket_addr).or_insert(SlaveMeta {
                            expected_offset: 0,
                            actual_offset: 0,
                            socket_addr,
                            wr: wr.clone(),
                            metadata: HashMap::new(),
                            pending_updates: Vec::new(),
                        });
                        for (cmd, args) in replica_config {
                            for arg in args {
                                slave_meta
                                    .metadata
                                    .entry(cmd.clone())
                                    .or_insert_with(Vec::new)
                                    .push(arg);
                            }
                        }
                    }
                    _ => {}
                };
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

    pub fn add_pending_update_resp(&mut self, resp: &RESPType) {
        for (_socket_addr, slave_meta) in self.slaves.iter_mut() {
            slave_meta.append_update(&resp.serialize());
        }
    }

    pub fn add_pending_update_cmd(&mut self, cmd: &Cmd) {
        for (_socket_addr, slave_meta) in self.slaves.iter_mut() {
            slave_meta.append_update(&cmd.to_resp_array_of_bulks().serialize());
        }
    }

    pub async fn apply_all_pending_updates(&mut self) -> u64 {
        let mut updates_done = 0;
        let slaves_clone: HashMap<SocketAddr, SlaveMeta> = self
            .slaves
            .iter()
            .filter_map(|(socket_addr, slave_meta)| {
                if !slave_meta.pending_updates.is_empty() {
                    return Some((*socket_addr, slave_meta.clone()));
                } else {
                    None
                }
            })
            .collect();
        let mut fetches = stream::iter(slaves_clone.into_iter().map(
            |(_socket_addr, mut slave_meta)| async move {
                return slave_meta.apply_pending_updates().await;
            },
        ))
        .buffer_unordered(8);
        while let Some(result) = fetches.next().await {
            match result {
                UpdateState::Success(socket_addr) => {
                    println!("[+] Success: Clear(SocketAddr: {:?})", socket_addr);
                    self.slaves
                        .get_mut(&socket_addr)
                        .unwrap()
                        .pending_updates
                        .clear();
                    updates_done += 1;
                }
                UpdateState::Failed(socket_addr) => {
                    println!("[+] Failed: Remove(SocketAddr: {:?})", socket_addr);
                    self.slaves.remove(&socket_addr);
                }
            };
        }
        return updates_done;
    }
}
