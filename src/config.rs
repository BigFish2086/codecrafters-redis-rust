use crate::{constants::DEFAULT_PORT, utils::random_string};
use anyhow::Context;
use futures::stream::{self, StreamExt};
use std::{
    collections::HashMap,
    env,
    fmt::{self, Error, Formatter},
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::{io::AsyncWriteExt, net::TcpStream};

#[derive(Debug)]
pub enum Role {
    Master,
    Slave {
        master_host: Ipv4Addr,
        master_port: u16,
    },
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Self::Master => write!(f, "role:master"),
            Self::Slave {
                master_host,
                master_port,
            } => {
                write!(
                    f,
                    "role:slave\nmaster_host:{}\nmaster_port:{}",
                    master_host, master_port
                )
            }
        }
    }
}

#[derive(Debug)]
pub struct ReplicaInfo {
    // TODO: for now, the assumption is that this server is either Master that has slaves or Slave
    // that replicate another master. Reconsider this approach after reaseraching the
    // replica-of-replica topic. In that case there are multiple challenges in this desgin, since
    // enum is either A or B. Ideas that may help:
    // - having a linked-list with branches in case if replica-B of replica-A of master-M must
    // replicate replica-A and not necessarily master-M (what will happen in case replica-A died?)
    // - if replica-B can replicate master-M directly, but for some reason it entered the network
    // via replica-A, so this design can get to work with little modifications, I think!
    pub role: Role,
    pub master_replid: String,
    pub master_repl_offset: u64,
}

impl fmt::Display for ReplicaInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(
            f,
            "# Replication\n{}\nmaster_replid:{}\nmaster_repl_offset:{}",
            self.role.to_string(),
            self.master_replid,
            self.master_repl_offset
        )
    }
}

#[derive(Debug)]
pub struct SlaveMeta {
    pub host_ip: IpAddr,
    // TODO: for now REPLCONF `capa` command is hard to parse and set for each replica on same host
    // Ip. to make things clear assume having 3 replicas (A, B, C) on same host Ip-X, with
    // different capas, right now the metadata should look like this:
    // { 'capa': { C1-A, C2-A, C-B, C-C, ... } }
    // which may be not correct if these `capa` affect the communectaions (and they should!)
    // between master and slaves.
    pub metadata: HashMap<String, Vec<String>>,
    // pending updates per listening-port, since as mentioned there could be multiple
    // listening-ports per same host-ip. in same scenario then:
    // { P-A: updates that replica (ip, port) didn't after fullsync as one big blob of bytes, ... }
    // NOTE: sending updates like that should work, since each command is sent as RESPType::Array
    // TODO: can have a limit_failures number to remove the port after that.
    pub pending_updates: HashMap<u16, Vec<u8>>,
}

impl SlaveMeta {
    pub fn new(host_ip: IpAddr) -> Self {
        Self {
            host_ip,
            metadata: HashMap::new(),
            pending_updates: HashMap::new(),
        }
    }

    pub fn append_update(&mut self, cmd: &Vec<u8>) {
        for (_port, updates) in self.pending_updates.iter_mut() {
            updates.extend_from_slice(cmd);
        }
    }

    pub async fn apply_pending_updates(&mut self) {
        enum UpdateState {
            Success(u16),
            Failed(u16),
        };
        let updates_clone: HashMap<u16, Vec<u8>> = self
            .pending_updates
            .iter()
            .filter_map(|(port, updates)| {
                if !updates.is_empty() {
                    Some((*port, updates.clone()))
                } else {
                    None
                }
            })
            .collect();
        let mut fetches = stream::iter(updates_clone.into_iter().map(|(port, updates)| {
            let host_ip = self.host_ip;
            async move {
                let socket_addr = SocketAddr::new(host_ip, port);
                match TcpStream::connect(socket_addr).await {
                    Ok(mut stream) => {
                        if let Err(_) = stream.write_all(&updates).await {
                            UpdateState::Failed(port)
                        } else {
                            UpdateState::Success(port)
                        }
                    }
                    Err(_) => UpdateState::Failed(port),
                }
            }
        }))
        .buffer_unordered(8);
        while let Some(result) = fetches.next().await {
            match result {
                UpdateState::Success(port) => {
                    self.pending_updates.get_mut(&port).unwrap().clear();
                }
                UpdateState::Failed(port) => {
                    self.pending_updates.remove(&port);
                }
            };
        }
    }
}

#[derive(Debug)]
pub struct Config {
    pub service_port: u16,
    pub replica_of: ReplicaInfo,
    pub slaves: HashMap<IpAddr, SlaveMeta>,
}

impl Config {
    pub fn get_info(&self, keyword: Option<String>) -> String {
        if keyword.is_none() {
            return self.to_string();
        }
        match keyword.unwrap().trim().to_lowercase().as_str() {
            "all" | "everything" | "default" => self.to_string(),
            "replication" => self.replica_info(),
            _ => String::from(""),
        }
    }

    pub fn replica_info(&self) -> String {
        self.replica_of.to_string()
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self.replica_info())
    }
}

impl TryFrom<env::Args> for Config {
    type Error = anyhow::Error;

    fn try_from(args: env::Args) -> anyhow::Result<Self> {
        let mut args = args.skip(1);
        let mut cfg = Self {
            service_port: DEFAULT_PORT,
            replica_of: ReplicaInfo {
                role: Role::Master,
                master_replid: random_string(40),
                master_repl_offset: 0,
            },
            slaves: HashMap::new(),
        };
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--port" => {
                    cfg.service_port = args
                        .next()
                        .context("usage --port <number:u16>")?
                        .trim()
                        .parse::<u16>()
                        .context("expected port to be valid u16 i.e in range 0-65535")?;
                }
                "--replicaof" => {
                    let mut master_host = args
                        .next()
                        .context("usage --replicaof <master_host:Ipv4Addr> <master_port:u16>")?
                        .trim()
                        .to_owned();
                    if master_host.to_lowercase() == "localhost" {
                        master_host = "127.0.0.1".to_owned();
                    }
                    let master_host = master_host
                        .parse::<Ipv4Addr>()
                        .context("expected master_host to be valid Ipv4Addr")?;
                    let master_port = args
                        .next()
                        .context("usage --replicaof <master_host:Ipv4Addr> <master_port:u16>")?
                        .trim()
                        .parse::<u16>()
                        .context("expected master_port to be valid u16 i.e in range 0-65535")?;
                    cfg.replica_of.role = Role::Slave {
                        master_host,
                        master_port,
                    };
                }
                _ => panic!("ERROR: unsported argument"),
            };
        }
        Ok(cfg)
    }
}
