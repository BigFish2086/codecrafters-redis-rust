use crate::{
    constants::{
        DEFAULT_PORT,
        DEFAULT_DB_FILEPATH,
    },
    utils::random_string
};
use anyhow::Context;
use std::{
    env,
    collections::HashMap,
    fmt::{self, Error, Formatter},
    net::Ipv4Addr,
    sync::Arc,
    path::PathBuf,
};
use tokio::{net::TcpStream, sync::Mutex};

pub enum Role {
    Master,
    Slave {
        master_host: Ipv4Addr,
        master_port: u16,
        master_connection: Option<Arc<Mutex<TcpStream>>>,
    },
}

impl fmt::Debug for Role {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "{}", self)
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Self::Master => write!(f, "role:master"),
            Self::Slave {
                master_host,
                master_port,
                ..
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
pub struct Config {
    pub service_port: u16,
    pub replica_of: ReplicaInfo,
    pub parameters: HashMap<String, String>,
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

    pub fn get_db_filepath(&self) -> PathBuf {
        let binding = String::default();
        let dir = self.parameters.get("dir").unwrap_or(&binding);
        let db_filename = self.parameters.get("dbfilename").unwrap_or(&binding);
        let mut db_filepath: PathBuf = [dir, db_filename].iter().collect();
        if !db_filepath.exists() {
            db_filepath = DEFAULT_DB_FILEPATH.to_string().into();
        }
        db_filepath
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
            parameters: HashMap::default(),
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
                        master_connection: None,
                    };
                }
                "--dir" => {
                    let dir = args
                        .next()
                        .context("usage --dir <rdb_directory_path:String>")?
                        .trim()
                        .to_owned();
                    cfg.parameters.insert("dir".to_string(), dir);
                }
                "--dbfilename" => {
                    let db_filename = args
                        .next()
                        .context("usage --dbfilename <rdb_filename:String>")?
                        .trim()
                        .to_owned();
                    cfg.parameters.insert("dbfilename".to_string(), db_filename);
                }
                _ => panic!("ERROR: unsported argument"),
            };
        }
        Ok(cfg)
    }
}
