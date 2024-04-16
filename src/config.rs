use anyhow::Context;
use std::fmt::{self, Error, Formatter};
use std::{env, net::Ipv4Addr};

const DEFAULT_PORT: u16 = 6379;

#[derive(Debug)]
pub enum Role {
    Master,
    Slave { host: Ipv4Addr, port: u16 },
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        match self {
            Self::Master => write!(f, "role:master"),
            Self::Slave { host, port } => {
                write!(f, "role:slave\nmaster_host:{}\nmaster_port:{}", host, port)
            }
        }
    }
}

#[derive(Debug)]
pub struct ReplicaInfo {
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
                master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
                master_repl_offset: 0,
            },
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
                        host: master_host,
                        port: master_port,
                    };
                }
                _ => panic!("ERROR: unsported argument"),
            };
        }
        Ok(cfg)
    }
}
