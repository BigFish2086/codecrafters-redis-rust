use anyhow::Context;
use std::fmt::{self, Error, Formatter};
use std::{env, net::Ipv4Addr};

#[derive(Debug)]
pub struct Master {
    pub host: Ipv4Addr,
    pub port: u16,
}

impl fmt::Display for Master {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        writeln!(f, "master_host:{}", self.host)?;
        writeln!(f, "master_port:{}", self.port)?;
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Config {
    pub service_port: u16,
    pub replica_of: Option<Master>,
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
        let mut info = String::from("# Replication\n");
        match &self.replica_of {
            Some(_) => {
                info.push_str("role:slave\n");
                info.push_str(&self.replica_of.as_ref().unwrap().to_string());
            }
            None => {
                info.push_str("role:master\n");
            }
        };
        info
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
        let mut cfg = Self::default();
        cfg.service_port = 6379;
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
                        .trim().to_owned();
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
                    cfg.replica_of = Some(Master {
                        host: master_host,
                        port: master_port,
                    });
                }
                _ => panic!("ERROR: unsported argument"),
            };
        }
        Ok(cfg)
    }
}
