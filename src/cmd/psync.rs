use async_trait::async_trait;

use crate::resp::RespType;
use crate::cmd::{Cmd, CmdError};
use crate::redis::{AMRedisDB, AMConfig, AMSlaves, db_as_rdb};
use crate::slave_meta::{WriteStream, SlaveMeta};
use crate::rdb::RDBHeader;
use crate::utils::unpack_bulk_string;

use std::net::SocketAddr;

pub struct Psync {
    pub replid: String,
    pub offset: i64,
    pub dict: AMRedisDB,
    pub config: AMConfig,
    pub slaves: AMSlaves,
    pub wr: WriteStream,
    pub socket_addr: SocketAddr,
}

#[async_trait]
impl Cmd for Psync {
    async fn run(&mut self) -> RespType {
        let rdb_header = RDBHeader {
            magic: String::from("REDIS"),
            rdb_version: 3,
            aux_settings: std::collections::HashMap::new(),
        };
        let mut rdb_content = rdb_header.as_rdb();
        rdb_content.extend_from_slice(&db_as_rdb(self.dict.clone()).await[..]);
        rdb_content.push(crate::constants::EOF);

        let mut msg: Vec<u8> = format!(
            "+FULLRESYNC {} 0\r\n",
            self.config.lock().await.replica_of.master_replid
        )
        .as_bytes()
        .to_vec();
        msg.extend_from_slice(format!("${}\r\n", rdb_content.len()).as_bytes());
        msg.extend_from_slice(&rdb_content);

        self.slaves
            .lock()
            .await
            .entry(self.socket_addr)
            .or_insert(SlaveMeta {
                expected_offset: 0,
                actual_offset: 0,
                lifetime_limit: 0,
                socket_addr: self.socket_addr,
                wr: self.wr.clone(),
                pending_updates: Vec::new(),
            });

        RespType::WildCard(msg)
    }
}

impl Psync {
    pub fn new<'a>(
        mut args_iter: &mut impl Iterator<Item = &'a RespType>,
        dict: AMRedisDB,
        config: AMConfig,
        slaves: AMSlaves,
        wr: Option<WriteStream>,
        socket_addr: Option<SocketAddr>,
    ) -> Result<Self, CmdError> {
        let replid =
            unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        let offset =
            unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        let offset = offset.parse::<i64>().map_err(|_| CmdError::InvalidArg)?;
        Ok(Self {
            replid,
            offset,
            dict,
            config,
            slaves,
            wr: wr.unwrap(),
            socket_addr: socket_addr.unwrap(),
        })
    }
}
