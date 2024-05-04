use async_trait::async_trait;
use crate::resp::RespType;
use crate::resp_array_of_bulks;

use crate::cmd::Cmd;
use crate::redis::{AMSlaves, AMConfig, add_pending_update_resp};

pub struct Ack;

#[async_trait]
impl Cmd for Ack {
    async fn run(&mut self) -> RespType {
        RespType::WildCard("".into())
    }
}

pub struct GetAck {
    pub slaves: AMSlaves,
    pub config: AMConfig,
}

#[async_trait]
impl Cmd for GetAck {
    async fn run(&mut self) -> RespType {
        add_pending_update_resp(self.slaves.clone(), &resp_array_of_bulks!("REPLCONF", "GETACK", "*")).await;
        resp_array_of_bulks!("REPLCONF", "ACK", self.config.lock().await.replica_of.master_repl_offset)
    }
}

