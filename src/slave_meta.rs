use crate::resp_array_of_bulks;
use crate::resp::RespType;

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
};
use tokio::{
    sync::Mutex,
    net::tcp::OwnedWriteHalf,
};

pub type WriteStream = Arc<Mutex<OwnedWriteHalf>>;

pub enum UpdateState {
    Success(SocketAddr, usize, usize),
    Failed(SocketAddr),
}

#[derive(Debug, Clone)]
pub struct SlaveMeta {
    pub expected_offset: usize,
    pub actual_offset: usize,
    pub lifetime_limit: usize,
    pub wr: WriteStream,
    pub socket_addr: SocketAddr,
    pub pending_updates: Vec<u8>,
}

impl SlaveMeta {
    pub fn append_update(&mut self, cmd: &Vec<u8>) {
        self.pending_updates.extend_from_slice(cmd);
    }

    pub async fn write_getack_cmd(&mut self) {
        let get_ack_cmd = resp_array_of_bulks!("REPLCONF", "GETACK", "*").serialize();
        let wr_guard = self.wr.lock().await;
        loop {
            if wr_guard.writable().await.is_ok() {
                match wr_guard.try_write(&get_ack_cmd) {
                    Ok(n) => {
                        println!("@ SlaveMeta write_getack_cmd: Written");
                        self.actual_offset += n;
                        break;
                    }
                    Err(ref e) if e.kind() == tokio::io::ErrorKind::WouldBlock => continue,
                    Err(_e) => break,
                }
            }
        }
        self.expected_offset += get_ack_cmd.len();
    }

    pub async fn apply_pending_updates(&mut self) -> UpdateState {
        if self.pending_updates.is_empty() {
            return UpdateState::Success(self.socket_addr, 0, 0);
        }
        let wr_guard = self.wr.lock().await;
        self.expected_offset += self.pending_updates.len();
        loop {
            if wr_guard.writable().await.is_ok() {
                match wr_guard.try_write(&self.pending_updates) {
                    Ok(n) => {
                        self.actual_offset += n;
                        println!(
                            "[+] Success: Write_ALL({:?}), ACTUAL_OFFSET({:?}), EXPECTED_OFFSET({:?})",
                            String::from_utf8_lossy(&self.pending_updates),
                            self.actual_offset,
                            self.expected_offset
                        );
                        return UpdateState::Success(self.socket_addr, self.expected_offset, self.actual_offset);
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

