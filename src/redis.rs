use crate::{
    config::Config,
    data_entry::{key_value_as_rdb, DataEntry, ValueType},
    resp::RespType,
    slave_meta::{SlaveMeta, UpdateState},
    stream_entry::StreamEntry,
};
use futures::stream::{self, StreamExt};
use std::{
    collections::{hash_map::Entry, HashMap},
    net::SocketAddr,
    sync::Arc,
};
use tokio::sync::{
    broadcast::{self, Receiver, Sender},
    Mutex,
};

pub type RedisDB = HashMap<ValueType, DataEntry>;
pub type StreamDB = HashMap<ValueType, StreamEntry>;

// TODO: what if entered dict has same key as streams?
pub type AMConfig = Arc<Mutex<Config>>;
pub type AMRedisDB = Arc<Mutex<RedisDB>>;
pub type AMStreams = Arc<Mutex<StreamDB>>;
pub type AMSlaves = Arc<Mutex<HashMap<SocketAddr, SlaveMeta>>>;
pub type AMStreamSenders = Arc<Mutex<HashMap<String, Sender<RespType>>>>;

pub async fn incr_master_repl_offset(cfg: AMConfig, value: u64) {
    cfg.lock().await.replica_of.master_repl_offset += value;
}

pub async fn get_stream_reciver(
    stream_senders: AMStreamSenders,
    key: &String,
) -> Receiver<RespType> {
    let mut stream_senders_guard = stream_senders.lock().await;
    println!("{:?}", stream_senders_guard.len());
    match stream_senders_guard.get(key) {
        Some(sender) => sender.subscribe(),
        _ => {
            let (sender, receiver) = broadcast::channel(1);
            stream_senders_guard.insert(key.clone(), sender);
            receiver
        }
    }
}

pub async fn add_pending_update_resp(slaves: AMSlaves, resp: &RespType) {
    for (_socket_addr, slave_meta) in slaves.lock().await.iter_mut() {
        slave_meta.append_update(&resp.serialize());
    }
}

pub async fn db_as_rdb(dict: AMRedisDB) -> Vec<u8> {
    let mut out: Vec<u8> = vec![];
    for (key, value) in dict.lock().await.iter() {
        out.extend_from_slice(&key_value_as_rdb(&key, &value)[..]);
    }
    out
}

pub async fn apply_all_pending_updates(slaves: AMSlaves) -> u64 {
    let mut updates_done = 0;
    let mut slaves_guard = slaves.lock().await;
    let slaves_clone: HashMap<SocketAddr, SlaveMeta> = slaves_guard
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
            UpdateState::Success(socket_addr, expected_incr, actual_incr) => {
                println!("[+] Success: Clear(SocketAddr: {:?})", socket_addr);
                let slave_meta = slaves_guard.get_mut(&socket_addr).unwrap();
                slave_meta.pending_updates.clear();
                slave_meta.expected_offset += expected_incr;
                slave_meta.actual_offset += actual_incr;
                updates_done += 1;
            }
            UpdateState::Failed(socket_addr) => {
                if let Entry::Occupied(mut slave) = slaves_guard.entry(socket_addr) {
                    if slave.get().lifetime_limit >= crate::constants::SLAVE_LIFETIME_LIMIT {
                        println!(
                            "[+] Failed for {:?} times => Remove(SocketAddr: {:?})",
                            slave.get().lifetime_limit,
                            socket_addr
                        );
                        slave.remove_entry();
                    } else {
                        println!(
                            "[+] Failed for {:?} times => will try {:?} later",
                            slave.get().lifetime_limit,
                            socket_addr
                        );
                        slave.get_mut().lifetime_limit += 1;
                    }
                }
            }
        };
    }
    drop(slaves_guard);
    return updates_done;
}
