use async_trait::async_trait;

use crate::cmd::{Cmd, CmdError};
use crate::resp::RespType;
use crate::redis::{AMStreamSenders, AMStreams, get_stream_reciver};
use crate::utils::unpack_bulk_string;
use crate::data_entry::ValueType;

use tokio::task::JoinSet;
use tokio::time::{self, Duration};

pub struct XRead {
    pub timeout: Option<Duration>,
    pub keys: Vec<String>,
    pub ids: Vec<String>,
    pub streams: AMStreams,
    pub stream_senders: AMStreamSenders,
}

#[async_trait]
impl Cmd for XRead {
    async fn run(&mut self) -> RespType {
        use RespType::{Array, BulkString, WildCard};
        let mut result = Vec::new();
        let mut has_items = false;
        for (key, id) in self.keys.iter().zip(self.ids.iter()) {
            let stream_key = ValueType::new(key.clone());
            let streams_guard = self.streams.lock().await;
            if let Some(stream_entry) = streams_guard.get(&stream_key) {
                let (resp, resp_has_empty) = stream_entry.query_xread(id.clone());
                has_items = has_items | resp_has_empty;
                result.push(Array(vec![BulkString(key.clone()), resp]))
            }
            drop(streams_guard);
        }
        if has_items == true {
            return Array(result);
        }
        if let Some(dur) = self.timeout {
            let block_read = async {
                let mut tasks = JoinSet::new();
                for key in self.keys.clone() {
                    let key = key.clone();
                    let mut receiver = get_stream_reciver(self.stream_senders.clone(), &key).await;
                    tasks.spawn(async move { (key, receiver.recv().await.unwrap()) });
                }
                tasks.join_next().await.expect("Join Set Tasks is Empty")
            };
            if let Ok(res) = time::timeout(dur, block_read).await {
                let (key, entry_resp) = res.unwrap();
                return Array(vec![Array(vec![BulkString(key.clone()), entry_resp])]);
            }
        }
        WildCard("$-1\r\n".into())
    }
}

impl XRead {
    pub fn new<'a>(
        mut args_iter: &mut impl Iterator<Item = &'a RespType>,
        streams: AMStreams,
        stream_senders: AMStreamSenders,
    ) -> Result<Self, CmdError> {
        let mut block = None;
        let option =
            unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        let array = match option.to_lowercase().as_str() {
            "block" => {
                let timeout = unpack_bulk_string(
                    args_iter.next().ok_or_else(|| CmdError::MissingArgs)?,
                )?;
                let timeout = timeout.parse::<u64>().map_err(|_| CmdError::InvalidArg)?;
                let timeout = Duration::from_millis(timeout);
                block = Some(timeout);

                let option = unpack_bulk_string(
                    args_iter.next().ok_or_else(|| CmdError::MissingArgs)?,
                )?;
                if option.to_lowercase().as_str() != "streams" {
                    return Err(CmdError::InvalidArg)?;
                }
                args_iter.collect::<Vec<_>>()
            }
            "streams" => args_iter.collect::<Vec<_>>(),
            _ => return Err(CmdError::InvalidArg)?,
        };
        let mut keys = Vec::new();
        let mut ids = Vec::new();
        for i in 0..array.len() / 2 {
            keys.push(unpack_bulk_string(&array[i])?);
            ids.push(unpack_bulk_string(&array[i + array.len() / 2])?);
        }
        println!("{:?}", keys);
        println!("{:?}", ids);
        Ok(XRead {
            timeout: block,
            keys,
            ids,
            streams,
            stream_senders,
        })
    }
}
