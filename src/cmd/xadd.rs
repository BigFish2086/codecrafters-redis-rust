use async_trait::async_trait;

use crate::cmd::{Cmd, CmdError, CmdType};
use crate::resp::RespType;
use crate::redis::{AMStreams, AMStreamSenders};
use crate::data_entry::ValueType;
use crate::stream_entry::StreamEntry;
use crate::utils::unpack_bulk_string;

use std::collections::BTreeMap;

pub struct XAdd {
    pub stream_key: String,
    pub stream_id: String,
    pub stream_data: BTreeMap<String, String>,
    pub streams: AMStreams,
    pub stream_senders: AMStreamSenders,
}

#[async_trait]
impl Cmd for XAdd {
    async fn run(&mut self) -> RespType {
        use RespType::{BulkString, SimpleError, Array};
        let key = ValueType::new(self.stream_key.clone());
        let mut stream_guard = self.streams.lock().await;
        let stream_entry = stream_guard.entry(key).or_insert(StreamEntry::new());
        match stream_entry.append_stream(self.stream_id.clone(), self.stream_data.clone()) {
            Ok(stored_id) => {
                if let Some(sender) = self.stream_senders.lock().await.get(&self.stream_key) {
                    let mut stream_id_array = Vec::new();
                    for (key, value) in self.stream_data.iter() {
                        stream_id_array.push(BulkString(key.clone()));
                        stream_id_array.push(BulkString(value.clone()));
                    }
                    let data = Array(vec![BulkString(stored_id.clone()), Array(stream_id_array)]);
                    sender.send(data);
                }
                BulkString(stored_id)
            }
            Err(reason) => SimpleError(reason),
        }
    }

    fn cmd_type(&self) -> CmdType {
        CmdType::XADD
    }
}

impl XAdd {
    pub fn new<'a>(
        mut args_iter: &mut impl Iterator<Item = &'a RespType>,
        streams: AMStreams,
        stream_senders: AMStreamSenders,
    ) -> Result<Self, CmdError> {
        let stream_key =
            unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        let stream_id =
            unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        let mut stream_data = BTreeMap::new();
        for key_value in args_iter.collect::<Vec<_>>().as_slice().chunks_exact(2) {
            let key = unpack_bulk_string(&key_value[0])?;
            let value = unpack_bulk_string(&key_value[1])?;
            stream_data.insert(key, value);
        }
        Ok(Self {
            stream_key,
            stream_id,
            stream_data,
            streams,
            stream_senders,
        })
    }
}
