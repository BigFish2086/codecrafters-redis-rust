use async_trait::async_trait;

use crate::cmd::{Cmd, CmdError};
use crate::resp::RespType;
use crate::redis::AMStreams;
use crate::data_entry::ValueType;
use crate::utils::unpack_bulk_string;

pub struct XRange {
    pub stream_key: String,
    pub start_id: String,
    pub end_id: String,
    pub streams: AMStreams,
}

#[async_trait]
impl Cmd for XRange {
    async fn run(&mut self) -> RespType {
        let stream_key = ValueType::new(self.stream_key.clone());
        match self.streams.lock().await.get(&stream_key) {
            Some(stream_entry) => {
                let (resp, is_resp_empty) = stream_entry.query_xrange(self.start_id.clone(), self.end_id.clone());
                resp
            }
            None => RespType::WildCard("*0\r\n".into()),
        }
    }
}

impl XRange {
    pub fn new<'a>(
        mut args_iter: &mut impl Iterator<Item = &'a RespType>,
        streams: AMStreams,
    ) -> Result<Self, CmdError> {
        let stream_key =
            unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        let start_id =
            unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        let end_id =
            unpack_bulk_string(args_iter.next().ok_or_else(|| CmdError::MissingArgs)?)?;
        Ok(Self {
            stream_key,
            start_id,
            end_id,
            streams,
        })
    }
}
