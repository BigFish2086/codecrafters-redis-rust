use anyhow::Context;
use std::collections::{HashMap, VecDeque};
use crate::utils;
use std::fmt;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct StreamID {
    pub millis: u128,
    pub seq: u64,
}

const INVALID_SEQ: u64 = u64::MAX;

impl StreamID {
    pub fn from_string(id: String) -> Self {
        match id.split("-").collect::<Vec<_>>().as_slice() {
            &[mt, sn @ "*"] => {
                let millis = mt.parse::<u128>().expect("Invalid Stream ID <millis:u128>");
                Self {
                    millis,
                    seq: INVALID_SEQ,
                }
            }
            &[mt, sn] => {
                let millis = mt.parse::<u128>().expect("Invalid Stream ID <millis:u128>");
                let seq = sn.parse::<u64>().expect("Invalid Stream ID <seq:u64>");
                Self { millis, seq }
            }
            &[star @ "*"] => {
                let millis = utils::gen_millis();
                Self {
                    millis,
                    seq: INVALID_SEQ,
                }
            }
            _ => panic!("Invalid Stream ID <millis:u128>:<seq:u64>"),
        }
    }
}

impl fmt::Display for StreamID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}-{}", self.millis, self.seq)
    }
}

#[derive(Debug)]
pub struct StreamEntry {
    stream_ids_order: VecDeque<StreamID>,
    data: HashMap<StreamID, HashMap<String, String>>,
}

impl StreamEntry {
    pub fn new() -> Self {
        Self {
            stream_ids_order: VecDeque::new(),
            data: HashMap::new(),
        }
    }
    pub fn append_stream(&mut self, stream_id: String, data: HashMap<String, String>) -> Result<String, String> {
        let mut stream_id = StreamID::from_string(stream_id);
        self.update_and_check_id(&mut stream_id)?;
        if self.data.get(&stream_id) == None {
            let result = stream_id.to_string();
            self.stream_ids_order.push_back(stream_id.clone());
            self.data.insert(stream_id, data);
            Ok(result)
        } else {
            Err("Stream already exists".to_owned())
        }
    }

    fn update_and_check_id(&self, id: &mut StreamID) -> Result<(), String> {
        if id.seq == INVALID_SEQ {
            if let Some(last_id) = self.stream_ids_order.back() {
                id.seq = last_id.seq + 1;
            } else {
                id.seq = 1;
            }
        }
        if id.millis == 0 && id.seq == 0 {
            return Err("The ID specified in XADD must be greater than 0-0".to_owned());
        }
        if let Some(last_id) = self.stream_ids_order.back() {
            if last_id.millis > id.millis || (last_id.millis == id.millis && last_id.seq > id.seq) {
                return Err("The ID specified in XADD is equal or smaller than the target stream top item".to_owned());
            }
        }
        Ok(())
    }
}
