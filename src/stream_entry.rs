use anyhow::Context;
use std::collections::{BTreeMap, HashMap, VecDeque};
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
    // TODO: can have just BTreeMap ?
    stream_ids_order: BTreeMap<u128, VecDeque<u64>>,
    data: HashMap<StreamID, HashMap<String, String>>,
}

impl StreamEntry {
    pub fn new() -> Self {
        Self {
            stream_ids_order: BTreeMap::new(),
            data: HashMap::new(),
        }
    }
    pub fn append_stream(&mut self, stream_id: String, data: HashMap<String, String>) -> Result<String, String> {
        let mut stream_id = StreamID::from_string(stream_id);
        self.update_id(&mut stream_id);
        self.check_id(&stream_id)?;
        if self.data.get(&stream_id) == None {
            let result = stream_id.to_string();
            self.stream_ids_order.entry(stream_id.millis).or_insert_with(VecDeque::new).push_back(stream_id.seq);
            self.data.insert(stream_id, data);
            Ok(result)
        } else {
            Err("The ID specified in XADD is equal or smaller than the target stream top item".to_owned())
        }
    }

    fn update_id(&self, id: &mut StreamID) {
        if id.seq == INVALID_SEQ {
            if let Some(last_id_millis) = self.stream_ids_order.get(&id.millis) {

                if let Some(last_id_seq) = last_id_millis.back() {
                    id.seq = last_id_seq + 1;
                } else {
                    id.seq = if id.millis == 0 { 1 } else { 0 };
                }

            } else {
                id.seq = if id.millis == 0 { 1 } else { 0 };
            }
        }
    }

    fn check_id(&self, id: &StreamID) -> Result<(), String> {
        // 0-0 not allowed
        if id.millis == 0 && id.seq == 0 {
            return Err("The ID specified in XADD must be greater than 0-0".to_owned());
        }
        // inserted id time must be greater than last inserted time
        // last_id seq in same "time" can't be greater than the next id to insert
        if let Some((last_id_millis, last_id_seq)) = self.stream_ids_order.last_key_value() {
            let last_id_seq = last_id_seq.back().unwrap_or(&0);
            if *last_id_millis > id.millis || (*last_id_millis == id.millis && *last_id_seq > id.seq) {
                return Err("The ID specified in XADD is equal or smaller than the target stream top item".to_owned());
            }
        }
        Ok(())
    }
}
