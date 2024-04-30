use anyhow::Context;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Bound::Included;
use crate::RESPType;
use crate::utils;
use std::fmt;

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct StreamID {
    pub millis: u128,
    pub seq: u64,
}

const INVALID_SEQ: u64 = u64::MAX;

impl StreamID {
    pub fn to_xrange(id: String) -> Self {
        match id.split("-").collect::<Vec<_>>().as_slice() {
            &[mt @ "", sn @ ""] => {
                // id was equal to `-`
                Self { millis: 0, seq: 0, }
            }
            &[mt, sn] => {
                let millis = mt.parse::<u128>().expect("Invalid Stream ID <millis:u128>");
                let seq = sn.parse::<u64>().expect("Invalid Stream ID <seq:u64>");
                Self { millis, seq }
            }
            &[elem @ "+"] => Self { millis: u128::MAX, seq: u64::MAX, },
            &[mt] => {
                let millis = mt.parse::<u128>().expect("Invalid Stream ID <millis:u128>");
                Self { millis, seq: 0, }
            }
            _ => panic!("Invalid Stream ID <millis:u128>:<seq:u64>"),
        }
    }

    pub fn to_xread(id: String) -> Self {
        match id.split("-").collect::<Vec<_>>().as_slice() {
            &[mt, sn] => {
                let millis = mt.parse::<u128>().expect("Invalid Stream ID <millis:u128>");
                let mut seq = sn.parse::<u64>().expect("Invalid Stream ID <seq:u64>");
                if millis == 0 && seq == 0 {
                    seq = 1;
                }
                Self { millis, seq }
            }
            &[mt] => {
                let millis = mt.parse::<u128>().expect("Invalid Stream ID <millis:u128>");
                Self { millis, seq: INVALID_SEQ, }
            }
            _ => panic!("Invalid Stream ID <millis:u128>:<seq:u64>"),
        }
    }

    pub fn to_xadd(id: String) -> Self {
        match id.split("-").collect::<Vec<_>>().as_slice() {
            &[mt, sn @ "*"] => {
                let millis = mt.parse::<u128>().expect("Invalid Stream ID <millis:u128>");
                Self { millis, seq: INVALID_SEQ, }
            }
            &[mt, sn] => {
                let millis = mt.parse::<u128>().expect("Invalid Stream ID <millis:u128>");
                let seq = sn.parse::<u64>().expect("Invalid Stream ID <seq:u64>");
                Self { millis, seq }
            }
            &[star @ "*"] => {
                let millis = utils::gen_millis();
                Self { millis, seq: INVALID_SEQ, }
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
    // TODO:
    // - can have just one BTreeMap ?
    // - can store millis -> to only the last_id_seq and not all of them?
    // - How to store them as Radix Trees as mentioned in the Redis Streams Docs?
    stream_ids_order: BTreeMap<u128, VecDeque<u64>>,
    data: HashMap<StreamID, BTreeMap<String, String>>,
}

impl StreamEntry {
    pub fn new() -> Self {
        Self {
            stream_ids_order: BTreeMap::new(),
            data: HashMap::new(),
        }
    }

    pub fn append_stream(&mut self, stream_id: String, data: BTreeMap<String, String>) -> Result<String, String> {
        let mut stream_id = StreamID::to_xadd(stream_id);
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

    fn read_specific_id_as_resp(&self, stream_id: &StreamID) -> RESPType {
        use RESPType::*;
        // TODO: ensure that id format is {}-{}
        let mut stream_id_array = Vec::new();
        if let Some(stream_id_data) = self.data.get(stream_id) {
            for (key, value) in stream_id_data.iter() {
                stream_id_array.push(BulkString(key.clone()));
                stream_id_array.push(BulkString(value.clone()));
            }
        }
       Array(vec![BulkString(stream_id.to_string()), Array(stream_id_array)])
    }

    fn read_multiple_seq_as_resp(&self, id_millis: u128) -> RESPType {
        use RESPType::*;
        let mut result = Vec::new();
        if let Some(&ref all_seq_numbers_per_time) = self.stream_ids_order.get(&id_millis) {
            for seq_number in all_seq_numbers_per_time {
                let stream_id = StreamID { millis: id_millis, seq: *seq_number };
                result.push(self.read_specific_id_as_resp(&stream_id));
            }
        }
        Array(result)
    }

    pub fn query_xread(&self, id: String) -> RESPType {
        use RESPType::*;
        let stream_id = StreamID::to_xread(id);
        let mut result = Vec::new();
        if stream_id.seq != INVALID_SEQ {
            result.push(self.read_specific_id_as_resp(&stream_id));
        } else {
            result.push(self.read_multiple_seq_as_resp(stream_id.millis));
        }
        
        Array(result)
    }

    pub fn query_xrange(&self, start_id: String, end_id: String) -> RESPType {
        use RESPType::*;
        // TODO: make sure that start less than end
        let mut start_id = StreamID::to_xrange(start_id);
        let mut end_id = StreamID::to_xrange(end_id);
        let mut result = Vec::new();
        for (&id_millis, &ref all_seq_numbers_per_time) in self.stream_ids_order.range((Included(&start_id.millis), Included(&end_id.millis))) {
            for seq_number in all_seq_numbers_per_time {
                if start_id.millis == id_millis && *seq_number < start_id.seq {
                    continue;
                }
                if end_id.millis == id_millis && *seq_number > end_id.seq {
                    break;
                }
                let stream_id = StreamID { millis: id_millis, seq: *seq_number };
                result.push(self.read_specific_id_as_resp(&stream_id));
            }
        }
        Array(result)
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
