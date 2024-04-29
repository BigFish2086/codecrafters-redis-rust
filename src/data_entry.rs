use crate::{
    constants::{COMPRESS_AT_LENGTH, EXPIRETIMEMS},
};

use tokio::time::{Duration, Instant};

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum ValueType {
    I8Int(i8),
    I16Int(i16),
    I32Int(i32),
    IntOrString(String),
    CompressedString {
        real_data_len: usize,
        compressed_data: Vec<u8>,
    },
}

impl ValueType {
    pub fn new(data: String) -> Self {
        if let Ok(as_i8) = data.parse::<i8>() {
            Self::I8Int(as_i8)
        } else if let Ok(as_i16) = data.parse::<i16>() {
            Self::I16Int(as_i16)
        } else if let Ok(as_i32) = data.parse::<i32>() {
            Self::I32Int(as_i32)
        } else if data.len() < COMPRESS_AT_LENGTH as usize {
            Self::IntOrString(data)
        } else {
            match lzf::compress(&data.as_bytes()) {
                Ok(compressed_data) => Self::CompressedString {
                    real_data_len: data.len(),
                    compressed_data,
                },
                Err(lzf::LzfError::NoCompressionPossible) => Self::IntOrString(data),
                _ => unreachable!(),
            }
        }
    }

    pub fn as_string(&self) -> String {
        use ValueType::*;
        match self {
            I8Int(x) => x.to_string(),
            I16Int(x) => x.to_string(),
            I32Int(x) => x.to_string(),
            IntOrString(x) => x.to_string(),
            CompressedString {
                real_data_len,
                compressed_data,
            } => {
                // TODO: handle errors better
                let decompressed = lzf::decompress(&compressed_data, *real_data_len).unwrap();
                String::from(String::from_utf8_lossy(&decompressed))
            }
        }
    }

    fn number_to_length_encoded(number: u32) -> Vec<u8> {
        match number {
            0..=63 => {
                // 0  = 0b0000_0000 => le_bytes [[00]00_0000]
                // 63 = 0b0011_1111 => le_bytes [[00]11_1111]
                // 0b[00][aa_aaaa] => 0b[type=00][num=...]
                vec![number as u8]
            }
            64..=16383 => {
                // 64    = 0b0000_0000_0100_0000 => le_bytes [[01]00_0000] [0000_0000]
                // 16383 = 0b0011_1111_1111_1111 => le_bytes [[01]11_1111] [0011_1111]
                // 0b[01][aa_aaaa][next_byte] =>
                // 0b[type=01][len=u16::from_le_bytes([next_byte, 0b00aa_aaaa])]
                //
                // the parser is using
                // length = ((bytes[0] & 0x3F) << 8) | bytes[1]
                let le_bytes = (number as u16).to_le_bytes();
                let first_byte = ((number >> 8) & 0b00111111) as u8 | 0b01000000;
                let second_byte = (number & 0xFF) as u8;
                vec![first_byte, second_byte]
            }
            16384..=u32::MAX => {
                // 16384    = 0x3FFF
                // u32::MAX = 0xFFFF
                // 0b[10][aa_aaaa][next_4_byte] =>
                // 0b[type=10][len=u32::from_le_bytes(next_4_byte)
                let mut out: Vec<u8> = vec![0b10000000];
                out.extend_from_slice(&number.to_le_bytes());
                out
            }
        }
    }

    fn as_length_encoded_value(&self) -> Vec<u8> {
        use ValueType::*;
        match self {
            I8Int(x) => {
                // value-type = 0x0 i.e string_encoded_value
                // len = 0xc0 = 192 = 0b1100_0000
                // - 0b1100_0000 & 0b1100_0000 = 3 => special format
                // - 0b1100_0000 & 0b0011_1111 = 0 => i8 encoded as string
                vec![0xc0, x.to_le_bytes()[0]]
            }
            I16Int(x) => {
                // value-type = 0x0 i.e string_encoded_value
                // len = 0xc1 = 193 = 0b1100_0000
                // - 0b1100_0001 & 0b1100_0000 = 3 => special format
                // - 0b1100_0001 & 0b0011_1111 = 1 => i16 encoded as string
                let x = x.to_le_bytes();
                vec![0xc1, x[0], x[1]]
            }
            I32Int(x) => {
                // value-type = 0x0 i.e string_encoded_value
                // len = 0xc2 = 194 = 0b1100_0000
                // - 0b1100_0002 & 0b1100_0000 = 3 => special format
                // - 0b1100_0002 & 0b0011_1111 = 2 => i32 encoded as string
                let x = x.to_le_bytes();
                vec![0xc2, x[0], x[1], x[2], x[3]]
            }
            IntOrString(x) => {
                let x_len = x.len() as u32;
                let mut out: Vec<u8> = Self::number_to_length_encoded(x.len() as u32);
                out.extend_from_slice(x.as_bytes());
                out
            }
            _ => panic!("Self Can't be Length Encoded Directly"),
        }
    }

    #[inline]
    pub fn as_rdb_value_type(&self) -> Vec<u8> {
        // TODO: for now all values considered of type String
        // this function is just in case of supporting other types later
        vec![0]
    }

    pub fn as_rdb(&self) -> Vec<u8> {
        use ValueType::*;
        match self {
            I8Int { .. } | I16Int { .. } | I32Int { .. } | IntOrString { .. } => {
                self.as_length_encoded_value()
            }
            CompressedString {
                real_data_len,
                compressed_data,
            } => {
                // 0b1100_0011 => (0b1100_0011 & 0b0000_00011) >> 6 = 3 i.e special_type
                // 0b1100_0011 => (0b1100_0011 & 0b0000_00011)      = 3 i.e compressed_string
                // comp_len as len_encoded
                // real_len as len_encoded
                // comp_data it self
                let mut out: Vec<u8> = vec![0b1100_0011];
                // TODO: I do think this should get recursive, however other parsers, doesn't
                // consider that idea at all. It's also not obivious from the mentioned source how
                // these lengthes should be encoded!
                let comp_len = Self::number_to_length_encoded(compressed_data.len() as u32);
                let real_len = Self::number_to_length_encoded(*real_data_len as u32);
                out.extend_from_slice(&comp_len);
                out.extend_from_slice(&real_len);
                out.extend_from_slice(&compressed_data);
                out
            }
        }
    }
}

#[derive(Debug)]
pub struct DataEntry {
    pub value: ValueType,
    pub created_at: Instant,
    // TODO: add distinction between expiry millis and expiry secs
    pub expired_millis: Option<Duration>,
}

impl DataEntry {
    pub fn new(data: String, expired_millis: Option<u64>) -> Self {
        Self {
            value: ValueType::new(data),
            expired_millis: expired_millis.map(|x| Duration::from_millis(x)),
            created_at: Instant::now(),
        }
    }

    pub fn is_expired(&self) -> bool {
        match self.expired_millis {
            Some(expiry) => self.created_at.elapsed() > expiry,
            None => false,
        }
    }
}

pub fn key_value_as_rdb(key: &ValueType, value: &DataEntry) -> Vec<u8> {
    // expiry_opcode|None expiry_4_bytes(secs)|expiry_8_bytes(millis)|None
    // 1 byte value_type (only support string encoded type for now i.e val_type=0)
    // string_encoded_key
    // encoded_value
    let mut out: Vec<u8> = vec![];
    match value.expired_millis {
        Some(expiry) => {
            out = vec![EXPIRETIMEMS];
            out.extend_from_slice(&(expiry.as_millis() as u64).to_le_bytes());
        }
        None => (),
    };
    out.push(value.value.as_rdb_value_type()[0]);
    out.extend_from_slice(&key.as_rdb()[..]);
    out.extend_from_slice(&value.value.as_rdb()[..]);
    out
}


