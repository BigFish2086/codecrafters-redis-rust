// https://rdb.fnordig.de/file_format.html

use crate::redis::DataEntry;
use std::collections::HashMap;
use tokio::time::{Duration, Instant};
use crate::utils::take_upto;

const MAGIC: &str = "REDIS";
const MAGIC_BYTES: usize = MAGIC.len();
const VERSION_BYTES: usize = 4;
const TIME_SECS_BYTES: usize = 4;
const TIME_MILLIS_BYTES: usize = 8;

macro_rules! rdb_opcode {
    ( $( ($opcode:expr, $konst:ident);)+) => {
        $( const $konst: u8 = $opcode; )+
    }
}

rdb_opcode! {
    (0xFF, EOF);
    (0xFE, SELECTDB);
    (0xFD, EXPIRETIME);
    (0xFC, EXPIRETIMEMS);
    (0xFB, RESIZEDB);
    (0xFA, AUX);
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum RDBParseError {
    #[error("ERROR: invalid magic bytes, should be `REDIS`")]
    InvalidMagicBytes,
    #[error("ERROR: invalid version number, should be of type `u32`")]
    InvalidVersion,
    #[error("ERROR: invalid encoded length")]
    InvalidLen,
    #[error("ERROR: invalid encoded integer")]
    InvalidInt,
    #[error("ERROR: invalid encoded uncompressed string")]
    InvalidUnCompStr,
    #[error("ERROR: invalid encoded compressed string")]
    InvalidCompStr,
    #[error("ERROR: invalid encoded time in secons")]
    InvalidTimeSecs,
    #[error("ERROR: invalid encoded time in milliseconds")]
    InvalidTimeMS,
    #[error("ERROR: invalid given value type or not implemented yet")]
    InvalidValType,
}

#[derive(Debug)]
pub enum RDBParsedLen {
    I8Int,
    I16Int,
    I32Int,
    CompressedString,
    IntOrString(i32),
}

#[derive(Debug)]
pub struct RDBFile {
    pub magic: String,
    pub rdb_version: u32,
    pub aux_settings: HashMap<String, String>,
    pub db: HashMap<String, DataEntry>,
}

type Result<T> = std::result::Result<T, RDBParseError>;

pub struct RDBParser {}

impl RDBParser {
    pub fn from_rdb(data: &mut &[u8]) -> Result<RDBFile> {
        let magic = Self::parse_magic(data)?;
        let rdb_version = Self::parse_version(data)?;
        let mut aux_settings: HashMap<String, String> = HashMap::new();
        let mut db: HashMap<String, DataEntry> = HashMap::new();
        while data.len() > 0 {
            let (opcode, rest) = data.split_first_chunk::<1>().unwrap();
            let opcode = opcode[0];
            match opcode {
                EOF => {
                    *data = rest;
                    break;
                }
                SELECTDB => {
                    *data = rest;
                    // TODO: for now discard this selector and support just one DB. later use the
                    // given selector to select a DB
                    let _db_selector = Self::parse_length_encoded_data(data)?;
                }
                RESIZEDB => {
                    *data = rest;
                    // TODO: for now discard given lengths by this opcode. later use the given
                    // lengths to resize each DB
                    let _db_len = Self::parse_integer(data)?;
                    let _expiry_db_len = Self::parse_integer(data)?;
                }
                AUX => {
                    *data = rest;
                    aux_settings.insert(
                        Self::parse_length_encoded_data(data)?,
                        Self::parse_length_encoded_data(data)?,
                    );
                }
                EXPIRETIME => {
                    *data = rest;
                    let expiry = Duration::from_millis(Self::parse_time_millis(data)?);
                    // TODO: for now discard the value type and just support only string encoded
                    // for simplicity. later use value_type and optimize memory by adjusting the
                    // value type for each key
                    let _val_type = Self::parse_value_type(data)?;
                    let key = Self::parse_length_encoded_data(data)?;
                    let value = Self::parse_length_encoded_data(data)?;
                    db.insert(
                        key,
                        DataEntry {
                            value,
                            created_at: Instant::now(), // XXX
                            expired_millis: Some(expiry),
                        },
                    );
                }
                EXPIRETIMEMS => {
                    *data = rest;
                    let expiry = Duration::from_secs(Self::parse_time_secs(data)? as u64);
                    // TODO: for now discard the value type and just support only string encoded
                    // for simplicity. later use value_type and optimize memory by adjusting the
                    // value type for each key
                    let _val_type = Self::parse_value_type(data)?;
                    let key = Self::parse_length_encoded_data(data)?;
                    let value = Self::parse_length_encoded_data(data)?;
                    db.insert(
                        key,
                        DataEntry {
                            value,
                            created_at: Instant::now(), // XXX
                            expired_millis: Some(expiry),
                        },
                    );
                }
                _ => {
                    // TODO: for now discard the value type and just support only string encoded
                    // for simplicity. later use value_type and optimize memory by adjusting the
                    // value type for each key
                    let _val_type = Self::parse_value_type(data)?;
                    let key = Self::parse_length_encoded_data(data)?;
                    let value = Self::parse_length_encoded_data(data)?;
                    db.insert(
                        key,
                        DataEntry {
                            value,
                            created_at: Instant::now(), // XXX
                            expired_millis: None,
                        },
                    );
                }
            };
        }
        Ok(RDBFile {
            magic,
            rdb_version,
            aux_settings,
            db,
        })
    }

    fn parse_magic(data: &mut &[u8]) -> Result<String> {
        let magic_bytes =
            take_upto::<MAGIC_BYTES>(data).ok_or_else(|| RDBParseError::InvalidMagicBytes)?;
        let magic_str = String::from_utf8_lossy(magic_bytes);
        if !magic_str.eq(MAGIC) {
            return Err(RDBParseError::InvalidMagicBytes);
        }
        Ok(magic_str.into_owned())
    }

    fn parse_version(data: &mut &[u8]) -> Result<u32> {
        let version_bytes =
            take_upto::<VERSION_BYTES>(data).ok_or_else(|| RDBParseError::InvalidVersion)?;
        let version_ascii = String::from_utf8_lossy(version_bytes);
        Ok(version_ascii
            .parse::<u32>()
            .map_err(|_| RDBParseError::InvalidVersion)?)
    }

    fn parse_length(data: &mut &[u8]) -> Result<RDBParsedLen> {
        use RDBParsedLen::*;
        let len_type = take_upto::<1>(data).ok_or_else(|| RDBParseError::InvalidLen)?[0];
        match (len_type & 0b11000000) >> 6 {
            0 => Ok(IntOrString((len_type & 0b00111111) as i32)),
            1 => {
                let next_byte = take_upto::<1>(data).ok_or_else(|| RDBParseError::InvalidLen)?[0];
                Ok(IntOrString(i32::from_le_bytes([
                    next_byte,
                    len_type & 0b00111111,
                    0x00,
                    0x00,
                ])))
            }
            2 => {
                let next_four_bytes =
                    take_upto::<4>(data).ok_or_else(|| RDBParseError::InvalidLen)?;
                Ok(IntOrString(i32::from_le_bytes(*next_four_bytes)))
            }
            3 => match len_type & 0b00111111 {
                0 => Ok(I8Int),
                1 => Ok(I16Int),
                2 => Ok(I32Int),
                3 => Ok(CompressedString),
                _ => Err(RDBParseError::InvalidLen),
            },
            _ => unreachable!(),
        }
    }

    fn parse_value_type(data: &mut &[u8]) -> Result<u8> {
        use RDBParseError::InvalidValType as err;
        let val_type = take_upto::<1>(data).ok_or_else(|| err)?;
        let val_type = u8::from_le_bytes(*val_type);
        match val_type {
            0 => Ok(0),
            _ => Err(err),
        }
    }

    fn parse_integer(data: &mut &[u8]) -> Result<i32> {
        use RDBParseError::InvalidInt as err;
        use RDBParsedLen::*;
        match Self::parse_length(data)? {
            I8Int => {
                let ibytes = take_upto::<1>(data).ok_or_else(|| err)?;
                Ok(i8::from_le_bytes(*ibytes) as i32)
            }
            I16Int => {
                let ibytes = take_upto::<2>(data).ok_or_else(|| err)?;
                Ok(i16::from_le_bytes(*ibytes) as i32)
            }
            I32Int => {
                let ibytes = take_upto::<4>(data).ok_or_else(|| err)?;
                Ok(i32::from_le_bytes(*ibytes))
            }
            IntOrString(num) => Ok(num),
            _ => Err(err),
        }
    }

    fn parse_length_encoded_data(data: &mut &[u8]) -> Result<String> {
        use RDBParseError::*;
        use RDBParsedLen::*;
        match Self::parse_length(data)? {
            I8Int => {
                let ibytes = take_upto::<1>(data).ok_or_else(|| InvalidInt)?;
                Ok((i8::from_le_bytes(*ibytes)).to_string())
            }
            I16Int => {
                let ibytes = take_upto::<2>(data).ok_or_else(|| InvalidInt)?;
                Ok((i16::from_le_bytes(*ibytes)).to_string())
            }
            I32Int => {
                let ibytes = take_upto::<4>(data).ok_or_else(|| InvalidInt)?;
                Ok(i32::from_le_bytes(*ibytes).to_string())
            }
            CompressedString => {
                let comp_len = Self::parse_integer(data)? as usize;
                let real_len = Self::parse_integer(data)? as usize;
                let ibytes = if data.len() >= comp_len {
                    let (left, right) = data.split_at(comp_len);
                    *data = right;
                    left
                } else {
                    return Err(InvalidCompStr);
                };
                let decompressed =
                    lzf::decompress(&ibytes, real_len).map_err(|_| InvalidCompStr)?;
                Ok(String::from(String::from_utf8_lossy(&decompressed)))
            }
            IntOrString(ilen) => {
                let ibytes = if data.len() >= ilen as usize {
                    let (left, right) = data.split_at(ilen as usize);
                    *data = right;
                    left
                } else {
                    return Err(InvalidUnCompStr);
                };
                Ok(String::from(String::from_utf8_lossy(ibytes)))
            }
        }
    }

    fn parse_time_secs(data: &mut &[u8]) -> Result<u32> {
        let ibytes =
            take_upto::<TIME_SECS_BYTES>(data).ok_or_else(|| RDBParseError::InvalidTimeSecs)?;
        Ok(u32::from_le_bytes(*ibytes))
    }

    fn parse_time_millis(data: &mut &[u8]) -> Result<u64> {
        let ibytes =
            take_upto::<TIME_MILLIS_BYTES>(data).ok_or_else(|| RDBParseError::InvalidTimeMS)?;
        Ok(u64::from_le_bytes(*ibytes))
    }
}
