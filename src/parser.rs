// Visit https://redis.io/docs/reference/protocol-spec to know more about this protocol specs

use crate::resp::RESPType;
use crate::constants::{CR, LF};

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum ParseError {
    #[error("ERROR: Invalid Input")]
    InvalidInput,
    #[error("ERROR: Incomplete Input")]
    IncompleteInput,
    #[error("ERROR: CRLF Not Found")]
    CRLFNotFound,
    #[error("ERROR: Unkonwn Symbol")]
    UnkownSymbol,
}

type ParseResult<'a> = std::result::Result<(RESPType, &'a [u8]), ParseError>;
type ParseCRLFResult<'a> = std::result::Result<(&'a [u8], &'a [u8]), ParseError>;

pub struct Parser {}

impl Parser {
    pub fn parse_resp<'a>(input: &'a [u8]) -> ParseResult<'a> {
        if input.len() == 0 || input[0] == 0 {
            return Err(ParseError::IncompleteInput);
        }
        let symbol = String::from_utf8_lossy(&input[0..1]);
        let symbol = symbol.as_ref();
        let remaining = &input[1..];
        match symbol {
            "+" => Self::parse_simple_string(remaining),
            "-" => Self::parse_simple_error(remaining),
            "$" => Self::parse_bulk_string(remaining),
            ":" => Self::parse_integer(remaining),
            "*" => Self::parse_array(remaining),
            _ => Err(ParseError::UnkownSymbol),
        }
    }

    pub fn parse_simple_string<'a>(input: &'a [u8]) -> ParseResult<'a> {
        Self::parse_until_crlf(input).map(|(result, remaining)| {
            let output = String::from(String::from_utf8_lossy(result));
            (RESPType::SimpleString(output), remaining)
        })
    }

    pub fn parse_simple_error<'a>(input: &'a [u8]) -> ParseResult<'a> {
        Self::parse_until_crlf(input).map(|(result, remaining)| {
            let output = String::from(String::from_utf8_lossy(result));
            (RESPType::SimpleError(output), remaining)
        })
    }

    pub fn parse_bulk_string<'a>(input: &'a [u8]) -> ParseResult<'a> {
        let (data_len, remaining) = Self::parse_until_crlf(input)?;
        let data_len = String::from(String::from_utf8_lossy(data_len));
        if data_len == "-1" {
            return Ok((RESPType::Null, "".as_bytes()));
        }
        match data_len.parse::<usize>() {
            Ok(data_len) => {
                let (data, remaining) = Self::parse_until_crlf(remaining)?;
                if data.len().lt(&data_len) {
                    Err(ParseError::IncompleteInput)
                } else if data.len().gt(&data_len) {
                    Err(ParseError::InvalidInput)
                } else {
                    let data = String::from(String::from_utf8_lossy(data));
                    Ok((RESPType::BulkString(data), remaining))
                }
            }
            Err(_) => Err(ParseError::InvalidInput),
        }
    }

    pub fn parse_integer<'a>(input: &'a [u8]) -> ParseResult<'a> {
        let (result, remaining) = Self::parse_until_crlf(input)?;
        let result = String::from(String::from_utf8_lossy(result));
        match result.parse::<i64>() {
            Ok(num) => Ok((RESPType::Integer(num), remaining)),
            Err(_) => Err(ParseError::InvalidInput),
        }
    }

    pub fn parse_array<'a>(input: &'a [u8]) -> ParseResult<'a> {
        let (data_len, mut remaining) = Self::parse_until_crlf(input)?;
        let data_len = String::from(String::from_utf8_lossy(data_len));
        if data_len == "-1" {
            return Ok((RESPType::Null, "".as_bytes()));
        }
        match data_len.parse::<usize>() {
            Ok(data_len) => {
                let mut elements: Vec<RESPType> = Vec::new();
                for _ in 0..data_len {
                    let (el, rem) = Self::parse_resp(remaining)?;
                    elements.push(el);
                    remaining = rem;
                }
                Ok((RESPType::Array(elements), remaining))
            }
            Err(_) => Err(ParseError::InvalidInput),
        }
    }

    pub fn parse_until_crlf<'a>(input: &'a [u8]) -> ParseCRLFResult<'a> {
        if input.len() == 0 {
            return Ok((&[0], &[0]));
        }
        for i in 0..input.len() - 1 {
            if input[i] == 0 {
                return Ok((&[0], &[0]));
            }
            if input[i] == CR && input[i + 1] == LF {
                return Ok((&input[0..i], &input[i + 2..]));
            }
        }
        Err(ParseError::CRLFNotFound)
    }
}
