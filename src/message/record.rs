use std::fmt::Debug;
use integer_encoding::VarInt;

#[derive(Debug)]
pub struct Record {
    pub length: i32,
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key_len: i32,
    pub key: Option<Vec<u8>>,
    pub value_len: i32,
    pub value: Option<Vec<u8>>,
    pub headers_count: i32,
    pub headers: Option<Vec<RecordHeader>>,
}

#[derive(Debug)]
pub struct RecordHeader {
    pub header_key: String,
    pub header_value: Option<Vec<u8>>,
}

impl RecordHeader {
    pub fn new<T: AsRef<[u8]>>(key: String, value: T) -> RecordHeader {
        RecordHeader {
            header_key: key,
            header_value: {
                if value.as_ref().is_empty() {
                    None
                } else {
                    Some(value.as_ref().into())
                }
            },
        }
    }

    pub fn size(&self) -> i32 {
        let mut size = self.header_key.len().required_space() + self.header_key.len();
        if let Some(ref header_value) = self.header_value {
            size = size + header_value.len().required_space() + header_value.len()
        }
        size as i32
    }
} 