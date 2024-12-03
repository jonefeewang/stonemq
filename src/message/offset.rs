use std::io::Read;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::request::{KafkaError, KafkaResult};

pub struct OffsetMetadata {
    pub offset: i64,
    pub metadata: String,
}

pub struct OffsetAndMetadata {
    pub offset_metadata: OffsetMetadata,
    pub commit_timestamp: i64,
    pub expire_timestamp: i64,
}
impl OffsetAndMetadata {
    pub fn serialize(&self) -> KafkaResult<Bytes> {
        let mut buf = BytesMut::new();
        buf.put_i64(self.offset_metadata.offset);
        buf.put_i32(self.offset_metadata.metadata.len() as i32);
        buf.put_slice(self.offset_metadata.metadata.as_bytes());
        buf.put_i64(self.commit_timestamp);
        buf.put_i64(self.expire_timestamp);
        Ok(buf.freeze())
    }
    pub fn deserialize(bytes: &[u8]) -> KafkaResult<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        let offset = cursor.get_i64();
        let metadata_len = cursor.get_i32();
        let mut metadata = vec![0; metadata_len as usize];
        cursor.read_exact(&mut metadata).unwrap();
        let commit_timestamp = cursor.get_i64();
        let expire_timestamp = cursor.get_i64();
        Ok(Self {
            offset_metadata: OffsetMetadata {
                offset,
                metadata: String::from_utf8(metadata).map_err(|e| {
                    KafkaError::CoordinatorNotAvailable(format!("无法解析metadata: {}", e))
                })?,
            },
            commit_timestamp,
            expire_timestamp,
        })
    }
}
