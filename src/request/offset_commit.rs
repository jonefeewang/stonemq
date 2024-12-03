use std::{collections::HashMap, io::Read};

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{message::TopicPartition, AppResult};

use super::errors::KafkaError;

#[derive(Debug)]
pub struct PartitionOffsetCommitData {
    pub partition_id: i32,
    pub offset: i64,
    pub metadata: Option<String>,
}
impl PartitionOffsetCommitData {
    pub fn serialize(&self) -> AppResult<Bytes> {
        let mut buf = BytesMut::new();
        buf.put_i32(self.partition_id);
        buf.put_i64(self.offset);
        if let Some(metadata) = &self.metadata {
            buf.put_i32(metadata.len() as i32);
            buf.put_slice(metadata.as_bytes());
        } else {
            buf.put_i32(-1);
        }
        Ok(buf.freeze())
    }
    pub fn deserialize(bytes: &[u8]) -> AppResult<Self> {
        let mut cursor = std::io::Cursor::new(bytes);
        let partition_id = cursor.get_i32();
        let offset = cursor.get_i64();
        let metadata_len = cursor.get_i32();
        let metadata = if metadata_len != -1 {
            let mut metadata = vec![0; metadata_len as usize];
            cursor.read_exact(&mut metadata)?;
            Some(String::from_utf8(metadata)?)
        } else {
            None
        };
        Ok(PartitionOffsetCommitData {
            partition_id,
            offset,
            metadata,
        })
    }
}

#[derive(Debug)]
pub struct OffsetCommitRequest {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub retention_time: i64,
    pub offset_data: HashMap<TopicPartition, PartitionOffsetCommitData>,
}

#[derive(Debug)]
pub struct OffsetCommitResponse {
    pub throttle_time_ms: i32,
    pub responses: HashMap<TopicPartition, Vec<(i32, KafkaError)>>,
}
impl OffsetCommitResponse {
    pub fn new(
        throttle_time_ms: i32,
        responses: HashMap<TopicPartition, Vec<(i32, KafkaError)>>,
    ) -> Self {
        OffsetCommitResponse {
            throttle_time_ms,
            responses,
        }
    }
}
