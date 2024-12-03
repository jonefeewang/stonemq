use bytes::BytesMut;

use crate::message::MemoryRecords;
use crate::protocol::base::ProtocolType;
use crate::protocol::base::{NPBytes, PrimaryType};
use crate::AppResult;

impl PrimaryType for MemoryRecords {
    fn decode(buffer: &mut BytesMut) -> AppResult<ProtocolType> {
        let n_pbytes_e = NPBytes::decode(buffer)?;

        if let ProtocolType::NPBytes(n_pbytes) = n_pbytes_e {
            if let Some(n_pbytes) = n_pbytes.value {
                return Ok(ProtocolType::Records(MemoryRecords::new(n_pbytes)));
            }
        }
        Ok(ProtocolType::Records(MemoryRecords::empty()))
    }

    fn encode(self, writer: &mut BytesMut) {
        let n_pbytes = NPBytes { value: self.buffer };
        n_pbytes.encode(writer)
    }

    fn wire_format_size(&self) -> usize {
        match &self.buffer {
            None => 4,
            Some(buffer) => 4 + buffer.len(),
        }
    }
}

#[test]
fn test_record_read_write() {
    //Test MemoryRecord
    let mut writer = BytesMut::new();
    let memory_record_value = MemoryRecords::new(BytesMut::from("test".as_bytes()));
    let memory_record_value_clone = memory_record_value.clone();
    memory_record_value.encode(&mut writer);
    let mut buffer = BytesMut::from(&writer[..]);
    let read_memory_record = MemoryRecords::decode(&mut buffer).unwrap();
    assert_eq!(
        read_memory_record,
        ProtocolType::Records(memory_record_value_clone)
    );
}
