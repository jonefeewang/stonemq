use bytes::{Buf, BytesMut};

use crate::AppResult;
use crate::message::MemoryRecords;
use crate::protocol::primary_types::{NPBytes, PrimaryType};
use crate::protocol::types::DataType;

impl PrimaryType for MemoryRecords {
    fn read_from(buffer: &mut BytesMut) -> AppResult<DataType> {
        let n_pbytes_e = NPBytes::read_from(buffer)?;

        if let DataType::NPBytes(n_pbytes) = n_pbytes_e {
            if let Some(n_pbytes) = n_pbytes.value {
                return Ok(DataType::Records(MemoryRecords::new(n_pbytes)));
            }
        }
        Ok(DataType::Records(MemoryRecords::empty()))
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        let n_pbytes = NPBytes { value: self.buffer };
        n_pbytes.write_to(buffer)
    }

    fn size(&self) -> usize {
        match &self.buffer {
            None => 4,
            Some(buffer) => 4 + buffer.remaining(),
        }
    }
}

#[test]
fn test_record_read_write() {
    //Test MemoryRecord
    let mut buffer = BytesMut::new();
    let memory_record_value = MemoryRecords::new(BytesMut::from("test".as_bytes()));
    let memory_record_value_clone = memory_record_value.clone();
    memory_record_value.write_to(&mut buffer).unwrap();
    let read_memory_record = MemoryRecords::read_from(&mut buffer).unwrap();
    assert_eq!(
        read_memory_record,
        DataType::Records(memory_record_value_clone)
    );
}
