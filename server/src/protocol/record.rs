use bytes::{Buf, BytesMut};
use tokio::io::AsyncWriteExt;

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

    async fn write_to<W>(self, writer: &mut W) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        let n_pbytes = NPBytes { value: self.buffer };
        n_pbytes.write_to(writer).await
    }

    fn size(&self) -> usize {
        match &self.buffer {
            None => 4,
            Some(buffer) => 4 + buffer.remaining(),
        }
    }
}

#[tokio::test]
async fn test_record_read_write() {
    //Test MemoryRecord
    let mut writer = Vec::new();
    let memory_record_value = MemoryRecords::new(BytesMut::from("test".as_bytes()));
    let memory_record_value_clone = memory_record_value.clone();
    memory_record_value.write_to(&mut writer).await.unwrap();
    let mut buffer = BytesMut::from(&writer[..]);
    let read_memory_record = MemoryRecords::read_from(&mut buffer).unwrap();
    assert_eq!(
        read_memory_record,
        DataType::Records(memory_record_value_clone)
    );
}
