use crate::message::MemoryRecord;
use crate::protocol::primary_types::{NPBytes, PrimaryType};
use crate::protocol::types::FieldTypeEnum;
use crate::AppError::NetworkReadError;
use crate::AppResult;
use bytes::{Buf, Bytes, BytesMut};
use std::borrow::Cow;

impl PrimaryType for MemoryRecord {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        let n_pbytes_e = NPBytes::read_from(buffer)?;

        if let FieldTypeEnum::NPBytesE(n_pbytes) = n_pbytes_e {
            return Ok(FieldTypeEnum::RecordsE(MemoryRecord::new(n_pbytes.buffer)));
        }
        Err(NetworkReadError(Cow::Owned(format!(
            "unexpected type {:?}",
            n_pbytes_e
        ))))
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        let n_pbytes = NPBytes {
            buffer: self.buffer().clone(),
        };
        return Ok(n_pbytes.write_to(buffer)?);
    }

    fn size(&self) -> usize {
        4 + self.buffer().remaining()
    }
}

#[test]
fn test_record_read_write() {
    //Test MemoryRecord
    let mut buffer = BytesMut::new();
    let memoryrecord_value = MemoryRecord::new(Bytes::from("test".as_bytes()));
    memoryrecord_value.write_to(&mut buffer).unwrap();
    let read_memoryrecord = MemoryRecord::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(
        read_memoryrecord,
        FieldTypeEnum::RecordsE(memoryrecord_value)
    );
}
