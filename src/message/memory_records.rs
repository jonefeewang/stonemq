use bytes::Buf;
use bytes::BytesMut;
use std::io::Cursor;

use crate::message::record_batch::RecordBatch;

#[derive(Clone, PartialEq, Eq)]
pub struct MemoryRecords {
    pub(crate) buffer: Option<BytesMut>,
}

impl std::fmt::Debug for MemoryRecords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryRecords")
            .field("buffer length", &self.buffer.as_ref().map(|b| b.len()))
            .finish()
    }
}

impl MemoryRecords {
    pub fn new(buffer: BytesMut) -> MemoryRecords {
        MemoryRecords {
            buffer: Some(buffer),
        }
    }

    pub fn empty() -> Self {
        MemoryRecords {
            buffer: Some(BytesMut::with_capacity(0)),
        }
    }

    fn next_batch_size(&self) -> Option<usize> {
        if let Some(buf) = &self.buffer {
            if 4 <= buf.len() {
                let mut cursor = Cursor::new(buf.as_ref());
                let _ = cursor.get_i64();
                let length = cursor.get_i32();
                return Some(length as usize);
            }
        }
        None
    }
    pub fn first_batch_base_offset(&self) -> Option<i64> {
        if let Some(buf) = &self.buffer {
            if !buf.is_empty() {
                let mut cursor = Cursor::new(buf.as_ref());
                let base_offset = cursor.get_i64();
                return Some(base_offset);
            }
        }
        None
    }

    pub fn size(&self) -> usize {
        self.buffer.as_ref().map(|buf| buf.len()).unwrap_or(0)
    }
}

impl Iterator for MemoryRecords {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let batch_size = self.next_batch_size();
        if let Some(batch_size) = batch_size {
            let buffer = self.buffer.as_mut()?;

            if batch_size > buffer.len() {
                return None;
            }

            let batch_buffer = buffer.split_to(batch_size + 12);
            Some(RecordBatch::new(batch_buffer))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::message::record_batch::RecordBatchBuilder;
    use crate::message::MemoryRecords;

    #[test]
    fn test_memory_records_iteration() {
        let mut builder = RecordBatchBuilder::default();
        builder.append_record(
            Some(0),
            Some(1000),
            "key1".as_bytes(),
            "value1".as_bytes(),
            None,
        );

        let batch = builder.build();
        let mut records = MemoryRecords::new(batch.buffer);

        let first_batch = records.next();
        assert!(first_batch.is_some());
        assert!(records.next().is_none());
    }
}
