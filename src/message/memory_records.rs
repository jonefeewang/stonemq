//! Memory-Based Record Storage Implementation
//!
//! This module provides an efficient, memory-based storage mechanism for message records.
//! It implements an iterator interface for processing record batches and provides
//! methods for managing record buffers in memory.
//!
//! # Design
//!
//! The implementation uses a BytesMut buffer to store records, providing:
//! - Zero-copy operations where possible
//! - Efficient memory management
//! - Iterator-based access to record batches
//!
//! # Usage
//!
//! Records can be:
//! - Created from existing BytesMut buffers
//! - Iterated over as RecordBatch instances
//! - Examined for size and offset information
//! - Managed with minimal memory overhead

use bytes::Buf;
use bytes::BytesMut;
use std::io::Cursor;

use crate::message::record_batch::RecordBatch;

/// In-memory storage for message records.
///
/// Provides efficient storage and access to message records using a BytesMut buffer.
/// Implements Iterator to allow sequential access to record batches.
///
/// # Fields
///
/// * `buffer` - Optional BytesMut buffer containing the record data
#[derive(Clone, PartialEq, Eq)]
pub struct MemoryRecords {
    pub(crate) buffer: Option<BytesMut>,
}

impl std::fmt::Debug for MemoryRecords {
    /// Formats the MemoryRecords for debugging.
    ///
    /// Shows the length of the internal buffer if present.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryRecords")
            .field("buffer length", &self.buffer.as_ref().map(|b| b.len()))
            .finish()
    }
}

impl MemoryRecords {
    /// Creates a new MemoryRecords instance with the provided buffer.
    ///
    /// # Arguments
    ///
    /// * `buffer` - BytesMut buffer containing record data
    ///
    /// # Returns
    ///
    /// A new MemoryRecords instance
    pub fn new(buffer: BytesMut) -> MemoryRecords {
        MemoryRecords {
            buffer: Some(buffer),
        }
    }

    /// Creates an empty MemoryRecords instance.
    ///
    /// # Returns
    ///
    /// A MemoryRecords instance with an empty buffer
    pub fn empty() -> Self {
        MemoryRecords {
            buffer: Some(BytesMut::with_capacity(0)),
        }
    }

    /// Determines the size of the next batch in the buffer.
    ///
    /// # Returns
    ///
    /// * `Option<usize>` - Size of the next batch if available
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

    /// Gets the base offset of the first batch in the records.
    ///
    /// # Returns
    ///
    /// * `Option<i64>` - Base offset if available
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

    /// Gets the total size of the records in bytes.
    ///
    /// # Returns
    ///
    /// Size of the internal buffer in bytes
    pub fn size(&self) -> usize {
        self.buffer.as_ref().map(|buf| buf.len()).unwrap_or(0)
    }
}

impl Iterator for MemoryRecords {
    type Item = RecordBatch;

    /// Gets the next record batch from the buffer.
    ///
    /// This method:
    /// 1. Checks if there's another batch available
    /// 2. Splits off the batch data from the buffer
    /// 3. Creates a new RecordBatch from the data
    ///
    /// # Returns
    ///
    /// * `Option<RecordBatch>` - Next batch if available
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

    /// Tests the iteration functionality of MemoryRecords.
    ///
    /// Verifies that:
    /// - Records can be created and iterated
    /// - Batch contents are correctly accessible
    /// - Iterator properly signals end of records
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
