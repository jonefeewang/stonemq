/// This module implements the RecordBatch functionality, which is a fundamental component
/// for managing message records in the messaging system. A RecordBatch represents a collection
/// of records that are stored and processed together for efficiency.
///
/// Key features:
/// - Efficient batch processing of records
/// - Support for record headers and metadata
/// - CRC validation for data integrity
/// - Flexible record building through RecordBatchBuilder
/// - Memory-efficient buffer management using BytesMut
///
use bytes::{Buf, BufMut, BytesMut};
use integer_encoding::VarInt;
use std::io::{Cursor, Write};

use tracing::error;

use crate::message::batch_header::BatchHeader;
use crate::message::constants::*;
use crate::message::record::{Record, RecordHeader};
use crate::{global_config, AppError, AppResult};

use super::MemoryRecords;

/// Represents a batch of records that are stored and processed together.
/// The batch includes a header with metadata and a collection of records.
/// All records within a batch share common properties like base offset and timestamp.
pub struct RecordBatch {
    /// The underlying buffer containing the batch data in a compact binary format
    pub(crate) buffer: BytesMut,
}

impl RecordBatch {
    /// Creates a new RecordBatch with the given buffer.
    ///
    /// # Arguments
    /// * `buffer` - A BytesMut buffer containing the batch data
    pub fn new(buffer: BytesMut) -> Self {
        RecordBatch { buffer }
    }

    /// Retrieves the header information from the batch.
    /// The header contains metadata such as offsets, timestamps, and record count.
    ///
    /// # Returns
    /// A BatchHeader struct containing the batch metadata
    pub fn header(&self) -> BatchHeader {
        let mut cursor = Cursor::new(self.buffer.as_ref());
        BatchHeader {
            first_offset: cursor.get_i64(),
            length: cursor.get_i32(),
            partition_leader_epoch: cursor.get_i32(),
            magic: cursor.get_i8(),
            crc: cursor.get_u32(),
            attributes: cursor.get_i16(),
            last_offset_delta: cursor.get_i32(),
            first_timestamp: cursor.get_i64(),
            max_timestamp: cursor.get_i64(),
            producer_id: cursor.get_i64(),
            producer_epoch: cursor.get_i16(),
            first_sequence: cursor.get_i32(),
            records_count: cursor.get_i32(),
        }
    }
    /// Merges this batch's buffer back into the original MemoryRecords.
    /// This is typically used after processing a split batch to recombine it.
    ///
    /// # Arguments
    /// * `records` - The MemoryRecords to merge this batch back into
    pub fn unsplit(self, records: &mut MemoryRecords) {
        if let Some(main_buffer) = &mut records.buffer {
            main_buffer.unsplit(self.buffer);
        }
    }
    /// Gets the number of records in this batch.
    ///
    /// # Returns
    /// The number of records as an i32
    pub fn records_count(&self) -> i32 {
        self.get_field(RECORDS_COUNT_OFFSET, |c| c.get_i32())
    }

    /// Gets the maximum timestamp in this batch.
    ///
    /// # Returns
    /// The maximum timestamp as an i64
    pub fn max_timestamp(&self) -> i64 {
        self.get_field(MAX_TIMESTAMP_OFFSET, |c| c.get_i64())
    }
    /// Gets the base (first) timestamp in this batch.
    ///
    /// # Returns
    /// The base timestamp as an i64
    pub fn base_timestamp(&self) -> i64 {
        self.get_field(FIRST_TIMESTAMP_OFFSET, |c| c.get_i64())
    }

    /// Gets the base offset of this batch.
    ///
    /// # Returns
    /// The base offset as an i64
    pub fn base_offset(&self) -> i64 {
        self.get_field(BASE_OFFSET_OFFSET, |c| c.get_i64())
    }
    /// Gets the last offset delta in this batch.
    ///
    /// # Returns
    /// The last offset delta as an i32
    pub fn last_offset_delta(&self) -> i32 {
        self.get_field(LAST_OFFSET_DELTA_OFFSET, |c| c.get_i32())
    }

    /// Helper method to get a field from the buffer at a specific offset.
    ///
    /// # Arguments
    /// * `offset` - The offset in the buffer to read from
    /// * `getter` - A function that reads the field value from a cursor
    ///
    /// # Returns
    /// The field value of type T
    fn get_field<T>(&self, offset: i32, getter: impl Fn(&mut Cursor<&[u8]>) -> T) -> T {
        let mut cursor = Cursor::new(self.buffer.as_ref());
        cursor.set_position(offset as u64);
        getter(&mut cursor)
    }

    /// Sets the base offset for this batch.
    ///
    /// # Arguments
    /// * `base_offset` - The new base offset value
    pub fn set_base_offset(&mut self, base_offset: i64) {
        let mut cursor = Cursor::new(self.buffer.as_mut());
        cursor.set_position(BASE_OFFSET_OFFSET as u64);
        cursor.write_all(&base_offset.to_be_bytes()).unwrap();
    }
    /// Sets the first timestamp for this batch.
    ///
    /// # Arguments
    /// * `first_timestamp` - The new first timestamp value
    ///
    /// # Returns
    /// An AppResult indicating success or failure
    pub fn set_first_timestamp(&mut self, first_timestamp: i64) -> AppResult<()> {
        let mut cursor = Cursor::new(self.buffer.as_mut());
        cursor.set_position(FIRST_TIMESTAMP_OFFSET as u64);
        cursor
            .write_all(&first_timestamp.to_be_bytes())
            .map_err(|e| {
                AppError::DetailedIoError(format!("write first timestamp error: {}", e))
            })?;
        Ok(())
    }

    /// Validates the batch to ensure it meets all requirements.
    /// This includes checking buffer size, offsets, magic value, and CRC.
    ///
    /// /// Validate the following:  
    /// - The buffer must not be empty and its length must exceed LOG_OVERHEAD.  
    /// - The base_offset must be 0.  
    /// - The batch_size must fall within the valid range (less than max_msg_size and greater than RECORD_BATCH_OVERHEAD).  
    /// - The magic value must be 2 (currently, only magic 2 is supported).  
    /// - The CRC check must pass.  
    /// - The record_count must be a non-negative number.  
    ///  
    ///
    /// # Returns
    /// An AppResult indicating whether the batch is valid
    pub fn validate_batch(&self) -> AppResult<()> {
        let mut cursor = Cursor::new(self.buffer.as_ref());

        let remaining = cursor.remaining();
        if remaining == 0 || remaining < LOG_OVERHEAD {
            error!("MemoryRecord is empty");
            return Err(AppError::InvalidRequest(
                "memoryRecord is empty".to_string(),
            ));
        }

        // deserialize batch header
        let base_offset = cursor.get_i64();
        let batch_size = cursor.get_i32();
        let _ = cursor.get_i32(); // partition leader epoch
        let magic = cursor.get_i8();

        let max_msg_size = global_config().general.max_msg_size;

        if base_offset != 0 {
            return Err(AppError::InvalidRequest(format!(
                "Base offset should be 0, but found {}",
                base_offset
            )));
        }

        if batch_size > max_msg_size {
            return Err(AppError::MessageTooLarge(format!(
                "Message size {} exceeds the maximum message size {}",
                batch_size, max_msg_size
            )));
        }
        if batch_size + (LOG_OVERHEAD as i32) < RECORD_BATCH_OVERHEAD {
            return Err(AppError::CorruptMessage(format!(
                "Message size {} is less than the record batch overhead {}",
                batch_size, RECORD_BATCH_OVERHEAD
            )));
        }

        // currently only support with magic 2
        if (0..=1).contains(&magic) {
            return Err(AppError::InvalidRequest(format!(
                "StoneMQ currently only support Magic 2, but found {}",
                magic
            )));
        }

        let batch_crc = cursor.get_u32();
        // validate crc
        cursor.set_position(ATTRIBUTES_OFFSET as u64);
        let crc_parts = &cursor.get_ref()[cursor.position() as usize..];
        let compute_crc = crc32c::crc32c(crc_parts);
        if compute_crc != batch_crc {
            return Err(AppError::CorruptMessage(format!(
                "CRC mismatch: expected {}, but found {}",
                compute_crc, batch_crc
            )));
        }
        //validate record count
        cursor.set_position(RECORDS_COUNT_OFFSET as u64);
        let record_count = cursor.get_i32();

        if record_count < 0 {
            return Err(AppError::CorruptMessage(format!(
                "Record count should be non-negative, but found {}",
                record_count
            )));
        }

        Ok(())
    }

    /// Retrieves all records from this batch.
    ///
    /// # Returns
    /// A vector containing all Record instances in this batch
    pub fn records(&self) -> Vec<Record> {
        let mut cursor = Cursor::new(self.buffer.as_ref());
        cursor.advance(RECORDS_COUNT_OFFSET as usize);
        let record_count = cursor.get_i32();

        let mut records = Vec::with_capacity(record_count as usize);
        for _ in 0..record_count {
            if let Some(record_length) = i32::decode_var(cursor.chunk()) {
                cursor.advance(record_length.1);
                Self::decode_record_body(&mut cursor, &mut records, record_length.0);
            }
        }
        records
    }

    /// Decodes a single record from the buffer.
    ///
    /// # Arguments
    /// * `cursor` - The cursor pointing to the record data
    /// * `records` - The vector to append the decoded record to
    /// * `record_length` - The length of the record to decode
    fn decode_record_body(
        cursor: &mut Cursor<&[u8]>,
        records: &mut Vec<Record>,
        record_length: i32,
    ) {
        let attributes = cursor.get_i8();

        let timestamp_delta = i64::decode_var(cursor.chunk())
            .map(|(timestamp_delta, read_size)| {
                cursor.advance(read_size);
                timestamp_delta
            })
            .unwrap();

        let offset_delta = i64::decode_var(cursor.chunk())
            .map(|(offset_delta, read_size)| {
                cursor.advance(read_size);
                offset_delta
            })
            .unwrap();

        let key_len = i32::decode_var(cursor.chunk())
            .map(|(key_len, read_size)| {
                cursor.advance(read_size);
                key_len
            })
            .unwrap();

        let mut key: Option<Vec<u8>> = None;
        let mut value: Option<Vec<u8>> = None;
        if key_len > 0 {
            key = Some(cursor.copy_to_bytes(key_len as usize).to_vec());
        }

        let value_len = i32::decode_var(cursor.chunk())
            .map(|(value_len, read_size)| {
                cursor.advance(read_size);
                value_len
            })
            .unwrap();
        if value_len > 0 {
            value = Some(cursor.copy_to_bytes(value_len as usize).to_vec());
        }

        let headers_count = i32::decode_var(cursor.chunk())
            .map(|(header_count, read_size)| {
                cursor.advance(read_size);
                header_count
            })
            .unwrap();

        let mut headers = None;
        if headers_count > 0 {
            headers = Some(vec![]);
            for _ in 0..headers_count {
                let header_key_len = i32::decode_var(cursor.chunk())
                    .map(|(header_key_len, read_size)| {
                        cursor.advance(read_size);
                        header_key_len
                    })
                    .unwrap();

                let header_key =
                    String::from_utf8(cursor.copy_to_bytes(header_key_len as usize).to_vec())
                        .unwrap();

                let value_len = i32::decode_var(cursor.chunk())
                    .map(|(value_len, read_size)| {
                        cursor.advance(read_size);
                        value_len
                    })
                    .unwrap();

                let mut header_value: Option<Vec<u8>> = None;
                if value_len > 0 {
                    header_value = Some(cursor.copy_to_bytes(value_len as usize).to_vec());
                }

                headers.as_mut().unwrap().push(RecordHeader {
                    header_key,
                    header_value,
                });
            }
        }

        records.push(Record {
            length: record_length,
            attributes,
            timestamp_delta,
            offset_delta: offset_delta as i32,
            key_len,
            key,
            value_len,
            value,
            headers_count,
            headers,
        });
    }
}

/// A builder for creating new RecordBatch instances.
/// Provides methods to append records and configure batch properties.
pub struct RecordBatchBuilder {
    buffer: BytesMut,
    _magic: i8,
    _attributes: i16,
    _last_offset: i64,
    _base_timestamp: i64,
    _base_offset: i64,
    _max_timestamp: i64,
    _record_count: i32,
}

impl Default for RecordBatchBuilder {
    fn default() -> Self {
        let mut builder = RecordBatchBuilder {
            buffer: BytesMut::with_capacity(RECORD_BATCH_OVERHEAD as usize),
            _magic: MAGIC,
            _attributes: 0,
            _last_offset: 0,
            _base_timestamp: 0,
            _base_offset: 0,
            _max_timestamp: 0,
            _record_count: 0,
        };
        builder.initialize_buffer();
        builder
    }
}
#[allow(dead_code)]
impl RecordBatchBuilder {
    /// Initializes the buffer with default values for batch metadata.
    fn initialize_buffer(&mut self) {
        self.buffer.put_i64(0); //first offset
        self.buffer.put_i32(0); //length
        self.buffer.put_i32(NO_PARTITION_LEADER_EPOCH);
        self.buffer.put_i8(MAGIC);
        self.buffer.put_i32(-1); //crc
        self.buffer.put_i16(ATTRIBUTES);
        self.buffer.put_i32(-1); //last offset delta
        self.buffer.put_i64(-1); //first time stamp
        self.buffer.put_i64(-1); //max timestamp
        self.buffer.put_i64(NO_PRODUCER_ID);
        self.buffer.put_i16(NO_PRODUCER_EPOCH);
        self.buffer.put_i32(NO_SEQUENCE);
        self.buffer.put_i32(0); //record count
    }
    /// Appends a record with a specific offset to the batch.
    ///
    /// # Arguments
    /// * `offset` - The offset for the record
    /// * `timestamp` - The timestamp for the record
    /// * `key` - The record key
    /// * `value` - The record value
    pub fn append_record_with_offset<T: AsRef<[u8]>>(
        &mut self,
        offset: i64,
        timestamp: i64,
        key: T,
        value: T,
    ) {
        self.append_record(Some(offset), Some(timestamp), key, value, None);
    }
    /// Appends a record to the batch with optional parameters.
    ///
    /// # Arguments
    /// * `offset` - Optional offset for the record
    /// * `timestamp` - Optional timestamp for the record
    /// * `key` - The record key
    /// * `value` - The record value
    /// * `headers` - Optional headers for the record
    pub fn append_record<T: AsRef<[u8]>>(
        &mut self,
        offset: Option<i64>,
        timestamp: Option<i64>,
        key: T,
        value: T,
        headers: Option<Vec<RecordHeader>>,
    ) {
        let offset = offset.unwrap_or_else(|| self.next_sequence_offset());
        if self._base_offset == 0 {
            self._base_offset = offset;
        }

        let offset_delta = offset - self._base_offset;
        self._last_offset = offset;

        let timestamp = timestamp.unwrap_or_else(Self::current_millis);
        if self._base_timestamp == 0 {
            self._base_timestamp = timestamp;
        }
        let timestamp_delta = timestamp.saturating_sub(self._base_timestamp);
        self._max_timestamp = timestamp;

        let key = key.as_ref();
        let value = value.as_ref();

        let key_size = Self::calculate_size(key);
        let value_size = Self::calculate_size(value);
        let headers_size = headers
            .as_ref()
            .map_or(0, |hs| hs.iter().map(|h| h.size()).sum::<i32>() as usize);

        let record_size = 1 // attributes
            + timestamp_delta.required_space()
            + offset_delta.required_space()
            + key_size
            + value_size
            + headers_size
            + headers.as_ref().map_or(0.required_space(), |headers| headers.len().required_space());

        self.write_record(
            record_size,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        );
        self._record_count += 1;
    }

    /// Writes a record to the buffer.
    ///
    /// # Arguments
    /// * `record_size` - The size of the record
    /// * `timestamp_delta` - The timestamp delta from the base timestamp
    /// * `offset_delta` - The offset delta from the base offset
    /// * `key` - The record key
    /// * `value` - The record value
    /// * `headers` - Optional headers for the record
    fn write_record<T: AsRef<[u8]>>(
        &mut self,
        record_size: usize,
        timestamp_delta: i64,
        offset_delta: i64,
        key: T,
        value: T,
        headers: Option<Vec<RecordHeader>>,
    ) {
        self.buffer
            .put_slice((record_size as i32).encode_var_vec().as_ref());
        self.buffer.put_i8(0); // attributes
        self.buffer
            .put_slice(timestamp_delta.encode_var_vec().as_ref());
        self.buffer
            .put_slice(offset_delta.encode_var_vec().as_ref());

        Self::append_data(&mut self.buffer, key.as_ref());
        Self::append_data(&mut self.buffer, value.as_ref());

        // Write headers count
        self.buffer.put_slice(
            headers
                .as_ref()
                .map_or(0i32, |headers| headers.len() as i32)
                .encode_var_vec()
                .as_ref(),
        );

        // Write headers
        if let Some(headers) = headers {
            for header in headers {
                self.buffer
                    .put_slice(header.header_key.len().encode_var_vec().as_ref());
                self.buffer.put_slice(header.header_key.as_bytes());

                if let Some(header_value) = header.header_value {
                    self.buffer
                        .put_slice(header_value.len().encode_var_vec().as_ref());
                    self.buffer.put_slice(&header_value);
                } else {
                    self.buffer.put_slice((-1).encode_var_vec().as_ref());
                }
            }
        }
    }

    /// Builds and returns a new RecordBatch.
    ///
    /// # Returns
    /// A new RecordBatch instance containing all appended records
    pub fn build(&mut self) -> RecordBatch {
        let mut cursor = Cursor::new(self.buffer.as_mut());

        // Write batch header fields
        cursor.set_position(0);
        cursor.write_all(&self._base_offset.to_be_bytes()).unwrap();

        cursor.set_position(LENGTH_OFFSET as u64);
        let length = cursor.remaining() as i32 - 4;
        cursor.write_all(&length.to_be_bytes()).unwrap();

        cursor.set_position(LAST_OFFSET_DELTA_OFFSET as u64);
        cursor
            .write_all(&((self._last_offset - self._base_offset) as i32).to_be_bytes())
            .unwrap();

        cursor.set_position(FIRST_TIMESTAMP_OFFSET as u64);
        cursor
            .write_all(&self._base_timestamp.to_be_bytes())
            .unwrap();

        cursor.set_position(MAX_TIMESTAMP_OFFSET as u64);
        cursor
            .write_all(&self._max_timestamp.to_be_bytes())
            .unwrap();

        cursor.set_position(RECORDS_COUNT_OFFSET as u64);
        cursor.write_all(&self._record_count.to_be_bytes()).unwrap();

        // Calculate and write CRC
        cursor.set_position(ATTRIBUTES_OFFSET as u64);
        let crc = crc32c::crc32c(cursor.chunk());
        cursor.set_position(CRC_OFFSET as u64);
        cursor.write_all(&(crc as i32).to_be_bytes()).unwrap();

        RecordBatch::new(self.buffer.split())
    }
    /// Gets the current time in milliseconds.
    ///
    /// # Returns
    /// The current time as an i64
    fn current_millis() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64
    }
    /// Calculates the size of data in VarInt encoding.
    ///
    /// # Arguments
    /// * `data` - The data to calculate size for
    ///
    /// # Returns
    /// The size in bytes
    fn calculate_size(data: &[u8]) -> usize {
        if data.is_empty() {
            (-1).required_space()
        } else {
            data.len().required_space() + data.len()
        }
    }
    /// Appends data to a BytesMut buffer.
    ///
    /// # Arguments
    /// * `buffer` - The buffer to append to
    /// * `data` - The data to append
    fn append_data(buffer: &mut BytesMut, data: &[u8]) {
        if data.is_empty() {
            buffer.put_slice((-1).encode_var_vec().as_ref());
        } else {
            buffer.put_slice((data.len() as i32).encode_var_vec().as_ref());
            buffer.put_slice(data);
        }
    }
    /// Gets the next sequence offset.
    ///
    /// # Returns
    /// The next offset as an i64
    fn next_sequence_offset(&mut self) -> i64 {
        let ret = self._last_offset;
        self._last_offset += 1;
        ret
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_batch_builder() {
        let mut builder = RecordBatchBuilder::default();

        // append first record
        let key1 = "key1";
        let value1 = "value1";
        builder.append_record(None, None, key1, value1, None);

        // append second record with timestamp and offset
        let key2 = "key2";
        let value2 = "value2";
        let offset = 5;
        let timestamp = 1234567890;
        builder.append_record_with_offset(offset, timestamp, key2, value2);

        // append third record with header
        let key3 = "key3";
        let value3 = "value3";
        let headers = vec![RecordHeader {
            header_key: "header1".to_string(),
            header_value: Some(b"header_value1".to_vec()),
        }];
        builder.append_record(None, None, key3, value3, Some(headers));

        // build RecordBatch
        let batch = builder.build();
        let header = batch.header();

        // validate header fields
        assert_eq!(header.magic, MAGIC);
        assert_eq!(header.attributes, ATTRIBUTES);
        assert_eq!(header.records_count, 3);
        assert_eq!(header.first_offset, 0);
        assert!(header.max_timestamp > 0);
    }
}
