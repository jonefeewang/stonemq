use bytes::{Buf, BufMut, BytesMut};
use integer_encoding::VarInt;
use std::io::{Cursor, Write};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::error;

use crate::message::batch_header::BatchHeader;
use crate::message::constants::*;
use crate::message::record::{Record, RecordHeader};
use crate::{global_config, AppError, AppResult};

use super::MemoryRecords;

pub struct RecordBatch {
    pub(crate) buffer: BytesMut,
}

impl RecordBatch {
    pub fn new(buffer: BytesMut) -> Self {
        RecordBatch { buffer }
    }

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
    // 将批次的 buffer 合并回原始 MemoryRecords
    pub fn unsplit(self, records: &mut MemoryRecords) {
        if let Some(main_buffer) = &mut records.buffer {
            main_buffer.unsplit(self.buffer);
        }
    }
    pub fn records_count(&self) -> i32 {
        self.get_field(RECORDS_COUNT_OFFSET, |c| c.get_i32())
    }

    pub fn max_timestamp(&self) -> i64 {
        self.get_field(MAX_TIMESTAMP_OFFSET, |c| c.get_i64())
    }
    pub fn base_timestamp(&self) -> i64 {
        self.get_field(FIRST_TIMESTAMP_OFFSET, |c| c.get_i64())
    }

    pub fn base_offset(&self) -> i64 {
        self.get_field(BASE_OFFSET_OFFSET, |c| c.get_i64())
    }
    pub fn last_offset_delta(&self) -> i32 {
        self.get_field(LAST_OFFSET_DELTA_OFFSET, |c| c.get_i32())
    }

    fn get_field<T>(&self, offset: i32, getter: impl Fn(&mut Cursor<&[u8]>) -> T) -> T {
        let mut cursor = Cursor::new(self.buffer.as_ref());
        cursor.set_position(offset as u64);
        getter(&mut cursor)
    }

    pub fn set_base_offset(&mut self, base_offset: i64) {
        let mut cursor = Cursor::new(self.buffer.as_mut());
        cursor.set_position(BASE_OFFSET_OFFSET as u64);
        cursor.write_all(&base_offset.to_be_bytes()).unwrap();
    }
    pub fn set_first_timestamp(&mut self, first_timestamp: i64) -> AppResult<()> {
        let mut cursor = Cursor::new(self.buffer.as_mut());
        cursor.set_position(FIRST_TIMESTAMP_OFFSET as u64);
        cursor.write_all(&first_timestamp.to_be_bytes())?;
        Ok(())
    }

    /// 验证 RecordBatch 的有效性
    ///
    /// 验证以下内容:
    /// - buffer 不能为空且长度必须大于 LOG_OVERHEAD
    /// - base_offset 必须为 0
    /// - batch_size 必须在合法范围内(小于 max_msg_size,大于 RECORD_BATCH_OVERHEAD)
    /// - magic 必须为 2 (当前仅支持 magic 2)
    /// - CRC 校验必须通过
    /// - record_count 必须为非负数
    ///
    /// # 返回
    /// - Ok(()) 如果验证通过
    /// - Err(AppError::RequestError) 如果验证失败,错误信息包含具体原因
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
        if batch_size < RECORD_BATCH_OVERHEAD {
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

impl RecordBatchBuilder {
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

    pub fn append_record_with_offset<T: AsRef<[u8]>>(
        &mut self,
        offset: i64,
        timestamp: i64,
        key: T,
        value: T,
    ) {
        self.append_record(Some(offset), Some(timestamp), key, value, None);
    }

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

    fn current_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64
    }

    fn calculate_size(data: &[u8]) -> usize {
        if data.is_empty() {
            (-1).required_space()
        } else {
            data.len().required_space() + data.len()
        }
    }

    fn append_data(buffer: &mut BytesMut, data: &[u8]) {
        if data.is_empty() {
            buffer.put_slice((-1).encode_var_vec().as_ref());
        } else {
            buffer.put_slice((data.len() as i32).encode_var_vec().as_ref());
            buffer.put_slice(data);
        }
    }

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

        // 添加第一条记录
        let key1 = "key1";
        let value1 = "value1";
        builder.append_record(None, None, key1, value1, None);

        // 添加第二条记录，带有时间戳和偏移量
        let key2 = "key2";
        let value2 = "value2";
        let offset = 5;
        let timestamp = 1234567890;
        builder.append_record_with_offset(offset, timestamp, key2, value2);

        // 添加第三条记录，带有header
        let key3 = "key3";
        let value3 = "value3";
        let headers = vec![RecordHeader {
            header_key: "header1".to_string(),
            header_value: Some(b"header_value1".to_vec()),
        }];
        builder.append_record(None, None, key3, value3, Some(headers));

        // 构建RecordBatch
        let batch = builder.build();
        let header = batch.header();

        // 验证header字段
        assert_eq!(header.magic, MAGIC);
        assert_eq!(header.attributes, ATTRIBUTES);
        assert_eq!(header.records_count, 3);
        assert_eq!(header.first_offset, 0);
        assert!(header.max_timestamp > 0);
    }
}
