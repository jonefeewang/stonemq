use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
use std::i64;
use std::io::Cursor;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, BytesMut};
use chrono::{DateTime, Local, TimeZone};
use integer_encoding::VarInt;

use crate::AppError::RequestError;
use crate::{global_config, AppResult};

// constants related to records
const OFFSET_OFFSET: usize = 0;
const OFFSET_LENGTH: usize = 8;
const SIZE_OFFSET: usize = OFFSET_OFFSET + OFFSET_LENGTH;
const SIZE_LENGTH: usize = 4;
const LOG_OVERHEAD: usize = SIZE_OFFSET + SIZE_LENGTH;

const MAGIC_OFFSET: usize = 16;
const MAGIC_LENGTH: usize = 1;
const HEADER_SIZE_UP_TO_MAGIC: usize = MAGIC_OFFSET + MAGIC_LENGTH;

// constants related to record batches
const BASE_OFFSET_OFFSET: i32 = 0;
const BASE_OFFSET_LENGTH: i32 = 8;
const LENGTH_OFFSET: i32 = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
const LENGTH_LENGTH: i32 = 4;
const PARTITION_LEADER_EPOCH_OFFSET: i32 = LENGTH_OFFSET + LENGTH_LENGTH;
const PARTITION_LEADER_EPOCH_LENGTH: i32 = 4;
const RB_MAGIC_OFFSET: i32 = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
const RB_MAGIC_LENGTH: i32 = 1;
const CRC_OFFSET: i32 = RB_MAGIC_OFFSET + RB_MAGIC_LENGTH;
const CRC_LENGTH: i32 = 4;
const ATTRIBUTES_OFFSET: i32 = CRC_OFFSET + CRC_LENGTH;
const ATTRIBUTE_LENGTH: i32 = 2;
const LAST_OFFSET_DELTA_OFFSET: i32 = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
const LAST_OFFSET_DELTA_LENGTH: i32 = 4;
const FIRST_TIMESTAMP_OFFSET: i32 = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
const FIRST_TIMESTAMP_LENGTH: i32 = 8;
const MAX_TIMESTAMP_OFFSET: i32 = FIRST_TIMESTAMP_OFFSET + FIRST_TIMESTAMP_LENGTH;
const MAX_TIMESTAMP_LENGTH: i32 = 8;
const PRODUCER_ID_OFFSET: i32 = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
const PRODUCER_ID_LENGTH: i32 = 8;
const PRODUCER_EPOCH_OFFSET: i32 = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
const PRODUCER_EPOCH_LENGTH: i32 = 2;
const BASE_SEQUENCE_OFFSET: i32 = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
const BASE_SEQUENCE_LENGTH: i32 = 4;
const RECORDS_COUNT_OFFSET: i32 = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
const RECORDS_COUNT_LENGTH: i32 = 4;
const RECORDS_OFFSET: i32 = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
pub const RECORD_BATCH_OVERHEAD: i32 = RECORDS_OFFSET;

const COMPRESSION_CODEC_MASK: i16 = 0x07;
const TRANSACTIONAL_FLAG_MASK: i16 = 0x10;
const CONTROL_FLAG_MASK: i32 = 0x20;
const TIMESTAMP_TYPE_MASK: i16 = 0x08;

const MAGIC: i8 = 2;
const NO_PRODUCER_ID: i64 = -1;
const NO_PRODUCER_EPOCH: i16 = -1;
const NO_SEQUENCE: i32 = -1;
const NO_PARTITION_LEADER_EPOCH: i32 = -1;
const ATTRIBUTES: i16 = 0;

#[derive(Debug)]
pub enum CompressionType {
    None(u8),
    Gzip(u8),
    Snappy(u8),
    Lz4(u8),
    Invalid(u8),
}
impl From<i16> for CompressionType {
    fn from(value: i16) -> Self {
        match value {
            0 => CompressionType::None(0),
            1 => CompressionType::Gzip(1),
            2 => CompressionType::Snappy(2),
            3 => CompressionType::Lz4(3),
            _ => CompressionType::Invalid(0),
        }
    }
}

/// A memory representation of a record batch.
/// This is used to store the record batch in memory before writing it to disk.
/// It is also used to read the record batch from disk into memory.
/// This structure can store message sets or record batches with magic values 0, 1, or 2.
/// However, currently, StoneMQ only supports record batches with a magic value of 2.
/// Therefore, this structure presently supports only record batches with a magic value of 2.
#[derive(Clone, PartialEq, Eq)]
pub struct MemoryRecords {
    pub buffer: Option<BytesMut>,
}
impl Debug for MemoryRecords {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
        MemoryRecords { buffer: None }
    }
    pub fn records_count(&self) -> i32 {
        self.get_field(RECORDS_COUNT_OFFSET, |c| c.get_i32(), 0)
    }

    pub fn max_timestamp(&self) -> i64 {
        self.get_field(MAX_TIMESTAMP_OFFSET, |c| c.get_i64(), 0)
    }

    pub fn base_offset(&self) -> i64 {
        self.get_field(BASE_OFFSET_OFFSET, |c| c.get_i64(), 0)
    }
    pub fn last_offset_delta(&self) -> i32 {
        self.get_field(LAST_OFFSET_DELTA_OFFSET, |c| c.get_i32(), 0)
    }

    fn get_field<T>(&self, offset: i32, getter: impl Fn(&mut Cursor<&[u8]>) -> T, default: T) -> T {
        if let Some(ref buffer) = &self.buffer {
            let mut cursor = Cursor::new(buffer.as_ref());
            cursor.set_position(offset as u64);
            getter(&mut cursor)
        } else {
            default
        }
    }
    pub fn set_base_offset(&mut self, base_offset: i64) -> AppResult<()> {
        if let Some(ref mut buffer) = &mut self.buffer {
            let mut cursor = Cursor::new(buffer.as_mut());
            // cursor.set_position(LAST_OFFSET_DELTA_OFFSET as u64);
            // let last_offset_delta = cursor.get_i32();
            // let base_offset = base_offset - 1 - last_offset_delta as i64;
            cursor.set_position(BASE_OFFSET_OFFSET as u64);
            cursor.write_all(&base_offset.to_be_bytes())?;
        }
        Ok(())
    }

    // deserialize and validate the record batches
    // currently only support magic 2, other magic will be treated as invalid and return error
    // magic 2 has only one record batch
    pub fn validate_batch(&self) -> AppResult<()> {
        if let Some(buffer) = &self.buffer {
            let mut cursor = Cursor::new(buffer.as_ref());

            let remaining = cursor.remaining();
            if remaining == 0 || remaining < LOG_OVERHEAD {
                return Err(RequestError(Cow::Borrowed("MemoryRecord is empty")));
            }

            // deserialize batch header
            let base_offset = cursor.get_i64();
            let batch_size = cursor.get_i32();
            let magic = cursor.get_i8();

            let max_msg_size = global_config().general.max_msg_size;

            if base_offset != 0 {
                return Err(RequestError(Cow::Owned(format!(
                    "Base offset should be 0, but found {}",
                    base_offset
                ))));
            }

            if batch_size > max_msg_size {
                return Err(RequestError(Cow::Owned(format!(
                    "Message size {} exceeds the maximum message size {}",
                    batch_size, max_msg_size
                ))));
            }
            if batch_size < RECORD_BATCH_OVERHEAD {
                return Err(RequestError(Cow::Owned(format!(
                    "Message size {} is less than the record batch overhead {}",
                    batch_size, RECORD_BATCH_OVERHEAD
                ))));
            }

            if !(0..=2).contains(&magic) {
                return Err(RequestError(Cow::Owned(format!(
                    "Magic byte should be 0, 1 or 2, but found {}",
                    magic
                ))));
            }
            // currently only support with magic 2
            if (0..=1).contains(&magic) {
                return Err(RequestError(Cow::Owned(format!(
                    "StoneMQ currently only support Magic 2, but found {}",
                    magic
                ))));
            }

            if (remaining as i32) < batch_size {
                return Err(RequestError(Cow::Owned(format!(
                    "Expected {}, but only {} remaining buffer size available.",
                    batch_size, remaining
                ))));
            }

            let crc = cursor.get_i32();
            // validate crc
            cursor.set_position(ATTRIBUTES_OFFSET as u64);
            let crc_parts = &cursor.get_ref()[..];
            let crc_value = crc32c::crc32c(crc_parts);
            if crc_value as i32 != crc {
                return Err(RequestError(Cow::Owned(format!(
                    "CRC mismatch: expected {}, but found {}",
                    crc_value, crc
                ))));
            }
            //validate record count
            cursor.set_position(RECORDS_COUNT_OFFSET as u64);
            let record_count = cursor.get_i32();

            if record_count < 0 {
                return Err(RequestError(Cow::Owned(format!(
                    "Record count should be non-negative, but found {}",
                    record_count
                ))));
            }
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.buffer.as_ref().map(|buf| buf.len()).unwrap_or(0)
    }

    // pub fn compression_type(&self) -> AppResult<CompressionType> {
    //     let mut buffer = self.records_buf.clone();
    //     buffer.advance(ATTRIBUTES_OFFSET as usize);
    //     let attributes = buffer.get_i16();
    //     let compression_codec = attributes & COMPRESSION_CODEC_MASK;
    //     let compression_type = CompressionType::from(compression_codec);
    //     if let CompressionType::Invalid(_) = compression_type {
    //         return Err(RequestError(Cow::Owned(format!(
    //             "Invalid compression codec: {}",
    //             compression_codec
    //         ))));
    //     }
    //     Ok(compression_type)
    // }
    //
    // pub fn deserialize_and_validate(
    //     &mut self,
    //     compression_buffer: &mut Vec<u8>,
    // ) -> AppResult<Vec<Record>> {
    //     // Since we process only one request at a time, there's no contention over buffer usage.
    //     // We clean it up before use.
    //     compression_buffer.clear();
    //     let compression_type = self.compression_type()?;
    //     let records = (0..self.header.record_count)
    //         .map(|i| match compression_type {
    //             CompressionType::None(_) => {
    //                 Self::read_from_uncompressed(&mut self.records_buf, &self.header)
    //             }
    //             CompressionType::Gzip(_) => {
    //                 let mut reader = GzDecoder::new(self.records_buf.as_ref());
    //                 reader.read_to_end(compression_buffer)?;
    //                 Self::read_from_uncompressed(compression_buffer.as_ref(), &self.header)
    //             }
    //             CompressionType::Snappy(_) => {
    //                 let mut reader = FrameDecoder::new(self.records_buf.as_ref());
    //                 reader.read_to_end(compression_buffer)?;
    //                 Self::read_from_uncompressed(compression_buffer.as_ref(), &self.header)
    //             }
    //             CompressionType::Lz4(_) => {
    //                 let mut reader = Decoder::new(self.records_buf.as_ref())?;
    //                 reader.read_to_end(compression_buffer)?;
    //                 Self::read_from_uncompressed(compression_buffer.as_ref(), &self.header)
    //             }
    //             _ => Err(RequestError(Cow::Owned(format!(
    //                 "Unsupported compression type: {:?}",
    //                 compression_type
    //             )))),
    //         })
    //         .collect::<AppResult<Vec<_>>>()?;
    //     if records.len() != self.header.record_count as usize {
    //         return Err(RequestError(Cow::Owned(format!(
    //             "Expected {} records, but found {}",
    //             self.header.record_count,
    //             records.len()
    //         ))));
    //     }
    //     Ok(records)
    // }
    // pub fn validate_records(&self) -> AppResult<()> {
    //     Ok(())
    // }
}

#[derive(Debug)]
pub struct Record {
    pub length: i32,
    pub attributes: i8,
    //这个字段比较特殊long型
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key_len: i32,
    pub key: Option<Vec<u8>>,
    pub value_len: i32,
    pub value: Option<Vec<u8>>,
    pub headers_count: i32,
    pub headers: Option<Vec<Header>>,
}

#[derive(Debug)]
pub struct Header {
    pub header_key: String,
    pub header_value: Option<Vec<u8>>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BatchHeader {
    pub first_offset: i64,
    pub length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub first_sequence: i32,
    //下边还有一个record count <i32>并未显式放到这里
}
impl Display for BatchHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let chrono_first_timestamp = Local.timestamp_millis_opt(self.first_timestamp).unwrap();
        let chrono_max_timestamp = Local.timestamp_millis_opt(self.max_timestamp).unwrap();
        f.debug_struct("BatchHeader")
            .field("first_offset", &self.first_offset)
            .field("length", &self.length)
            .field("partition_leader_epoch", &self.partition_leader_epoch)
            .field("magic", &self.magic)
            .field("crc", &self.crc)
            .field("attributes", &self.attributes)
            .field("last_offset_delta", &self.last_offset_delta)
            .field("first_timestamp", &chrono_first_timestamp)
            .field("max_timestamp", &chrono_max_timestamp)
            .field("producer_id", &self.producer_id)
            .field("producer_epoch", &self.producer_epoch)
            .field("first_sequence", &self.first_sequence)
            .finish()
    }
}

pub struct MemoryRecordBuilder {
    buffer: BytesMut,
    magic: i8,
    attributes: i16,
    last_offset: i64,
    base_timestamp: i64,
    base_offset: i64,
    max_timestamp: i64,
    record_count: i32,
}

impl Default for MemoryRecordBuilder {
    fn default() -> Self {
        let mut record_batch = MemoryRecordBuilder {
            buffer: BytesMut::with_capacity(RECORD_BATCH_OVERHEAD as usize),
            magic: MAGIC,
            attributes: 0,
            last_offset: 0,
            base_timestamp: 0,
            base_offset: 0,
            max_timestamp: 0,
            record_count: 0,
        };
        record_batch.buffer.put_i64(0); //first offset
        record_batch.buffer.put_i32(0); //length 预留空间
        record_batch.buffer.put_i32(NO_PARTITION_LEADER_EPOCH); //partition leader epoch
        record_batch.buffer.put_i8(MAGIC); //magic
        record_batch.buffer.put_i32(-1); //crc
        record_batch.buffer.put_i16(ATTRIBUTES); //attributes
        record_batch.buffer.put_i32(-1); //last offset delta
        record_batch.buffer.put_i64(-1); //first time stamp
        record_batch.buffer.put_i64(-1); //max timestamp
        record_batch.buffer.put_i64(NO_PRODUCER_ID); //producer id
        record_batch.buffer.put_i16(NO_PRODUCER_EPOCH); //producer epoch
        record_batch.buffer.put_i32(NO_SEQUENCE); //first sequence
        record_batch.buffer.put_i32(0); //record count
        record_batch
    }
}

impl MemoryRecordBuilder {
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
        headers: Option<Vec<Header>>,
    ) {
        let _initial_size = self.buffer.remaining();
        let offset = offset.unwrap_or_else(|| self.next_sequence_offset());
        if self.base_offset == 0 {
            self.base_offset = offset;
        }

        let offset_delta = offset - self.base_offset;
        self.last_offset = offset;

        let timestamp = timestamp.unwrap_or_else(|| Self::current_millis());
        if self.base_timestamp == 0 {
            self.base_timestamp = timestamp;
        }
        let timestamp_delta = timestamp.saturating_sub(self.base_timestamp);
        self.max_timestamp = timestamp;

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

        self.buffer
            .put_slice((record_size as i32).encode_var_vec().as_ref()); // length
        self.buffer.put_bytes(0, 1); // attributes
        self.buffer
            .put_slice(timestamp_delta.encode_var_vec().as_ref()); // timestamp delta
        self.buffer
            .put_slice(offset_delta.encode_var_vec().as_ref()); // offset delta

        Self::append_data(&mut self.buffer, key);
        Self::append_data(&mut self.buffer, value);

        println!("offset:{}", offset_delta);

        // header length
        self.buffer.put_slice(
            headers
                .as_ref()
                .map_or(0i32, |headers| headers.len() as i32)
                .encode_var_vec()
                .as_ref(),
        );

        // headers
        if let Some(hs) = headers {
            for header in hs {
                self.buffer
                    .put_slice(header.header_key.len().encode_var_vec().as_ref());
                self.buffer.put_slice(&header.header_key.as_bytes());

                if let Some(ref header_value) = header.header_value {
                    self.buffer
                        .put_slice(header_value.len().encode_var_vec().as_ref());
                    self.buffer.put_slice(header_value.as_ref());
                } else {
                    self.buffer.put_i32(-1);
                }
            }
        }

        self.record_count += 1;
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

    pub fn build(&mut self) -> MemoryRecords {
        //补写record batch的头部
        let mut src = Cursor::new(self.buffer.as_mut());
        src.set_position(0);
        src.write_all(&self.base_offset.to_be_bytes()).unwrap(); //base offset
        src.set_position(LENGTH_OFFSET as u64);
        let length = src.remaining() as i32 - 4; //减去length自身长度
        src.write_all(&length.to_be_bytes()).unwrap(); //length
        src.set_position(LAST_OFFSET_DELTA_OFFSET as u64); //last offset delta
        src.write_all(&((self.last_offset - self.base_offset) as i32).to_be_bytes())
            .unwrap();
        src.set_position(FIRST_TIMESTAMP_OFFSET as u64);
        src.write_all(&self.base_timestamp.to_be_bytes()).unwrap(); //first timestamp
        src.set_position(MAX_TIMESTAMP_OFFSET as u64);
        src.write_all(&self.max_timestamp.to_be_bytes()).unwrap(); //max timestamp
        src.set_position(RECORDS_COUNT_OFFSET as u64);
        src.write_all(&self.record_count.to_be_bytes()).unwrap(); //record count

        //最后填充crc字段
        src.set_position(ATTRIBUTES_OFFSET as u64);
        let crc_parts = src.chunk();
        println!("crc parts:");
        crc_parts.iter().enumerate().for_each(|(index, byte)| {
            print!("{:02X} ", byte);
            if crc_parts.len() == index + 1 {
                println!();
            }
        });
        let crc_value = crc32c::crc32c(crc_parts);
        let checksum = crc_value as i32;
        println!("crc:{},{}", checksum, crc_value);
        src.set_position(CRC_OFFSET as u64);
        src.write_all(&checksum.to_be_bytes()).unwrap(); //crc

        let record_batch_buffer = self.buffer.split();
        MemoryRecords {
            buffer: Some(record_batch_buffer),
        }
    }

    fn next_sequence_offset(&mut self) -> i64 {
        let ret = self.last_offset;
        self.last_offset += 1;
        ret
    }
}

impl Header {
    pub fn new<T: AsRef<[u8]>>(key: String, value: T) -> Header {
        Header {
            header_key: key,
            header_value: {
                if value.as_ref().is_empty() {
                    None
                } else {
                    Some(value.as_ref().into())
                }
            },
        }
    }
    pub fn size(&self) -> i32 {
        let mut size = self.header_key.len().required_space() + self.header_key.len();
        if let Some(ref header_value) = self.header_value {
            size = size + header_value.len().required_space() + header_value.len()
        }
        size as i32
    }
}

impl MemoryRecords {
    pub fn records(&self) -> Option<Vec<Record>> {
        if let Some(ref buffer) = self.buffer {
            let mut cursor = Cursor::new(buffer.as_ref());
            let remaining = cursor.remaining();
            if remaining == 0 || remaining <= RECORDS_COUNT_OFFSET as usize {
                return None;
            }
            cursor.advance(RECORDS_COUNT_OFFSET as usize);
            let record_count = cursor.get_i32();
            if record_count > 0 {
                let mut records: Vec<Record> = Vec::with_capacity(record_count as usize);
                for _ in 0..record_count {
                    if let Some(record_length) = i32::decode_var(cursor.chunk()) {
                        cursor.advance(record_length.1);
                        Self::decode_record_body(&mut cursor, &mut records, record_length.0);
                    }
                }
                return Some(records);
            }
        }

        None
    }

    /*
     key_len/value_len如果是空的话，这两个值会写入-1，在解析时需要判断下
     record attributes 一直会被写入为0
    */
    fn decode_record_body<'a>(
        cursor: &mut Cursor<&[u8]>,
        records: &'a mut Vec<Record>,
        record_length: i32,
    ) -> &'a Vec<Record> {
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
                headers.as_mut().unwrap().push(Header {
                    header_key,
                    header_value,
                })
            }
        }
        records.push(Record {
            length: record_length,
            attributes,
            timestamp_delta,
            offset_delta: (offset_delta as i32),
            key_len,
            key,
            value_len,
            value,
            headers,
            headers_count,
        });

        records
    }

    pub fn batch_header(&self) -> Option<BatchHeader> {
        if let Some(buffer) = &self.buffer {
            let mut cursor = Cursor::new(buffer.as_ref());
            let batch_header = BatchHeader {
                first_offset: cursor.get_i64(),
                length: cursor.get_i32(),
                partition_leader_epoch: cursor.get_i32(),
                magic: cursor.get_i8(),
                crc: cursor.get_i32(),
                attributes: cursor.get_i16(),
                last_offset_delta: cursor.get_i32(),
                first_timestamp: cursor.get_i64(),
                max_timestamp: cursor.get_i64(),
                producer_id: cursor.get_i64(),
                producer_epoch: cursor.get_i16(),
                first_sequence: cursor.get_i32(),
            };
            Some(batch_header)
        } else {
            None
        }
    }
}
