use std::fmt::{Debug, Formatter};
use std::i64;
use std::io::Cursor;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use getset::Getters;
use integer_encoding::VarInt;

const BASE_OFFSET_OFFSET: i32 = 0;
const BASE_OFFSET_LENGTH: i32 = 8;
const LENGTH_OFFSET: i32 = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
const LENGTH_LENGTH: i32 = 4;
const PARTITION_LEADER_EPOCH_OFFSET: i32 = LENGTH_OFFSET + LENGTH_LENGTH;
const PARTITION_LEADER_EPOCH_LENGTH: i32 = 4;
const MAGIC_OFFSET: i32 = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
const MAGIC_LENGTH: i32 = 1;
const CRC_OFFSET: i32 = MAGIC_OFFSET + MAGIC_LENGTH;
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

const COMPRESSION_CODEC_MASK: u8 = 0x07;
const TRANSACTIONAL_FLAG_MASK: u8 = 0x10;
const CONTROL_FLAG_MASK: i32 = 0x20;
const TIMESTAMP_TYPE_MASK: u8 = 0x08;

const MAGIC: i8 = 2;
const NO_PRODUCER_ID: i64 = -1;
const NO_PRODUCER_EPOCH: i16 = -1;
const NO_SEQUENCE: i32 = -1;
const NO_PARTITION_LEADER_EPOCH: i32 = -1;
const ATTRIBUTES: i16 = 0;

pub struct MemoryRecordBatchBuilder {
    buffer: BytesMut,
    magic: i8,
    attributes: i16,
    last_offset: i64,
    base_timestamp: i64,
    base_offset: i64,
    max_timestamp: i64,
    record_count: i32,
}

#[derive(Debug)]
pub struct BatchHeader {
    first_offset: i64,
    length: i32,
    partition_leader_epoch: i32,
    magic: i8,
    crc: i32,
    attributes: i16,
    last_offset_delta: i32,
    first_timestamp: i64,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    first_sequence: i32,
    record_count: i32,
    //下边还有一个record count <i32>并未显式放到这里
}
#[derive(Getters)]
pub struct MemoryRecordBatch {
    #[get = "pub"]
    buffer: Bytes,
}

#[derive(Debug)]
pub struct Record {
    length: i32,
    attributes: i8,
    //这个字段比较特殊long型
    timestamp_delta: i64,
    offset_delta: i32,
    key_len: i32,
    key: Option<Vec<u8>>,
    value_len: i32,
    value: Option<Vec<u8>>,
    headers_count: i32,
    headers: Option<Vec<Header>>,
}

#[derive(Debug)]
pub struct Header {
    header_key: String,
    header_value: Option<Vec<u8>>,
}

impl MemoryRecordBatchBuilder {
    /*
    主要用于服务端测试，buffer的创建没有做优化
    */
    pub fn new() -> MemoryRecordBatchBuilder {
        let mut record_batch = MemoryRecordBatchBuilder {
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

    pub fn build(&mut self) -> MemoryRecordBatch {
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
        MemoryRecordBatch {
            buffer: record_batch_buffer.freeze(),
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

impl MemoryRecordBatch {
    pub fn records(&self) -> Option<Vec<Record>> {
        let mut buffer = self.buffer.clone();

        //确保至少能解析出一个record count
        let remaining = buffer.remaining();
        if remaining == 0 || remaining <= RECORDS_COUNT_OFFSET as usize {
            return None;
        }
        let _ = buffer.split_to(RECORDS_COUNT_OFFSET as usize);
        let record_count = buffer.get_i32();
        if record_count > 0 {
            let mut records: Vec<Record> = vec![];
            (0..record_count).for_each(|_| {
                let record_length = i32::decode_var(buffer.as_ref());
                if let Some((record_length, read_size)) = record_length {
                    buffer.advance(read_size); //跳过刚解析过的record长度字段
                    Self::decode_record_body(&mut buffer, &mut records, record_length);
                }
            });
            return Some(records);
        }
        None
    }

    /*
     key_len/value_len如果是空的话，这两个值会写入-1，在解析时需要判断下
     record attributes 一直会被写入为0
    */
    fn decode_record_body<'a>(
        buffer: &mut Bytes,
        mut records: &'a mut Vec<Record>,
        record_length: i32,
    ) -> &'a Vec<Record> {
        let attributes = buffer.get_i8();

        let timestamp_delta = i64::decode_var(buffer.as_ref())
            .map(|(timestamp_delta, read_size)| {
                buffer.advance(read_size);
                timestamp_delta
            })
            .unwrap();

        let offset_delta = i64::decode_var(buffer.as_ref())
            .map(|(offset_delta, read_size)| {
                buffer.advance(read_size);
                offset_delta
            })
            .unwrap();

        let key_len = i32::decode_var(buffer.as_ref())
            .map(|(key_len, read_size)| {
                buffer.advance(read_size);
                key_len
            })
            .unwrap();

        let mut key: Option<Vec<u8>> = None;
        let mut value: Option<Vec<u8>> = None;
        if key_len > 0 {
            key = Some(buffer.split_to(key_len as usize).into());
        }
        let value_len = i32::decode_var(buffer.as_ref())
            .map(|(value_len, read_size)| {
                buffer.advance(read_size);
                value_len
            })
            .unwrap();
        if value_len > 0 {
            value = Some(buffer.split_to(value_len as usize).into());
        }
        let headers_count = i32::decode_var(buffer.as_ref())
            .map(|(header_count, read_size)| {
                buffer.advance(read_size);
                header_count
            })
            .unwrap();

        let mut headers = None;
        if headers_count > 0 {
            //有header
            headers = Some(vec![]);
            (0..headers_count).for_each(|_| {
                let header_key_len = i32::decode_var(buffer.as_ref())
                    .map(|(header_key_len, read_size)| {
                        buffer.advance(read_size);
                        header_key_len
                    })
                    .unwrap();

                let header_key: String =
                    String::from_utf8(buffer.split_off(header_key_len as usize).as_ref().into())
                        .unwrap();
                let value_len = i32::decode_var(buffer.as_ref())
                    .map(|(value_len, read_size)| {
                        buffer.advance(read_size);
                        value_len
                    })
                    .unwrap();
                let mut header_value: Option<Vec<u8>> = None;
                if value_len > 0 {
                    header_value = Some(buffer.split_off(value_len as usize).into());
                }
                headers.as_mut().unwrap().push(Header {
                    header_key,
                    header_value,
                })
            });
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

    pub fn batch_header(&self) -> BatchHeader {
        let mut buffer = self.buffer().clone();
        BatchHeader {
            first_offset: buffer.get_i64(),
            length: buffer.get_i32(),
            partition_leader_epoch: buffer.get_i32(),
            magic: buffer.get_i8(),
            crc: buffer.get_i32(),
            attributes: buffer.get_i16(),
            last_offset_delta: buffer.get_i32(),
            first_timestamp: buffer.get_i64(),
            max_timestamp: buffer.get_i64(),
            producer_id: buffer.get_i64(),
            producer_epoch: buffer.get_i16(),
            first_sequence: buffer.get_i32(),
            record_count: buffer.get_i32(),
        }
    }
}
impl Debug for MemoryRecordBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut buffer = self.buffer.clone();
        f.debug_struct("MemoryRecordBatch")
            .field("first offset", &buffer.get_i64())
            .field("length", &buffer.get_i64())
            .field("partition leader epoch", &buffer.get_i64())
            .field("Magic", &buffer.get_i64())
            .field("CRC", &buffer.get_i64())
            .field("Attributes", &buffer.get_i64())
            .field("Last Offset Delta", &buffer.get_i64())
            .field("First timestamp", &buffer.get_i64())
            .field("Max Timestamp", &buffer.get_i64())
            .field("Producer Id", &buffer.get_i64())
            .field("Producer Epoch", &buffer.get_i64())
            .field("First sequence", &buffer.get_i64())
            .finish()
    }
}
