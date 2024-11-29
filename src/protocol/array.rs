use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};

use crate::message::MemoryRecords;
use crate::protocol::primary_types::{
    Bool, NPBytes, NPString, PBytes, PString, PVarInt, PVarLong, PrimaryType, I16, I32, I64, I8,
    U32,
};
use crate::protocol::types::DataType;
use crate::protocol::value_set::ValueSet;

///
/// 注意: ArrayType作为类型使用的时候，这里的p_type是arrayOf(schema),
/// 当表示值的时候，这里的p_type是arrayOf(ValueSet)
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArrayType {
    pub can_be_empty: bool,
    pub p_type: Arc<DataType>,
    pub values: Option<Vec<DataType>>,
}

impl ArrayType {
    pub fn decode(&self, buffer: &mut BytesMut) -> DataType {
        let ary_size = buffer.get_i32();
        // trace!("array size: {}", ary_size);
        let p_type = match &*self.p_type {
            DataType::Schema(schema) => DataType::ValueSet(ValueSet::new(schema.clone())),
            other_type => other_type.clone(),
        };
        if ary_size < 0 && self.can_be_empty {
            let ary = ArrayType {
                can_be_empty: true,
                p_type: Arc::new(p_type),
                values: None,
            };
            return DataType::Array(ary);
        } else if ary_size < 0 {
            panic!("array size {} can not be negative", ary_size);
        }
        let mut values: Vec<DataType> = Vec::with_capacity(ary_size as usize);
        for _ in 0..ary_size {
            let result = match &*self.p_type {
                DataType::Schema(schema) => schema.clone().read_from(buffer).into(),
                DataType::Bool(_) => Bool::decode(buffer),
                DataType::I8(_) => I8::decode(buffer),
                DataType::I16(_) => I16::decode(buffer),
                DataType::I32(_) => I32::decode(buffer),
                DataType::U32(_) => U32::decode(buffer),
                DataType::I64(_) => I64::decode(buffer),
                DataType::PString(_) => PString::decode(buffer),
                DataType::NPString(_) => NPString::decode(buffer),
                DataType::PBytes(_) => PBytes::decode(buffer),
                DataType::NPBytes(_) => NPBytes::decode(buffer),
                DataType::PVarInt(_) => PVarInt::decode(buffer),
                DataType::PVarLong(_) => PVarLong::decode(buffer),
                DataType::Records(_) => MemoryRecords::decode(buffer),
                //should never happen
                DataType::Array(_) => {
                    panic!("unexpected array in array");
                }
                //should never happen
                DataType::ValueSet(_) => panic!("unexpected schema data type in array"),
            };
            values.push(result);
        }
        let ary = ArrayType {
            can_be_empty: self.can_be_empty,
            p_type: Arc::new(p_type),
            values: Some(values),
        };
        DataType::Array(ary)
    }

    ///
    /// 将ArrayType写入到缓冲区, 消耗掉自己，之后不能再使用
    pub fn encode(&self, writer: &mut BytesMut) {
        match &self.values {
            None => {
                writer.put_i32(-1);
            }
            Some(values) => {
                writer.put_i32(values.len() as i32);
                for value in values {
                    match value {
                        DataType::Bool(bool) => bool.encode(writer),
                        DataType::I8(i8) => i8.encode(writer),
                        DataType::I16(i16) => i16.encode(writer),
                        DataType::I32(i32) => i32.encode(writer),
                        DataType::U32(u32) => u32.encode(writer),
                        DataType::I64(i64) => i64.encode(writer),
                        DataType::PString(string) => string.encode(writer),
                        DataType::NPString(npstring) => npstring.encode(writer),
                        DataType::PBytes(bytes) => bytes.encode(writer),
                        DataType::NPBytes(npbytes) => npbytes.encode(writer),
                        DataType::PVarInt(pvarint) => pvarint.encode(writer),
                        DataType::PVarLong(pvarlong) => pvarlong.encode(writer),
                        DataType::Records(records) => records.encode(writer),
                        DataType::ValueSet(value_set) => value_set.write_to(writer),
                        //should never happen
                        DataType::Schema(_) | DataType::Array(_) => {
                            panic!("unexpected array of schema");
                        }
                    }
                }
            }
        }
    }

    pub fn size(&self) -> usize {
        let mut total_size = 4;
        match &self.values {
            None => return total_size,
            Some(values) => {
                for value in values {
                    total_size += match value {
                        DataType::Bool(bool) => bool.wire_format_size(),
                        DataType::I8(i8) => i8.wire_format_size(),
                        DataType::I16(i16) => i16.wire_format_size(),
                        DataType::I32(i32) => i32.wire_format_size(),
                        DataType::U32(u32) => u32.wire_format_size(),
                        DataType::I64(i64) => i64.wire_format_size(),
                        DataType::PString(string) => string.wire_format_size(),
                        DataType::NPString(npstring) => npstring.wire_format_size(),
                        DataType::PBytes(bytes) => bytes.wire_format_size(),
                        DataType::NPBytes(npbytes) => npbytes.wire_format_size(),
                        DataType::PVarInt(pvarint) => pvarint.wire_format_size(),
                        DataType::PVarLong(pvarlong) => pvarlong.wire_format_size(),
                        DataType::Array(array) => array.size(),
                        DataType::Records(records) => records.wire_format_size(),
                        //should never happen
                        DataType::Schema(_) => {
                            //array of schema should not be here
                            panic!("unexpected array of schema");
                        }
                        DataType::ValueSet(valueset) => valueset.size(),
                    };
                }
            }
        }
        total_size
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_array_read_write() {
        use super::*;
        let mut writer = BytesMut::new();
        let array = ArrayType {
            can_be_empty: false,
            p_type: Arc::new(DataType::I32(I32::default())),
            values: Some(vec![
                DataType::I32(I32 { value: 1 }),
                DataType::I32(I32 { value: 2 }),
                DataType::I32(I32 { value: 3 }),
            ]),
        };
        let array_clone = array.clone();
        array.encode(&mut writer);

        let mut buffer = BytesMut::from(&writer[..]);
        let read_array = array_clone.decode(&mut buffer);
        assert_eq!(read_array, DataType::Array(array_clone));
    }
    #[test]
    fn test_array_read_write_empty() {
        use super::*;
        let mut writer = BytesMut::new();
        let array = ArrayType {
            can_be_empty: true,
            p_type: Arc::new(DataType::I32(I32::default())),
            values: None,
        };
        let array_clone = array.clone();
        array.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_array = array_clone.decode(&mut buffer);
        assert_eq!(read_array, DataType::Array(array_clone));
    }
}
