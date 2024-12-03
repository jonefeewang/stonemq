use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};

use crate::message::MemoryRecords;
use crate::protocol::base::ProtocolType;
use crate::protocol::base::{
    Bool, NPBytes, NPString, PBytes, PString, PVarInt, PVarLong, I16, I32, I64, I8, U32,
};
use crate::protocol::schema::ValueSet;
use crate::{AppError, AppResult};

///
/// 注意: ArrayType作为类型使用的时候，这里的p_type是arrayOf(schema),
/// 当表示值的时候，这里的p_type是arrayOf(ValueSet)
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ArrayType {
    pub can_be_empty: bool,
    pub p_type: Arc<ProtocolType>,
    pub values: Option<Vec<ProtocolType>>,
}

impl ArrayType {
    pub fn decode(&self, buffer: &mut BytesMut) -> AppResult<ProtocolType> {
        if buffer.remaining() < 4 {
            return Err(AppError::MalformedProtocol(
                "can not read a array, insufficient data".into(),
            ));
        }
        let ary_size = buffer.get_i32();
        let p_type = match &*self.p_type {
            ProtocolType::Schema(schema) => ProtocolType::ValueSet(ValueSet::new(schema.clone())),
            other_type => other_type.clone(),
        };
        if ary_size < 0 && self.can_be_empty {
            let ary = ArrayType {
                can_be_empty: true,
                p_type: Arc::new(p_type),
                values: None,
            };
            return Ok(ProtocolType::Array(ary));
        } else if ary_size < 0 {
            return Err(AppError::MalformedProtocol(format!(
                "array size {} can not be negative",
                ary_size
            )));
        }
        let mut values: Vec<ProtocolType> = Vec::with_capacity(ary_size as usize);
        for _ in 0..ary_size {
            let result = match &*self.p_type {
                ProtocolType::Schema(schema) => {
                    let ret = schema.clone().read_from(buffer)?;
                    ProtocolType::ValueSet(ret)
                }
                ProtocolType::Bool(_) => Bool::decode(buffer)?,
                ProtocolType::I8(_) => I8::decode(buffer)?,
                ProtocolType::I16(_) => I16::decode(buffer)?,
                ProtocolType::I32(_) => I32::decode(buffer)?,
                ProtocolType::U32(_) => U32::decode(buffer)?,
                ProtocolType::I64(_) => I64::decode(buffer)?,
                ProtocolType::PString(_) => PString::decode(buffer)?,
                ProtocolType::NPString(_) => NPString::decode(buffer)?,
                ProtocolType::PBytes(_) => PBytes::decode(buffer)?,
                ProtocolType::NPBytes(_) => NPBytes::decode(buffer)?,
                ProtocolType::PVarInt(_) => PVarInt::decode(buffer)?,
                ProtocolType::PVarLong(_) => PVarLong::decode(buffer)?,
                ProtocolType::Records(_) => MemoryRecords::decode(buffer)?,
                //should never happen
                ProtocolType::Array(_) => {
                    panic!("unexpected array in array");
                }
                //should never happen
                ProtocolType::ValueSet(_) => panic!("unexpected schema data type in array"),
            };
            values.push(result);
        }
        let ary = ArrayType {
            can_be_empty: self.can_be_empty,
            p_type: Arc::new(p_type),
            values: Some(values),
        };
        Ok(ProtocolType::Array(ary))
    }

    pub fn encode(self, writer: &mut BytesMut) {
        match self.values {
            None => {
                writer.put_i32(-1);
            }
            Some(values) => {
                writer.put_i32(values.len() as i32);
                for value in values {
                    match value {
                        ProtocolType::Bool(bool) => bool.encode(writer),
                        ProtocolType::I8(i8) => i8.encode(writer),
                        ProtocolType::I16(i16) => i16.encode(writer),
                        ProtocolType::I32(i32) => i32.encode(writer),
                        ProtocolType::U32(u32) => u32.encode(writer),
                        ProtocolType::I64(i64) => i64.encode(writer),
                        ProtocolType::PString(string) => string.encode(writer),
                        ProtocolType::NPString(npstring) => npstring.encode(writer),
                        ProtocolType::PBytes(bytes) => bytes.encode(writer),
                        ProtocolType::NPBytes(npbytes) => npbytes.encode(writer),
                        ProtocolType::PVarInt(pvarint) => pvarint.encode(writer),
                        ProtocolType::PVarLong(pvarlong) => pvarlong.encode(writer),
                        ProtocolType::Records(records) => records.encode(writer),
                        ProtocolType::ValueSet(value_set) => value_set.write_to(writer),
                        //should never happen
                        ProtocolType::Schema(_) | ProtocolType::Array(_) => {
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
                        ProtocolType::Bool(bool) => bool.wire_format_size(),
                        ProtocolType::I8(i8) => i8.wire_format_size(),
                        ProtocolType::I16(i16) => i16.wire_format_size(),
                        ProtocolType::I32(i32) => i32.wire_format_size(),
                        ProtocolType::U32(u32) => u32.wire_format_size(),
                        ProtocolType::I64(i64) => i64.wire_format_size(),
                        ProtocolType::PString(string) => string.wire_format_size(),
                        ProtocolType::NPString(npstring) => npstring.wire_format_size(),
                        ProtocolType::PBytes(bytes) => bytes.wire_format_size(),
                        ProtocolType::NPBytes(npbytes) => npbytes.wire_format_size(),
                        ProtocolType::PVarInt(pvarint) => pvarint.wire_format_size(),
                        ProtocolType::PVarLong(pvarlong) => pvarlong.wire_format_size(),
                        ProtocolType::Array(array) => array.size(),
                        ProtocolType::Records(records) => records.wire_format_size(),
                        //should never happen
                        ProtocolType::Schema(_) => {
                            //array of schema should not be here
                            panic!("unexpected array of schema");
                        }
                        ProtocolType::ValueSet(valueset) => valueset.size(),
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
            p_type: Arc::new(ProtocolType::I32(I32::default())),
            values: Some(vec![
                ProtocolType::I32(I32 { value: 1 }),
                ProtocolType::I32(I32 { value: 2 }),
                ProtocolType::I32(I32 { value: 3 }),
            ]),
        };
        let array_clone = array.clone();
        array.encode(&mut writer);

        let mut buffer = BytesMut::from(&writer[..]);
        let read_array = array_clone.decode(&mut buffer).unwrap();
        assert_eq!(read_array, ProtocolType::Array(array_clone));
    }
    #[test]
    fn test_array_read_write_empty() {
        use super::*;
        let mut writer = BytesMut::new();
        let array = ArrayType {
            can_be_empty: true,
            p_type: Arc::new(ProtocolType::I32(I32::default())),
            values: None,
        };
        let array_clone = array.clone();
        array.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_array = array_clone.decode(&mut buffer).unwrap();
        assert_eq!(read_array, ProtocolType::Array(array_clone));
    }
}
