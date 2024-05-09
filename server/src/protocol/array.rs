use std::borrow::Cow;
use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};

use crate::AppError::NetworkReadError;
use crate::AppResult;
use crate::message::MemoryRecords;
use crate::protocol::primary_types::{
    Bool, I16, I32, I64, I8, NPBytes, NPString, PBytes, PrimaryType, PString, PVarInt, PVarLong,
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
    pub fn read_from(&self, buffer: &mut BytesMut) -> AppResult<DataType> {
        let ary_size = buffer.get_i32();
        let p_type = match &*self.p_type {
            DataType::Schema(schema) => DataType::SchemaValues(ValueSet::new(schema.clone())),
            other_type => other_type.clone(),
        };
        if ary_size < 0 && self.can_be_empty {
            let ary = ArrayType {
                can_be_empty: true,
                p_type: Arc::new(p_type),
                values: None,
            };
            return Ok(DataType::Array(ary));
        } else if ary_size < 0 {
            return Err(NetworkReadError(Cow::Owned(format!(
                "array size {} can not be negative",
                ary_size
            ))));
        }
        let mut values: Vec<DataType> = Vec::with_capacity(ary_size as usize);
        for _ in 0..ary_size {
            let result = match &*self.p_type {
                DataType::Schema(schema) => schema
                    .clone()
                    .read_from(buffer)
                    .map(|value_set: ValueSet| value_set.into()),
                DataType::Bool(_) => Bool::read_from(buffer),
                DataType::I8(_) => I8::read_from(buffer),
                DataType::I16(_) => I16::read_from(buffer),
                DataType::I32(_) => I32::read_from(buffer),
                DataType::U32(_) => U32::read_from(buffer),
                DataType::I64(_) => I64::read_from(buffer),
                DataType::PString(_) => PString::read_from(buffer),
                DataType::NPString(_) => NPString::read_from(buffer),
                DataType::PBytes(_) => PBytes::read_from(buffer),
                DataType::NPBytes(_) => NPBytes::read_from(buffer),
                DataType::PVarInt(_) => PVarInt::read_from(buffer),
                DataType::PVarLong(_) => PVarLong::read_from(buffer),
                DataType::Records(_) => MemoryRecords::read_from(buffer),
                //should never happen
                DataType::Array(_) => {
                    Err(NetworkReadError(Cow::Borrowed("unexpected array in array")))
                }
                //should never happen
                DataType::SchemaValues(_) => Err(NetworkReadError(Cow::Borrowed(
                    "unexpected schema data type in array",
                ))),
            };
            values.push(result?);
        }
        let ary = ArrayType {
            can_be_empty: self.can_be_empty,
            p_type: Arc::new(p_type),
            values: Some(values),
        };
        Ok(DataType::Array(ary))
    }

    ///
    /// 将ArrayType写入到缓冲区, 消耗掉自己，之后不能再使用
    pub fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        match self.values {
            None => {
                buffer.put_i32(-1);
                return Ok(());
            }
            Some(values) => {
                buffer.put_i32(values.len() as i32);
                for value in values {
                    match value {
                        DataType::Bool(bool) => {
                            bool.write_to(buffer)?;
                        }
                        DataType::I8(i8) => i8.write_to(buffer)?,
                        DataType::I16(i16) => i16.write_to(buffer)?,
                        DataType::I32(i32) => i32.write_to(buffer)?,
                        DataType::U32(u32) => u32.write_to(buffer)?,
                        DataType::I64(i64) => i64.write_to(buffer)?,
                        DataType::PString(string) => string.write_to(buffer)?,
                        DataType::NPString(npstring) => npstring.write_to(buffer)?,
                        DataType::PBytes(bytes) => bytes.write_to(buffer)?,
                        DataType::NPBytes(npbytes) => npbytes.write_to(buffer)?,
                        DataType::PVarInt(pvarint) => pvarint.write_to(buffer)?,
                        DataType::PVarLong(pvarlong) => pvarlong.write_to(buffer)?,
                        DataType::Array(array) => array.write_to(buffer)?,
                        DataType::Records(records) => records.write_to(buffer)?,
                        DataType::SchemaValues(structure) => structure.write_to(buffer)?,
                        //should never happen
                        DataType::Schema(_) => {
                            return Err(NetworkReadError(Cow::Borrowed(
                                "unexpected array of schema",
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        let mut total_size = 4;
        match &self.values {
            None => return total_size,
            Some(values) => {
                for value in values {
                    total_size += match value {
                        DataType::Bool(bool) => bool.size(),
                        DataType::I8(i8) => i8.size(),
                        DataType::I16(i16) => i16.size(),
                        DataType::I32(i32) => i32.size(),
                        DataType::U32(u32) => u32.size(),
                        DataType::I64(i64) => i64.size(),
                        DataType::PString(string) => string.size(),
                        DataType::NPString(npstring) => npstring.size(),
                        DataType::PBytes(bytes) => bytes.size(),
                        DataType::NPBytes(npbytes) => npbytes.size(),
                        DataType::PVarInt(pvarint) => pvarint.size(),
                        DataType::PVarLong(pvarlong) => pvarlong.size(),
                        DataType::Array(array) => array.size(),
                        DataType::Records(records) => records.size(),
                        //should never happen
                        DataType::Schema(_) => {
                            //array of schema should not be here
                            panic!("unexpected array of schema");
                        }
                        DataType::SchemaValues(schema_data) => schema_data.size(),
                    };
                }
            }
        }
        total_size
    }
}
mod tests {
    #[test]
    fn test_array_read_write() {
        use super::*;
        let mut buffer = BytesMut::new();
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
        array.write_to(&mut buffer).unwrap();
        let read_array = array_clone.read_from(&mut buffer).unwrap();
        assert_eq!(read_array, DataType::Array(array_clone));
    }
    #[test]
    fn test_array_read_write_empty() {
        use super::*;
        let mut buffer = BytesMut::new();
        let array = ArrayType {
            can_be_empty: true,
            p_type: Arc::new(DataType::I32(I32::default())),
            values: None,
        };
        let array_clone = array.clone();
        array.write_to(&mut buffer).unwrap();
        let read_array = array_clone.read_from(&mut buffer).unwrap();
        assert_eq!(read_array, DataType::Array(array_clone));
    }
}
