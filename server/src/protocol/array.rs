use std::borrow::Cow;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};

use crate::message::MemoryRecord;
use crate::protocol::primary_types::{
    Bool, NPBytes, NPString, PBytes, PString, PVarInt, PVarLong, PrimaryType, I16, I32, I64, I8,
    U32,
};
use crate::protocol::structure::Struct;
use crate::protocol::types::FieldTypeEnum;
use crate::protocol::utils::BufferUtils;
use crate::AppError::NetworkReadError;
use crate::AppResult;

///
/// 复合类型，由基本类型组合而成，作为schema的一个单位
///
trait CompoundType {
    ///
    /// 复合类型，有基本类型组合构成
    ///
    /// 都是从缓冲区里读取，不管来自网络还是文件，stonemq使用Bytes做缓冲区,因此这里的类型直接使用`Bytes`
    /// 从缓冲区里读取时，不需要复制，可以共享使用  
    /// 从缓冲区里写入时，也不需要复制，也可以共享缓冲区里的内容  
    ///
    /// 这里使用Enum来代替trait object，避免使用动态绑定，减少性能损失。    
    /// 因此，基本类型和符合类型这两个trait基本没起到作用，仅用作功能的集合
    ///
    fn read_from(&self, buffer: &mut Bytes) -> AppResult<FieldTypeEnum>;
    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()>;
    fn size(&self) -> usize;
}

///
/// Array这个类型
#[derive(Debug, Clone)]
pub struct Array {
    pub can_be_empty: bool,
    pub p_type: Arc<FieldTypeEnum>,
    pub values: Option<Vec<FieldTypeEnum>>,
}

impl Array {
    pub fn values(&self) -> &Option<Vec<FieldTypeEnum>> {
        &self.values
    }
}

impl PartialEq for Array {
    fn eq(&self, other: &Self) -> bool {
        self.can_be_empty == other.can_be_empty
            && self.p_type == other.p_type
            && self.values == other.values
    }
}

impl Eq for Array {}

impl Array {
    pub fn read_from(&self, buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        let ary_size = BufferUtils::safe_read_i32(buffer)?;
        let p_type = match &*self.p_type {
            FieldTypeEnum::SchemaE(schema) => {
                FieldTypeEnum::StructE(Struct::from_schema(Arc::clone(schema)))
            }
            other_type => other_type.clone(),
        };
        if ary_size < 0 && self.can_be_empty {
            let ary = Array {
                can_be_empty: true,
                p_type: Arc::new(p_type),
                values: None,
            };
            return Ok(FieldTypeEnum::ArrayE(ary));
        } else if ary_size < 0 {
            return Err(NetworkReadError(Cow::Owned(format!(
                "array size {} can not be negative",
                ary_size
            ))));
        }
        let mut values: Vec<FieldTypeEnum> = Vec::with_capacity(ary_size as usize);
        for _ in 0..ary_size {
            let result = match &*self.p_type {
                FieldTypeEnum::SchemaE(schema) => schema.read_from(buffer),
                FieldTypeEnum::BoolE(_) => Bool::read_from(buffer),
                FieldTypeEnum::I8E(_) => I8::read_from(buffer),
                FieldTypeEnum::I16E(_) => I16::read_from(buffer),
                FieldTypeEnum::I32E(_) => I32::read_from(buffer),
                FieldTypeEnum::U32E(_) => U32::read_from(buffer),
                FieldTypeEnum::I64E(_) => I64::read_from(buffer),
                FieldTypeEnum::PStringE(_) => PString::read_from(buffer),
                FieldTypeEnum::NPStringE(_) => NPString::read_from(buffer),
                FieldTypeEnum::PBytesE(_) => PBytes::read_from(buffer),
                FieldTypeEnum::NPBytesE(_) => NPBytes::read_from(buffer),
                FieldTypeEnum::PVarIntE(_) => PVarInt::read_from(buffer),
                FieldTypeEnum::PVarLongE(_) => PVarLong::read_from(buffer),
                FieldTypeEnum::RecordsE(_) => MemoryRecord::read_from(buffer),
                //should never happen
                FieldTypeEnum::ArrayE(_) => {
                    Err(NetworkReadError(Cow::Borrowed("unexpected array in array")))
                }
                //should never happen
                FieldTypeEnum::StructE(_) => {
                    Err(NetworkReadError(Cow::Borrowed("can not read a struct")))
                }
            };
            values.push(result?);
        }
        let ary = Array {
            can_be_empty: self.can_be_empty,
            p_type: Arc::new(p_type),
            values: Some(values),
        };
        Ok(FieldTypeEnum::ArrayE(ary))
    }

    pub fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        match &self.values {
            None => {
                buffer.put_i32(-1);
                return Ok(());
            }
            Some(values) => {
                buffer.put_i32(values.len() as i32);
                for value in values {
                    match value {
                        FieldTypeEnum::BoolE(bool) => {
                            bool.write_to(buffer)?;
                        }
                        FieldTypeEnum::I8E(i8) => i8.write_to(buffer)?,
                        FieldTypeEnum::I16E(i16) => i16.write_to(buffer)?,
                        FieldTypeEnum::I32E(i32) => i32.write_to(buffer)?,
                        FieldTypeEnum::U32E(u32) => u32.write_to(buffer)?,
                        FieldTypeEnum::I64E(i64) => i64.write_to(buffer)?,
                        FieldTypeEnum::PStringE(string) => string.write_to(buffer)?,
                        FieldTypeEnum::NPStringE(npstring) => npstring.write_to(buffer)?,
                        FieldTypeEnum::PBytesE(bytes) => bytes.write_to(buffer)?,
                        FieldTypeEnum::NPBytesE(npbytes) => npbytes.write_to(buffer)?,
                        FieldTypeEnum::PVarIntE(pvarint) => pvarint.write_to(buffer)?,
                        FieldTypeEnum::PVarLongE(pvarlong) => pvarlong.write_to(buffer)?,
                        FieldTypeEnum::ArrayE(array) => array.write_to(buffer)?,
                        FieldTypeEnum::RecordsE(records) => records.write_to(buffer)?,
                        //should never happen
                        FieldTypeEnum::SchemaE(_) => {
                            return Err(NetworkReadError(Cow::Borrowed(
                                "unexpected array of schema",
                            )));
                        }
                        FieldTypeEnum::StructE(structure) => structure.write_to(buffer)?,
                    }
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        todo!()
    }
}

#[test]
fn test_array_read_write() {
    let mut buffer = BytesMut::new();
    let array = Array {
        can_be_empty: false,
        p_type: Arc::new(FieldTypeEnum::I32E(I32::default())),
        values: Some(vec![
            FieldTypeEnum::I32E(I32 { value: 1 }),
            FieldTypeEnum::I32E(I32 { value: 2 }),
            FieldTypeEnum::I32E(I32 { value: 3 }),
        ]),
    };
    array.write_to(&mut buffer).unwrap();
    let read_array = array.read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_array, FieldTypeEnum::ArrayE(array));
}
