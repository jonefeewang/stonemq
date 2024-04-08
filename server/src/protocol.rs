use std::borrow::Cow;
use std::clone::Clone;
use std::collections::HashMap;
use std::convert::TryFrom;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use integer_encoding::VarInt;
use once_cell::sync::Lazy;

use crate::message::MemoryRecord;
use crate::protocol::Acks::{All, Leader};
use crate::protocol::ApiKey::{Metadata, Produce};
use crate::AppError::{InvalidValue, NetworkReadError, NetworkWriteError};
use crate::{AppError, AppResult};

///
/// Enum在这里模拟多态
/// 1.在需要返回多态类型的值、或需要多态类型的参数时，可以使用Enum来代替
///
#[derive(Debug, Clone)]
enum TypeEnum<'s> {
    BoolE(Bool),
    I8E(I8),
    I16E(I16),
    I32E(I32),
    U32E(U32),
    I64E(I64),
    PStringE(PString),
    NPStringE(NPString),
    PBytesE(PBytes),
    NPBytesE(NPBytes),
    PVarIntE(PVarInt),
    PVarLongE(PVarLong),
    ArrayE(Array<'s>),
    RecordsE(MemoryRecord),
    SchemaE(Schema),
    StructE(Struct<'s>),
}
trait Type {
    fn read(&self, buffer: &mut Bytes) -> AppResult<TypeEnum>;
    fn write(&self, buffer: &mut BytesMut, value: TypeEnum) -> AppResult<()>;
}
trait PrimaryType {
    ///
    /// 都是从缓冲区里读取，不管是来自于网络还是来自于文件，stonemq使用Bytes做缓冲区
    /// 因此这里的类型直接使用Bytes
    /// 从缓冲区里读取时，不需要复制，可以共享使用
    /// 从缓冲区里写入时，也不需要复制，直接消耗即可
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>>;
    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()>;
}
struct TypeUtils {}

impl TypeUtils {
    fn safe_read_i8(buffer: &mut Bytes) -> AppResult<i8> {
        if buffer.remaining() < 1 {
            return Err(NetworkReadError(Cow::Borrowed(
                "Insufficient buffer size for i8.",
            )));
        }
        Ok(buffer.get_i8())
    }
    fn safe_read_i16(buffer: &mut Bytes) -> AppResult<i16> {
        if buffer.remaining() < 2 {
            return Err(NetworkReadError(Cow::Borrowed(
                "Insufficient buffer size for i16.",
            )));
        }
        Ok(buffer.get_i16())
    }
    fn safe_read_i32(buffer: &mut Bytes) -> AppResult<i32> {
        if buffer.remaining() < 4 {
            return Err(NetworkReadError(Cow::Borrowed(
                "Insufficient buffer size for i32.",
            )));
        }
        Ok(buffer.get_i32())
    }

    fn has_bytes_for_write(buffer: &mut BytesMut, bytes: usize) -> AppResult<()> {
        if buffer.remaining_mut() < bytes {
            return Err(NetworkWriteError(Cow::Owned(format!(
                "Insufficient buffer size for {}.",
                bytes
            ))));
        }
        Ok(())
    }
    fn has_bytes_to_read(buffer: &mut Bytes, bytes: usize) -> AppResult<()> {
        if buffer.remaining() < bytes {
            return Err(NetworkReadError(Cow::Owned(format!(
                "not enough bytes remains. need{},but only {}.",
                bytes,
                buffer.remaining()
            ))));
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
struct Bool {
    value: bool,
}
impl Bool {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        let value = TypeUtils::safe_read_i8(buffer)?;
        match value {
            0 => Ok(TypeEnum::BoolE(Bool { value: false })),
            1 => Ok(TypeEnum::BoolE(Bool { value: true })),
            _ => Err(NetworkReadError(Cow::Borrowed("Invalid value for bool."))),
        }
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        TypeUtils::has_bytes_for_write(buffer, 1)?;
        let value = if self.value { 1 } else { 0 };
        buffer.put_i8(value);
        Ok(())
    }
}
#[derive(Debug, Default, Clone)]
struct I8 {
    value: i8,
}
impl PrimaryType for I8 {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        Ok(TypeEnum::I8E(I8 {
            value: TypeUtils::safe_read_i8(buffer)?,
        }))
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        TypeUtils::has_bytes_for_write(buffer, 1)?;
        buffer.put_i8(self.value);
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
struct I16 {
    value: i16,
}
impl PrimaryType for I16 {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        Ok(TypeEnum::I16E(I16 {
            value: TypeUtils::safe_read_i16(buffer)?,
        }))
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        TypeUtils::has_bytes_for_write(buffer, 2)?;
        buffer.put_i16(self.value);
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
struct I32 {
    value: i32,
}
impl PrimaryType for I32 {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        let value = TypeUtils::safe_read_i32(buffer)?;
        Ok(TypeEnum::I32E(I32 { value }))
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        TypeUtils::has_bytes_for_write(buffer, 4)?;
        buffer.put_i32(self.value);
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
struct U32 {
    value: u32,
}
impl PrimaryType for U32 {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        TypeUtils::has_bytes_to_read(buffer, 4)?;
        Ok(TypeEnum::U32E(U32 {
            value: buffer.get_u32(),
        }))
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        TypeUtils::has_bytes_for_write(buffer, 4)?;
        buffer.put_u32(self.value);
        Ok(())
    }
}
#[derive(Debug, Default, Clone)]
struct I64 {
    value: i64,
}
impl PrimaryType for I64 {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        TypeUtils::has_bytes_to_read(buffer, 8)?;
        Ok(TypeEnum::I64E(I64 {
            value: buffer.get_i64(),
        }))
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        TypeUtils::has_bytes_for_write(buffer, 8)?;
        buffer.put_i64(self.value);
        Ok(())
    }
}
#[derive(Debug, Default, Clone)]
struct PString {
    value: String,
}

impl PrimaryType for PString {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        let length = TypeUtils::safe_read_i16(buffer)?;
        if length < 0 {
            return Err(NetworkReadError(Cow::Owned(format!(
                "String length: {:?} can not be negative",
                length
            ))));
        }
        TypeUtils::has_bytes_to_read(buffer, length as usize)?;
        return Ok(TypeEnum::PStringE(PString {
            value: String::from_utf8(buffer.split_to(length as usize).to_vec())?,
        }));
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        let length = self.value.len();
        // length(short)+length bytes
        TypeUtils::has_bytes_for_write(buffer, 2 + length)?;
        // length
        buffer.put_i16(length as i16);
        // body bytes
        buffer.put_slice(self.value.into_bytes().as_slice());
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
struct NPString {
    value: String,
}
impl PrimaryType for NPString {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        let length = TypeUtils::safe_read_i16(buffer)?;
        if length < 0 {
            //empty string
            return Ok(TypeEnum::NPStringE(NPString {
                value: String::new(),
            }));
        }
        TypeUtils::has_bytes_to_read(buffer, length as usize)?;
        return Ok(TypeEnum::NPStringE(NPString {
            value: String::from_utf8(buffer.split_to(length as usize).to_vec())?,
        }));
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        TypeUtils::has_bytes_for_write(buffer, 2)?;
        if self.value.is_empty() {
            buffer.put_i16(-1i16);
            return Ok(());
        }
        TypeUtils::has_bytes_for_write(buffer, self.value.len())?;
        buffer.put_slice(self.value.into_bytes().as_slice());
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
struct PBytes {
    buffer: Bytes,
}
impl PrimaryType for PBytes {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        let length = TypeUtils::safe_read_i32(buffer)?;
        if length < 0 {
            return Err(NetworkReadError(Cow::Borrowed("can not read a i32")));
        }
        TypeUtils::has_bytes_to_read(buffer, length as usize)?;
        Ok(TypeEnum::PBytesE(PBytes {
            buffer: buffer.split_to(length as usize),
        }))
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        TypeUtils::has_bytes_for_write(buffer, 4)?;
        buffer.put_i32(self.buffer.remaining() as i32);
        TypeUtils::has_bytes_for_write(buffer, self.buffer.remaining())?;
        buffer.put(self.buffer);
        Ok(())
    }
}
#[derive(Debug, Default, Clone)]
struct NPBytes {
    buffer: Bytes,
}
impl PrimaryType for NPBytes {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        let length = TypeUtils::safe_read_i32(buffer)?;
        if length < 0 {
            //empty bytes for kafka Nullable Bytes Type
            return Ok(TypeEnum::NPBytesE(NPBytes::default()));
        }
        TypeUtils::has_bytes_to_read(buffer, length as usize)?;
        Ok(TypeEnum::NPBytesE(NPBytes {
            buffer: buffer.split_to(length as usize),
        }))
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        TypeUtils::has_bytes_for_write(buffer, 4)?;
        if self.buffer.is_empty() {
            buffer.put_i32(-1);
            return Ok(());
        }
        buffer.put_i32(self.buffer.remaining() as i32);
        TypeUtils::has_bytes_for_write(buffer, self.buffer.remaining())?;
        buffer.put(self.buffer);
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
struct PVarInt {
    value: i32,
}
impl PrimaryType for PVarInt {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        let var = i32::decode_var(buffer.as_ref());
        return if let Some((value, read_size)) = var {
            //跳过刚解析过的record长度字段
            buffer.advance(read_size);
            Ok(TypeEnum::PVarIntE(PVarInt { value }))
        } else {
            Err(NetworkReadError(Cow::Borrowed("can not read a varint")))
        };
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        let var = i32::encode_var_vec(self.value);
        TypeUtils::has_bytes_for_write(buffer, var.len())?;
        buffer.put_slice(var.as_slice());
        Ok(())
    }
}
#[derive(Debug, Default, Clone)]
struct PVarLong {
    value: i64,
}
impl PrimaryType for PVarLong {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        let var = i64::decode_var(buffer.as_ref());
        return if let Some((value, read_size)) = var {
            //跳过刚解析过的record长度字段
            buffer.advance(read_size);
            Ok(TypeEnum::PVarLongE(PVarLong { value }))
        } else {
            Err(NetworkReadError(Cow::Borrowed("can not read a varlong")))
        };
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        let var = self.value.encode_var_vec();
        TypeUtils::has_bytes_for_write(buffer, var.len())?;
        buffer.put_slice(var.as_slice());
        Ok(())
    }
}

impl PrimaryType for MemoryRecord {
    fn read_from<'c>(buffer: &mut Bytes) -> AppResult<TypeEnum<'c>> {
        let n_pbytes_e = NPBytes::read_from(buffer)?;

        if let TypeEnum::NPBytesE(n_pbytes) = n_pbytes_e {
            return Ok(TypeEnum::RecordsE(MemoryRecord {
                buffer: n_pbytes.buffer,
            }));
        }
        Err(NetworkReadError(Cow::Owned(format!(
            "unexpected type {:?}",
            n_pbytes_e
        ))))
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        let n_pbytes = NPBytes {
            buffer: self.buffer,
        };
        return Ok(n_pbytes.write_to(buffer)?);
    }
}

#[derive(Debug, Clone)]
struct Array<'s> {
    can_be_empty: bool,
    p_type: Box<TypeEnum<'s>>,
    values: Option<Vec<TypeEnum<'s>>>,
}

impl<'s> Array<'s> {
    fn read_from(&self, buffer: &mut Bytes) -> AppResult<TypeEnum> {
        let ary_size = TypeUtils::safe_read_i32(buffer)?;
        if ary_size < 0 && self.can_be_empty {
            let ary = Array {
                can_be_empty: true,
                p_type: self.p_type.clone(),
                values: None,
            };
            return Ok(TypeEnum::ArrayE(ary));
        } else if ary_size < 0 {
            return Err(NetworkReadError(Cow::Owned(format!(
                "array size {} can not be negative",
                ary_size
            ))));
        }
        let mut values: Vec<TypeEnum> = Vec::with_capacity(ary_size as usize);
        for _ in 0..ary_size {
            let result = match &*self.p_type {
                TypeEnum::SchemaE(schema) => schema.read_fields(buffer),
                TypeEnum::BoolE(_) => Bool::read_from(buffer),
                TypeEnum::I8E(_) => I8::read_from(buffer),
                TypeEnum::I16E(_) => I16::read_from(buffer),
                TypeEnum::I32E(_) => I32::read_from(buffer),
                TypeEnum::U32E(_) => U32::read_from(buffer),
                TypeEnum::I64E(_) => I64::read_from(buffer),
                TypeEnum::PStringE(_) => PString::read_from(buffer),
                TypeEnum::NPStringE(_) => NPString::read_from(buffer),
                TypeEnum::PBytesE(_) => PBytes::read_from(buffer),
                TypeEnum::NPBytesE(_) => NPBytes::read_from(buffer),
                TypeEnum::PVarIntE(_) => PVarInt::read_from(buffer),
                TypeEnum::PVarLongE(_) => PVarLong::read_from(buffer),
                TypeEnum::RecordsE(_) => MemoryRecord::read_from(buffer),
                TypeEnum::ArrayE(_) => Err(NetworkReadError(Cow::Borrowed(
                    "can not read a array in array",
                ))),
                TypeEnum::StructE(_) => {
                    Err(NetworkReadError(Cow::Borrowed("can not read a struct")))
                }
            };
            values.push(result?);
        }
        let ary = Array {
            can_be_empty: self.can_be_empty,
            p_type: self.p_type.clone(),
            values: Some(values),
        };
        Ok(TypeEnum::ArrayE(ary))
    }

    fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        todo!()
    }
}

#[derive(Debug, Clone)]
struct Field {
    index: i32,
    name: &'static str,
    p_type: TypeEnum<'static>,
}
#[derive(Debug, Clone)]
struct Struct<'s> {
    schema: &'s Schema,
    values: Vec<TypeEnum<'s>>,
}

#[derive(Debug, Clone, Default)]
struct Schema {
    fields: Vec<Field>,
    fields_by_name: HashMap<&'static str, Field>,
}
impl Schema {
    fn read_fields(&self, buffer: &mut Bytes) -> AppResult<TypeEnum> {
        let mut values = Vec::new();
        for field in &self.fields {
            let result = match &field.p_type {
                TypeEnum::SchemaE(schema) => schema.read_fields(buffer),
                TypeEnum::BoolE(_) => Bool::read_from(buffer),
                TypeEnum::I8E(_) => I8::read_from(buffer),
                TypeEnum::I16E(_) => I16::read_from(buffer),
                TypeEnum::I32E(_) => I32::read_from(buffer),
                TypeEnum::U32E(_) => U32::read_from(buffer),
                TypeEnum::I64E(_) => I64::read_from(buffer),
                TypeEnum::PStringE(_) => PString::read_from(buffer),
                TypeEnum::NPStringE(_) => NPString::read_from(buffer),
                TypeEnum::PBytesE(_) => PBytes::read_from(buffer),
                TypeEnum::NPBytesE(_) => NPBytes::read_from(buffer),
                TypeEnum::PVarIntE(_) => PVarInt::read_from(buffer),
                TypeEnum::PVarLongE(_) => PVarLong::read_from(buffer),
                TypeEnum::ArrayE(array) => array.read_from(buffer),
                TypeEnum::RecordsE(_) => MemoryRecord::read_from(buffer),
                TypeEnum::StructE(_) => {
                    return Err(NetworkReadError(Cow::Borrowed("can not read a struct")));
                }
            };
            values.push(result?);
        }
        Ok(TypeEnum::StructE(Struct {
            schema: self,
            values,
        }))
    }
}

///
/// format: `TopicName [Partition MessageSetSize MessageSet]`
///
static TOPIC_PRODUCE_DATA_V0: Lazy<Schema> = Lazy::new(|| {
    // inner schema: [Partition MessageSetSize MessageSet]

    //Partition
    let partition_field = Field {
        index: 0,
        name: "partition",
        p_type: TypeEnum::I32E(I32::default()),
    };
    //MessageSetSize MessageSet
    let record_set_field = Field {
        index: 1,
        name: "record_set",
        p_type: TypeEnum::RecordsE(MemoryRecord::default()),
    };

    let mut fields_by_name: HashMap<&str, Field> = HashMap::new();
    fields_by_name.insert("partition", partition_field.clone());
    fields_by_name.insert("record_set", record_set_field.clone());
    let inner_schema = Schema {
        fields_by_name,
        fields: vec![partition_field, record_set_field],
    };

    // outer schema: TopicName inner_schema
    let data_ary = TypeEnum::ArrayE(Array {
        can_be_empty: false,
        p_type: Box::new(TypeEnum::SchemaE(inner_schema)),
        values: None,
    });
    let mut fields_by_name: HashMap<&str, Field> = HashMap::new();
    //TopicName
    let topic_filed = Field {
        index: 0,
        name: "topic",
        p_type: TypeEnum::PStringE(PString::default()),
    };
    let data_field = Field {
        index: 1,
        name: "data",
        p_type: data_ary,
    };
    fields_by_name.insert("topic", topic_filed.clone());
    fields_by_name.insert("data", data_field.clone());
    Schema {
        fields_by_name,
        fields: vec![topic_filed, data_field],
    }
});
///
/// format: `RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]`
///
static PRODUCE_REQUEST_SCHEMA_V0: Lazy<Schema> = Lazy::new(|| {
    let mut fields_by_name = HashMap::new();
    let acks_field = Field {
        index: 0,
        name: "acks",
        p_type: TypeEnum::I8E(I8::default()),
    };
    let timeout_filed = Field {
        index: 1,
        name: "timeout",
        p_type: TypeEnum::I16E(I16::default()),
    };
    let topic_data_filed = Field {
        index: 2,
        name: "topic_data",
        p_type: TypeEnum::ArrayE(Array {
            can_be_empty: false,
            p_type: Box::new(TypeEnum::SchemaE(TOPIC_PRODUCE_DATA_V0.clone())),
            values: None,
        }),
    };
    fields_by_name.insert("acks", acks_field.clone());
    fields_by_name.insert("timeout", timeout_filed.clone());
    fields_by_name.insert("topic_data", topic_data_filed.clone());

    Schema {
        fields_by_name,
        fields: vec![acks_field, timeout_filed, topic_data_filed],
    }
});

#[derive(Debug, Copy, Clone)]
pub enum ApiKey {
    Produce,
    Metadata,
}
#[derive(Debug, Clone, Copy)]
pub enum ApiVersion {
    V0,
    V1,
    V2,
    V3,
}
pub enum Acks {
    All(i8),
    Leader(i8),
    None(i8),
}
impl TryFrom<i16> for Acks {
    type Error = AppError;

    fn try_from(ack: i16) -> Result<Self, Self::Error> {
        match ack {
            -1 => Ok(All(-1)),
            1 => Ok(Leader(1)),
            0 => Ok(Acks::None(0)),
            invalid => Err(InvalidValue("ack field", invalid.to_string())),
        }
    }
}
impl TryFrom<i16> for ApiKey {
    type Error = AppError;

    fn try_from(value: i16) -> AppResult<ApiKey> {
        match value {
            0 => Ok(Produce),
            1 => Ok(Metadata),
            invalid => Err(InvalidValue("api key", invalid.to_string())),
        }
    }
}
