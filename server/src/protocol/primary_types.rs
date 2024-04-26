use crate::message::MemoryRecord;
use crate::protocol::types::FieldTypeEnum;
use crate::protocol::utils::BufferUtils;
use crate::AppError::NetworkReadError;
use crate::AppResult;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use integer_encoding::VarInt;
use std::borrow::Cow;

///
/// 基本类型，构成schema的最小单位
///
/// 都是从缓冲区里读取，不管来自网络还是文件，stonemq使用Bytes做缓冲区,因此这里的类型直接使用`Bytes`
/// 从缓冲区里读取时，不需要复制，可以共享使用  
/// 从缓冲区里写入时，也不需要复制，也可以共享缓冲区里的内容
///
/// 这里使用Enum来代替trait object，避免使用动态绑定，减少性能损失。  
/// 因此，基本类型和符合类型这两个trait基本没起到作用，仅用作功能的集合
///
///
pub trait PrimaryType {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum>;
    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()>;
    fn size(&self) -> usize;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Bool {
    pub value: bool,
}
impl From<bool> for Bool {
    fn from(value: bool) -> Self {
        Bool { value }
    }
}
impl PrimaryType for Bool {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        let value = BufferUtils::safe_read_i8(buffer)?;
        match value {
            0 => Ok(FieldTypeEnum::BoolE(Bool { value: false })),
            1 => Ok(FieldTypeEnum::BoolE(Bool { value: true })),
            _ => Err(NetworkReadError(Cow::Borrowed("Invalid value for bool."))),
        }
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        BufferUtils::can_write_nbytes(buffer, 1)?;
        let value = if self.value { 1 } else { 0 };
        buffer.put_i8(value);
        Ok(())
    }

    fn size(&self) -> usize {
        1
    }
}
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct I8 {
    pub value: i8,
}
impl From<i8> for I8 {
    fn from(value: i8) -> Self {
        I8 { value }
    }
}
impl PrimaryType for I8 {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        Ok(FieldTypeEnum::I8E(I8 {
            value: BufferUtils::safe_read_i8(buffer)?,
        }))
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        BufferUtils::can_write_nbytes(buffer, 1)?;
        buffer.put_i8(self.value);
        Ok(())
    }

    fn size(&self) -> usize {
        1
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct I16 {
    pub value: i16,
}
impl From<i16> for I16 {
    fn from(value: i16) -> Self {
        I16 { value }
    }
}

impl PrimaryType for I16 {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        Ok(FieldTypeEnum::I16E(I16 {
            value: BufferUtils::safe_read_i16(buffer)?,
        }))
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        BufferUtils::can_write_nbytes(buffer, 2)?;
        buffer.put_i16(self.value);
        Ok(())
    }

    fn size(&self) -> usize {
        2
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct I32 {
    pub value: i32,
}
impl From<i32> for I32 {
    fn from(value: i32) -> Self {
        I32 { value }
    }
}

impl PrimaryType for I32 {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        let value = BufferUtils::safe_read_i32(buffer)?;
        Ok(FieldTypeEnum::I32E(I32 { value }))
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        BufferUtils::can_write_nbytes(buffer, 4)?;
        buffer.put_i32(self.value);
        Ok(())
    }

    fn size(&self) -> usize {
        4
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct U32 {
    value: u32,
}

impl From<u32> for U32 {
    fn from(value: u32) -> Self {
        U32 { value }
    }
}
impl PrimaryType for U32 {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        BufferUtils::can_read_nbytes(buffer, 4)?;
        Ok(FieldTypeEnum::U32E(U32 {
            value: buffer.get_u32(),
        }))
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        BufferUtils::can_write_nbytes(buffer, 4)?;
        buffer.put_u32(self.value);
        Ok(())
    }

    fn size(&self) -> usize {
        4
    }
}
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct I64 {
    value: i64,
}
impl From<i64> for I64 {
    fn from(value: i64) -> Self {
        I64 { value }
    }
}
impl PrimaryType for I64 {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        BufferUtils::can_read_nbytes(buffer, 8)?;
        Ok(FieldTypeEnum::I64E(I64 {
            value: buffer.get_i64(),
        }))
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        BufferUtils::can_write_nbytes(buffer, 8)?;
        buffer.put_i64(self.value);
        Ok(())
    }

    fn size(&self) -> usize {
        8
    }
}
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PString {
    pub value: String,
}
impl From<String> for PString {
    fn from(value: String) -> Self {
        PString { value }
    }
}

impl PrimaryType for PString {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        let length = BufferUtils::safe_read_i16(buffer)?;
        if length < 0 {
            return Err(NetworkReadError(Cow::Owned(format!(
                "String length: {:?} can not be negative",
                length
            ))));
        }
        BufferUtils::can_read_nbytes(buffer, length as usize)?;
        Ok(FieldTypeEnum::PStringE(PString {
            value: String::from_utf8(buffer.split_to(length as usize).to_vec())?,
        }))
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        let length = self.value.len();
        // length(short)+length bytes
        BufferUtils::can_write_nbytes(buffer, 2 + length)?;
        // length
        buffer.put_i16(length as i16);
        // body bytes
        buffer.put_slice(self.value.clone().into_bytes().as_slice());
        Ok(())
    }

    fn size(&self) -> usize {
        2 + self.value.len()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NPString {
    value: String,
}

impl From<String> for NPString {
    fn from(value: String) -> Self {
        NPString { value }
    }
}

impl PrimaryType for NPString {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        let length = BufferUtils::safe_read_i16(buffer)?;
        if length < 0 {
            //empty string
            return Ok(FieldTypeEnum::NPStringE(NPString {
                value: String::new(),
            }));
        }
        BufferUtils::can_read_nbytes(buffer, length as usize)?;
        Ok(FieldTypeEnum::NPStringE(NPString {
            value: String::from_utf8(buffer.split_to(length as usize).to_vec())?,
        }))
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        BufferUtils::can_write_nbytes(buffer, 2)?;
        if self.value.is_empty() {
            buffer.put_i16(-1i16);
            return Ok(());
        } else {
            buffer.put_i16(self.value.len() as i16);
        }

        BufferUtils::can_write_nbytes(buffer, self.value.len())?;

        buffer.put_slice(self.value.clone().into_bytes().as_slice());
        Ok(())
    }

    fn size(&self) -> usize {
        2 + self.value.len()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PBytes {
    buffer: Bytes,
}
impl From<Bytes> for PBytes {
    fn from(buffer: Bytes) -> Self {
        PBytes { buffer }
    }
}

impl PrimaryType for PBytes {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        let length = BufferUtils::safe_read_i32(buffer)?;
        if length < 0 {
            return Err(NetworkReadError(Cow::Borrowed("can not read a i32")));
        }
        BufferUtils::can_read_nbytes(buffer, length as usize)?;
        Ok(FieldTypeEnum::PBytesE(PBytes {
            buffer: buffer.split_to(length as usize),
        }))
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        BufferUtils::can_write_nbytes(buffer, 4)?;
        buffer.put_i32(self.buffer.remaining() as i32);
        BufferUtils::can_write_nbytes(buffer, self.buffer.remaining())?;
        buffer.put(self.buffer.clone());
        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.buffer.remaining()
    }
}
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NPBytes {
    pub buffer: Bytes,
}
impl PrimaryType for NPBytes {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        let length = BufferUtils::safe_read_i32(buffer)?;
        if length < 0 {
            //empty bytes for kafka Nullable Bytes Type
            return Ok(FieldTypeEnum::NPBytesE(NPBytes::default()));
        }
        BufferUtils::can_read_nbytes(buffer, length as usize)?;
        Ok(FieldTypeEnum::NPBytesE(NPBytes {
            buffer: buffer.split_to(length as usize),
        }))
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        BufferUtils::can_write_nbytes(buffer, 4)?;
        if self.buffer.is_empty() {
            buffer.put_i32(-1);
            return Ok(());
        }
        buffer.put_i32(self.buffer.remaining() as i32);
        BufferUtils::can_write_nbytes(buffer, self.buffer.remaining())?;
        buffer.put(self.buffer.clone());
        Ok(())
    }

    fn size(&self) -> usize {
        if self.buffer.is_empty() {
            return 4;
        }
        return 4 + self.buffer.remaining();
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PVarInt {
    value: i32,
}
// Import the trait that contains the `decode_var` function.

impl PrimaryType for PVarInt {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        let var = i32::decode_var(buffer.as_ref());
        return if let Some((value, read_size)) = var {
            //跳过刚解析过的record长度字段
            buffer.advance(read_size);
            Ok(FieldTypeEnum::PVarIntE(PVarInt { value }))
        } else {
            Err(NetworkReadError(Cow::Borrowed("can not read a varint")))
        };
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        let var = i32::encode_var_vec(self.value);
        BufferUtils::can_write_nbytes(buffer, var.len())?;
        buffer.put_slice(var.as_slice());
        Ok(())
    }

    fn size(&self) -> usize {
        i32::required_space(self.value)
    }
}
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PVarLong {
    value: i64,
}
impl PrimaryType for PVarLong {
    fn read_from(buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        let var = i64::decode_var(buffer.as_ref());
        return if let Some((value, read_size)) = var {
            //跳过刚解析过的record长度字段
            buffer.advance(read_size);
            Ok(FieldTypeEnum::PVarLongE(PVarLong { value }))
        } else {
            Err(NetworkReadError(Cow::Borrowed("can not read a varlong")))
        };
    }

    fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        let var = self.value.encode_var_vec();
        BufferUtils::can_write_nbytes(buffer, var.len())?;
        buffer.put_slice(var.as_slice());
        Ok(())
    }

    fn size(&self) -> usize {
        i64::required_space(self.value)
    }
}

#[test]
fn test_primary_type_read_write() {
    let mut buffer = BytesMut::new();

    // Test Bool
    buffer.clear();
    let bool_value = Bool { value: true };
    bool_value.write_to(&mut buffer).unwrap();
    let read_bool = Bool::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_bool, FieldTypeEnum::BoolE(bool_value));

    // Test I8
    let mut buffer = BytesMut::new();
    let i8_value = I8 { value: 127 };
    i8_value.write_to(&mut buffer).unwrap();
    let read_i8 = I8::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_i8, FieldTypeEnum::I8E(i8_value));

    // Test I16
    let mut buffer = BytesMut::new();
    let i16_value = I16 { value: 32767 };
    i16_value.write_to(&mut buffer).unwrap();
    let read_i16 = I16::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_i16, FieldTypeEnum::I16E(i16_value));

    // Test I32
    let mut buffer = BytesMut::new();
    let i32_value = I32 { value: 2147483647 };
    i32_value.write_to(&mut buffer).unwrap();
    let read_i32 = I32::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_i32, FieldTypeEnum::I32E(i32_value));

    // Test U32
    let mut buffer = BytesMut::new();
    let u32_value = U32 { value: 4294967295 };
    u32_value.write_to(&mut buffer).unwrap();
    let read_u32 = U32::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_u32, FieldTypeEnum::U32E(u32_value));

    // Test I64
    let mut buffer = BytesMut::new();
    let i64_value = I64 {
        value: 9223372036854775807,
    };
    i64_value.write_to(&mut buffer).unwrap();
    let read_i64 = I64::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_i64, FieldTypeEnum::I64E(i64_value));

    // Test PString
    let mut buffer = BytesMut::new();
    let pstring_value = PString {
        value: "test".to_string(),
    };
    pstring_value.write_to(&mut buffer).unwrap();
    let read_pstring = PString::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_pstring, FieldTypeEnum::PStringE(pstring_value));

    // Test NPString
    let mut buffer = BytesMut::new();
    let npstring_value = NPString {
        value: "test".to_string(),
    };
    npstring_value.write_to(&mut buffer).unwrap();
    let read_npstring = NPString::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_npstring, FieldTypeEnum::NPStringE(npstring_value));

    //Test PBytes
    let mut buffer = BytesMut::new();
    let pbytes_value = PBytes {
        buffer: Bytes::from("test".as_bytes()),
    };
    pbytes_value.write_to(&mut buffer).unwrap();
    let read_pbytes = PBytes::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_pbytes, FieldTypeEnum::PBytesE(pbytes_value));

    //Test NPBytes
    let mut buffer = BytesMut::new();
    let npbytes_value = NPBytes {
        buffer: Bytes::from("test".as_bytes()),
    };
    npbytes_value.write_to(&mut buffer).unwrap();
    let read_npbytes = NPBytes::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_npbytes, FieldTypeEnum::NPBytesE(npbytes_value));

    //Test PVarInt
    let mut buffer = BytesMut::new();
    let pvarint_value = PVarInt { value: 12345 };
    pvarint_value.write_to(&mut buffer).unwrap();
    let read_pvarint = PVarInt::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_pvarint, FieldTypeEnum::PVarIntE(pvarint_value));

    //Test PVarLong
    let mut buffer = BytesMut::new();
    let pvarlong_value = PVarLong { value: 1234567890 };
    pvarlong_value.write_to(&mut buffer).unwrap();
    let read_pvarlong = PVarLong::read_from(&mut buffer.freeze()).unwrap();
    assert_eq!(read_pvarlong, FieldTypeEnum::PVarLongE(pvarlong_value));
}
