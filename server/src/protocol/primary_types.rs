use std::borrow::Cow;
use std::fmt::Debug;

use bytes::{Buf, BufMut, BytesMut};
use integer_encoding::VarInt;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::trace;

use crate::AppError::NetworkReadError;
use crate::AppResult;
use crate::protocol::types::DataType;

///
/// Define StoneMQ primary types.
/// Primary types include Bool, I8, I16, I32, U32, I64, PString, NPString, PBytes
/// , NPBytes, PVarInt, PVarLong.
/// Array/Schema/SchemaData are composite types defined in their respective modules.
/// And implement the conversion from the rust type to the primary type
///
macro_rules! define_type {
    ($type_name:ident, $inner_type:ty) => {
        #[derive(Debug, Default, Clone, PartialEq, Eq)]
        pub struct $type_name {
            pub value: $inner_type,
        }
        impl From<$inner_type> for $type_name {
            fn from(value: $inner_type) -> Self {
                Self { value }
            }
        }
    };
}
///
/// Implement the PrimaryType trait for the primary types(structs).
///
macro_rules! implement_primary_type {
    ($type:ident, $return_type:ident, $read_method:ident, $write_method:ident, $size:expr) => {
        impl PrimaryType for $type {
            fn read_from(buffer: &mut BytesMut) -> AppResult<DataType> {
                let value = buffer.$read_method();
                Ok(DataType::$return_type($type { value }))
            }
            async fn write_to<W>(self, writer: &mut W) -> AppResult<()>
            where
                W: AsyncWriteExt + Unpin,
            {
                writer.$write_method(self.value).await?;
                Ok(())
            }
            fn size(&self) -> usize {
                $size
            }
        }
    };
}
///
/// Implement the PrimaryType trait for the variable types(PVarInt PVarLong).
macro_rules! implement_var_type {
    ($t:ident, $read_method:path, $write_method:ident, $encode_var_vec:path, $required_space:path, $data_type:ident) => {
        impl PrimaryType for $t {
            fn read_from(buffer: &mut BytesMut) -> AppResult<DataType> {
                let var = $read_method(buffer.as_ref());
                return if let Some((value, read_size)) = var {
                    // Skip the record length field that was just parsed.
                    buffer.advance(read_size);
                    Ok(DataType::$data_type($t { value }))
                } else {
                    Err(NetworkReadError(Cow::Borrowed(concat!(
                        "can not read a ",
                        stringify!($t)
                    ))))
                };
            }

            async fn write_to<W>(self, writer: &mut W) -> AppResult<()>
            where
                W: AsyncWriteExt + Unpin,
            {
                let var = $encode_var_vec(self.value);
                writer.write_all(var.as_slice()).await?;
                Ok(())
            }

            fn size(&self) -> usize {
                $required_space(self.value)
            }
        }
    };
}

/// Fundamental types, constituting the smallest unit of a schema.
///
/// All are read from a buffer, whether originating from a network or a file.
/// StoneMQ utilizes Bytes for its buffer, thus the types are directly used from Bytes.
/// When reading from the buffer, there is no need for duplication; the data can be shared.
/// Similarly, when writing to the buffer, duplication is unnecessary; content within the buffer
/// can be shared as well.
///
/// Here, Enums are used in place of trait objects to avoid dynamic binding
/// and reduce performance loss.
/// Consequently, the basic(primary type) and composite type(array/schema/schema_data)
/// have minimal impact,serving merely as a collection of functionalities.
pub trait PrimaryType {
    fn read_from(buffer: &mut BytesMut) -> AppResult<DataType>;
    async fn write_to<W>(self, writer: &mut W) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin;
    fn size(&self) -> usize;
}

define_type!(Bool, bool);
define_type!(I8, i8);
define_type!(I16, i16);
define_type!(I32, i32);
define_type!(U32, u32);
define_type!(I64, i64);
define_type!(PString, String);
define_type!(NPString, Option<String>);
define_type!(PBytes, BytesMut);
define_type!(NPBytes, Option<BytesMut>);
define_type!(PVarInt, i32);
define_type!(PVarLong, i64);

implement_primary_type!(I8, I8, get_i8, write_i8, 1);
implement_primary_type!(I16, I16, get_i16, write_i16, 2);
implement_primary_type!(I32, I32, get_i32, write_i32, 4);
implement_primary_type!(U32, U32, get_u32, write_u32, 4);
implement_primary_type!(I64, I64, get_i64, write_i64, 8);

implement_var_type!(
    PVarInt,
    i32::decode_var,
    put_i32,
    i32::encode_var_vec,
    i32::required_space,
    PVarInt
);
implement_var_type!(
    PVarLong,
    i64::decode_var,
    put_i64,
    i64::encode_var_vec,
    i64::required_space,
    PVarLong
);

impl PrimaryType for PBytes {
    fn read_from(buffer: &mut BytesMut) -> AppResult<DataType> {
        let length = buffer.get_i32();
        if length < 0 {
            Err(NetworkReadError(Cow::Borrowed(
                "can not read a PBytes, length is negative",
            )))
        } else {
            Ok(DataType::PBytes(PBytes {
                value: buffer.split_to(length as usize),
            }))
        }
    }
    async fn write_to<W>(mut self, writer: &mut W) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        let length = self.value.remaining();
        writer.write_i32(length as i32).await?;
        writer.write_buf(&mut self.value).await?;
        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.value.remaining()
    }
}

impl PrimaryType for NPBytes {
    fn read_from(buffer: &mut BytesMut) -> AppResult<DataType> {
        let length = buffer.get_i32();
        if length < 0 {
            Ok(DataType::NPBytes(NPBytes { value: None }))
        } else {
            Ok(DataType::NPBytes(NPBytes {
                value: Some(buffer.split_to(length as usize)),
            }))
        }
    }
    async fn write_to<W>(self, writer: &mut W) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        if let Some(mut value) = self.value {
            let length = value.remaining();
            writer.write_i32(length as i32).await?;
            writer.write_buf(&mut value).await?;
        } else {
            writer.write_i32(-1).await?;
        }
        Ok(())
    }
    fn size(&self) -> usize {
        if let Some(value) = &self.value {
            4 + value.remaining()
        } else {
            4
        }
    }
}

impl PrimaryType for PString {
    fn read_from(buffer: &mut BytesMut) -> AppResult<DataType> {
        let length = buffer.get_i16();
        if length < 0 {
            Err(NetworkReadError(Cow::Owned(format!(
                "String length: {:?} can not be negative",
                length
            ))))
        } else {
            trace!("Reading PString with length: {}", length);
            Ok(DataType::PString(PString {
                value: String::from_utf8(buffer.split_to(length as usize).to_vec())?,
            }))
        }
    }
    async fn write_to<W>(self, writer: &mut W) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        let length = self.value.len();
        writer.write_i16(length as i16).await?;
        writer.write_all(self.value.as_bytes()).await?;
        Ok(())
    }
    fn size(&self) -> usize {
        2 + self.value.len()
    }
}
impl PrimaryType for NPString {
    fn read_from(buffer: &mut BytesMut) -> AppResult<DataType> {
        let length = buffer.get_i16();
        if length < 0 {
            Ok(DataType::NPString(NPString { value: None }))
        } else {
            trace!("Reading NPString with length: {}", length);
            Ok(DataType::NPString(NPString {
                value: Some(String::from_utf8(
                    buffer.split_to(length as usize).to_vec(),
                )?),
            }))
        }
    }
    async fn write_to<W>(mut self, writer: &mut W) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        if let Some(value) = self.value {
            let length = value.len();
            writer.write_i16(length as i16).await?;
            writer.write_all(value.as_bytes()).await?;
        } else {
            writer.write_i16(-1).await?;
        }
        Ok(())
    }
    fn size(&self) -> usize {
        if let Some(value) = &self.value {
            2 + value.len()
        } else {
            2
        }
    }
}

impl PrimaryType for Bool {
    fn read_from(buffer: &mut BytesMut) -> AppResult<DataType> {
        let value = buffer.get_i8();
        match value {
            0 => Ok(DataType::Bool(Bool { value: false })),
            1 => Ok(DataType::Bool(Bool { value: true })),
            _ => Err(NetworkReadError(Cow::Borrowed("Invalid value for bool."))),
        }
    }

    async fn write_to<W>(mut self, writer: &mut W) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin,
    {
        let value = if self.value { 1 } else { 0 };
        writer.write_i8(value).await?;
        Ok(())
    }

    fn size(&self) -> usize {
        1
    }
}

mod test {
    use bytes::BytesMut;

    use super::*;

    #[tokio::test]
    async fn test_primary_type_read_write() {
        let mut writer = Vec::new();

        // Test Bool
        let bool_value = Bool { value: true };
        bool_value.write_to(&mut writer).await.unwrap();
        let mut bytes_mut = BytesMut::from(&writer[..]);
        let read_bool = Bool::read_from(&mut bytes_mut).unwrap();
        assert_eq!(read_bool, DataType::Bool(Bool { value: true }));

        // Test I8
        writer.clear();
        let i8_value = I8 { value: 127 };
        let i8_value_clone = i8_value.clone();
        i8_value.write_to(&mut writer).await.unwrap();
        let mut buffer = BytesMut::from(&writer[..]);
        let read_i8 = I8::read_from(&mut buffer).unwrap();
        assert_eq!(read_i8, DataType::I8(i8_value_clone));

        // Test I16
        writer.clear();
        let i16_value = I16 { value: 32767 };
        let i16_value_clone = i16_value.clone();
        i16_value.write_to(&mut writer).await.unwrap();
        let mut buffer = BytesMut::from(&writer[..]);
        let read_i16 = I16::read_from(&mut buffer).unwrap();
        assert_eq!(read_i16, DataType::I16(i16_value_clone));

        // Test I32
        writer.clear();
        let i32_value = I32 { value: 2147483647 };
        let i32_value_clone = i32_value.clone();
        i32_value.write_to(&mut writer).await.unwrap();
        let mut buffer = BytesMut::from(&writer[..]);
        let read_i32 = I32::read_from(&mut buffer).unwrap();
        assert_eq!(read_i32, DataType::I32(i32_value_clone));

        // Test U32
        writer.clear();
        let u32_value = U32 { value: 4294967295 };
        let u32_value_clone = u32_value.clone();
        u32_value.write_to(&mut writer).await.unwrap();
        let mut buffer = BytesMut::from(&writer[..]);
        let read_u32 = U32::read_from(&mut buffer).unwrap();
        assert_eq!(read_u32, DataType::U32(u32_value_clone));

        // Test I64
        writer.clear();
        let i64_value = I64 {
            value: 9223372036854775807,
        };
        let i64_value_clone = i64_value.clone();
        i64_value.write_to(&mut writer).await.unwrap();
        let mut buffer = BytesMut::from(&writer[..]);
        let read_i64 = I64::read_from(&mut buffer).unwrap();
        assert_eq!(read_i64, DataType::I64(i64_value_clone));

        // Test PString
        writer.clear();
        let pstring_value = PString {
            value: "test".to_string(),
        };
        let pstring_value_clone = pstring_value.clone();
        pstring_value.write_to(&mut writer).await.unwrap();
        let mut buffer = BytesMut::from(&writer[..]);
        let read_pstring = PString::read_from(&mut buffer).unwrap();
        assert_eq!(read_pstring, DataType::PString(pstring_value_clone));

        // Test NPString
        buffer.clear();
        let npstring_value = NPString {
            value: Some("test".to_string()),
        };
        let npstring_value_clone = npstring_value.clone();
        npstring_value.write_to(&mut writer).await.unwrap();
        let mut buffer = BytesMut::from(&writer[..]);
        let read_npstring = NPString::read_from(&mut buffer).unwrap();
        assert_eq!(read_npstring, DataType::NPString(npstring_value_clone));

        //Test PBytes
        writer.clear();
        let pbytes_value = PBytes {
            value: BytesMut::from("test".as_bytes()),
        };
        let pbytes_value_clone = pbytes_value.clone();
        pbytes_value.write_to(&mut writer).await.unwrap();
        let mut buffer = BytesMut::from(&writer[..]);
        let read_pbytes = PBytes::read_from(&mut buffer).unwrap();
        assert_eq!(read_pbytes, DataType::PBytes(pbytes_value_clone));

        //Test NPBytes
        writer.clear();
        let npbytes_value = NPBytes {
            value: Some(BytesMut::from("test".as_bytes())),
        };
        let npbytes_value_clone = npbytes_value.clone();
        npbytes_value.write_to(&mut writer).await.unwrap();
        let mut buffer = BytesMut::from(&writer[..]);
        let read_npbytes = NPBytes::read_from(&mut buffer).unwrap();
        assert_eq!(read_npbytes, DataType::NPBytes(npbytes_value_clone));

        //Test PVarInt
        writer.clear();
        let pvarint_value = PVarInt { value: 12345 };
        let pvarint_value_clone = pvarint_value.clone();
        pvarint_value.write_to(&mut writer).await.unwrap();
        let mut buffer = BytesMut::from(&writer[..]);
        let read_pvarint = PVarInt::read_from(&mut buffer).unwrap();
        assert_eq!(read_pvarint, DataType::PVarInt(pvarint_value_clone));

        //Test PVarLong
        writer.clear();
        let pvarlong_value = PVarLong { value: 1234567890 };
        let pvarlong_value_clone = pvarlong_value.clone();
        pvarlong_value.write_to(&mut writer).await.unwrap();
        let mut buffer = BytesMut::from(&writer[..]);
        let read_pvarlong = PVarLong::read_from(&mut buffer).unwrap();
        assert_eq!(read_pvarlong, DataType::PVarLong(pvarlong_value_clone));
    }
}
