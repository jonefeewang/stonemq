use std::fmt::Debug;

use bytes::{Buf, BufMut, BytesMut};
use integer_encoding::VarInt;

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
            fn decode(buffer: &mut BytesMut) -> DataType {
                let value = buffer.$read_method();
                DataType::$return_type($type { value })
            }
            fn encode(&self, writer: &mut BytesMut) {
                writer.$write_method(self.value);
            }
            fn wire_format_size(&self) -> usize {
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
            fn decode(buffer: &mut BytesMut) -> DataType {
                let var = $read_method(buffer.as_ref());
                return if let Some((value, read_size)) = var {
                    // Skip the record length field that was just parsed.
                    buffer.advance(read_size);
                    DataType::$data_type($t { value })
                } else {
                    panic!("can not read a {}", stringify!($t));
                };
            }

            fn encode(&self, writer: &mut BytesMut) {
                let var = $encode_var_vec(self.value);
                writer.put_slice(var.as_slice());
            }

            fn wire_format_size(&self) -> usize {
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
    fn decode(buffer: &mut BytesMut) -> DataType;
    fn encode(&self, writer: &mut BytesMut);

    fn wire_format_size(&self) -> usize;
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

implement_primary_type!(I8, I8, get_i8, put_i8, 1);
implement_primary_type!(I16, I16, get_i16, put_i16, 2);
implement_primary_type!(I32, I32, get_i32, put_i32, 4);
implement_primary_type!(U32, U32, get_u32, put_u32, 4);
implement_primary_type!(I64, I64, get_i64, put_i64, 8);

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
    fn decode(buffer: &mut BytesMut) -> DataType {
        let length = buffer.get_i32();
        if length < 0 {
            panic!("can not read a PBytes, length is negative");
        } else {
            DataType::PBytes(PBytes {
                value: buffer.split_to(length as usize),
            })
        }
    }
    fn encode(&self, writer: &mut BytesMut) {
        let length = self.value.remaining();
        writer.put_i32(length as i32);
        writer.put_slice(&self.value);
    }

    fn wire_format_size(&self) -> usize {
        4 + self.value.remaining()
    }
}

impl PrimaryType for NPBytes {
    fn decode(buffer: &mut BytesMut) -> DataType {
        let length = buffer.get_i32();
        if length < 0 {
            DataType::NPBytes(NPBytes { value: None })
        } else {
            DataType::NPBytes(NPBytes {
                value: Some(buffer.split_to(length as usize)),
            })
        }
    }
    fn encode(&self, writer: &mut BytesMut) {
        if let Some(value) = &self.value {
            let length = value.remaining();
            writer.put_i32(length as i32);
            writer.put_slice(value);
        } else {
            writer.put_i32(-1);
        }
    }
    fn wire_format_size(&self) -> usize {
        if let Some(value) = &self.value {
            4 + value.remaining()
        } else {
            4
        }
    }
}

impl PrimaryType for PString {
    fn decode(buffer: &mut BytesMut) -> DataType {
        let length = buffer.get_i16();
        if length < 0 {
            panic!("String length: {:?} can not be negative", length);
        } else {
            // trace!("Reading PString with length: {}", length);
            DataType::PString(PString {
                value: String::from_utf8(buffer.split_to(length as usize).to_vec()).unwrap(),
            })
        }
    }
    fn encode(&self, writer: &mut BytesMut) {
        let length = self.value.len();
        writer.put_i16(length as i16);
        writer.put_slice(self.value.as_bytes());
    }
    fn wire_format_size(&self) -> usize {
        2 + self.value.len()
    }
}
impl PrimaryType for NPString {
    fn decode(buffer: &mut BytesMut) -> DataType {
        let length = buffer.get_i16();
        if length < 0 {
            DataType::NPString(NPString { value: None })
        } else {
            // trace!("Reading NPString with length: {}", length);
            DataType::NPString(NPString {
                value: Some(String::from_utf8(buffer.split_to(length as usize).to_vec()).unwrap()),
            })
        }
    }
    fn encode(&self, writer: &mut BytesMut) {
        if let Some(value) = &self.value {
            let length = value.len();
            writer.put_i16(length as i16);
            writer.put_slice(value.as_bytes());
        } else {
            writer.put_i16(-1);
        }
    }
    fn wire_format_size(&self) -> usize {
        if let Some(value) = &self.value {
            2 + value.len()
        } else {
            2
        }
    }
}

impl PrimaryType for Bool {
    fn decode(buffer: &mut BytesMut) -> DataType {
        let value = buffer.get_i8();
        match value {
            0 => DataType::Bool(Bool { value: false }),
            1 => DataType::Bool(Bool { value: true }),
            _ => panic!("Invalid value for bool."),
        }
    }

    fn encode(&self, writer: &mut BytesMut) {
        let value = if self.value { 1 } else { 0 };
        writer.put_i8(value);
    }

    fn wire_format_size(&self) -> usize {
        1
    }
}
#[cfg(test)]
mod test {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_primary_type_read_write() {
        let mut writer = BytesMut::new();

        // Test Bool
        let bool_value = Bool { value: true };
        bool_value.encode(&mut writer);
        let mut bytes_mut = BytesMut::from(&writer[..]);
        let read_bool = Bool::decode(&mut bytes_mut);
        assert_eq!(read_bool, DataType::Bool(Bool { value: true }));

        // Test I8
        writer.clear();
        let i8_value = I8 { value: 127 };
        let i8_value_clone = i8_value.clone();
        i8_value.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_i8 = I8::decode(&mut buffer);
        assert_eq!(read_i8, DataType::I8(i8_value_clone));

        // Test I16
        writer.clear();
        let i16_value = I16 { value: 32767 };
        let i16_value_clone = i16_value.clone();
        i16_value.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_i16 = I16::decode(&mut buffer);
        assert_eq!(read_i16, DataType::I16(i16_value_clone));

        // Test I32
        writer.clear();
        let i32_value = I32 { value: 2147483647 };
        let i32_value_clone = i32_value.clone();
        i32_value.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_i32 = I32::decode(&mut buffer);
        assert_eq!(read_i32, DataType::I32(i32_value_clone));

        // Test U32
        writer.clear();
        let u32_value = U32 { value: 4294967295 };
        let u32_value_clone = u32_value.clone();
        u32_value.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_u32 = U32::decode(&mut buffer);
        assert_eq!(read_u32, DataType::U32(u32_value_clone));

        // Test I64
        writer.clear();
        let i64_value = I64 {
            value: 9223372036854775807,
        };
        let i64_value_clone = i64_value.clone();
        i64_value.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_i64 = I64::decode(&mut buffer);
        assert_eq!(read_i64, DataType::I64(i64_value_clone));

        // Test PString
        writer.clear();
        let pstring_value = PString {
            value: "test".to_string(),
        };
        let pstring_value_clone = pstring_value.clone();
        pstring_value.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_pstring = PString::decode(&mut buffer);
        assert_eq!(read_pstring, DataType::PString(pstring_value_clone));

        // Test NPString
        buffer.clear();
        let npstring_value = NPString {
            value: Some("test".to_string()),
        };
        let npstring_value_clone = npstring_value.clone();
        npstring_value.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_npstring = NPString::decode(&mut buffer);
        assert_eq!(read_npstring, DataType::NPString(npstring_value_clone));

        //Test PBytes
        writer.clear();
        let pbytes_value = PBytes {
            value: BytesMut::from("test".as_bytes()),
        };
        let pbytes_value_clone = pbytes_value.clone();
        pbytes_value.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_pbytes = PBytes::decode(&mut buffer);
        assert_eq!(read_pbytes, DataType::PBytes(pbytes_value_clone));

        //Test NPBytes
        writer.clear();
        let npbytes_value = NPBytes {
            value: Some(BytesMut::from("test".as_bytes())),
        };
        let npbytes_value_clone = npbytes_value.clone();
        npbytes_value.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_npbytes = NPBytes::decode(&mut buffer);
        assert_eq!(read_npbytes, DataType::NPBytes(npbytes_value_clone));

        //Test PVarInt
        writer.clear();
        let pvarint_value = PVarInt { value: 12345 };
        let pvarint_value_clone = pvarint_value.clone();
        pvarint_value.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_pvarint = PVarInt::decode(&mut buffer);
        assert_eq!(read_pvarint, DataType::PVarInt(pvarint_value_clone));

        //Test PVarLong
        writer.clear();
        let pvarlong_value = PVarLong { value: 1234567890 };
        let pvarlong_value_clone = pvarlong_value.clone();
        pvarlong_value.encode(&mut writer);
        let mut buffer = BytesMut::from(&writer[..]);
        let read_pvarlong = PVarLong::decode(&mut buffer);
        assert_eq!(read_pvarlong, DataType::PVarLong(pvarlong_value_clone));
    }
}
