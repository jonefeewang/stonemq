use bytes::BytesMut;

use crate::message::MemoryRecords;
use crate::protocol::base::ProtocolType;
use crate::protocol::base::{Bool, NPBytes, NPString, PBytes, PString, I16, I32, I64, I8, U32};
use crate::protocol::schema_base::ValueSet;
use crate::protocol::types::ArrayType;

///
/// convert rust types to `DataType` enum variant
///
macro_rules! define_from_rust_type_to_datatype {
    ($(($variant:ident,$type:ty, $conversion:expr)),*) => {
        $(
        impl From<$type> for ProtocolType {
                fn from(value: $type) -> Self {
                    ProtocolType::$variant($variant { value: $conversion(value) })
                }
            }
        )*
    };
}
///
/// convert `DataType` enum variant to rust types
///
macro_rules! define_from_datatype_to_rust_type {
    ($variant:ident,$type:ty ) => {
        impl std::convert::From<ProtocolType> for $type {
            fn from(value: ProtocolType) -> Self {
                match value {
                    ProtocolType::$variant(data) => data.value,
                    field => {
                        let error_message =
                            format!("Expected {:?} but found {:?}", stringify!($variant), field);
                        panic!("{}", error_message);
                    }
                }
            }
        }
    };
}
///////////////////////////////////////////  DataType to Rust type ///////////////////////////////////////////

define_from_datatype_to_rust_type!(I8, i8);
define_from_datatype_to_rust_type!(I16, i16);
define_from_datatype_to_rust_type!(I32, i32);
define_from_datatype_to_rust_type!(U32, u32);
define_from_datatype_to_rust_type!(I64, i64);
define_from_datatype_to_rust_type!(PString, String);
define_from_datatype_to_rust_type!(Bool, bool);

impl From<ProtocolType> for MemoryRecords {
    fn from(value: ProtocolType) -> Self {
        match value {
            ProtocolType::Records(records) => records,
            field => {
                let error_message = format!("Expected Records but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}
impl<'a> From<&'a ProtocolType> for &'a MemoryRecords {
    fn from(value: &'a ProtocolType) -> Self {
        match value {
            ProtocolType::Records(record) => record,
            field => {
                let error_message = format!("Expected Records but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}

impl<'a> From<&'a ProtocolType> for &'a String {
    fn from(value: &'a ProtocolType) -> Self {
        match value {
            ProtocolType::PString(data) => &data.value,
            field => {
                let error_message = format!("Expected PString but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}

impl From<ProtocolType> for Option<String> {
    fn from(value: ProtocolType) -> Self {
        match value {
            ProtocolType::NPString(data) => data.value,
            field => {
                let error_message = format!("Expected NPString but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}
impl From<ProtocolType> for ValueSet {
    fn from(value: ProtocolType) -> Self {
        match value {
            ProtocolType::ValueSet(values) => values,
            field => {
                let error_message = format!("Expected SchemaValues but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}

impl From<ProtocolType> for ArrayType {
    fn from(value: ProtocolType) -> Self {
        match value {
            ProtocolType::Array(array) => array,
            field => {
                let error_message = format!("Expected Array but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}
impl<'a> From<&'a ProtocolType> for &'a ArrayType {
    fn from(value: &'a ProtocolType) -> Self {
        match value {
            ProtocolType::Array(array) => array,
            field => {
                let error_message = format!("Expected &Array but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}

impl ProtocolType {
    pub fn into_i16_type<T: From<i16>>(self) -> T {
        match self {
            ProtocolType::I16(data) => data.value.into(),
            field => {
                let error_message = format!("Expected I16 but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}

impl From<ProtocolType> for NPBytes {
    fn from(value: ProtocolType) -> Self {
        match value {
            ProtocolType::NPBytes(data) => data,
            field => {
                let error_message = format!("Expected NPBytes but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}
impl From<ProtocolType> for PBytes {
    fn from(value: ProtocolType) -> Self {
        match value {
            ProtocolType::PBytes(data) => data,
            field => {
                let error_message = format!("Expected PBytes but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}

/////////////////////////////////////////// Rust type to DataType ///////////////////////////////////////////

define_from_rust_type_to_datatype!(
    (Bool, bool, |v| v),
    (I8, i8, |v| v),
    (I16, i16, |v| v),
    (I32, i32, |v| v),
    (U32, u32, |v| v),
    (I64, i64, |v| v),
    (PBytes, BytesMut, |v| v)
);
impl From<Option<BytesMut>> for ProtocolType {
    fn from(value: Option<BytesMut>) -> Self {
        ProtocolType::NPBytes(NPBytes { value })
    }
}

impl From<String> for ProtocolType {
    fn from(value: String) -> Self {
        ProtocolType::PString(PString { value })
    }
}
impl From<Option<String>> for ProtocolType {
    fn from(value: Option<String>) -> Self {
        ProtocolType::NPString(NPString { value })
    }
}
impl From<&str> for ProtocolType {
    fn from(value: &str) -> Self {
        ProtocolType::PString(PString {
            value: value.to_string(),
        })
    }
}
impl From<Option<&str>> for ProtocolType {
    fn from(value: Option<&str>) -> Self {
        ProtocolType::NPString(NPString {
            value: value.map(|v| v.to_string()),
        })
    }
}

/////////////////////////////////////////// StoneMQ struct type to DataType ///////////////////////////////////////////

impl From<ValueSet> for ProtocolType {
    fn from(value: ValueSet) -> Self {
        ProtocolType::ValueSet(value)
    }
}

impl From<MemoryRecords> for ProtocolType {
    fn from(value: MemoryRecords) -> Self {
        ProtocolType::Records(value)
    }
}
