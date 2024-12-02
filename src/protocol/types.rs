use std::sync::Arc;

use bytes::BytesMut;

use crate::message::MemoryRecords;
use crate::protocol::array::ArrayType;
use crate::protocol::primary_types::{
    Bool, NPBytes, NPString, PBytes, PString, PVarInt, PVarLong, PrimaryType, I16, I32, I64, I8,
    U32,
};
use crate::protocol::schema::Schema;
use crate::protocol::value_set::ValueSet;

use super::{Acks, ApiKey, ApiVersion};

///
/// convert rust types to `DataType` enum variant
///
macro_rules! define_from_rust_type_to_datatype {
    ($(($variant:ident,$type:ty, $conversion:expr)),*) => {
        $(
        impl From<$type> for DataType {
                fn from(value: $type) -> Self {
                    DataType::$variant($variant { value: $conversion(value) })
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
        impl std::convert::From<DataType> for $type {
            fn from(value: DataType) -> Self {
                match value {
                    DataType::$variant(data) => data.value,
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

///
/// compare `DataType` enum
macro_rules! eq_match {
    ($self:ident, $other:ident, $( $pattern:ident ),+ $(,)?) => {
        match ($self, $other) {
            $( (DataType::$pattern(a), DataType::$pattern(b)) => a == b, )+
            (DataType::Schema(a), DataType::Schema(b)) => Arc::ptr_eq(a, b) || **a == **b,
            _ => false,
        }
    }
}

///
/// convert array of rust type to array of DataType type
macro_rules! array_of {
    ($func_name:ident, $type:ty) => {
        pub fn $func_name(value: $type) -> DataType {
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(value.into()),
                values: None,
            })
        }
    };
}

///
/// StoneMQ data types(enum and structs) used in the schema.
///
/// The `DataType` enum is used to represent different data types/structures within our schema.
/// It acts as a medium for working with many different types in a uniform way.
///
/// The `DataType` variants act as type placeholders in the schema. However, in places where we need
/// these types to also hold values, the inner struct of each enum variant provides that functionality.
/// So, each variant's inner struct facilitates two roles:
/// 1. As a type placeholder.
/// 2. As a value holder for the particular type.
#[derive(Debug, Clone)]
pub enum DataType {
    // Represents a boolean type.
    Bool(Bool),

    // Represents a signed 8-bit integer type.
    I8(I8),

    // Represents a signed 16-bit integer type.
    I16(I16),

    // Represents a signed 32-bit integer type.
    I32(I32),

    // Represents an unsigned 32-bit integer type.
    U32(U32),

    // Represents a signed 64-bit integer type.
    I64(I64),

    // Represents a `PString`
    // (Protocol String format, distinguished from the standard String).
    PString(PString),

    // Represents a `NPString`
    // (Nullable Protocol String format, distinguished from the standard String).
    NPString(NPString),

    // Represents byte data under `PBytes`(Protocol Bytes).
    // (Protocol Bytes format, distinguished from the Bytes crate).
    PBytes(PBytes),

    // Represents byte data under `NPBytes`(Nullable Protocol Bytes).
    // (Nullable Protocol Bytes format, distinguished from the Bytes crate).
    NPBytes(NPBytes),

    // Represents a variant int format under `PVarInt`(Protocol VarInt).
    // (Protocol VarInt format, distinguished from crate type VarInt).
    PVarInt(PVarInt),

    // Represents a variant long format under `PVarLong`(Protocol VarLong).
    // (Protocol VarLong format, distinguished from crate type VarLong).
    PVarLong(PVarLong),

    // Represents an array of any FieldTypes.
    // array里包含一个DataType, 这里是一个循环引用，这里使用Arc来包装
    // 不能使用Rc包装的原因: 因为Schema要作为全局变量使用，需要在多个地方引用，而Rc只能在单线程中使用
    Array(ArrayType),

    // Represents the record type in memory.
    Records(MemoryRecords),

    // Represents a pointer to a schema.
    // 为了能在其他的数据结构体里引用这里边包装的Schema，这里使用Arc包装
    // 不能使用Rc包装的原因: 因为Schema要作为全局变量使用，需要在多个地方引用，而Rc只能在单线程中使用
    Schema(Arc<Schema>),

    // Represents a schema's data(value) type.
    ValueSet(ValueSet),
}

impl PartialEq for DataType {
    fn eq(&self, other: &Self) -> bool {
        eq_match! {
            self, other,
            Bool, I8, I16, I32, U32, I64, PString,
            NPString, PBytes, NPBytes, PVarInt, PVarLong,
            Array, Records, ValueSet
        }
    }
}
impl Eq for DataType {}

impl DataType {
    ///
    /// 生成一个array of value set 数据类型
    pub fn array_of_value_set(values: Vec<DataType>, schema: Arc<Schema>) -> DataType {
        DataType::Array(ArrayType {
            can_be_empty: false,
            p_type: Arc::new(DataType::ValueSet(ValueSet::new(schema))),
            values: Some(values),
        })
    }
    ///
    /// 生成一个array of schema 数据类型
    pub fn array_of_schema(schema: Arc<Schema>) -> DataType {
        DataType::Array(ArrayType {
            can_be_empty: false,
            p_type: Arc::new(DataType::Schema(schema)),
            values: None,
        })
    }
    pub fn array_of<T: Default + Into<DataType>>(value: Option<Vec<T>>) -> DataType {
        let values = match value {
            None => None,
            Some(value) => {
                let ary: Vec<DataType> = value.into_iter().map(|v| v.into()).collect();
                Some(ary)
            }
        };
        let can_be_empty = match &values {
            None => true,
            Some(ary) => ary.is_empty(),
        };

        DataType::Array(ArrayType {
            can_be_empty,
            p_type: Arc::new(T::default().into()),
            values,
        })
    }
    array_of!(array_of_i32_type, i32);
    array_of!(array_of_string_type, String);
    pub fn size(&self) -> i32 {
        match self {
            DataType::Bool(bool) => bool.wire_format_size() as i32,
            DataType::I8(i8) => i8.wire_format_size() as i32,
            DataType::I16(i16) => i16.wire_format_size() as i32,
            DataType::I32(i32) => i32.wire_format_size() as i32,
            DataType::U32(u32) => u32.wire_format_size() as i32,
            DataType::I64(i64) => i64.wire_format_size() as i32,
            DataType::PString(string) => string.wire_format_size() as i32,
            DataType::NPString(nstring) => nstring.wire_format_size() as i32,
            DataType::PBytes(bytes) => bytes.wire_format_size() as i32,
            DataType::NPBytes(nbytes) => nbytes.wire_format_size() as i32,
            DataType::PVarInt(varint) => varint.wire_format_size() as i32,
            DataType::PVarLong(varlong) => varlong.wire_format_size() as i32,
            DataType::Array(array) => array.size() as i32,
            DataType::Records(records) => records.wire_format_size() as i32,
            DataType::Schema(_) => {
                panic!("Unexpected calculation of schema size");
            }
            DataType::ValueSet(valueset) => valueset.size() as i32,
        }
    }
}

///////////////////////////////////////////  DataType to Rust type ///////////////////////////////////////////

define_from_datatype_to_rust_type!(I8, i8);
define_from_datatype_to_rust_type!(I16, i16);
define_from_datatype_to_rust_type!(I32, i32);
define_from_datatype_to_rust_type!(U32, u32);
define_from_datatype_to_rust_type!(I64, i64);
define_from_datatype_to_rust_type!(PString, String);
define_from_datatype_to_rust_type!(Bool, bool);

impl From<DataType> for MemoryRecords {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Records(records) => records,
            field => {
                let error_message = format!("Expected Records but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}
impl<'a> From<&'a DataType> for &'a MemoryRecords {
    fn from(value: &'a DataType) -> Self {
        match value {
            DataType::Records(record) => record,
            field => {
                let error_message = format!("Expected Records but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}

impl<'a> From<&'a DataType> for &'a String {
    fn from(value: &'a DataType) -> Self {
        match value {
            DataType::PString(data) => &data.value,
            field => {
                let error_message = format!("Expected PString but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}

impl From<DataType> for Option<String> {
    fn from(value: DataType) -> Self {
        match value {
            DataType::NPString(data) => match data.value {
                None => None,
                Some(value) => Some(value),
            },
            field => {
                let error_message = format!("Expected MPString but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}
impl From<DataType> for ValueSet {
    fn from(value: DataType) -> Self {
        match value {
            DataType::ValueSet(values) => values,
            field => {
                let error_message = format!("Expected SchemaValues but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}

impl From<DataType> for ArrayType {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Array(array) => array,
            field => {
                let error_message = format!("Expected Array but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}
impl<'a> From<&'a DataType> for &'a ArrayType {
    fn from(value: &'a DataType) -> Self {
        match value {
            DataType::Array(array) => array,
            field => {
                let error_message = format!("Expected &Array but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}

impl DataType {
    pub fn into_i16_type<T: From<i16>>(self) -> T {
        match self {
            DataType::I16(data) => data.value.into(),
            field => {
                let error_message = format!("Expected I16 but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}

impl From<DataType> for NPBytes {
    fn from(value: DataType) -> Self {
        match value {
            DataType::NPBytes(data) => data,
            field => {
                let error_message = format!("Expected NPBytes but found {:?}", field);
                panic!("{}", error_message);
            }
        }
    }
}
impl From<DataType> for PBytes {
    fn from(value: DataType) -> Self {
        match value {
            DataType::PBytes(data) => data,
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
impl From<Option<BytesMut>> for DataType {
    fn from(value: Option<BytesMut>) -> Self {
        DataType::NPBytes(NPBytes { value })
    }
}

impl From<String> for DataType {
    fn from(value: String) -> Self {
        DataType::PString(PString { value })
    }
}
impl From<Option<String>> for DataType {
    fn from(value: Option<String>) -> Self {
        DataType::NPString(NPString { value })
    }
}
impl From<&str> for DataType {
    fn from(value: &str) -> Self {
        DataType::PString(PString {
            value: value.to_string(),
        })
    }
}
impl From<Option<&str>> for DataType {
    fn from(value: Option<&str>) -> Self {
        DataType::NPString(NPString {
            value: value.map(|v| v.to_string()),
        })
    }
}

/////////////////////////////////////////// StoneMQ struct type to DataType ///////////////////////////////////////////
impl From<ApiVersion> for DataType {
    fn from(value: ApiVersion) -> Self {
        DataType::I16(I16 {
            value: value as i16,
        })
    }
}
impl From<ApiKey> for DataType {
    fn from(value: ApiKey) -> Self {
        DataType::I16(I16 {
            value: value as i16,
        })
    }
}
impl From<Acks> for DataType {
    fn from(value: Acks) -> Self {
        match value {
            Acks::All => (-1).into(),
            Acks::Leader => 1.into(),
            Acks::None => 0.into(),
        }
    }
}

impl From<ValueSet> for DataType {
    fn from(value: ValueSet) -> Self {
        DataType::ValueSet(value)
    }
}

impl From<MemoryRecords> for DataType {
    fn from(value: MemoryRecords) -> Self {
        DataType::Records(value)
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryInto;

    use super::*;

    #[test]
    fn test_datatype_conversions() {
        // Test conversion from i8 to DataType
        let i8_value = 8i8;
        let data_type: DataType = i8_value.into();
        assert_eq!(data_type, DataType::I8(I8 { value: 8i8 }));

        // Test conversion from DataType to i8
        let data_type = DataType::I8(I8 { value: 8i8 });
        let value: Result<i8, _> = data_type.try_into();
        assert_eq!(value.unwrap(), 8i8);

        // Test conversion from String to DataType
        let string_value = "test".to_string();
        let data_type: DataType = string_value.into();
        assert_eq!(
            data_type,
            DataType::PString(PString {
                value: "test".to_string()
            })
        );

        // Test conversion from DataType to String
        let data_type = DataType::PString(PString {
            value: "test".to_string(),
        });
        let value: Result<String, _> = data_type.try_into();
        assert_eq!(value.unwrap(), "test".to_string());

        // Test conversion from Option<String> to DataType
        let string_value = Some("test".to_string());
        let data_type: DataType = string_value.into();
        assert_eq!(
            data_type,
            DataType::NPString(NPString {
                value: Some("test".to_string())
            })
        );

        // Test conversion from DataType to Option<String>
        let data_type = DataType::NPString(NPString {
            value: Some("test".to_string()),
        });
        let value: Result<Option<String>, _> = data_type.try_into();
        assert_eq!(value.unwrap(), Some("test".to_string()));

        // Test conversion from Option<Bytes> to DataType
        let bytes_value = Some(BytesMut::from("test"));
        let data_type: DataType = bytes_value.into();

        assert_eq!(
            data_type,
            DataType::NPBytes(NPBytes {
                value: Some(BytesMut::from("test".as_bytes()))
            })
        );
    }

    #[test]
    fn test_datatype_equality() {
        // Test equality for DataType::I8
        let data_type1 = DataType::I8(I8 { value: 8i8 });
        let data_type2 = DataType::I8(I8 { value: 8i8 });
        assert_eq!(data_type1, data_type2);

        // Test equality for DataType::PString
        let data_type1 = DataType::PString(PString {
            value: "test".to_string(),
        });
        let data_type2 = DataType::PString(PString {
            value: "test".to_string(),
        });
        assert_eq!(data_type1, data_type2);
    }
}
