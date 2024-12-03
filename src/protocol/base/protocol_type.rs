use std::sync::Arc;

use crate::message::MemoryRecords;
use crate::protocol::base::{
    Bool, NPBytes, NPString, PBytes, PString, PVarInt, PVarLong, PrimaryType, I16, I32, I64, I8,
    U32,
};
use crate::protocol::schema_base::Schema;
use crate::protocol::schema_base::ValueSet;
use crate::protocol::types::ArrayType;
///
/// compare `DataType` enum
macro_rules! eq_match {
    ($self:ident, $other:ident, $( $pattern:ident ),+ $(,)?) => {
        match ($self, $other) {
            $( (ProtocolType::$pattern(a), ProtocolType::$pattern(b)) => a == b, )+
            (ProtocolType::Schema(a), ProtocolType::Schema(b)) => Arc::ptr_eq(a, b) || **a == **b,
            _ => false,
        }
    }
}

///
/// convert array of rust type to array of DataType type
macro_rules! array_of {
    ($func_name:ident, $type:ty) => {
        pub fn $func_name(value: $type) -> ProtocolType {
            ProtocolType::Array(ArrayType {
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
pub enum ProtocolType {
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

impl PartialEq for ProtocolType {
    fn eq(&self, other: &Self) -> bool {
        eq_match! {
            self, other,
            Bool, I8, I16, I32, U32, I64, PString,
            NPString, PBytes, NPBytes, PVarInt, PVarLong,
            Array, Records, ValueSet
        }
    }
}
impl Eq for ProtocolType {}

impl ProtocolType {
    ///
    /// 生成一个array of value set 数据类型
    pub fn array_of_value_set(values: Vec<ProtocolType>, schema: Arc<Schema>) -> ProtocolType {
        ProtocolType::Array(ArrayType {
            can_be_empty: false,
            p_type: Arc::new(ProtocolType::ValueSet(ValueSet::new(schema))),
            values: Some(values),
        })
    }
    ///
    /// 生成一个array of schema 数据类型
    pub fn array_of_schema(schema: Arc<Schema>) -> ProtocolType {
        ProtocolType::Array(ArrayType {
            can_be_empty: false,
            p_type: Arc::new(ProtocolType::Schema(schema)),
            values: None,
        })
    }
    pub fn array_of<T: Default + Into<ProtocolType>>(value: Option<Vec<T>>) -> ProtocolType {
        let values = match value {
            None => None,
            Some(value) => {
                let ary: Vec<ProtocolType> = value.into_iter().map(|v| v.into()).collect();
                Some(ary)
            }
        };
        let can_be_empty = match &values {
            None => true,
            Some(ary) => ary.is_empty(),
        };

        ProtocolType::Array(ArrayType {
            can_be_empty,
            p_type: Arc::new(T::default().into()),
            values,
        })
    }
    array_of!(array_of_i32_type, i32);
    array_of!(array_of_string_type, String);
    pub fn size(&self) -> i32 {
        match self {
            ProtocolType::Bool(bool) => bool.wire_format_size() as i32,
            ProtocolType::I8(i8) => i8.wire_format_size() as i32,
            ProtocolType::I16(i16) => i16.wire_format_size() as i32,
            ProtocolType::I32(i32) => i32.wire_format_size() as i32,
            ProtocolType::U32(u32) => u32.wire_format_size() as i32,
            ProtocolType::I64(i64) => i64.wire_format_size() as i32,
            ProtocolType::PString(string) => string.wire_format_size() as i32,
            ProtocolType::NPString(nstring) => nstring.wire_format_size() as i32,
            ProtocolType::PBytes(bytes) => bytes.wire_format_size() as i32,
            ProtocolType::NPBytes(nbytes) => nbytes.wire_format_size() as i32,
            ProtocolType::PVarInt(varint) => varint.wire_format_size() as i32,
            ProtocolType::PVarLong(varlong) => varlong.wire_format_size() as i32,
            ProtocolType::Array(array) => array.size() as i32,
            ProtocolType::Records(records) => records.wire_format_size() as i32,
            ProtocolType::Schema(_) => {
                panic!("Unexpected calculation of schema size");
            }
            ProtocolType::ValueSet(valueset) => valueset.size() as i32,
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_datatype_conversions() {
        // Test conversion from i8 to DataType
        let i8_value = 8i8;
        let data_type: ProtocolType = i8_value.into();
        assert_eq!(data_type, ProtocolType::I8(I8 { value: 8i8 }));

        // Test conversion from DataType to i8
        let data_type = ProtocolType::I8(I8 { value: 8i8 });
        let value: i8 = data_type.into();
        assert_eq!(value, 8i8);

        // Test conversion from String to DataType
        let string_value = "test".to_string();
        let data_type: ProtocolType = string_value.into();
        assert_eq!(
            data_type,
            ProtocolType::PString(PString {
                value: "test".to_string()
            })
        );

        // Test conversion from DataType to String
        let data_type = ProtocolType::PString(PString {
            value: "test".to_string(),
        });
        let value: String = data_type.into();
        assert_eq!(value, "test".to_string());

        // Test conversion from Option<String> to DataType
        let string_value = Some("test".to_string());
        let data_type: ProtocolType = string_value.into();
        assert_eq!(
            data_type,
            ProtocolType::NPString(NPString {
                value: Some("test".to_string())
            })
        );

        // Test conversion from DataType to Option<String>
        let data_type = ProtocolType::NPString(NPString {
            value: Some("test".to_string()),
        });
        let value: Option<String> = data_type.into();
        assert_eq!(value, Some("test".to_string()));

        // Test conversion from Option<Bytes> to DataType
        let bytes_value = Some(BytesMut::from("test"));
        let data_type: ProtocolType = bytes_value.into();

        assert_eq!(
            data_type,
            ProtocolType::NPBytes(NPBytes {
                value: Some(BytesMut::from("test".as_bytes()))
            })
        );
    }

    #[test]
    fn test_datatype_equality() {
        // Test equality for DataType::I8
        let data_type1 = ProtocolType::I8(I8 { value: 8i8 });
        let data_type2 = ProtocolType::I8(I8 { value: 8i8 });
        assert_eq!(data_type1, data_type2);

        // Test equality for DataType::PString
        let data_type1 = ProtocolType::PString(PString {
            value: "test".to_string(),
        });
        let data_type2 = ProtocolType::PString(PString {
            value: "test".to_string(),
        });
        assert_eq!(data_type1, data_type2);
    }
}
