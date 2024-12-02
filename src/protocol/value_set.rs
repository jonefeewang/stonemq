use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::BytesMut;

use crate::protocol::array::ArrayType;
use crate::protocol::primary_types::PrimaryType;
use crate::protocol::schema::Schema;
use crate::protocol::types::DataType;

///
/// ValueSet是一个schema相对应的值，同样也是一个有序的序列，这里使用BTreeMap来存储
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ValueSet {
    // 这里是一个指向schema的引用, 但是ValueSet本身也是一个DataType，DataType是一个递归结构体，如果使用
    // 普通引用的话，会造成循环引用，引用解析非常复杂，所以这里使用Arc
    // 不能使用Rc包装的原因: 因为Schema要作为全局变量使用，需要在多个地方引用，而Rc只能在单线程中使用
    pub schema: Arc<Schema>,
    pub values: BTreeMap<i32, DataType>,
}
impl ValueSet {
    pub fn new(schema: Arc<Schema>) -> ValueSet {
        ValueSet {
            schema,
            values: BTreeMap::new(),
        }
    }

    pub fn size(&self) -> usize {
        let mut total_size = 0usize;
        for data in self.values.values() {
            let size = match data {
                DataType::Bool(bool) => bool.wire_format_size(),
                DataType::I8(i8) => i8.wire_format_size(),
                DataType::I16(i16) => i16.wire_format_size(),
                DataType::I32(i32) => i32.wire_format_size(),
                DataType::U32(u32) => u32.wire_format_size(),
                DataType::I64(i164) => i164.wire_format_size(),
                DataType::PString(pstring) => pstring.wire_format_size(),
                DataType::NPString(npstring) => npstring.wire_format_size(),
                DataType::PBytes(pbytes) => pbytes.wire_format_size(),
                DataType::NPBytes(npbytes) => npbytes.wire_format_size(),
                DataType::PVarInt(pvarint) => pvarint.wire_format_size(),
                DataType::PVarLong(pvarlong) => pvarlong.wire_format_size(),
                DataType::Array(array) => array.size(),
                DataType::Records(records) => records.wire_format_size(),
                DataType::ValueSet(data) => data.size(),
                DataType::Schema(_) => {
                    panic!("Schema type should not be in the values");
                }
            };
            total_size += size;
        }
        total_size
    }

    pub fn sub_valueset_of_ary_field(&self, field_name: &'static str) -> ValueSet {
        let array_field = self.schema.get_field(field_name);
        let array_type: &ArrayType = (&array_field.p_type).into();

        if let DataType::Schema(ref schema) = &*array_type.p_type {
            ValueSet {
                schema: schema.clone(),
                values: BTreeMap::new(),
            }
        } else {
            panic!("Array type must be schema type")
        }
    }

    pub fn sub_valueset_of_schema_field(&self, field_name: &'static str) -> ValueSet {
        let schema_field = self.schema.get_field(field_name);
        if let DataType::Schema(ref schema) = schema_field.p_type {
            ValueSet::new(schema.clone())
        } else {
            panic!("field type must be schema type")
        }
    }

    pub fn append_field_value(&mut self, field_name: &'static str, new_value: DataType) {
        //check if field exists
        let field = self.schema.get_field(field_name);
        self.values.insert(field.index, new_value);
        if field.index + 1 != self.values.len() as i32 {
            panic!(
                "field index not match, expect:{},actual:{} with filed name:{}",
                field.index + 1,
                self.values.len(),
                field_name
            );
        }
    }

    pub fn get_field_value(&mut self, field_name: &'static str) -> DataType {
        let index = self.schema.get_field_index(field_name);
        if let Some(field) = self.values.remove(&index) {
            field
        } else {
            let error_message = format!("field not found:{} in value set", field_name);
            panic!("{}", error_message);
        }
    }

    pub fn write_to(self, writer: &mut BytesMut) {
        for (_, value) in self.values {
            match value {
                DataType::Bool(bool) => bool.encode(writer),
                DataType::I8(i8) => i8.encode(writer),
                DataType::I16(i16) => i16.encode(writer),
                DataType::I32(i32) => i32.encode(writer),
                DataType::U32(u32) => u32.encode(writer),
                DataType::I64(i64) => i64.encode(writer),
                DataType::PString(string) => string.encode(writer),
                DataType::NPString(npstring) => npstring.encode(writer),
                DataType::PBytes(bytes) => bytes.encode(writer),
                DataType::NPBytes(npbytes) => npbytes.encode(writer),
                DataType::PVarInt(pvarint) => pvarint.encode(writer),
                DataType::PVarLong(pvarlong) => pvarlong.encode(writer),
                DataType::Array(array) => array.encode(writer),
                DataType::Records(records) => records.encode(writer),
                //should never happen
                DataType::Schema(schema) => {
                    panic!("unexpected type schema:{:?}", schema);
                }
                // 只允许value set嵌套 valueset 或 array
                DataType::ValueSet(sub_value_set) => sub_value_set.write_to(writer),
            }
        }
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_schema_data_read_write() {
        use super::*;
        use crate::protocol::primary_types::PString;
        use crate::protocol::primary_types::I32;
        let mut writer = BytesMut::new();
        let schema = Arc::new(Schema::from_fields_desc_vec(vec![
            (0, "field1", DataType::I32(I32::default())),
            (1, "field2", DataType::PString(PString::default())),
        ]));
        let schema_clone = Arc::clone(&schema);
        let mut value_set = ValueSet::new(schema);
        value_set.append_field_value("field1", DataType::I32(I32 { value: 1 }));
        value_set.append_field_value(
            "field2",
            DataType::PString(PString {
                value: "test".to_string(),
            }),
        );
        let value_set_clone = value_set.clone();

        // write
        value_set.write_to(&mut writer);

        // read
        let mut buffer = BytesMut::from(&writer[..]);
        let read_value_set = schema_clone.read_from(&mut buffer).unwrap();
        assert_eq!(read_value_set, value_set_clone);
    }

    #[test]
    fn test_two_hierarchy_schema_read_write() {
        struct Inner {
            inner_field1: i32,
            inner_field2: String,
        }

        struct Outer {
            outer_field1: i32,
            outer_field2: Vec<Inner>,
        }
        use super::*;
        use crate::protocol::primary_types::{PString, I32};
        const INNER_FIELD1: &str = "inner_field1";
        const INNER_FIELD2: &str = "inner_field2";
        const OUTER_FIELD1: &str = "outer_field1";
        const OUTER_FIELD2: &str = "outer_field2";

        //create schema
        let fields_desc = vec![
            (0, INNER_FIELD1, DataType::I32(I32::default())),
            (1, INNER_FIELD2, DataType::PString(PString::default())),
        ];
        let inner_schema = Schema::from_fields_desc_vec(fields_desc);
        use crate::protocol::array::ArrayType;
        let array = ArrayType {
            can_be_empty: false,
            p_type: Arc::new(inner_schema.into()),
            values: None,
        };

        let outer_fields_desc = vec![
            (0, OUTER_FIELD1, DataType::I32(I32::default())),
            (1, OUTER_FIELD2, DataType::Array(array)),
        ];

        let outer_schema = Arc::new(Schema::from_fields_desc_vec(outer_fields_desc));

        //create data
        let inner1 = Inner {
            inner_field1: 1,
            inner_field2: "test".to_string(),
        };
        let inner2 = Inner {
            inner_field1: 2,
            inner_field2: "test2".to_string(),
        };
        let outer = Outer {
            outer_field1: 1,
            outer_field2: vec![inner1, inner2],
        };

        //to structure
        let mut outer_value_set = ValueSet::new(outer_schema.clone());
        outer_value_set.append_field_value(OUTER_FIELD1, outer.outer_field1.into());

        let mut inner_array = Vec::with_capacity(outer.outer_field2.len());
        for inner in outer.outer_field2 {
            let mut inner_value_set = outer_value_set.sub_valueset_of_ary_field(OUTER_FIELD2);

            inner_value_set.append_field_value(INNER_FIELD1, inner.inner_field1.into());
            inner_value_set.append_field_value(INNER_FIELD2, inner.inner_field2.into());
            inner_array.push(DataType::ValueSet(inner_value_set));
        }

        let schema = Schema::sub_schema_of_ary_field(outer_value_set.schema.clone(), OUTER_FIELD2);
        let array = DataType::array_of_value_set(inner_array, schema);

        outer_value_set.append_field_value(OUTER_FIELD2, array);

        let outer_value_set_clone = outer_value_set.clone();

        //write to buffer
        let mut writer = BytesMut::new();
        outer_value_set.write_to(&mut writer);

        //read from buffer
        let mut buffer = BytesMut::from(&writer[..]);
        let read_outer_structure = outer_schema.read_from(&mut buffer).unwrap();
        //check
        assert_eq!(read_outer_structure, outer_value_set_clone);
    }

    #[test]
    fn test_nested_value_set() {
        use super::*;
        use crate::protocol::primary_types::{PString, I32};

        // 创建内部schema
        let inner_schema = Arc::new(Schema::from_fields_desc_vec(vec![
            (0, "inner_field1", DataType::I32(I32::default())),
            (1, "inner_field2", DataType::PString(PString::default())),
        ]));

        // 创建外部schema,包含一个内部schema字段
        let outer_schema = Arc::new(Schema::from_fields_desc_vec(vec![
            (0, "outer_field1", DataType::I32(I32::default())),
            (1, "outer_field2", DataType::Schema(inner_schema.clone())),
        ]));

        // 创建外部value set
        let mut outer_value_set = ValueSet::new(outer_schema.clone());
        outer_value_set.append_field_value("outer_field1", DataType::I32(I32 { value: 1 }));

        // 创建内部value set
        let mut inner_value_set = outer_value_set.sub_valueset_of_schema_field("outer_field2");
        inner_value_set.append_field_value("inner_field1", DataType::I32(I32 { value: 2 }));
        inner_value_set.append_field_value(
            "inner_field2",
            DataType::PString(PString {
                value: "test".to_string(),
            }),
        );

        // 将内部value set添加到外部value set
        outer_value_set.append_field_value("outer_field2", DataType::ValueSet(inner_value_set));

        let outer_value_set_clone = outer_value_set.clone();

        // 写入buffer
        let mut writer = BytesMut::new();
        outer_value_set.write_to(&mut writer);

        // 从buffer读取
        let mut buffer = BytesMut::from(&writer[..]);
        let read_outer_value_set = outer_schema.read_from(&mut buffer).unwrap();

        // 验证
        assert_eq!(read_outer_value_set, outer_value_set_clone);
    }
}
