use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::BytesMut;

use crate::protocol::base::{PrimaryType, ProtocolType};
use crate::protocol::schema::Schema;
use crate::protocol::types::ArrayType;

///
/// ValueSet是一个schema相对应的值，同样也是一个有序的序列，这里使用BTreeMap来存储
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ValueSet {
    // 这里是一个指向schema的引用, 但是ValueSet本身也是一个DataType，DataType是一个递归结构体，如果使用
    // 普通引用的话，会造成循环引用，引用解析非常复杂，所以这里使用Arc
    // 不能使用Rc包装的原因: 因为Schema要作为全局变量使用，需要在多个地方引用，而Rc只能在单线程中使用
    pub schema: Arc<Schema>,
    pub values: BTreeMap<i32, ProtocolType>,
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
                ProtocolType::Bool(bool) => bool.wire_format_size(),
                ProtocolType::I8(i8) => i8.wire_format_size(),
                ProtocolType::I16(i16) => i16.wire_format_size(),
                ProtocolType::I32(i32) => i32.wire_format_size(),
                ProtocolType::U32(u32) => u32.wire_format_size(),
                ProtocolType::I64(i164) => i164.wire_format_size(),
                ProtocolType::PString(pstring) => pstring.wire_format_size(),
                ProtocolType::NPString(npstring) => npstring.wire_format_size(),
                ProtocolType::PBytes(pbytes) => pbytes.wire_format_size(),
                ProtocolType::NPBytes(npbytes) => npbytes.wire_format_size(),
                ProtocolType::PVarInt(pvarint) => pvarint.wire_format_size(),
                ProtocolType::PVarLong(pvarlong) => pvarlong.wire_format_size(),
                ProtocolType::Array(array) => array.size(),
                ProtocolType::Records(records) => records.wire_format_size(),
                ProtocolType::ValueSet(data) => data.size(),
                ProtocolType::Schema(_) => {
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

        if let ProtocolType::Schema(ref schema) = &*array_type.p_type {
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
        if let ProtocolType::Schema(ref schema) = schema_field.p_type {
            ValueSet::new(schema.clone())
        } else {
            panic!("field type must be schema type")
        }
    }

    pub fn append_field_value(&mut self, field_name: &'static str, new_value: ProtocolType) {
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

    pub fn get_field_value(&mut self, field_name: &'static str) -> ProtocolType {
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
                ProtocolType::Bool(bool) => bool.encode(writer),
                ProtocolType::I8(i8) => i8.encode(writer),
                ProtocolType::I16(i16) => i16.encode(writer),
                ProtocolType::I32(i32) => i32.encode(writer),
                ProtocolType::U32(u32) => u32.encode(writer),
                ProtocolType::I64(i64) => i64.encode(writer),
                ProtocolType::PString(string) => string.encode(writer),
                ProtocolType::NPString(npstring) => npstring.encode(writer),
                ProtocolType::PBytes(bytes) => bytes.encode(writer),
                ProtocolType::NPBytes(npbytes) => npbytes.encode(writer),
                ProtocolType::PVarInt(pvarint) => pvarint.encode(writer),
                ProtocolType::PVarLong(pvarlong) => pvarlong.encode(writer),
                ProtocolType::Array(array) => array.encode(writer),
                ProtocolType::Records(records) => records.encode(writer),
                //should never happen
                ProtocolType::Schema(schema) => {
                    panic!("unexpected type schema:{:?}", schema);
                }
                // 只允许value set嵌套 valueset 或 array
                ProtocolType::ValueSet(sub_value_set) => sub_value_set.write_to(writer),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_schema_data_read_write() {
        use super::*;
        use crate::protocol::base::{PString, I32};
        let mut writer = BytesMut::new();
        let schema = Arc::new(Schema::from_fields_desc_vec(vec![
            (0, "field1", ProtocolType::I32(I32::default())),
            (1, "field2", ProtocolType::PString(PString::default())),
        ]));
        let schema_clone = Arc::clone(&schema);
        let mut value_set = ValueSet::new(schema);
        value_set.append_field_value("field1", ProtocolType::I32(I32 { value: 1 }));
        value_set.append_field_value(
            "field2",
            ProtocolType::PString(PString {
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
        use crate::protocol::base::{PString, I32};
        const INNER_FIELD1: &str = "inner_field1";
        const INNER_FIELD2: &str = "inner_field2";
        const OUTER_FIELD1: &str = "outer_field1";
        const OUTER_FIELD2: &str = "outer_field2";

        //create schema
        let fields_desc = vec![
            (0, INNER_FIELD1, ProtocolType::I32(I32::default())),
            (1, INNER_FIELD2, ProtocolType::PString(PString::default())),
        ];
        let inner_schema = Schema::from_fields_desc_vec(fields_desc);
        let array = ArrayType {
            can_be_empty: false,
            p_type: Arc::new(inner_schema.into()),
            values: None,
        };

        let outer_fields_desc = vec![
            (0, OUTER_FIELD1, ProtocolType::I32(I32::default())),
            (1, OUTER_FIELD2, ProtocolType::Array(array)),
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
            inner_array.push(ProtocolType::ValueSet(inner_value_set));
        }

        let schema = Schema::sub_schema_of_ary_field(outer_value_set.schema.clone(), OUTER_FIELD2);
        let array = ProtocolType::array_of_value_set(inner_array, schema);

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
        use crate::protocol::base::{PString, I32};

        // 创建内部schema
        let inner_schema = Arc::new(Schema::from_fields_desc_vec(vec![
            (0, "inner_field1", ProtocolType::I32(I32::default())),
            (1, "inner_field2", ProtocolType::PString(PString::default())),
        ]));

        // 创建外部schema,包含一个内部schema字段
        let outer_schema = Arc::new(Schema::from_fields_desc_vec(vec![
            (0, "outer_field1", ProtocolType::I32(I32::default())),
            (
                1,
                "outer_field2",
                ProtocolType::Schema(inner_schema.clone()),
            ),
        ]));

        // 创建外部value set
        let mut outer_value_set = ValueSet::new(outer_schema.clone());
        outer_value_set.append_field_value("outer_field1", ProtocolType::I32(I32 { value: 1 }));

        // 创建内部value set
        let mut inner_value_set = outer_value_set.sub_valueset_of_schema_field("outer_field2");
        inner_value_set.append_field_value("inner_field1", ProtocolType::I32(I32 { value: 2 }));
        inner_value_set.append_field_value(
            "inner_field2",
            ProtocolType::PString(PString {
                value: "test".to_string(),
            }),
        );

        // 将内部value set添加到外部value set
        outer_value_set.append_field_value("outer_field2", ProtocolType::ValueSet(inner_value_set));

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
