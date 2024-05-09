use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::BytesMut;

use crate::AppError::{NetworkWriteError, ProtocolError};
use crate::AppResult;
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
                DataType::Bool(bool) => bool.size(),
                DataType::I8(i8) => i8.size(),
                DataType::I16(i16) => i16.size(),
                DataType::I32(i32) => i32.size(),
                DataType::U32(u32) => u32.size(),
                DataType::I64(i164) => i164.size(),
                DataType::PString(pstring) => pstring.size(),
                DataType::NPString(npstring) => npstring.size(),
                DataType::PBytes(pbytes) => pbytes.size(),
                DataType::NPBytes(npbytes) => npbytes.size(),
                DataType::PVarInt(pvarint) => pvarint.size(),
                DataType::PVarLong(pvarlong) => pvarlong.size(),
                DataType::Array(array) => array.size(),
                DataType::Records(records) => records.size(),
                DataType::SchemaValues(data) => data.size(),
                DataType::Schema(_) => {
                    panic!("Schema type should not be in the values")
                }
            };
            total_size += size;
        }
        total_size
    }

    pub fn create_from_array_of_schema(&self, field_name: &'static str) -> AppResult<ValueSet> {
        let array_field = self.schema.get_field(field_name)?;
        let array_type: &ArrayType = (&array_field.p_type).try_into()?;

        if let DataType::Schema(ref schema) = &*array_type.p_type {
            let value_set = ValueSet {
                schema: schema.clone(),
                values: BTreeMap::new(),
            };
            Ok(value_set)
        } else {
            Err(ProtocolError(Cow::Borrowed(
                "Array type must be schema type",
            )))
        }
    }

    ///
    /// Append a field value
    ///
    pub fn append_field_value(
        &mut self,
        field_name: &'static str,
        new_value: DataType,
    ) -> AppResult<()> {
        //check if field exists
        let field = self.schema.get_field(field_name)?;
        self.values.insert(field.index, new_value);
        if field.index + 1 != self.values.len() as i32 {
            return Err(ProtocolError(Cow::Owned(format!(
                "field index not match, expect:{},actual:{}",
                field.index + 1,
                self.values.len()
            ))));
        };
        Ok(())
    }
    ///
    /// Get a field value
    ///
    pub fn get_field_value(&mut self, field_name: &'static str) -> AppResult<DataType> {
        let index = self.schema.get_field_index(field_name)?;
        if let Some(field) = self.values.remove(&index) {
            Ok(field)
        } else {
            let error_message = format!("field not found:{} in value set", field_name);
            Err(ProtocolError(Cow::Owned(error_message)))
        }
    }

    ///
    /// 将values写进stream, 消耗掉自己，方法调用后，自己就不能再使用了
    ///
    ///
    pub fn write_to(self, buffer: &mut BytesMut) -> AppResult<()> {
        for (_, value) in self.values {
            match value {
                DataType::Bool(bool) => bool.write_to(buffer)?,
                DataType::I8(i8) => i8.write_to(buffer)?,
                DataType::I16(i16) => i16.write_to(buffer)?,
                DataType::I32(i32) => i32.write_to(buffer)?,
                DataType::U32(u32) => u32.write_to(buffer)?,
                DataType::I64(i64) => i64.write_to(buffer)?,
                DataType::PString(string) => string.write_to(buffer)?,
                DataType::NPString(npstring) => npstring.write_to(buffer)?,
                DataType::PBytes(bytes) => bytes.write_to(buffer)?,
                DataType::NPBytes(npbytes) => npbytes.write_to(buffer)?,
                DataType::PVarInt(pvarint) => pvarint.write_to(buffer)?,
                DataType::PVarLong(pvarlong) => pvarlong.write_to(buffer)?,
                DataType::Array(array) => array.write_to(buffer)?,
                DataType::Records(records) => records.write_to(buffer)?,
                //should never happen
                DataType::Schema(schema) => {
                    return Err(NetworkWriteError(Cow::Owned(format!(
                        "unexpected type schema:{:?}",
                        schema
                    ))));
                }
                //should never happen
                DataType::SchemaValues(structure) => {
                    return Err(NetworkWriteError(Cow::Owned(format!(
                        "unexpected type schema:{:?}",
                        structure
                    ))));
                }
            }
        }
        Ok(())
    }
}

mod test {

    #[test]
    fn test_schema_data_read_write() {
        use super::*;
        use crate::protocol::primary_types::PString;
        use crate::protocol::primary_types::I32;
        let mut buffer = BytesMut::new();
        let schema = Arc::new(Schema::from_fields_desc_vec(vec![
            (0, "field1", DataType::I32(I32::default())),
            (1, "field2", DataType::PString(PString::default())),
        ]));
        let schema_clone = Arc::clone(&schema);
        let mut value_set = ValueSet::new(schema);
        value_set
            .append_field_value("field1", DataType::I32(I32 { value: 1 }))
            .unwrap();
        value_set
            .append_field_value(
                "field2",
                DataType::PString(PString {
                    value: "test".to_string(),
                }),
            )
            .unwrap();
        let value_set_clone = value_set.clone();
        value_set.write_to(&mut buffer).unwrap();

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
        use crate::protocol::primary_types::{I32, PString};
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
        outer_value_set
            .append_field_value(OUTER_FIELD1, outer.outer_field1.into())
            .unwrap();

        let mut inner_array = Vec::with_capacity(outer.outer_field2.len());
        for inner in outer.outer_field2 {
            let mut inner_value_set = outer_value_set
                .create_from_array_of_schema(OUTER_FIELD2)
                .unwrap();

            inner_value_set
                .append_field_value(INNER_FIELD1, inner.inner_field1.into())
                .unwrap();
            inner_value_set
                .append_field_value(INNER_FIELD2, inner.inner_field2.into())
                .unwrap();
            inner_array.push(DataType::SchemaValues(inner_value_set));
        }

        let schema =
            Schema::get_array_field_schema(outer_value_set.schema.clone(), OUTER_FIELD2).unwrap();
        let array = DataType::array_of_value_set(inner_array, schema);

        outer_value_set
            .append_field_value(OUTER_FIELD2, array)
            .unwrap();

        let outer_value_set_clone = outer_value_set.clone();

        //write to buffer
        let mut buffer = BytesMut::new();
        outer_value_set.write_to(&mut buffer).unwrap();

        //read from buffer
        let read_outer_structure = outer_schema.read_from(&mut buffer).unwrap();
        //check
        assert_eq!(read_outer_structure, outer_value_set_clone);
    }
}
