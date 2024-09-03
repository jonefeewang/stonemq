use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tokio::io::AsyncWriteExt;

use crate::AppError::{IllegalStateError, NetworkWriteError, ProtocolError};
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

    pub fn size(&self) -> AppResult<usize> {
        let mut total_size = 0usize;
        for data in self.values.values() {
            let size = match data {
                DataType::Bool(bool) => Ok(bool.size()),
                DataType::I8(i8) => Ok(i8.size()),
                DataType::I16(i16) => Ok(i16.size()),
                DataType::I32(i32) => Ok(i32.size()),
                DataType::U32(u32) => Ok(u32.size()),
                DataType::I64(i164) => Ok(i164.size()),
                DataType::PString(pstring) => Ok(pstring.size()),
                DataType::NPString(npstring) => Ok(npstring.size()),
                DataType::PBytes(pbytes) => Ok(pbytes.size()),
                DataType::NPBytes(npbytes) => Ok(npbytes.size()),
                DataType::PVarInt(pvarint) => Ok(pvarint.size()),
                DataType::PVarLong(pvarlong) => Ok(pvarlong.size()),
                DataType::Array(array) => Ok(array.size()?),
                DataType::Records(records) => Ok(records.size()),
                DataType::ValueSet(data) => data.size(),
                DataType::Schema(_) => Err(IllegalStateError(Cow::Borrowed(
                    "Schema type should not be in the values",
                ))),
            };
            total_size += size?;
        }
        Ok(total_size)
    }

    pub fn sub_valueset_of_ary_field(&self, field_name: &'static str) -> AppResult<ValueSet> {
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
                "field index not match, expect:{},actual:{} with filed name:{}",
                field.index + 1,
                self.values.len(),
                field_name
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
    pub fn write_to<W>(self, writer: &mut W) -> BoxFuture<'_, AppResult<()>>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        async move {
            for (_, value) in self.values {
                match value {
                    DataType::Bool(bool) => bool.write_to(writer).await?,
                    DataType::I8(i8) => i8.write_to(writer).await?,
                    DataType::I16(i16) => i16.write_to(writer).await?,
                    DataType::I32(i32) => i32.write_to(writer).await?,
                    DataType::U32(u32) => u32.write_to(writer).await?,
                    DataType::I64(i64) => i64.write_to(writer).await?,
                    DataType::PString(string) => string.write_to(writer).await?,
                    DataType::NPString(npstring) => npstring.write_to(writer).await?,
                    DataType::PBytes(bytes) => bytes.write_to(writer).await?,
                    DataType::NPBytes(npbytes) => npbytes.write_to(writer).await?,
                    DataType::PVarInt(pvarint) => pvarint.write_to(writer).await?,
                    DataType::PVarLong(pvarlong) => pvarlong.write_to(writer).await?,
                    DataType::Array(array) => array.write_to(writer).await?,
                    DataType::Records(records) => records.write_to(writer).await?,
                    //should never happen
                    DataType::Schema(schema) => {
                        return Err(NetworkWriteError(Cow::Owned(format!(
                            "unexpected type schema:{:?}",
                            schema
                        ))));
                    }
                    //should never happen
                    DataType::ValueSet(structure) => {
                        return Err(NetworkWriteError(Cow::Owned(format!(
                            "unexpected type schema:{:?}",
                            structure
                        ))));
                    }
                }
            }
            Ok(())
        }
        .boxed()
    }
}

mod test {
    use bytes::BytesMut;

    #[tokio::test]
    async fn test_schema_data_read_write() {
        use super::*;
        use crate::protocol::primary_types::PString;
        use crate::protocol::primary_types::I32;
        let mut writer = Vec::new();
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

        // write
        value_set.write_to(&mut writer).await.unwrap();

        // read
        let mut buffer = BytesMut::from(&writer[..]);
        let read_value_set = schema_clone.read_from(&mut buffer).unwrap();
        assert_eq!(read_value_set, value_set_clone);
    }

    #[tokio::test]
    async fn test_two_hierarchy_schema_read_write() {
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
                .sub_valueset_of_ary_field(OUTER_FIELD2)
                .unwrap();

            inner_value_set
                .append_field_value(INNER_FIELD1, inner.inner_field1.into())
                .unwrap();
            inner_value_set
                .append_field_value(INNER_FIELD2, inner.inner_field2.into())
                .unwrap();
            inner_array.push(DataType::ValueSet(inner_value_set));
        }

        let schema =
            Schema::sub_schema_of_ary_field(outer_value_set.schema.clone(), OUTER_FIELD2).unwrap();
        let array = DataType::array_of_value_set(inner_array, schema);

        outer_value_set
            .append_field_value(OUTER_FIELD2, array)
            .unwrap();

        let outer_value_set_clone = outer_value_set.clone();

        //write to buffer
        let mut writer = Vec::new();
        outer_value_set.write_to(&mut writer).await.unwrap();

        //read from buffer
        let mut buffer = BytesMut::from(&writer[..]);
        let read_outer_structure = outer_schema.read_from(&mut buffer).unwrap();
        //check
        assert_eq!(read_outer_structure, outer_value_set_clone);
    }
}
