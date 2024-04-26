use crate::protocol::array::Array;
use crate::protocol::field::Field;
use crate::protocol::primary_types::{
    Bool, NPBytes, NPString, PBytes, PString, PVarInt, PVarLong, PrimaryType, I16, I32, I64, I8,
    U32,
};
use crate::protocol::schema::Schema;
use crate::protocol::types::FieldTypeEnum;
use crate::request::RequestHeader;
use crate::AppError::{NetworkWriteError, ProtocolError};
use crate::AppResult;
use bytes::BytesMut;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct Struct {
    pub schema: Arc<Schema>,
    pub values: Vec<FieldTypeEnum>,
}

impl PartialEq for Struct {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema && self.values == other.values
    }
}
impl Eq for Struct {}

impl Struct {
    pub fn get_schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
    pub fn from_schema(schema: Arc<Schema>) -> Struct {
        Struct {
            schema,
            values: Vec::new(),
        }
    }
    pub fn new_produce_request_struct(request_header: &RequestHeader) -> Struct {
        let schema = Schema::get_produce_request_schema(request_header);
        Struct {
            schema: Arc::clone(&schema),
            values: Vec::with_capacity(schema.fields.len()),
        }
    }
    pub fn new_struct_from_schema_field(&self, field_name: &str) -> AppResult<Struct> {
        if let Some(field) = self.schema.fields_by_name.get(field_name) {
            return match &field.p_type {
                FieldTypeEnum::ArrayE(array) => {
                    let schema = match &*array.p_type {
                        FieldTypeEnum::SchemaE(schema) => schema,
                        _ => {
                            return Err(ProtocolError(Cow::Borrowed(
                                "Array type must be schema type",
                            )))
                        }
                    };
                    let structure = Struct {
                        schema: Arc::clone(schema),
                        values: Vec::with_capacity(schema.fields.len()),
                    };
                    Ok(structure)
                }
                enum_type => Err(ProtocolError(Cow::Owned(format!(
                    "Unsupported type: {:?} for struct creation",
                    enum_type
                )))),
            };
        } else {
            Err(ProtocolError(Cow::Owned(format!(
                "Field {} does not exist in the schema",
                field_name
            ))))
        }
    }
    ///
    /// 追加一个field的值
    pub fn append_field_value(
        &mut self,
        field_name: &str,
        new_value: FieldTypeEnum,
    ) -> AppResult<()> {
        //check if field exists
        if let Some(field) = self.schema.fields_by_name.get(field_name) {
            self.values.push(new_value);
            if field.index + 1 != self.values.len() as i32 {
                return Err(ProtocolError(Cow::Owned(format!(
                    "field index not match, expect:{},actual:{}",
                    field.index + 1,
                    self.values.len()
                ))));
            };
            Ok(())
        } else {
            let error_message = format!("unknown field:{}", field_name);
            Err(ProtocolError(Cow::Owned(error_message)))
        }
    }
    ///
    /// 获取某个field的值
    pub fn get_field_value(&self, field_name: &str) -> AppResult<&FieldTypeEnum> {
        if let Some(field) = self.schema.fields_by_name.get(field_name) {
            Ok(&self.values[field.index as usize])
        } else {
            let error_message = format!("unknown field:{}", field_name);
            Err(ProtocolError(Cow::Owned(error_message)))
        }
    }

    ///
    /// 将struct写入到buffer内
    ///
    pub fn write_to(&self, buffer: &mut BytesMut) -> AppResult<()> {
        for field in &self.values {
            debug!("write field:{:?}", field);
            match &field {
                FieldTypeEnum::BoolE(bool) => bool.write_to(buffer)?,
                FieldTypeEnum::I8E(i8) => i8.write_to(buffer)?,
                FieldTypeEnum::I16E(i16) => i16.write_to(buffer)?,
                FieldTypeEnum::I32E(i32) => i32.write_to(buffer)?,
                FieldTypeEnum::U32E(u32) => u32.write_to(buffer)?,
                FieldTypeEnum::I64E(i64) => i64.write_to(buffer)?,
                FieldTypeEnum::PStringE(string) => string.write_to(buffer)?,
                FieldTypeEnum::NPStringE(npstring) => npstring.write_to(buffer)?,
                FieldTypeEnum::PBytesE(bytes) => bytes.write_to(buffer)?,
                FieldTypeEnum::NPBytesE(npbytes) => npbytes.write_to(buffer)?,
                FieldTypeEnum::PVarIntE(pvarint) => pvarint.write_to(buffer)?,
                FieldTypeEnum::PVarLongE(pvarlong) => pvarlong.write_to(buffer)?,
                FieldTypeEnum::ArrayE(array) => array.write_to(buffer)?,
                FieldTypeEnum::RecordsE(records) => records.write_to(buffer)?,
                //should never happen
                FieldTypeEnum::SchemaE(schema) => {
                    return Err(NetworkWriteError(Cow::Owned(format!(
                        "unexpected type schema:{:?}",
                        schema
                    ))));
                }
                //should never happen
                FieldTypeEnum::StructE(structure) => {
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

#[test]
fn test_struct_read_write() {
    let mut buffer = BytesMut::new();
    let schema = Arc::new(Schema {
        fields: vec![
            Field {
                index: 0,
                name: "field1",
                p_type: FieldTypeEnum::I32E(I32::default()),
            },
            Field {
                index: 1,
                name: "field2",
                p_type: FieldTypeEnum::PStringE(PString::default()),
            },
        ],
        fields_by_name: HashMap::new(),
    });
    let struct_value = Struct {
        schema: Arc::clone(&schema),
        values: vec![
            FieldTypeEnum::I32E(I32 { value: 1 }),
            FieldTypeEnum::PStringE(PString {
                value: "test".to_string(),
            }),
        ],
    };
    struct_value.write_to(&mut buffer).unwrap();
    let read_struct = schema
        .read_struct_from_buffer(&mut buffer.freeze())
        .unwrap();
    assert_eq!(read_struct, struct_value);
}

struct Inner {
    inner_field1: i32,
    inner_field2: String,
}
struct Outer {
    outer_field1: i32,
    outer_field2: Vec<Inner>,
}
#[test]
fn test_two_hierarchy_schema_read_write() {
    const INNER_FIELD1: &str = "inner_field1";
    const INNER_FIELD2: &str = "inner_field2";
    const OUTER_FIELD1: &str = "outer_field1";
    const OUTER_FIELD2: &str = "outer_field2";

    //create schema
    let filed1 = Field {
        index: 0,
        name: INNER_FIELD1,
        p_type: FieldTypeEnum::I32E(I32::default()),
    };
    let field2 = Field {
        index: 1,
        name: INNER_FIELD2,
        p_type: FieldTypeEnum::PStringE(PString::default()),
    };
    let mut fields_by_name = HashMap::new();
    fields_by_name.insert(INNER_FIELD1, filed1.clone());
    fields_by_name.insert(INNER_FIELD2, field2.clone());

    let inner_schema = Arc::new(FieldTypeEnum::SchemaE(Arc::new(Schema {
        fields: vec![filed1.clone(), field2.clone()],
        fields_by_name,
    })));

    let array = Array {
        can_be_empty: false,
        p_type: inner_schema,
        values: None,
    };

    let outer_field1 = Field {
        index: 0,
        name: OUTER_FIELD1,
        p_type: FieldTypeEnum::from_i32(0),
    };
    let outer_field2 = Field {
        index: 1,
        name: OUTER_FIELD2,
        p_type: FieldTypeEnum::ArrayE(array),
    };

    let mut fields_by_name = HashMap::new();
    fields_by_name.insert(OUTER_FIELD1, outer_field1.clone());
    fields_by_name.insert(OUTER_FIELD2, outer_field2.clone());

    let outer_schema = Schema {
        fields: vec![outer_field1.clone(), outer_field2.clone()],
        fields_by_name,
    };

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
    let outer_schema = Arc::new(outer_schema);
    let mut outer_structure = Struct::from_schema(Arc::clone(&outer_schema));
    outer_structure
        .append_field_value(OUTER_FIELD1, FieldTypeEnum::from_i32(outer.outer_field1))
        .unwrap();

    let mut inner_array = Vec::with_capacity(outer.outer_field2.len());
    for inner in outer.outer_field2 {
        let mut inner_structure = outer_structure
            .new_struct_from_schema_field(OUTER_FIELD2)
            .unwrap();

        inner_structure
            .append_field_value(INNER_FIELD1, FieldTypeEnum::from_i32(inner.inner_field1))
            .unwrap();
        inner_structure
            .append_field_value(
                INNER_FIELD2,
                FieldTypeEnum::from_string(&inner.inner_field2),
            )
            .unwrap();
        inner_array.push(FieldTypeEnum::StructE(inner_structure));
    }

    let schema =
        Schema::get_array_field_schema(outer_structure.get_schema(), OUTER_FIELD2).unwrap();
    let array = FieldTypeEnum::array_enum_from_structure_vec(inner_array, schema);

    outer_structure
        .append_field_value(OUTER_FIELD2, array)
        .unwrap();

    //write to buffer
    let mut buffer = BytesMut::new();
    outer_structure.write_to(&mut buffer).unwrap();

    //read from buffer
    let read_outer_structure = Arc::clone(&outer_schema)
        .read_struct_from_buffer(&mut buffer.freeze())
        .unwrap();
    //check
    assert_eq!(read_outer_structure, outer_structure);
}
