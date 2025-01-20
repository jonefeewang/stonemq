use std::collections::HashMap;
use std::sync::Arc;

use bytes::BytesMut;

use crate::message::MemoryRecords;
use crate::protocol::base::ProtocolType;
use crate::protocol::base::{
    Bool, NPBytes, NPString, PBytes, PString, PVarInt, PVarLong, PrimaryType, I16, I32, I64, I8,
    U32,
};
use crate::protocol::schema_base::Field;
use crate::protocol::schema_base::ValueSet;
use crate::protocol::types::ArrayType;
use crate::AppResult;

///
/// Schema contains a series of fields, which are ordered and indicate the definition of the schema
/// The position of the field in the schema is consistent with its index
/// To facilitate searching for the schema by name, a HashMap is used for mapping
#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub fields_index_by_name: HashMap<&'static str, i32>,
    pub fields: Vec<Field>,
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl Eq for Schema {}

impl Schema {
    pub fn from_fields_desc_vec(fields_desc: Vec<(i32, &'static str, ProtocolType)>) -> Schema {
        let mut fields = Vec::with_capacity(fields_desc.len());
        let mut fields_index_by_name = HashMap::with_capacity(fields_desc.len());
        for (index, name, p_type) in fields_desc {
            fields.push(Field {
                index,
                name,
                p_type,
            });
            fields_index_by_name.insert(name, index);
            // Ensure that both fields and fields_index_by_name are consistent.
            assert_eq!(fields.len() as i32, index + 1);
        }
        Schema {
            fields,
            fields_index_by_name,
        }
    }

    pub fn read_from(self: Arc<Schema>, buffer: &mut BytesMut) -> AppResult<ValueSet> {
        let mut value_set = ValueSet::new(self.clone());
        for field in &self.fields {
            // trace!("read field:{}", field.name);
            let result = match &field.p_type {
                ProtocolType::Bool(_) => Bool::decode(buffer)?,
                ProtocolType::I8(_) => I8::decode(buffer)?,
                ProtocolType::I16(_) => I16::decode(buffer)?,
                ProtocolType::I32(_) => I32::decode(buffer)?,
                ProtocolType::U32(_) => U32::decode(buffer)?,
                ProtocolType::I64(_) => I64::decode(buffer)?,
                ProtocolType::PString(_) => PString::decode(buffer)?,
                ProtocolType::NPString(_) => NPString::decode(buffer)?,
                ProtocolType::PBytes(_) => PBytes::decode(buffer)?,
                ProtocolType::NPBytes(_) => NPBytes::decode(buffer)?,
                ProtocolType::PVarInt(_) => PVarInt::decode(buffer)?,
                ProtocolType::PVarLong(_) => PVarLong::decode(buffer)?,
                ProtocolType::Array(array) => array.decode(buffer)?,
                ProtocolType::Records(_) => MemoryRecords::decode(buffer)?,
                //should never happen
                ProtocolType::ValueSet(value_set) => {
                    panic!("unexpected type schema:{:?}", value_set);
                }

                // 只允许schema嵌套schema/array
                ProtocolType::Schema(schema) => {
                    let schema_clone = schema.clone();
                    let sub_value_set = schema_clone.read_from(buffer)?;
                    ProtocolType::ValueSet(sub_value_set)
                }
            };
            value_set.append_field_value(field.name, result);
        }
        Ok(value_set)
    }

    //
    // Retrieve the schema of an array field
    pub fn sub_schema_of_ary_field(self: Arc<Schema>, name: &'static str) -> Arc<Schema> {
        let field = self.get_field(name);
        let array_type: &ArrayType = (&field.p_type).into();

        if let ProtocolType::Schema(schema) = array_type.p_type.as_ref() {
            schema.clone()
        } else {
            panic!("not a schema field:{:?}", array_type.p_type.as_ref())
        }
    }

    pub fn get_field_index(&self, name: &'static str) -> i32 {
        *self.fields_index_by_name.get(name).unwrap()
    }
    pub fn get_field(&self, name: &'static str) -> &Field {
        let index = self.fields_index_by_name.get(name).unwrap();
        self.fields.get(*index as usize).unwrap()
    }
    pub fn has_field(&self, name: &'static str) -> bool {
        self.fields_index_by_name.contains_key(name)
    }
}
impl From<Schema> for ProtocolType {
    fn from(value: Schema) -> Self {
        ProtocolType::Schema(Arc::new(value))
    }
}
