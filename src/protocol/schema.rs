use std::collections::HashMap;
use std::sync::Arc;

use bytes::BytesMut;

use crate::message::MemoryRecords;
use crate::protocol::array::ArrayType;
use crate::protocol::field::Field;
use crate::protocol::primary_types::{
    Bool, NPBytes, NPString, PBytes, PString, PVarInt, PVarLong, PrimaryType, I16, I32, I64, I8,
    U32,
};
use crate::protocol::types::DataType;
use crate::protocol::value_set::ValueSet;
use crate::AppResult;

///
/// Schema包含一系列字段，字段是有序的，表明了该schema的定义
/// field在schema中的位置和自己的index是一致的
/// 为了方便按名字查找schema，这里使用了HashMap做了映射
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
    pub fn from_fields_desc_vec(fields_desc: Vec<(i32, &'static str, DataType)>) -> Schema {
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
                DataType::Bool(_) => Bool::decode(buffer)?,
                DataType::I8(_) => I8::decode(buffer)?,
                DataType::I16(_) => I16::decode(buffer)?,
                DataType::I32(_) => I32::decode(buffer)?,
                DataType::U32(_) => U32::decode(buffer)?,
                DataType::I64(_) => I64::decode(buffer)?,
                DataType::PString(_) => PString::decode(buffer)?,
                DataType::NPString(_) => NPString::decode(buffer)?,
                DataType::PBytes(_) => PBytes::decode(buffer)?,
                DataType::NPBytes(_) => NPBytes::decode(buffer)?,
                DataType::PVarInt(_) => PVarInt::decode(buffer)?,
                DataType::PVarLong(_) => PVarLong::decode(buffer)?,
                DataType::Array(array) => array.decode(buffer)?,
                DataType::Records(_) => MemoryRecords::decode(buffer)?,
                //should never happen
                DataType::ValueSet(value_set) => {
                    panic!("unexpected type schema:{:?}", value_set);
                }

                // 只允许schema嵌套schema/array
                DataType::Schema(schema) => {
                    let schema_clone = schema.clone();
                    let sub_value_set = schema_clone.read_from(buffer)?;
                    DataType::ValueSet(sub_value_set)
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

        if let DataType::Schema(schema) = array_type.p_type.as_ref() {
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
impl From<Schema> for DataType {
    fn from(value: Schema) -> Self {
        DataType::Schema(Arc::new(value))
    }
}
