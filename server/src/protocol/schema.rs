use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::BytesMut;
use tracing::trace;

use crate::AppError::{NetworkReadError, ProtocolError};
use crate::AppResult;
use crate::message::MemoryRecords;
use crate::protocol::array::ArrayType;
use crate::protocol::field::Field;
use crate::protocol::primary_types::{
    Bool, I16, I32, I64, I8, NPBytes, NPString, PBytes, PrimaryType, PString, PVarInt, PVarLong,
    U32,
};
use crate::protocol::types::DataType;
use crate::protocol::value_set::ValueSet;

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
            trace!("read field:{}", field.name);
            let result = match &field.p_type {
                DataType::Bool(_) => Bool::read_from(buffer),
                DataType::I8(_) => I8::read_from(buffer),
                DataType::I16(_) => I16::read_from(buffer),
                DataType::I32(_) => I32::read_from(buffer),
                DataType::U32(_) => U32::read_from(buffer),
                DataType::I64(_) => I64::read_from(buffer),
                DataType::PString(_) => PString::read_from(buffer),
                DataType::NPString(_) => NPString::read_from(buffer),
                DataType::PBytes(_) => PBytes::read_from(buffer),
                DataType::NPBytes(_) => NPBytes::read_from(buffer),
                DataType::PVarInt(_) => PVarInt::read_from(buffer),
                DataType::PVarLong(_) => PVarLong::read_from(buffer),
                DataType::Array(array) => array.read_from(buffer),
                DataType::Records(_) => MemoryRecords::read_from(buffer),
                //should never happen
                DataType::ValueSet(value_set) => {
                    return Err(NetworkReadError(Cow::Owned(format!(
                        "unexpected type schema:{:?}",
                        value_set
                    ))));
                }
                //should never happen
                DataType::Schema(schema) => {
                    return Err(NetworkReadError(Cow::Owned(format!(
                        "unexpected type schema:{:?}",
                        schema
                    ))));
                }
            };
            value_set.append_field_value(field.name, result?)?;
        }
        Ok(value_set)
    }

    fn size(&self) -> usize {
        todo!()
    }

    //
    // Retrieve the schema of an array field
    pub fn sub_schema_of_ary_field(
        self: Arc<Schema>,
        name: &'static str,
    ) -> AppResult<Arc<Schema>> {
        let field = self.get_field(name)?;
        let array_type: &ArrayType = (&field.p_type).try_into()?;

        if let DataType::Schema(schema) = array_type.p_type.as_ref() {
            Ok(schema.clone())
        } else {
            Err(NetworkReadError(Cow::Owned(format!(
                "not a schema field:{:?}",
                array_type.p_type.as_ref()
            ))))
        }
    }

    pub fn get_field_index(&self, name: &'static str) -> AppResult<i32> {
        return if let Some(index) = self.fields_index_by_name.get(name) {
            Ok(*index)
        } else {
            Err(ProtocolError(Cow::Owned(format!("unknown field:{}", name))))
        };
    }
    pub fn get_field(&self, name: &'static str) -> AppResult<&Field> {
        return if let Some(index) = self.fields_index_by_name.get(name) {
            if let Some(field) = self.fields.get(*index as usize) {
                Ok(field)
            } else {
                Err(ProtocolError(Cow::Owned(format!(
                    "can not find field in schema vec {}",
                    name
                ))))
            }
        } else {
            Err(ProtocolError(Cow::Owned(format!("unknown field:{}", name))))
        };
    }
}
impl From<Schema> for DataType {
    fn from(value: Schema) -> Self {
        DataType::Schema(Arc::new(value))
    }
}
