use crate::message::MemoryRecord;
use crate::protocol::api_schemas::produce::PRODUCE_REQUEST_SCHEMA_V0;
use crate::protocol::field::Field;
use crate::protocol::primary_types::{
    Bool, NPBytes, NPString, PBytes, PString, PVarInt, PVarLong, PrimaryType, I16, I32, I64, I8,
    U32,
};
use crate::protocol::structure::Struct;
use crate::protocol::types::FieldTypeEnum;
use crate::request::RequestHeader;
use crate::AppError::{NetworkReadError, ProtocolError};
use crate::AppResult;
use bytes::Bytes;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub fields: Vec<Field>,
    pub fields_by_name: HashMap<&'static str, Field>,
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields && self.fields_by_name == other.fields_by_name
    }
}
impl Eq for Schema {}

impl Schema {
    ///
    /// 根据schema,从bytes里读出一个struct, struct实际是schema和values的组合体
    ///
    pub fn read_from(self: &Arc<Schema>, buffer: &mut Bytes) -> AppResult<FieldTypeEnum> {
        let mut values = Vec::new();
        for field in &self.fields {
            debug!("read field:{:?}", field);
            let result = match &field.p_type {
                FieldTypeEnum::BoolE(_) => Bool::read_from(buffer),
                FieldTypeEnum::I8E(_) => I8::read_from(buffer),
                FieldTypeEnum::I16E(_) => I16::read_from(buffer),
                FieldTypeEnum::I32E(_) => I32::read_from(buffer),
                FieldTypeEnum::U32E(_) => U32::read_from(buffer),
                FieldTypeEnum::I64E(_) => I64::read_from(buffer),
                FieldTypeEnum::PStringE(_) => PString::read_from(buffer),
                FieldTypeEnum::NPStringE(_) => NPString::read_from(buffer),
                FieldTypeEnum::PBytesE(_) => PBytes::read_from(buffer),
                FieldTypeEnum::NPBytesE(_) => NPBytes::read_from(buffer),
                FieldTypeEnum::PVarIntE(_) => PVarInt::read_from(buffer),
                FieldTypeEnum::PVarLongE(_) => PVarLong::read_from(buffer),
                FieldTypeEnum::ArrayE(array) => array.read_from(buffer),
                FieldTypeEnum::RecordsE(_) => MemoryRecord::read_from(buffer),
                //should never happen
                FieldTypeEnum::StructE(structure) => {
                    return Err(NetworkReadError(Cow::Owned(format!(
                        "unexpected type schema:{:?}",
                        structure
                    ))));
                }
                //should never happen
                FieldTypeEnum::SchemaE(schema) => {
                    return Err(NetworkReadError(Cow::Owned(format!(
                        "unexpected type schema:{:?}",
                        schema
                    ))));
                }
            };
            values.push(result?);
        }
        Ok(FieldTypeEnum::StructE(Struct {
            schema: Arc::clone(self),
            values,
        }))
    }

    fn size(&self) -> usize {
        todo!()
    }

    pub fn get_produce_request_schema(request_header: &RequestHeader) -> Arc<Schema> {
        Arc::clone(&PRODUCE_REQUEST_SCHEMA_V0)
    }
    ///
    /// 获取某个array field对应的schema引用
    pub fn get_array_field_schema(schema: Arc<Schema>, name: &str) -> AppResult<Arc<Schema>> {
        let get_error = |message: &str| Err(ProtocolError(Cow::Owned(message.to_string())));

        let field = match schema.fields_by_name.get(name) {
            None => return get_error(&format!("unknown field:{}", name)),
            Some(field) => field,
        };

        if let FieldTypeEnum::ArrayE(array) = &field.p_type {
            if let FieldTypeEnum::SchemaE(schema) = array.p_type.as_ref() {
                Ok(Arc::clone(schema))
            } else {
                get_error(&format!("not a schema field:{:?}", array.p_type.as_ref()))
            }
        } else {
            get_error(&format!("not a array field:{:?}", &field.p_type))
        }
    }

    pub fn read_struct_from_buffer(self: &Arc<Schema>, buffer: &mut Bytes) -> AppResult<Struct> {
        match self.read_from(buffer)? {
            FieldTypeEnum::StructE(structure) => Ok(structure),
            _ => Err(ProtocolError("Unexpected type".into())),
        }
    }
}
