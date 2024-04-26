use std::sync::Arc;

use crate::message::MemoryRecord;
use crate::protocol::array::Array;
use crate::protocol::primary_types::{
    Bool, NPBytes, NPString, PBytes, PString, PVarInt, PVarLong, I16, I32, I64, I8, U32,
};
use crate::protocol::schema::Schema;
use crate::protocol::structure::Struct;
use crate::protocol::Acks;
use crate::protocol::Acks::{All, Leader};

///
/// Enum在这里模拟多态  
/// 1.在需要返回多态类型的值、或需要多态类型的参数时，可以使用Enum来代替
///
/// 类型设计:  
/// Schema中基本类型是不包含value的，只是一个类型占用符，按理说只需要普通的enum variant即可
/// ，但是在实现schema递归读写的时候，我们无法在variant上实现读写一类的方法，所以这里使用了结构体类型的变体，
/// 将方法实现在变体内部的结构体类型上。但是在确实需要一个value时，有需要类型包含value，所以变体内部的结构体必须
/// 包含value，所以这里的结构体类型实现了两种功能，第一种是类型占位符，第二种是类型和类型的值。在schema里的类型
/// 使用到了第一种功能，在struct的value数组内使用了第二种功能
///
#[derive(Debug, Clone)]
pub enum FieldTypeEnum {
    BoolE(Bool),
    I8E(I8),
    I16E(I16),
    I32E(I32),
    U32E(U32),
    I64E(I64),
    PStringE(PString),
    NPStringE(NPString),
    PBytesE(PBytes),
    NPBytesE(NPBytes),
    PVarIntE(PVarInt),
    PVarLongE(PVarLong),
    ArrayE(Array),
    RecordsE(MemoryRecord),
    SchemaE(Arc<Schema>),
    StructE(Struct),
}

impl PartialEq for FieldTypeEnum {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FieldTypeEnum::BoolE(a), FieldTypeEnum::BoolE(b)) => a == b,
            (FieldTypeEnum::I8E(a), FieldTypeEnum::I8E(b)) => a == b,
            (FieldTypeEnum::I16E(a), FieldTypeEnum::I16E(b)) => a == b,
            (FieldTypeEnum::I32E(a), FieldTypeEnum::I32E(b)) => a == b,
            (FieldTypeEnum::U32E(a), FieldTypeEnum::U32E(b)) => a == b,
            (FieldTypeEnum::I64E(a), FieldTypeEnum::I64E(b)) => a == b,
            (FieldTypeEnum::PStringE(a), FieldTypeEnum::PStringE(b)) => a == b,
            (FieldTypeEnum::NPStringE(a), FieldTypeEnum::NPStringE(b)) => a == b,
            (FieldTypeEnum::PBytesE(a), FieldTypeEnum::PBytesE(b)) => a == b,
            (FieldTypeEnum::NPBytesE(a), FieldTypeEnum::NPBytesE(b)) => a == b,
            (FieldTypeEnum::PVarIntE(a), FieldTypeEnum::PVarIntE(b)) => a == b,
            (FieldTypeEnum::PVarLongE(a), FieldTypeEnum::PVarLongE(b)) => a == b,
            (FieldTypeEnum::ArrayE(a), FieldTypeEnum::ArrayE(b)) => a == b,
            (FieldTypeEnum::RecordsE(a), FieldTypeEnum::RecordsE(b)) => a == b,
            (FieldTypeEnum::SchemaE(a), FieldTypeEnum::SchemaE(b)) => a == b,
            (FieldTypeEnum::StructE(a), FieldTypeEnum::StructE(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for FieldTypeEnum {}

impl FieldTypeEnum {
    pub fn from_bool(value: bool) -> FieldTypeEnum {
        FieldTypeEnum::BoolE(Bool { value })
    }
    pub fn from_i8(value: i8) -> FieldTypeEnum {
        FieldTypeEnum::I8E(I8 { value })
    }
    pub fn from_acks(acks: &Acks) -> FieldTypeEnum {
        let i8_value = match acks {
            All(i8) => i8,
            Leader(i8) => i8,
            Acks::None(i8) => i8,
        };
        Self::from_i8(*i8_value)
    }
    pub fn from_i16(value: i16) -> FieldTypeEnum {
        FieldTypeEnum::I16E(I16 { value })
    }
    pub fn from_i32(value: i32) -> FieldTypeEnum {
        FieldTypeEnum::I32E(I32 { value })
    }
    pub fn from_string(value: &str) -> FieldTypeEnum {
        FieldTypeEnum::PStringE(PString {
            value: value.to_string(),
        })
    }
    pub fn array_enum_from_structure_vec(
        structure: Vec<FieldTypeEnum>,
        schema: Arc<Schema>,
    ) -> FieldTypeEnum {
        FieldTypeEnum::ArrayE(Array {
            can_be_empty: false,
            p_type: Arc::new(FieldTypeEnum::StructE(Struct::from_schema(schema))),
            values: Some(structure),
        })
    }
}
