use crate::protocol::base::ProtocolType;

#[derive(Debug, Clone)]
pub struct Field {
    pub index: i32,
    pub name: &'static str,
    pub p_type: ProtocolType,
}

impl PartialEq for Field {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.name == other.name && self.p_type == other.p_type
    }
}

impl Eq for Field {}
