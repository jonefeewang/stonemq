use std::collections::HashMap;

use dashmap::DashMap;

#[derive(Debug)]
pub struct MemberMetadata {
    id: String,
    client_id: String,
    client_host: String,
    group_id: String,
    rebalance_timeout: i32,
    session_timeout: i32,
    protocol_type: String,
    supported_protocols: Vec<(String, Vec<u8>)>,
}
#[derive(Debug)]
pub struct GroupMetadata {
    id: String,
    generation_id: i32,
    members: HashMap<String, MemberMetadata>,
    offset: HashMap<String, i64>,
    leader_id: String,
    protocol: String,
}

pub struct GroupConfig {
    group_min_session_timeout: i32,
    group_max_session_timeout: i32,
    group_initial_rebalance_delay: i32,
}

pub struct GroupMetadataManager {
    group_metadata: DashMap<String, GroupMetadata>,
}

impl GroupMetadataManager {
    pub fn new() -> Self {
        Self {
            group_metadata: DashMap::new(),
        }
    }
    pub fn add_group(&self, group_id: String, group_metadata: GroupMetadata) {
        todo!()
    }
    pub fn store_group(&self, group_id: String, group_assignment: HashMap<String, Vec<u8>>) {
        todo!()
    }
    pub fn store_offset(&self, group_id: String, offset: HashMap<String, i64>) {
        todo!()
    }
    pub fn get_offset(&self, group_id: String, member_id: String) -> Option<i64> {
        todo!()
    }
    pub fn load_groups(&self) -> Vec<String> {
        todo!()
    }
}
