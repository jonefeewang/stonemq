use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet};
use std::io::Read;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tracing::debug;

use crate::request::KafkaError;
use crate::request::KafkaResult;

use super::GroupMetadata;
use super::GroupState;
use super::MemberMetadata;
impl GroupMetadata {
    /// create new group metadata
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            id: group_id.into(),
            generation_id: 0,
            protocol_type: None,
            protocol: None,
            members: HashMap::new(),
            state: GroupState::Empty,
            new_member_added: false,
            leader_id: None,
        }
    }

    /// add new member
    pub fn add_member(&mut self, member: MemberMetadata) {
        if self.members.is_empty() {
            self.protocol_type = Some(member.protocol_type.clone());
        }
        if self.leader_id.is_none() {
            self.leader_id = Some(member.id.clone());
        }

        self.members.insert(member.id.clone(), member);
    }

    /// remove member
    pub fn remove_member(&mut self, member_id: &str) -> Option<MemberMetadata> {
        let removed_member = self.members.remove(member_id);
        if member_id == self.leader_id.as_deref().unwrap() {
            self.leader_id = self.members.keys().next().map(|k| k.to_string());
        }
        removed_member
    }

    /// check if the group contains the specified member
    pub fn has_member(&self, member_id: &str) -> bool {
        self.members.contains_key(member_id)
    }

    /// get all members' mutable references
    pub fn members(&mut self) -> Vec<&mut MemberMetadata> {
        self.members.values_mut().collect()
    }

    /// get the maximum rebalance timeout for the group
    pub fn max_rebalance_timeout(&self) -> i32 {
        self.members
            .values()
            .map(|m| m.rebalance_timeout)
            .max()
            .unwrap_or(0)
    }

    /// get the metadata of the current member that matches the group's selected protocol
    pub fn current_member_metadata(&self) -> BTreeMap<String, Bytes> {
        self.members
            .iter()
            .filter_map(|(id, member)| {
                self.protocol
                    .as_ref()
                    .and_then(|p| member.metadata(p))
                    .map(|metadata| (id.clone(), metadata))
            })
            .collect()
    }

    /// select a protocol for the group
    pub fn select_protocol(&self) -> Result<String, KafkaError> {
        let candidates = self.candidate_protocols();
        if candidates.is_empty() {
            return Err(KafkaError::InconsistentGroupProtocol(
                "No common protocol found".into(),
            ));
        }

        let votes = self
            .all_member_metadata()
            .iter()
            .map(|member| member.vote(&candidates))
            .fold(HashMap::new(), |mut acc, protocol| {
                *acc.entry(protocol).or_insert(0) += 1;
                acc
            });

        votes
            .into_iter()
            .max_by_key(|&(_, count)| count)
            .map(|(protocol, _)| protocol)
            .ok_or_else(|| KafkaError::InconsistentGroupProtocol("No protocol selected".into()))
    }

    /// get the protocols supported by all members
    fn candidate_protocols(&self) -> HashSet<String> {
        self.members
            .values()
            .map(|member| member.protocols())
            .fold(None, |acc: Option<HashSet<String>>, protocols| match acc {
                None => Some(protocols),
                Some(acc) => Some(acc.intersection(&protocols).cloned().collect()),
            })
            .unwrap_or_default()
    }

    /// check if the group supports the given protocol list
    pub fn is_support_protocols(&self, protocols: &HashSet<String>) -> bool {
        self.members.is_empty() || self.candidate_protocols().intersection(protocols).count() > 0
    }

    /// get the members that have not yet rejoined
    pub fn not_yet_rejoined_members(&self) -> Vec<String> {
        self.members
            .values()
            .filter(|m| m.join_group_cb_sender.is_none())
            .map(|m| m.id.clone())
            .collect()
    }

    /// cancel all members' assignment
    #[allow(dead_code)]
    pub fn cancel_all_member_assignment(&mut self) {
        self.members.values_mut().for_each(|member| {
            member.assignment = None;
        });
    }

    pub fn serialize(&self) -> KafkaResult<Bytes> {
        let mut buffer = BytesMut::new();

        // protocol_type - string
        match &self.protocol_type {
            None => buffer.put_i32(-1),
            Some(protocol_type) => {
                buffer.put_i32(protocol_type.len() as i32);
                buffer.put(protocol_type.as_bytes());
            }
        }
        // generation_id - int32
        buffer.put_i32(self.generation_id);
        // protocol - string
        match &self.protocol {
            None => buffer.put_i32(-1),
            Some(protocol) => {
                buffer.put_i32(protocol.len() as i32);
                buffer.put(protocol.as_bytes());
            }
        }
        // leader_id - string
        match &self.leader_id {
            None => buffer.put_i32(-1),
            Some(leader_id) => {
                buffer.put_i32(leader_id.len() as i32);
                buffer.put(leader_id.as_bytes());
            }
        }

        // members length - int32
        buffer.put_i32(self.members.len() as i32);

        for member in self.members.values() {
            let protocol = self.protocol.as_ref().unwrap();
            let serialized = member.serialize(protocol)?;
            buffer.put_u32(serialized.len() as u32);
            buffer.put(serialized);
        }
        Ok(buffer.freeze())
    }

    pub fn deserialize(data: &[u8], group_id: &str) -> KafkaResult<Self> {
        let mut cursor = std::io::Cursor::new(data);

        // protocol_type - string
        let protocol_type_len = cursor.get_i32();
        let protocol_type = if protocol_type_len == -1 {
            None
        } else {
            let mut protocol_type_bytes = vec![0; protocol_type_len as usize];
            cursor.read_exact(&mut protocol_type_bytes).unwrap();
            Some(String::from_utf8(protocol_type_bytes).map_err(|e| {
                KafkaError::CoordinatorNotAvailable(format!("无法解析protocol_type: {}", e))
            })?)
        };

        // generation_id - int32
        let generation_id = cursor.get_i32();

        // protocol - string
        let protocol_len = cursor.get_i32();
        let protocol = if protocol_len == -1 {
            None
        } else {
            let mut protocol_bytes = vec![0; protocol_len as usize];
            cursor.read_exact(&mut protocol_bytes).unwrap();
            Some(String::from_utf8(protocol_bytes).map_err(|e| {
                KafkaError::CoordinatorNotAvailable(format!("无法解析protocol: {}", e))
            })?)
        };

        // leader_id - string
        let leader_id_len = cursor.get_i32();
        let leader_id = if leader_id_len == -1 {
            None
        } else {
            let mut leader_id_bytes = vec![0; leader_id_len as usize];
            cursor.read_exact(&mut leader_id_bytes).unwrap();
            Some(String::from_utf8(leader_id_bytes).map_err(|e| {
                KafkaError::CoordinatorNotAvailable(format!("无法解析leader_id: {}", e))
            })?)
        };

        // members length - int32
        let members_len = cursor.get_i32() as usize;

        let mut members = HashMap::new();
        for _ in 0..members_len {
            let member_len = cursor.get_u32() as usize;
            let mut member_data = vec![0; member_len];
            cursor.read_exact(&mut member_data).unwrap();

            let member = MemberMetadata::deserialize(
                &member_data,
                group_id,
                &protocol_type.clone().unwrap(),
                &protocol.clone().unwrap(),
            )?;
            members.insert(member.id().to_string(), member);
        }

        Ok(Self {
            id: group_id.to_string(),
            protocol_type: protocol_type.clone(),
            generation_id,
            protocol: protocol.clone(),
            leader_id: leader_id.clone(),
            members,
            state: GroupState::Stable,
            new_member_added: false,
        })
    }

    // Getters and setters
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    pub fn protocol(&self) -> Option<&str> {
        self.protocol.as_deref()
    }

    pub fn state(&self) -> GroupState {
        self.state
    }

    pub fn is(&self, state: GroupState) -> bool {
        self.state == state
    }

    pub fn can_rebalance(&self) -> bool {
        GroupState::can_transition_to(self.state, GroupState::PreparingRebalance)
    }

    pub fn transition_to(&mut self, state: GroupState) {
        debug!("transition_to: {:?} -> {:?}", self.state, state);
        self.state = state;
    }

    pub fn init_next_generation(&mut self) {
        if self.members.is_empty() {
            self.generation_id += 1;
            self.protocol = None;
            self.transition_to(GroupState::Empty);
        } else {
            self.generation_id += 1;
            self.new_member_added = false;
            self.protocol = self.select_protocol().ok();
            self.transition_to(GroupState::AwaitingSync);
        }
    }

    pub fn new_member_added(&self) -> bool {
        self.new_member_added
    }

    pub fn set_new_member_added(&mut self) {
        self.new_member_added = true;
    }

    pub fn reset_new_member_added(&mut self) {
        self.new_member_added = false;
    }

    pub fn get_member(&self, member_id: &str) -> Option<&MemberMetadata> {
        self.members.get(member_id)
    }

    pub fn get_mut_member(&mut self, member_id: &str) -> Option<&mut MemberMetadata> {
        self.members.get_mut(member_id)
    }

    pub fn all_member_metadata(&self) -> Vec<&MemberMetadata> {
        self.members.values().collect()
    }
    pub fn protocol_type(&self) -> Option<&str> {
        self.protocol_type.as_deref()
    }
    pub fn leader_id(&self) -> Option<&str> {
        self.leader_id.as_deref()
    }
}
