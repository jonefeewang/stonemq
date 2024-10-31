use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::BytesMut;
use dashmap::DashMap;

use crate::{
    protocol::api_schemas::consumer_protocol::ProtocolMetadata,
    request::errors::{KafkaError, KafkaResult},
};

#[derive(Debug)]
pub struct MemberMetadata {
    id: String,
    client_id: String,
    client_host: String,
    group_id: String,
    rebalance_timeout: i32,
    session_timeout: i32,
    protocol_type: String,
    supported_protocols: Vec<ProtocolMetadata>,
    assignment: Option<BytesMut>,
    is_awaiting_join_result: bool,
}
impl MemberMetadata {
    pub fn new(
        id: String,
        client_id: String,
        client_host: String,
        group_id: String,
        rebalance_timeout: i32,
        session_timeout: i32,
        protocol_type: String,
        supported_protocols: Vec<ProtocolMetadata>,
    ) -> Self {
        Self {
            id,
            client_id,
            client_host,
            group_id,
            rebalance_timeout,
            session_timeout,
            protocol_type,
            supported_protocols,
            assignment: None,
            is_awaiting_join_result: false,
        }
    }
    pub fn match_protocol(&self, protocols: Vec<ProtocolMetadata>) -> bool {
        self.supported_protocols
            .iter()
            .any(|p| protocols.contains(&p))
    }
}

#[derive(Debug)]
pub struct GroupMetadata {
    id: String,
    generation_id: i32,
    members: HashMap<String, MemberMetadata>,
    offset: HashMap<String, i64>,
    leader_id: String,
    protocol: String,
    state: GroupState,
    protocol_type: Option<String>,
    new_member_added: bool,
}
impl GroupMetadata {
    pub fn new(group_id: String) -> Self {
        Self {
            id: group_id,
            generation_id: 0,
            members: HashMap::new(),
            leader_id: "".to_string(),
            protocol: "".to_string(),
            offset: HashMap::new(),
            state: GroupState::Empty,
            protocol_type: None,
            new_member_added: false,
        }
    }
    pub fn add_member(&mut self, member_metadata: MemberMetadata) -> KafkaResult<()> {
        if self.members.is_empty() {
            self.protocol_type = Some(member_metadata.protocol_type.clone());
        }
        if self.id != member_metadata.group_id {
            return Err(KafkaError::InvalidGroupId(member_metadata.group_id));
        }
        if self.protocol_type_equals(&member_metadata.protocol_type) {
            return Err(KafkaError::InconsistentGroupProtocol(
                member_metadata.protocol_type,
            ));
        }

        let supported_protocols = member_metadata
            .supported_protocols
            .iter()
            .map(|p| p.name.clone())
            .collect();
        if !self.supports_protocols(&supported_protocols) {
            return Err(KafkaError::InconsistentGroupProtocol(
                supported_protocols.into_iter().collect(),
            ));
        }
        if self.leader_id.is_empty() {
            self.leader_id = member_metadata.id.clone();
        }

        self.members
            .insert(member_metadata.id.clone(), Arc::new(member_metadata));
        Ok(())
    }
    pub fn supports_protocols(&self, protocols: &HashSet<String>) -> bool {
        self.protocols().intersection(protocols).count() > 0
    }
    pub fn protocols(&self) -> HashSet<String> {
        self.members
            .values()
            .map(|m| m.protocol_type.clone())
            .collect()
    }
    pub fn is(&self, state: GroupState) -> bool {
        matches!(&self.state, state)
    }
    pub fn not(&self, state: GroupState) -> bool {
        !self.is(state)
    }
    pub fn has_member(&self, member_id: &str) -> bool {
        self.members.contains_key(member_id)
    }
    pub fn protocol_type_equals(&self, protocol_type: &str) -> bool {
        self.protocol_type == Some(protocol_type.to_string())
    }
    pub fn current_state(&self) -> &GroupState {
        &self.state
    }
    pub fn leader_id(&self) -> &str {
        &self.leader_id
    }
    pub fn get_member(&self, member_id: &str) -> Option<MemberMetadata> {
        self.members.get(member_id).map(|m| m.clone())
    }
    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn can_rebalance(&self) -> bool {
        GroupState::can_transition_to(self.state, GroupState::PreparingRebalance)
    }
    pub fn cancel_all_member_assignment(&mut self) {
        self.members.iter_mut().for_each(|(_, member)| {
            member.assignment = None;
        });
    }
    pub fn new_member_added(&self) -> bool {
        self.new_member_added
    }
    pub fn reset_new_member_added(&mut self) {
        self.new_member_added = false;
    }
    pub fn not_yet_rejoined_members(&self) -> Vec<String> {
        self.members
            .values()
            .filter(|m| !m.is_awaiting_join_result)
            .map(|m| m.id.clone())
            .collect()
    }
}

pub struct GroupMetadataManager {
    group_metadata: DashMap<String, Arc<Mutex<GroupMetadata>>>,
}

impl GroupMetadataManager {
    pub fn new() -> Self {
        Self {
            group_metadata: DashMap::new(),
        }
    }

    pub fn add_group(&self, group_metadata: GroupMetadata) -> GroupMetadata {
        todo!()
    }
    pub fn get_group(&self, group_id: &str) -> Option<GroupMetadata> {
        todo!()
    }
    pub fn store_group(&self, group_id: &str, group_assignment: HashMap<String, Vec<u8>>) {
        todo!()
    }
    pub fn store_offset(&self, group_id: &str, offset: HashMap<String, i64>) {
        todo!()
    }
    pub fn get_offset(&self, group_id: &str, member_id: &str) -> Option<i64> {
        todo!()
    }
    pub fn load_groups(&self) -> Vec<String> {
        todo!()
    }
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GroupState {
    /// Group is preparing to rebalance
    ///
    /// action: respond to heartbeats with REBALANCE_IN_PROGRESS
    ///         respond to sync group with REBALANCE_IN_PROGRESS
    ///         remove member on leave group request
    ///         park join group requests from new or existing members until all expected members have joined
    ///         allow offset commits from previous generation
    ///         allow offset fetch requests
    /// transition: some members have joined by the timeout => AwaitingSync
    ///             all members have left the group => Empty
    ///             group is removed by partition emigration => Dead
    PreparingRebalance = 1,

    /// Group is awaiting state assignment from the leader
    ///
    /// action: respond to heartbeats with REBALANCE_IN_PROGRESS
    ///         respond to offset commits with REBALANCE_IN_PROGRESS
    ///         park sync group requests from followers until transition to Stable
    ///         allow offset fetch requests
    /// transition: sync group with state assignment received from leader => Stable
    ///             join group from new member or existing member with updated metadata => PreparingRebalance
    ///             leave group from existing member => PreparingRebalance
    ///             member failure detected => PreparingRebalance
    ///             group is removed by partition emigration => Dead
    AwaitingSync = 2,

    /// Group is stable
    ///
    /// action: respond to member heartbeats normally
    ///         respond to sync group from any member with current assignment
    ///         respond to join group from followers with matching metadata with current group metadata
    ///         allow offset commits from member of current generation
    ///         allow offset fetch requests
    /// transition: member failure detected via heartbeat => PreparingRebalance
    ///             leave group from existing member => PreparingRebalance
    ///             leader join-group received => PreparingRebalance
    ///             follower join-group with new metadata => PreparingRebalance
    ///             group is removed by partition emigration => Dead
    Stable = 3,

    /// Group has no more members and its metadata is being removed
    ///
    /// action: respond to join group with UNKNOWN_MEMBER_ID
    ///         respond to sync group with UNKNOWN_MEMBER_ID
    ///         respond to heartbeat with UNKNOWN_MEMBER_ID
    ///         respond to leave group with UNKNOWN_MEMBER_ID
    ///         respond to offset commit with UNKNOWN_MEMBER_ID
    ///         allow offset fetch requests
    /// transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
    Dead = 4,

    /// Group has no more members, but lingers until all offsets have expired. This state
    /// also represents groups which use Kafka only for offset commits and have no members.
    ///
    /// action: respond normally to join group from new members
    ///         respond to sync group with UNKNOWN_MEMBER_ID
    ///         respond to heartbeat with UNKNOWN_MEMBER_ID
    ///         respond to leave group with UNKNOWN_MEMBER_ID
    ///         respond to offset commit with UNKNOWN_MEMBER_ID
    ///         allow offset fetch requests
    /// transition: last offsets removed in periodic expiration task => Dead
    ///             join group from a new member => PreparingRebalance
    ///             group is removed by partition emigration => Dead
    ///             group is removed by expiration => Dead
    Empty = 5,
}
impl GroupState {
    pub const fn can_transition_to(current: GroupState, target: GroupState) -> bool {
        match (current, target) {
            (_, GroupState::Dead) => true,  // 任何状态都可以转换到Dead
            (GroupState::Dead, _) => false, // Dead状态不能转换到其他状态
            (GroupState::Empty, GroupState::PreparingRebalance) => true,
            (GroupState::Stable, GroupState::PreparingRebalance) => true,
            (GroupState::AwaitingSync, GroupState::PreparingRebalance) => true,
            (GroupState::PreparingRebalance, GroupState::AwaitingSync) => true,
            (GroupState::PreparingRebalance, GroupState::Empty) => true,
            (GroupState::AwaitingSync, GroupState::Stable) => true,
            _ => false,
        }
    }
}
