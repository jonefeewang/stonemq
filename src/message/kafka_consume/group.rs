use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use tokio::{
    sync::{oneshot, RwLock, RwLockWriteGuard},
    time::Instant,
};

use crate::{
    protocol::api_schemas::consumer_protocol::ProtocolMetadata,
    request::{
        consumer_group::SyncGroupResponse,
        errors::{KafkaError, KafkaResult},
    },
};

use super::coordinator::JoinGroupResult;

#[derive(Debug)]
pub struct MemberMetadata {
    id: String,
    client_id: String,
    client_host: String,
    group_id: String,
    rebalance_timeout: i32,
    session_timeout: i32,
    protocol_type: String,
    supported_protocols: Vec<Arc<ProtocolMetadata>>,
    assignment: Option<Bytes>,
    join_group_cb_sender: Option<oneshot::Sender<JoinGroupResult>>,
    sync_group_cb_sender: Option<oneshot::Sender<SyncGroupResponse>>,
    last_heartbeat: Instant,
    is_leaving: bool,
}
impl MemberMetadata {
    pub fn new(
        id: &str,
        client_id: &str,
        client_host: &str,
        group_id: &str,
        rebalance_timeout: i32,
        session_timeout: i32,
        protocol_type: &str,
        supported_protocols: Vec<Arc<ProtocolMetadata>>,
    ) -> Self {
        Self {
            id: id.to_string(),
            client_id: client_id.to_string(),
            client_host: client_host.to_string(),
            group_id: group_id.to_string(),
            rebalance_timeout,
            session_timeout,
            protocol_type: protocol_type.to_string(),
            supported_protocols,
            assignment: None,
            join_group_cb_sender: None,
            sync_group_cb_sender: None,
            last_heartbeat: Instant::now(),
            is_leaving: false,
        }
    }
    pub fn update_supported_protocols(&mut self, protocols: Vec<ProtocolMetadata>) {
        self.supported_protocols = protocols.into_iter().map(|p| Arc::new(p)).collect();
    }

    /// 检查提供的协议元数据是否与当前存储的元数据匹配
    ///
    /// # 返回值
    /// 如果提供的协议元数据与当前存储的元数据匹配,返回 true,否则返回 false
    pub fn protocol_equals(&self, protocols: &Vec<ProtocolMetadata>) -> bool {
        for (index, protocol) in protocols.iter().enumerate() {
            if self.supported_protocols[index].name != protocol.name
                || self.supported_protocols[index].metadata != protocol.metadata
            {
                return false;
            }
        }
        true
    }
    /// 对候选协议进行投票
    pub fn vote(&self, candidates: &HashSet<String>) -> String {
        // 从成员支持的协议中选择第一个也在候选列表中的协议
        self.protocols()
            .intersection(candidates)
            .next()
            .cloned()
            .unwrap_or_default()
    }
    fn protocols(&self) -> HashSet<String> {
        self.supported_protocols
            .iter()
            .map(|p| p.name.clone())
            .collect()
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn set_join_group_callback_channel(&mut self, tx: oneshot::Sender<JoinGroupResult>) {
        self.join_group_cb_sender = Some(tx);
    }
    pub fn take_join_group_callback_channel(&mut self) -> oneshot::Sender<JoinGroupResult> {
        self.join_group_cb_sender.take().unwrap()
    }
    pub fn set_sync_group_callback_channel(&mut self, tx: oneshot::Sender<SyncGroupResponse>) {
        self.sync_group_cb_sender = Some(tx);
    }
    pub fn take_sync_group_callback_channel(&mut self) -> oneshot::Sender<SyncGroupResponse> {
        self.sync_group_cb_sender.take().unwrap()
    }
    pub fn metadata(&self, protocol: &str) -> Option<Bytes> {
        self.supported_protocols
            .iter()
            .find(|p| p.name == protocol)
            .map(|p| p.metadata.clone())
    }
    pub fn assignment(&self) -> Option<Bytes> {
        self.assignment.clone()
    }
    pub fn set_assignment(&mut self, assignment: Bytes) {
        self.assignment = Some(assignment);
    }
    pub fn last_heartbeat(&self) -> Instant {
        self.last_heartbeat
    }
    pub fn update_last_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
    }
    pub fn session_timeout(&self) -> i32 {
        self.session_timeout
    }
    pub fn is_awaiting_join(&self) -> bool {
        self.join_group_cb_sender.is_some()
    }
    pub fn is_awaiting_sync(&self) -> bool {
        self.sync_group_cb_sender.is_some()
    }
    pub fn is_leaving(&self) -> bool {
        self.is_leaving
    }
    pub fn set_leaving(&mut self) {
        self.is_leaving = true;
    }
}

#[derive(Debug)]
pub struct GroupMetadata {
    id: String,
    generation_id: i32,
    members: HashMap<String, MemberMetadata>,
    offset: HashMap<String, i64>,
    leader_id: Option<String>,
    /// assignor.name 选出的协议名称
    protocol: Option<String>,
    state: GroupState,
    protocol_type: Option<String>,
    new_member_added: bool,
}
impl GroupMetadata {
    pub fn new(group_id: &str) -> Self {
        Self {
            id: group_id.to_string(),
            generation_id: 0,
            members: HashMap::new(),
            leader_id: None,
            protocol: None,
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
        if self.leader_id.is_none() {
            self.leader_id = Some(member_metadata.id.clone());
        }

        self.members
            .insert(member_metadata.id.clone(), member_metadata);
        Ok(())
    }
    pub fn remove_member(&mut self, member_id: &str) {
        self.members.remove(member_id);
    }
    pub fn all_members(&mut self) -> Vec<&mut MemberMetadata> {
        self.members.values_mut().collect()
    }

    pub fn supports_protocols(&self, protocols: &HashSet<String>) -> bool {
        self.members.is_empty() || self.candidate_protocols().intersection(protocols).count() > 0
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
    pub fn get_member(&self, member_id: &str) -> Option<&MemberMetadata> {
        self.members.get(member_id)
    }
    pub fn get_mut_member(&mut self, member_id: &str) -> Option<&mut MemberMetadata> {
        self.members.get_mut(member_id)
    }
    pub fn protocol_type_equals(&self, protocol_type: &str) -> bool {
        self.protocol_type == Some(protocol_type.to_string())
    }
    pub fn current_state(&self) -> &GroupState {
        &self.state
    }
    pub fn leader_id(&self) -> Option<&str> {
        self.leader_id.as_deref()
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
    pub fn set_new_member_added(&mut self) {
        self.new_member_added = true;
    }
    pub fn reset_new_member_added(&mut self) {
        self.new_member_added = false;
    }
    pub fn not_yet_rejoined_members(&self) -> Vec<String> {
        self.members
            .values()
            .filter(|m| m.join_group_cb_sender.is_none())
            .map(|m| m.id.clone())
            .collect()
    }

    /// 获取当前成员的匹配组协议的元数据
    pub fn current_member_metadata(&self) -> HashMap<String, Bytes> {
        self.members
            .values()
            .map(|m| {
                (
                    m.id.clone(),
                    m.metadata(self.protocol.as_ref().unwrap()).unwrap(),
                )
            })
            .collect()
    }
    pub fn protocol(&self) -> Option<&str> {
        self.protocol.as_deref()
    }
    pub fn transition_to(&mut self, state: GroupState) {
        self.state = state;
    }
    /// 获取组内所有成员中最大的 rebalance 超时时间
    pub fn max_rebalance_timeout_ms(&self) -> i32 {
        self.members
            .values()
            .fold(0, |timeout, member| timeout.max(member.rebalance_timeout))
    }
    /// 为组选择一个协议
    ///
    /// # 返回值
    /// 返回得票最多的协议名称
    ///
    /// # Errors
    /// 如果组内没有成员,会返回错误
    pub fn select_protocol(&self) -> KafkaResult<String> {
        // 获取所有成员都支持的协议列表
        let candidates = self.candidate_protocols();

        // 让每个成员对候选协议进行投票
        let votes: Vec<(String, usize)> = self
            .all_member_metadata()
            .iter()
            // 每个成员对候选协议进行投票
            .map(|member| member.vote(&candidates))
            // 收集所有投票
            .fold(HashMap::new(), |mut acc, protocol| {
                *acc.entry(protocol).or_insert(0) += 1;
                acc
            })
            // 转换成Vec以便找出最大值
            .into_iter()
            .collect();

        // 找出得票最多的协议
        let vote = votes
            .iter()
            .max_by_key(|&(_, count)| count)
            .map(|(protocol, _)| protocol)
            .unwrap();

        Ok(vote.clone())
    }

    /// 获取所有成员共同支持的协议列表
    fn candidate_protocols(&self) -> HashSet<String> {
        self.all_member_metadata()
            .iter()
            .map(|member| member.protocols())
            .fold(None, |acc: Option<HashSet<String>>, protocols| match acc {
                None => Some(protocols),
                Some(acc) => Some(acc.intersection(&protocols).cloned().collect()),
            })
            .unwrap_or_default()
    }

    /// 获取所有成员的元数据
    pub fn all_member_metadata(&self) -> Vec<&MemberMetadata> {
        self.members.values().collect()
    }
    pub fn init_next_generation(&mut self) {
        self.generation_id += 1;
        self.new_member_added = false;
    }
}

pub struct GroupMetadataManager {
    group_metadata: DashMap<String, Arc<RwLock<GroupMetadata>>>,
}

impl GroupMetadataManager {
    pub fn new() -> Self {
        Self {
            group_metadata: DashMap::new(),
        }
    }

    pub fn add_group(&self, group_metadata: GroupMetadata) -> Arc<RwLock<GroupMetadata>> {
        self.group_metadata
            .entry(group_metadata.id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(group_metadata)))
            .value()
            .clone()
    }
    pub fn get_group(&self, group_id: &str) -> Option<Arc<RwLock<GroupMetadata>>> {
        self.group_metadata.get(group_id).map(|g| g.clone())
    }
    pub fn store_group(
        &self,
        write_lock: &RwLockWriteGuard<GroupMetadata>,
        group_assignment: Option<&HashMap<String, Bytes>>,
    ) -> KafkaResult<()> {
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
