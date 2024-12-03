use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::Read,
    sync::Arc,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use rocksdb::{IteratorMode, Options, SliceTransform, DB};
use tokio::{
    sync::{oneshot, RwLock, RwLockWriteGuard},
    time::Instant,
};
use tracing::{error, info, trace};

use crate::{
    global_config,
    message::TopicPartition,
    request::{
        consumer_group::{
            PartitionOffsetCommitData, PartitionOffsetData, ProtocolMetadata, SyncGroupResponse,
        },
        errors::{KafkaError, KafkaResult},
    },
};

use super::coordinator::JoinGroupResult;

/// 表示消费者组成员的元数据
#[derive(Debug)]
pub struct MemberMetadata {
    // 基础信息
    id: String,
    client_id: String,
    client_host: String,
    _group_id: String,

    // 超时配置
    rebalance_timeout: i32,
    session_timeout: i32,

    // 协议相关
    protocol_type: String,
    supported_protocols: Vec<Protocol>,
    assignment: Option<Bytes>,

    // 回调通道
    join_group_cb_sender: Option<oneshot::Sender<JoinGroupResult>>,
    sync_group_cb_sender: Option<oneshot::Sender<SyncGroupResponse>>,

    // 状态信息
    last_heartbeat: Instant,
    is_leaving: bool,
}

/// 协议信息
#[derive(Debug, Clone)]
struct Protocol {
    name: String,
    metadata: Bytes,
}

impl MemberMetadata {
    /// 创建新的成员元数据
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: impl Into<String>,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
        group_id: impl Into<String>,
        rebalance_timeout: i32,
        session_timeout: i32,
        protocol_type: impl Into<String>,
        supported_protocols: Vec<(String, Bytes)>,
    ) -> Self {
        Self {
            id: id.into(),
            client_id: client_id.into(),
            client_host: client_host.into(),
            _group_id: group_id.into(),
            rebalance_timeout,
            session_timeout,
            protocol_type: protocol_type.into(),
            supported_protocols: supported_protocols
                .into_iter()
                .map(|(name, metadata)| Protocol { name, metadata })
                .collect(),
            assignment: None,
            join_group_cb_sender: None,
            sync_group_cb_sender: None,
            last_heartbeat: Instant::now(),
            is_leaving: false,
        }
    }

    /// 更新支持的协议列表
    pub fn update_supported_protocols(&mut self, protocols: Vec<ProtocolMetadata>) {
        self.supported_protocols = protocols
            .into_iter()
            .map(|p| Protocol {
                name: p.name,
                metadata: Bytes::copy_from_slice(&p.metadata),
            })
            .collect();
    }

    /// 检查提供的协议元数据是否与当前存储的元数据匹配
    pub fn protocol_matches(&self, protocols: &[ProtocolMetadata]) -> bool {
        if protocols.len() != self.supported_protocols.len() {
            return false;
        }

        protocols.iter().enumerate().all(|(i, protocol)| {
            let current = &self.supported_protocols[i];
            current.name == protocol.name && current.metadata == protocol.metadata
        })
    }

    /// 获取支持的协议名称集合
    fn protocols(&self) -> HashSet<String> {
        self.supported_protocols
            .iter()
            .map(|p| p.name.clone())
            .collect()
    }

    /// 对候选协议进行投票
    fn vote(&self, candidates: &HashSet<String>) -> String {
        self.protocols()
            .intersection(candidates)
            .next()
            .cloned()
            .unwrap_or_default()
    }

    // Getters
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn metadata(&self, protocol: &str) -> Option<Bytes> {
        self.supported_protocols
            .iter()
            .find(|p| p.name == protocol)
            .map(|p| p.metadata.clone())
    }

    // Assignment 相关方法
    pub fn assignment(&self) -> Option<Bytes> {
        self.assignment.clone()
    }

    pub fn set_assignment(&mut self, assignment: Bytes) {
        self.assignment = Some(assignment);
    }

    // Callback channel 相关方法
    pub fn set_join_group_callback(&mut self, tx: oneshot::Sender<JoinGroupResult>) {
        self.join_group_cb_sender = Some(tx);
    }

    pub fn take_join_group_callback(&mut self) -> oneshot::Sender<JoinGroupResult> {
        self.join_group_cb_sender.take().unwrap()
    }

    pub fn set_sync_group_callback(&mut self, tx: oneshot::Sender<SyncGroupResponse>) {
        self.sync_group_cb_sender = Some(tx);
    }

    pub fn take_sync_group_callback(&mut self) -> Option<oneshot::Sender<SyncGroupResponse>> {
        self.sync_group_cb_sender.take()
    }

    // Heartbeat 相关方法
    pub fn last_heartbeat(&self) -> Instant {
        self.last_heartbeat
    }

    pub fn update_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
    }

    // 状态检查方法
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

    pub fn serialize(&self, group_protocol: &str) -> KafkaResult<Bytes> {
        let mut buffer = BytesMut::new();
        // id - string
        buffer.put_u32(self.id.len() as u32);
        buffer.put(self.id.as_bytes());
        // client_id - string
        buffer.put_u32(self.client_id.len() as u32);
        buffer.put(self.client_id.as_bytes());
        // client_host - string
        buffer.put_u32(self.client_host.len() as u32);
        buffer.put(self.client_host.as_bytes());
        // session_timeout - int32
        buffer.put_i32(self.session_timeout);
        // rebalance_timeout - int32
        buffer.put_i32(self.rebalance_timeout);
        // subscription
        if let Some(metadata) = self.metadata(group_protocol) {
            buffer.put_i32(metadata.len() as i32);
            buffer.put(metadata);
        } else {
            buffer.put_i32(-1);
        }
        // assignment
        if let Some(assignment) = self.assignment() {
            buffer.put_i32(assignment.len() as i32); // 修改为i32以匹配反序列化
            buffer.put(assignment);
        } else {
            buffer.put_i32(-1);
        }

        Ok(buffer.freeze())
    }
    pub fn deserialize(
        data: &[u8],
        group_id: &str,
        protocol_type: &String,
        protocol: &String,
    ) -> KafkaResult<Self> {
        let mut cursor = std::io::Cursor::new(data);

        // id - string
        let id_len = cursor.get_u32() as usize;
        let mut id_bytes = vec![0; id_len];
        cursor.read_exact(&mut id_bytes).unwrap();
        let id = String::from_utf8(id_bytes)
            .map_err(|e| KafkaError::CoordinatorNotAvailable(format!("无法解析id: {}", e)))?;

        // client_id - string
        let client_id_len = cursor.get_u32() as usize;
        let mut client_id_bytes = vec![0; client_id_len];
        cursor.read_exact(&mut client_id_bytes).unwrap();
        let client_id = String::from_utf8(client_id_bytes).map_err(|e| {
            KafkaError::CoordinatorNotAvailable(format!("无法解析client_id: {}", e))
        })?;

        // client_host - string
        let client_host_len = cursor.get_u32() as usize;
        let mut client_host_bytes = vec![0; client_host_len];
        cursor.read_exact(&mut client_host_bytes).unwrap();
        let client_host = String::from_utf8(client_host_bytes).map_err(|e| {
            KafkaError::CoordinatorNotAvailable(format!("无法解析client_host: {}", e))
        })?;

        // session_timeout - int32
        let session_timeout = cursor.get_i32();

        // rebalance_timeout - int32
        let rebalance_timeout = cursor.get_i32();

        // subscription metadata
        let metadata_len = cursor.get_i32();
        let mut metadata: Option<Bytes> = None;
        if metadata_len > 0 {
            let mut buf = vec![0; metadata_len as usize];
            cursor.read_exact(&mut buf).unwrap();
            metadata = Some(Bytes::from(buf));
        }

        // assignment
        let assignment_len = cursor.get_i32(); // 修改为get_i32以匹配序列化
        let mut assignment: Option<Bytes> = None;
        if assignment_len > 0 {
            let mut buf = vec![0; assignment_len as usize];
            cursor.read_exact(&mut buf).unwrap();
            assignment = Some(Bytes::from(buf));
        }

        let mut member = Self::new(
            id,
            client_id,
            client_host,
            group_id,
            rebalance_timeout,
            session_timeout,
            protocol_type,
            vec![(protocol.to_string(), metadata.unwrap())],
        );

        // 设置assignment
        if let Some(assignment) = assignment {
            member.set_assignment(assignment);
        }

        Ok(member)
    }
}

// 组状态
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
    /// 检查是否可以从当前状态转换到目标状态
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

/// 消费者组元数据
#[derive(Debug)]
pub struct GroupMetadata {
    id: String,
    generation_id: i32,
    protocol_type: Option<String>,
    protocol: Option<String>,
    members: HashMap<String, MemberMetadata>,
    state: GroupState,
    new_member_added: bool,
    leader_id: Option<String>,
}

impl GroupMetadata {
    /// 创建新的组元数据
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

    /// 添加新成员
    pub fn add_member(&mut self, member: MemberMetadata) {
        if self.members.is_empty() {
            self.protocol_type = Some(member.protocol_type.clone());
        }
        if self.leader_id.is_none() {
            self.leader_id = Some(member.id.clone());
        }

        self.members.insert(member.id.clone(), member);
    }

    /// 移除成员
    pub fn remove_member(&mut self, member_id: &str) -> Option<MemberMetadata> {
        let removed_member = self.members.remove(member_id);
        if member_id == self.leader_id.as_deref().unwrap() {
            self.leader_id = self.members.keys().next().map(|k| k.to_string());
        }
        removed_member
    }

    /// 检查组是否包含指定成员  
    pub fn has_member(&self, member_id: &str) -> bool {
        self.members.contains_key(member_id)
    }

    /// 获取所有成员的可变引用
    pub fn members(&mut self) -> Vec<&mut MemberMetadata> {
        self.members.values_mut().collect()
    }

    /// 获取组的最大重新平衡超时时间
    pub fn max_rebalance_timeout(&self) -> i32 {
        self.members
            .values()
            .map(|m| m.rebalance_timeout)
            .max()
            .unwrap_or(0)
    }

    /// 获取当前成员的匹配组选定协议的元数据
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

    /// 为组选择一个协议
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

    /// 获取所有成员共同支持的协议列表
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

    /// 检查组是否支持给定的协议列表
    pub fn is_support_protocols(&self, protocols: &HashSet<String>) -> bool {
        self.members.is_empty() || self.candidate_protocols().intersection(protocols).count() > 0
    }

    /// 获取未重新加入的成员列表
    pub fn not_yet_rejoined_members(&self) -> Vec<String> {
        self.members
            .values()
            .filter(|m| m.join_group_cb_sender.is_none())
            .map(|m| m.id.clone())
            .collect()
    }

    /// 取消所有成员的分配
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

#[derive(Debug)]
pub struct GroupMetadataManager {
    groups: DashMap<String, Arc<RwLock<GroupMetadata>>>,
}

impl GroupMetadataManager {
    const GROUP_PREFIX: &str = "group";
    const OFFSET_PREFIX: &str = "offset";
    pub fn group_db_key(group_id: &str) -> String {
        format!("{}:{}", Self::GROUP_PREFIX, group_id)
    }
    pub fn offset_db_key(group_id: &str, topic_partition: &TopicPartition) -> String {
        format!("{}:{}:{}", Self::OFFSET_PREFIX, group_id, topic_partition)
    }
    pub fn new(groups: DashMap<String, Arc<RwLock<GroupMetadata>>>) -> Self {
        Self { groups }
    }

    pub fn add_group(&self, group_metadata: GroupMetadata) -> Arc<RwLock<GroupMetadata>> {
        self.groups
            .entry(group_metadata.id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(group_metadata)))
            .value()
            .clone()
    }
    pub fn get_group(&self, group_id: &str) -> Option<Arc<RwLock<GroupMetadata>>> {
        self.groups.get(group_id).map(|g| g.clone())
    }
    pub fn store_group(&self, write_lock: &RwLockWriteGuard<GroupMetadata>) -> KafkaResult<()> {
        let group_id = write_lock.id.clone();
        let group_data = write_lock.serialize()?;

        let db_path = &global_config().general.local_db_path;
        let db = DB::open_default(db_path).unwrap();
        let result = db.put(Self::group_db_key(&group_id), group_data);
        if result.is_err() {
            return Err(KafkaError::CoordinatorNotAvailable(group_id.clone()));
        }
        Ok(())
    }
    pub fn store_offset(
        &self,
        group_id: &str,
        member_id: &str,
        offsets: HashMap<TopicPartition, PartitionOffsetCommitData>,
    ) -> KafkaResult<()> {
        let db_path = &global_config().general.local_db_path;
        let db = DB::open_default(db_path).unwrap();
        for (topic_partition, offset_and_metadata) in offsets {
            let key = Self::offset_db_key(group_id, &topic_partition);
            let value = offset_and_metadata.serialize();
            if let Err(e) = value {
                error!("序列化offset失败: {}", e);
                return Err(KafkaError::Unknown(format!(
                    "group id:{}  member id:{} 序列化offset失败: {}",
                    group_id, member_id, e
                )));
            } else {
                let value = value.unwrap();
                let result = db.put(&key, &value);
                if result.is_err() {
                    let error_msg = format!(
                        "group id:{}  member id:{} 存储offset失败: {:?}",
                        group_id,
                        member_id,
                        result.err().unwrap()
                    );
                    error!("{}", error_msg);
                    return Err(KafkaError::Unknown(error_msg));
                } else {
                    trace!("存储offset成功: {}, value: {:?}", key, value);
                }
            }
        }
        Ok(())
    }
    pub fn get_offset(
        &self,
        group_id: &str,
        partitions: Option<Vec<TopicPartition>>,
    ) -> KafkaResult<HashMap<TopicPartition, PartitionOffsetData>> {
        let db_path = &global_config().general.local_db_path;
        let db = DB::open_default(db_path).unwrap();
        let mut offsets = HashMap::new();
        for partition in partitions.unwrap_or_default() {
            let key = Self::offset_db_key(group_id, &partition);
            let value = db.get(key);
            let partition_id = partition.partition;
            if let Ok(Some(value)) = value {
                // 如果offset存在，则返回offset
                let partition_offset_data = PartitionOffsetCommitData::deserialize(&value);
                if let Ok(partition_offset_data) = partition_offset_data {
                    offsets.insert(
                        partition,
                        PartitionOffsetData {
                            partition_id,
                            offset: partition_offset_data.offset,
                            metadata: partition_offset_data.metadata,
                            error: KafkaError::None,
                        },
                    );
                } else {
                    return Err(KafkaError::Unknown(format!(
                        "group id:{} 反序列化offset失败: {}",
                        group_id,
                        partition_offset_data.err().unwrap()
                    )));
                }
            } else {
                // 如果offset不存在，则返回0
                offsets.insert(
                    partition,
                    PartitionOffsetData {
                        partition_id,
                        offset: 0,
                        metadata: None,
                        error: KafkaError::None,
                    },
                );
            }
        }
        trace!("获取offset成功: {:?}", offsets);
        Ok(offsets)
    }
    pub fn load() -> Self {
        // 配置 RocksDB 选项
        let mut opts = Options::default();
        opts.create_if_missing(true);

        // 假设所有 Group ID 前缀为 "group:"
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(6));

        // 打开数据库
        let db = DB::open(&opts, &global_config().general.local_db_path).expect("无法打开 RocksDB");

        // 设置迭代器模式
        let mode = IteratorMode::From(Self::GROUP_PREFIX.as_bytes(), rocksdb::Direction::Forward);
        let iter = db.iterator(mode);

        // 执行前缀扫描
        let groups = DashMap::new();
        for result in iter {
            if let Ok((key, value)) = result {
                if key.starts_with(Self::GROUP_PREFIX.as_bytes()) {
                    // 处理键值对
                    let group_id = String::from_utf8_lossy(&key).to_string();
                    let group_metadata = GroupMetadata::deserialize(&value, &group_id).unwrap();
                    info!("加载组元数据: {} {:#?}", group_id, group_metadata);
                    groups.insert(group_id, Arc::new(RwLock::new(group_metadata)));
                } else {
                    // 超过前缀范围，停止扫描
                    break;
                }
            } else {
                error!("加载组元数据失败: {}", result.err().unwrap());
            }
        }
        Self::new(groups)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_member(id: &str) -> MemberMetadata {
        MemberMetadata::new(
            id,
            "client-1",
            "host-1",
            "group-1",
            3000,
            1000,
            "consumer",
            vec![("proto-1".to_string(), Bytes::from("meta-1"))],
        )
    }

    #[test]
    fn test_add_and_remove_member() {
        let mut group = GroupMetadata::new("test-group");
        let member = create_test_member("member-1");

        group.add_member(member);
        assert!(group.new_member_added());

        let removed = group.remove_member("member-1");
        assert!(removed.is_some());
    }

    #[test]
    fn test_protocol_selection() {
        let mut group = GroupMetadata::new("test-group");

        // Add members with different protocols
        let member1 = create_test_member("member-1");
        let member2 = create_test_member("member-2");

        group.add_member(member1);
        group.add_member(member2);

        let protocol = group.select_protocol();
        assert!(protocol.is_ok());
    }
}
