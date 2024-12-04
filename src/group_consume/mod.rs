mod coordinator;
mod delayed_join;
mod group_metadata;
mod group_metadata_manger;
mod group_state;
mod member_metadata;

pub use coordinator::GroupCoordinator;

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use coordinator::JoinGroupResult;
use dashmap::DashMap;
use group_state::GroupState;
use tokio::{
    sync::{oneshot, RwLock},
    time::Instant,
};

use crate::request::SyncGroupResponse;

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

#[derive(Debug)]
pub struct GroupMetadataManager {
    groups: DashMap<String, Arc<RwLock<GroupMetadata>>>,
}

/// 协议信息
#[derive(Debug, Clone)]
struct Protocol {
    name: String,
    metadata: Bytes,
}
