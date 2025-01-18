/// Module for managing consumer group coordination and metadata.
/// This module provides functionality for consumer group management, including:
/// - Group membership and state management
/// - Group coordination and rebalancing
/// - Member metadata tracking
/// - Delayed operation handling
mod coordinator;
mod delayed_join;
mod group_metadata;
mod group_metadata_manger;
mod group_state;
mod member_metadata;

pub use coordinator::GroupCoordinator;
use rocksdb::DB;

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

/// Metadata for a consumer group.
/// Contains information about the group's members, state, and protocols.
#[derive(Debug)]
pub struct GroupMetadata {
    /// Unique identifier for the group
    id: String,
    /// Current generation ID of the group, incremented on each rebalance
    generation_id: i32,
    /// Type of protocol used by the group (e.g. "consumer")
    protocol_type: Option<String>,
    /// Selected protocol for the group
    protocol: Option<String>,
    /// Map of member IDs to their metadata
    members: HashMap<String, MemberMetadata>,
    /// Current state of the group
    state: GroupState,
    /// Flag indicating if a new member was added
    new_member_added: bool,
    /// ID of the group's leader member
    leader_id: Option<String>,
}

/// Metadata for an individual member in a consumer group.
/// Contains member-specific information and configuration.
#[derive(Debug)]
pub struct MemberMetadata {
    // Basic information
    /// Unique identifier for the member
    id: String,
    /// Client identifier
    client_id: String,
    /// Host address of the client
    client_host: String,
    /// ID of the group this member belongs to
    _group_id: String,

    // Timeout configuration
    /// Maximum time (in ms) to wait for rebalance
    rebalance_timeout: i32,
    /// Session timeout (in ms) after which member is considered dead
    session_timeout: i32,

    // Protocol information
    /// Type of protocol used by this member
    protocol_type: String,
    /// List of protocols supported by this member
    supported_protocols: Vec<Protocol>,
    /// Current partition assignment for this member
    assignment: Option<Bytes>,

    // Callback channels
    join_group_cb_sender: Option<oneshot::Sender<JoinGroupResult>>,
    sync_group_cb_sender: Option<oneshot::Sender<SyncGroupResponse>>,

    // State information
    last_heartbeat: Instant,
    is_leaving: bool,
}

#[derive(Debug)]
pub struct GroupMetadataManager {
    groups: DashMap<String, Arc<RwLock<GroupMetadata>>>,
    db: DB,
}

/// 协议信息
#[derive(Debug, Clone)]
struct Protocol {
    name: String,
    metadata: Bytes,
}
