use std::{collections::HashMap, sync::Arc, time::Instant};

use crossbeam::atomic::AtomicCell;
use parking_lot::{RwLock, RwLockWriteGuard};
use tokio::sync::{broadcast, mpsc::Sender, oneshot};
use uuid::Uuid;

use crate::{
    message::kafka_consume::group::{GroupMetadata, GroupState, MemberMetadata},
    protocol::api_schemas::consumer_protocol::ProtocolMetadata,
    request::{
        consumer_group::{
            FindCoordinatorRequest, FindCoordinatorResponse, JoinGroupRequest, JoinGroupResponse,
        },
        errors::{ErrorCode, KafkaError, KafkaResult},
    },
    service::{GroupConfig, Node},
    utils::DelayedSyncOperationPurgatory,
    AppResult,
};

use super::{
    delayed_join::{DelayedHeartbeat, DelayedJoin, InitialDelayedJoin},
    group::{self, GroupMetadataManager},
};

pub struct GroupCoordinator {
    active: AtomicCell<bool>,
    node: Node,
    group_config: GroupConfig,
    group_manager: GroupMetadataManager,
    delayed_join_purgatory: Arc<DelayedSyncOperationPurgatory<DelayedJoin>>,
    initial_delayed_join_purgatory: Arc<DelayedSyncOperationPurgatory<InitialDelayedJoin>>,
    delayed_heartbeat_purgatory: Arc<DelayedSyncOperationPurgatory<DelayedHeartbeat>>,
}

impl GroupCoordinator {
    pub async fn new(
        node: Node,
        group_manager: GroupMetadataManager,
        group_config: GroupConfig,
        delayed_join_purgatory: Arc<DelayedSyncOperationPurgatory<DelayedJoin>>,
        initial_delayed_join_purgatory: Arc<DelayedSyncOperationPurgatory<InitialDelayedJoin>>,
        delayed_heartbeat_purgatory: Arc<DelayedSyncOperationPurgatory<DelayedHeartbeat>>,
    ) -> Self {
        Self {
            active: AtomicCell::new(false),
            node,
            group_config,
            group_manager,
            delayed_join_purgatory,
            initial_delayed_join_purgatory,
            delayed_heartbeat_purgatory,
        }
    }
    pub async fn startup(
        group_config: GroupConfig,
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: Sender<()>,
        node: Node,
    ) -> Self {
        let delayed_join_purgatory = DelayedSyncOperationPurgatory::<DelayedJoin>::new(
            "delayed_join_purgatory",
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
        )
        .await;

        let initial_delayed_join_purgatory =
            DelayedSyncOperationPurgatory::<InitialDelayedJoin>::new(
                "initial_delayed_join_purgatory",
                notify_shutdown.clone(),
                shutdown_complete_tx.clone(),
            )
            .await;
        let delayed_heartbeat_purgatory = DelayedSyncOperationPurgatory::<DelayedHeartbeat>::new(
            "delayed_heartbeat_purgatory",
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
        )
        .await;

        let group_manager = GroupMetadataManager::new();
        Self::new(
            node,
            group_manager,
            group_config,
            delayed_join_purgatory,
            initial_delayed_join_purgatory,
            delayed_heartbeat_purgatory,
        )
        .await
    }

    pub fn find_coordinator(&self, _: &str) -> AppResult<FindCoordinatorResponse> {
        // 因为stonemq目前支持单机，所以coordinator就是自身
        let response: FindCoordinatorResponse = self.node.clone().into();
        Ok(response)
    }
    pub async fn handle_join_group(
        self: Arc<Self>,
        request: JoinGroupRequest,
    ) -> KafkaResult<JoinGroupResult> {
        if request.group_id.is_empty() {
            return Ok(self.join_error(request.member_id.clone(), ErrorCode::InvalidGroupId));
        } else if !self.active.load() {
            return Ok(self.join_error(
                request.member_id.clone(),
                ErrorCode::CoordinatorNotAvailable,
            ));
        } else if request.session_timeout < self.group_config.group_min_session_timeout
            || request.session_timeout > self.group_config.group_max_session_timeout
        {
            return Ok(self.join_error(request.member_id.clone(), ErrorCode::InvalidSessionTimeout));
        }

        let group = self.group_manager.get_group(&request.group_id);
        return if let Some(group) = group {
            self.do_join_group(request, group).await
        } else if request.member_id.is_empty() {
            let group = GroupMetadata::new(&request.group_id);
            let group = self.group_manager.add_group(group);
            self.do_join_group(request, group).await
        } else {
            Ok(self.join_error(request.member_id.clone(), ErrorCode::UnknownMemberId))
        };
    }
    async fn do_join_group(
        self: Arc<Self>,
        request: JoinGroupRequest,
        group: Arc<RwLock<GroupMetadata>>,
    ) -> KafkaResult<JoinGroupResult> {
        let mut locked_group = group.write();
        if !locked_group.is(GroupState::Empty)
            && (!locked_group.protocol_type_equals(&request.protocol_type)
                || !locked_group.supports_protocols(
                    &request
                        .group_protocols
                        .iter()
                        .map(|p| p.name.clone())
                        .collect(),
                ))
        {
            // new member join group, but group is not empty, and protocol type or supported protocols are not match
            return Ok(self.join_error(
                request.member_id.clone(),
                ErrorCode::InconsistentGroupProtocol,
            ));
        } else if !request.member_id.is_empty() && !locked_group.has_member(&request.member_id) {
            // member_id is not empty, but not in group
            return Ok(self.join_error(request.member_id.clone(), ErrorCode::UnknownMemberId));
        } else {
            let self_clone = self.clone();
            let result = match locked_group.current_state() {
                GroupState::Dead => {
                    // some one else has deleted this group, so this member can't join
                    return Ok(
                        self.join_error(request.member_id.clone(), ErrorCode::UnknownMemberId)
                    );
                }
                GroupState::PreparingRebalance => {
                    // group is in rebalancing, new member and existing member are open to join

                    if request.member_id.is_empty() {
                        // new member join group
                        self.add_member_and_rebalance(request, &mut locked_group, group.clone())
                            .await
                    } else {
                        // existing member join group
                        locked_group
                            .update_member_protocols(&request.member_id, request.group_protocols);
                        let (tx, rx) = oneshot::channel();
                        locked_group.update_member_awaiting_join_result(&request.member_id, tx);
                        Ok(rx.await.unwrap())
                    }
                }
                GroupState::AwaitingSync => {
                    if request.member_id.is_empty() {
                        // new member join group
                        self.add_member_and_rebalance(request, &mut locked_group, group.clone())
                            .await
                    } else {
                        let member_id = request.member_id.clone();
                        let leader_id = locked_group.leader_id().unwrap().to_string();
                        let member = locked_group.get_member(&member_id).unwrap();
                        if member.id() == leader_id
                            || !member.match_protocol(&request.group_protocols)
                        {
                            // member is joining with the same metadata (which could be because it failed to
                            // receive the initial JoinGroup response), so just return current group information
                            // for the current generation.
                            return Ok(JoinGroupResult {
                                members: {
                                    if request.member_id == locked_group.leader_id().unwrap() {
                                        Some(locked_group.get_all_member_protocols())
                                    } else {
                                        None
                                    }
                                },
                                member_id: request.member_id.clone(),
                                generation_id: locked_group.generation_id(),
                                sub_protocol: locked_group.protocol().unwrap().to_string(),
                                leader_id: locked_group.leader_id().unwrap().to_string(),
                                error: None,
                            });
                        } else {
                            // member has changed protocol, need rebalance
                            locked_group.update_member_protocols(
                                &request.member_id,
                                request.group_protocols,
                            );
                            let (tx, rx) = oneshot::channel();
                            locked_group.update_member_awaiting_join_result(&request.member_id, tx);
                            Ok(rx.await.unwrap())
                        }
                    }
                }
                GroupState::Empty | GroupState::Stable => {
                    if request.member_id.is_empty() {
                        // new member join group, and group is empty
                        self.add_member_and_rebalance(request, &mut locked_group, group.clone())
                            .await
                    } else {
                        // existing member join group
                        let member_id = request.member_id.clone();
                        let leader_id = locked_group.leader_id().unwrap().to_string();
                        let member = locked_group.get_member(&member_id).unwrap();
                        if member.id() == leader_id
                            || !member.match_protocol(&request.group_protocols)
                        {
                            // member is the leader or has changed protocol, need rebalance
                            locked_group.update_member_protocols(
                                &request.member_id,
                                request.group_protocols,
                            );
                            let (tx, rx) = oneshot::channel();
                            locked_group.update_member_awaiting_join_result(&request.member_id, tx);
                            Ok(rx.await.unwrap())
                        } else {
                            // member is not the leader and has not changed protocol, just return current group information
                            Ok(JoinGroupResult {
                                members: None,
                                member_id: request.member_id.clone(),
                                generation_id: locked_group.generation_id(),
                                sub_protocol: locked_group.protocol().unwrap().to_string(),
                                leader_id: locked_group.leader_id().unwrap().to_string(),
                                error: None,
                            })
                        }
                    }
                }
            };
            if locked_group.is(GroupState::PreparingRebalance) {
                self_clone
                    .delayed_join_purgatory
                    .check_and_complete(locked_group.id());
            }
            result
        }
    }

    async fn add_member_and_rebalance(
        self: Arc<Self>,
        request: JoinGroupRequest,
        locked_group: &mut RwLockWriteGuard<'_, GroupMetadata>,
        group: Arc<RwLock<GroupMetadata>>,
    ) -> KafkaResult<JoinGroupResult> {
        let member_id = format!("{}-{}", request.client_id, Uuid::new_v4().to_string());
        let mut member = MemberMetadata::new(
            &member_id,
            &request.client_id,
            &request.client_host,
            &locked_group.id(),
            request.session_timeout,
            request.rebalance_timeout,
            &request.protocol_type,
            request
                .group_protocols
                .into_iter()
                .map(|p| Arc::new(p))
                .collect(),
        );
        let (tx, rx) = oneshot::channel();
        member.set_callback_channel(tx);
        locked_group.add_member(member)?;
        self.prepare_rebalance(locked_group, group).await;
        Ok(rx.await.unwrap())
    }
    async fn prepare_rebalance(
        self: Arc<Self>,
        locked_group: &mut RwLockWriteGuard<'_, GroupMetadata>,
        group: Arc<RwLock<GroupMetadata>>,
    ) {
        if !locked_group.can_rebalance() {
            // 已经有组内成员已经发起延迟操作了
            return;
        }
        if locked_group.is(GroupState::AwaitingSync) {
            // cancel all member's assignment
            locked_group.cancel_all_member_assignment();
        }

        if locked_group.is(GroupState::Empty) {
            // 初始化延迟加入
            let initial_delayed_join = InitialDelayedJoin::new(
                self.clone(),
                group,
                self.initial_delayed_join_purgatory.clone(),
                self.group_config.group_initial_rebalance_delay,
                self.group_config.group_initial_rebalance_delay,
                (locked_group.rebalance_timeout_ms()
                    - self.group_config.group_initial_rebalance_delay)
                    .max(0),
            );
            locked_group.transition_to(GroupState::PreparingRebalance);
            self.initial_delayed_join_purgatory
                .try_complete_else_watch(initial_delayed_join, vec![locked_group.id().to_string()])
                .await;
        } else {
            // 延迟加入
            let delayed_join = DelayedJoin::new(
                self.clone(),
                group.clone(),
                locked_group.rebalance_timeout_ms() as u64,
            );
            locked_group.transition_to(GroupState::PreparingRebalance);
            self.delayed_join_purgatory
                .try_complete_else_watch(delayed_join, vec![locked_group.id().to_string()])
                .await;
        }
    }
    pub fn can_complete_join(&self, group: Arc<RwLock<GroupMetadata>>) -> bool {
        todo!()
    }
    pub fn on_complete_join(
        &self,
        group: Arc<RwLock<GroupMetadata>>,
    ) -> KafkaResult<JoinGroupResult> {
        // remove all not yet rejoined members
        let mut locked_group = group.write();
        let not_yet_rejoined_members = locked_group.not_yet_rejoined_members();
        for member_id in not_yet_rejoined_members {
            locked_group.remove_member(&member_id);
        }

        if !locked_group.is(GroupState::Dead) {
            locked_group.init_next_generation();

            if locked_group.is(GroupState::Empty) {
                self.group_manager.store_group(locked_group, None);
            } else {
                // 给所有等待加入的成员发送加入结果
                let leader_id = locked_group.leader_id().unwrap().to_string();
                let generation_id = locked_group.generation_id();
                let sub_protocol = locked_group.protocol().unwrap().to_string();
                let all_member_protocols = locked_group.get_all_member_protocols();

                for member in locked_group.all_members() {
                    let join_result = JoinGroupResult {
                        members: if member.id() == leader_id {
                            Some(all_member_protocols.clone())
                        } else {
                            None
                        },
                        member_id: member.id().to_string(),
                        generation_id,
                        sub_protocol: sub_protocol.clone(),
                        leader_id: leader_id.clone(),
                        error: None,
                    };
                    member.take_callback_channel().send(join_result);
                }
            }
        }

        todo!()
    }
    fn complete_and_schedule_next_heartbeat_expiry(
        &self,
        group: Arc<Mutex<GroupMetadata>>,
        member: &mut MemberMetadata,
    ) {
        member.last_heartbeat = Instant::now();

        todo!()
    }

    pub fn handle_heartbeat(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> KafkaResult<()> {
        if !self.active.load() {
            return Err(KafkaError::CoordinatorNotAvailable(group_id.to_string()));
        } else {
            let group = self.group_manager.get_group(group_id);
            if let Some(group) = group {
                match group.current_state() {
                    GroupState::Dead => {
                        return Err(KafkaError::UnknownMemberId(member_id.to_string()));
                    }
                    GroupState::Empty => {
                        return Err(KafkaError::UnknownMemberId(member_id.to_string()));
                    }
                    GroupState::AwaitingSync => {
                        if !group.has_member(member_id) {
                            return Err(KafkaError::UnknownMemberId(member_id.to_string()));
                        } else {
                            return Err(KafkaError::RebalanceInProgress(group_id.to_string()));
                        }
                    }
                    GroupState::PreparingRebalance => {
                        if !group.has_member(member_id) {
                            return Err(KafkaError::UnknownMemberId(member_id.to_string()));
                        } else if generation_id != group.generation_id() {
                            return Err(KafkaError::IllegalGeneration(group_id.to_string()));
                        } else {
                            let member = group.get_member(member_id);
                            self.complete_and_schedule_next_heartbeat_expiry(group, member);
                            return Err(KafkaError::RebalanceInProgress(group_id.to_string()));
                        }
                    }
                    GroupState::Stable => {
                        if !group.has_member(member_id) {
                            return Err(KafkaError::UnknownMemberId(member_id.to_string()));
                        } else if generation_id != group.generation_id() {
                            return Err(KafkaError::IllegalGeneration(group_id.to_string()));
                        } else {
                            let member = group.get_member(member_id);
                            self.complete_and_schedule_next_heartbeat_expiry(group, member);
                            return Ok(());
                        }
                    }
                }
            } else {
                return Err(KafkaError::UnknownMemberId(member_id.to_string()));
            }
        }
    }

    fn join_error(&self, member_id: String, error: ErrorCode) -> JoinGroupResult {
        JoinGroupResult {
            members: None,
            member_id,
            generation_id: 0,
            sub_protocol: "".to_string(),
            leader_id: "".to_string(),
            error: Some(error),
        }
    }
}

#[derive(Debug)]
pub struct JoinGroupResult {
    pub members: Option<HashMap<String, Vec<Arc<ProtocolMetadata>>>>,
    pub member_id: String,
    pub generation_id: i32,
    pub sub_protocol: String,
    pub leader_id: String,
    pub error: Option<ErrorCode>,
}
