use std::{collections::HashMap, sync::Arc, time::Instant};

use bytes::Bytes;
use crossbeam::atomic::AtomicCell;
use tokio::sync::{broadcast, mpsc::Sender, oneshot, RwLock, RwLockWriteGuard};
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    message::kafka_consume::group::{GroupMetadata, GroupState, MemberMetadata},
    protocol::api_schemas::consumer_protocol::ProtocolMetadata,
    request::{
        consumer_group::{FindCoordinatorResponse, JoinGroupRequest, SyncGroupResponse},
        errors::{ErrorCode, KafkaError, KafkaResult},
    },
    service::{GroupConfig, Node},
    utils::DelayedAsyncOperationPurgatory,
    AppResult,
};

use super::{
    delayed_join::{DelayedHeartbeat, DelayedJoin, InitialDelayedJoin},
    group::GroupMetadataManager,
};

pub struct GroupCoordinator {
    active: AtomicCell<bool>,
    node: Node,
    group_config: GroupConfig,
    group_manager: GroupMetadataManager,
    delayed_join_purgatory: Arc<DelayedAsyncOperationPurgatory<DelayedJoin>>,
    initial_delayed_join_purgatory: Arc<DelayedAsyncOperationPurgatory<InitialDelayedJoin>>,
    delayed_heartbeat_purgatory: Arc<DelayedAsyncOperationPurgatory<DelayedHeartbeat>>,
}

impl GroupCoordinator {
    pub async fn new(
        node: Node,
        group_manager: GroupMetadataManager,
        group_config: GroupConfig,
        delayed_join_purgatory: Arc<DelayedAsyncOperationPurgatory<DelayedJoin>>,
        initial_delayed_join_purgatory: Arc<DelayedAsyncOperationPurgatory<InitialDelayedJoin>>,
        delayed_heartbeat_purgatory: Arc<DelayedAsyncOperationPurgatory<DelayedHeartbeat>>,
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
        let delayed_join_purgatory = DelayedAsyncOperationPurgatory::<DelayedJoin>::new(
            "delayed_join_purgatory",
            notify_shutdown.clone(),
            shutdown_complete_tx.clone(),
        )
        .await;

        let initial_delayed_join_purgatory =
            DelayedAsyncOperationPurgatory::<InitialDelayedJoin>::new(
                "initial_delayed_join_purgatory",
                notify_shutdown.clone(),
                shutdown_complete_tx.clone(),
            )
            .await;
        let delayed_heartbeat_purgatory = DelayedAsyncOperationPurgatory::<DelayedHeartbeat>::new(
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
        let mut locked_group = group.write().await;
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
                    // if the group is marked as dead, it means some other thread has just removed the group
                    // from the coordinator metadata; this is likely that the group has migrated to some other
                    // coordinator OR the group is in a transient unstable phase. Let the member retry
                    // joining without the specified member id,
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
                        let (tx, rx) = oneshot::channel();
                        self.update_member_and_rebalance(
                            &request.member_id,
                            request.group_protocols,
                            tx,
                            &mut locked_group,
                            group.clone(),
                        )
                        .await;
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
                        if member.protocol_equals(&request.group_protocols) {
                            // member is joining with the same metadata (which could be because it failed to
                            // receive the initial JoinGroup response), so just return current group information
                            // for the current generation.
                            return Ok(JoinGroupResult {
                                members: {
                                    if request.member_id == leader_id {
                                        Some(locked_group.current_member_metadata())
                                    } else {
                                        None
                                    }
                                },
                                member_id: request.member_id.clone(),
                                generation_id: locked_group.generation_id(),
                                sub_protocol: locked_group.protocol().unwrap().to_string(),
                                leader_id,
                                error: None,
                            });
                        } else {
                            // member has changed protocol, need rebalance
                            let (tx, rx) = oneshot::channel();
                            self.update_member_and_rebalance(
                                &request.member_id,
                                request.group_protocols,
                                tx,
                                &mut locked_group,
                                group.clone(),
                            )
                            .await;
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
                            || !member.protocol_equals(&request.group_protocols)
                        {
                            // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                            // The latter allows the leader to trigger rebalances for changes affecting assignment
                            // which do not affect the member metadata (such as topic metadata changes for the consumer)
                            let (tx, rx) = oneshot::channel();
                            self.update_member_and_rebalance(
                                &request.member_id,
                                request.group_protocols,
                                tx,
                                &mut locked_group,
                                group.clone(),
                            )
                            .await;

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
        member.set_join_group_callback_channel(tx);
        locked_group.add_member(member)?;
        if locked_group.is(GroupState::PreparingRebalance) && locked_group.generation_id() == 0 {
            locked_group.set_new_member_added();
        }
        self.maybe_prepare_rebalance(locked_group, group).await;
        Ok(rx.await.unwrap())
    }
    /// 在拿到group写锁的情况下, 更新成员并触发rebalance，同时延迟的join操作需要一个独立的group
    async fn update_member_and_rebalance(
        self: Arc<Self>,
        member_id: &str,
        protocols: Vec<ProtocolMetadata>,
        tx: oneshot::Sender<JoinGroupResult>,
        locked_group: &mut RwLockWriteGuard<'_, GroupMetadata>,
        group: Arc<RwLock<GroupMetadata>>,
    ) {
        let member = locked_group.get_member(member_id).unwrap();
        member.update_supported_protocols(protocols);
        member.set_join_group_callback_channel(tx);
        self.maybe_prepare_rebalance(locked_group, group).await;
    }
    async fn maybe_prepare_rebalance(
        self: Arc<Self>,
        locked_group: &mut RwLockWriteGuard<'_, GroupMetadata>,
        group: Arc<RwLock<GroupMetadata>>,
    ) {
        if !locked_group.can_rebalance() {
            // 已经有组内成员已经发起延迟操作了
            return;
        }
        self.prepare_rebalance(locked_group, group).await;
    }
    async fn prepare_rebalance(
        self: Arc<Self>,
        locked_group: &mut RwLockWriteGuard<'_, GroupMetadata>,
        group: Arc<RwLock<GroupMetadata>>,
    ) {
        let group_id = locked_group.id().to_string();
        if locked_group.is(GroupState::AwaitingSync) {
            // cancel all member's assignment
            self.reset_and_progate_assignment_error(
                locked_group,
                group.clone(),
                KafkaError::RebalanceInProgress(group_id.clone()),
            );
        }

        if locked_group.is(GroupState::Empty) {
            // 初始化延迟加入
            let initial_delayed_join = InitialDelayedJoin::new(
                self.clone(),
                group,
                self.initial_delayed_join_purgatory.clone(),
                self.group_config.group_initial_rebalance_delay,
                self.group_config.group_initial_rebalance_delay,
                (locked_group.max_rebalance_timeout_ms()
                    - self.group_config.group_initial_rebalance_delay)
                    .max(0),
            );
            locked_group.transition_to(GroupState::PreparingRebalance);

            // 释放锁, 因为下边的调用会再次尝试获取锁，否则会造成死锁
            drop(locked_group);

            self.initial_delayed_join_purgatory
                .try_complete_else_watch(initial_delayed_join, vec![group_id])
                .await;
        } else {
            // 延迟加入
            let delayed_join = DelayedJoin::new(
                self.clone(),
                group,
                locked_group.max_rebalance_timeout_ms() as u64,
            );
            locked_group.transition_to(GroupState::PreparingRebalance);

            // 释放锁, 因为下边的调用会再次尝试获取锁，否则会造成死锁
            drop(locked_group);
            self.delayed_join_purgatory
                .try_complete_else_watch(delayed_join, vec![group_id])
                .await;
        }
    }
    pub async fn can_complete_join(&self, group: Arc<RwLock<GroupMetadata>>) -> bool {
        let lock_read_group = group.read().await;
        if lock_read_group.not_yet_rejoined_members().is_empty() {
            return true;
        }
        false
    }
    pub async fn on_complete_join(&self, group: Arc<RwLock<GroupMetadata>>) {
        // remove all not yet rejoined members
        let mut locked_write_group = group.write().await;
        let not_yet_rejoined_members = locked_write_group.not_yet_rejoined_members();
        for member_id in not_yet_rejoined_members {
            locked_write_group.remove_member(&member_id);
        }

        if !locked_write_group.is(GroupState::Dead) {
            locked_write_group.init_next_generation();

            if locked_write_group.is(GroupState::Empty) {
                let result = self.group_manager.store_group(locked_write_group, None);
                if result.is_err() {
                    error!("store group error: {}", result.unwrap_err());
                }
            } else {
                // 给所有等待加入的成员发送加入结果
                let leader_id = locked_write_group.leader_id().unwrap().to_string();
                let generation_id = locked_write_group.generation_id();
                let sub_protocol = locked_write_group.protocol().unwrap().to_string();
                let all_member_protocols = locked_write_group.current_member_metadata();

                for member in locked_write_group.all_members() {
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
                    member.take_join_group_callback_channel().send(join_result);
                }
            }
        }

        todo!()
    }
    fn complete_and_schedule_next_heartbeat_expiry(
        &self,
        group: Arc<RwLock<GroupMetadata>>,
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

    pub async fn handle_sync_group(
        self: Arc<Self>,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        group_assignment: HashMap<String, Bytes>,
    ) -> KafkaResult<SyncGroupResponse> {
        if !self.active.load() {
            return Err(KafkaError::CoordinatorNotAvailable(group_id.to_string()));
        }
        let group = self.group_manager.get_group(group_id);
        if let Some(group) = group {
            self.do_sync_group(group, member_id, generation_id, group_assignment)
                .await
        } else {
            return Err(KafkaError::UnknownMemberId(member_id.to_string()));
        }
    }
    async fn do_sync_group(
        self: Arc<Self>,
        group: Arc<RwLock<GroupMetadata>>,
        member_id: &str,
        generation_id: i32,
        group_assignment: HashMap<String, Bytes>,
    ) -> KafkaResult<SyncGroupResponse> {
        let group_clone = group.clone();
        let mut locked_write_group = group.write().await;
        let group_id = locked_write_group.id().to_string();
        if locked_write_group.get_member(member_id).is_none() {
            return Err(KafkaError::UnknownMemberId(member_id.to_string()));
        } else if generation_id != locked_write_group.generation_id() {
            return Err(KafkaError::IllegalGeneration(group_id.to_string()));
        } else {
            let current_state = locked_write_group.current_state();
            return match current_state {
                GroupState::Empty | GroupState::Dead => {
                    Err(KafkaError::UnknownMemberId(member_id.to_string()))
                }
                GroupState::PreparingRebalance => {
                    return Err(KafkaError::RebalanceInProgress(group_id.to_string()));
                }
                GroupState::Stable => {
                    // if the group is stable, we just return the current assignment
                    let member = locked_write_group.get_member(member_id).unwrap();
                    let result =
                        SyncGroupResponse::new(ErrorCode::None, 0, member.assignment().unwrap());
                    self.complete_and_schedule_next_heartbeat_expiry(group, member);
                    Ok(result)
                }
                GroupState::AwaitingSync => {
                    let member = locked_write_group.get_member(member_id).unwrap();
                    let (tx, rx) = oneshot::channel();
                    member.set_sync_group_callback_channel(tx);
                    // if this is the leader, then we can attempt to persist state and transition to stable
                    if member.id() == locked_write_group.leader_id().unwrap().to_string() {
                        info!(
                            "Assignment received from leader for group {} for generation {}",
                            group_id, generation_id
                        );
                        // fill any missing members with an empty assignment
                        let mut all_members = locked_write_group.all_members();
                        for member in all_members {
                            if !group_assignment.contains_key(member.id()) {
                                group_assignment.insert(member.id().to_string(), Bytes::new());
                            }
                        }
                        let store_result = self
                            .group_manager
                            .store_group(locked_write_group, group_assignment);
                        // another member may have joined the group while we were awaiting this callback,
                        // so we must ensure we are still in the AwaitingSync state and the same generation
                        // when it gets invoked. if we have transitioned to another state, then do nothing
                        if locked_write_group.is(GroupState::AwaitingSync)
                            && generation_id == locked_write_group.generation_id()
                        {
                            if store_result.is_ok() {
                                self.set_and_progate_assignment(
                                    &mut locked_write_group,
                                    group_clone,
                                    group_assignment,
                                );
                                locked_write_group.transition_to(GroupState::Stable);
                            } else {
                                // 设置错误并准备重新平衡
                                self.reset_and_progate_assignment_error(
                                    &mut locked_write_group,
                                    group_clone,
                                    store_result.unwrap_err(),
                                );
                                self.maybe_prepare_rebalance(&mut locked_write_group, group_clone)
                                    .await;
                            }
                        }
                    }
                    Ok(rx.await.unwrap())
                }
            };
        }
    }

    fn set_and_progate_assignment(
        &self,
        locked_write_group: &mut RwLockWriteGuard<'_, GroupMetadata>,
        group: Arc<RwLock<GroupMetadata>>,
        mut assignment: HashMap<String, Bytes>,
    ) {
        for member in locked_write_group.all_members() {
            member.set_assignment(assignment.remove(member.id()).unwrap());
        }
        self.progate_assignment(locked_write_group, group, &KafkaError::None);
    }
    fn reset_and_progate_assignment_error(
        &self,
        locked_write_group: &mut RwLockWriteGuard<'_, GroupMetadata>,
        group: Arc<RwLock<GroupMetadata>>,
        error: KafkaError,
    ) {
        for member in locked_write_group.all_members() {
            member.set_assignment(Bytes::new());
        }
        self.progate_assignment(locked_write_group, group, &error);
    }
    fn progate_assignment(
        &self,
        locked_write_group: &mut RwLockWriteGuard<'_, GroupMetadata>,
        group: Arc<RwLock<GroupMetadata>>,
        error: &KafkaError,
    ) {
        for member in locked_write_group.all_members() {
            member
                .take_sync_group_callback_channel()
                .send(SyncGroupResponse::new(
                    error.into(),
                    0,
                    member.assignment().unwrap(),
                ));
            self.complete_and_schedule_next_heartbeat_expiry(group, member);
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
    pub members: Option<HashMap<String, Bytes>>,
    pub member_id: String,
    pub generation_id: i32,
    pub sub_protocol: String,
    pub leader_id: String,
    pub error: Option<ErrorCode>,
}
