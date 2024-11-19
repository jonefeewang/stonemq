use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use crossbeam::atomic::AtomicCell;
use tokio::{
    sync::{broadcast, mpsc::Sender, oneshot, RwLock, RwLockWriteGuard},
    time::Instant,
};
use tracing::{debug, error, info, trace};
use uuid::Uuid;

use crate::{
    message::{
        kafka_consume::group::{GroupMetadata, GroupState, MemberMetadata},
        offset::OffsetAndMetadata,
        TopicPartition,
    },
    protocol::api_schemas::consumer_protocol::ProtocolMetadata,
    request::{
        consumer_group::{
            FindCoordinatorRequest, FindCoordinatorResponse, JoinGroupRequest,
            PartitionOffsetCommitData, PartitionOffsetData, SyncGroupResponse,
        },
        errors::{ErrorCode, KafkaError, KafkaResult},
        RequestContext,
    },
    service::{GroupConfig, Node},
    utils::DelayedAsyncOperationPurgatory,
    AppResult,
};

use super::{
    delayed_join::{DelayedHeartbeat, DelayedJoin, InitialDelayedJoin},
    group::GroupMetadataManager,
};

#[derive(Debug)]
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
    async fn new(
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

        let group_manager = GroupMetadataManager::load();
        let coordinator = Self::new(
            node,
            group_manager,
            group_config,
            delayed_join_purgatory,
            initial_delayed_join_purgatory,
            delayed_heartbeat_purgatory,
        )
        .await;

        coordinator.active.store(true);
        debug!(
            "group coordinator startup success :{:?}",
            coordinator.active.load()
        );
        coordinator
    }

    pub async fn find_coordinator(
        &self,
        request: FindCoordinatorRequest,
    ) -> AppResult<FindCoordinatorResponse> {
        // 因为stonemq目前支持单机，所以coordinator就是自身
        let response: FindCoordinatorResponse = self.node.clone().into();
        Ok(response)
    }
    pub async fn handle_join_group(
        self: Arc<Self>,
        mut request: JoinGroupRequest,
        request_context: &RequestContext<'_>,
    ) -> KafkaResult<JoinGroupResult> {
        if let Some(client_id) = request_context.request_header.client_id.clone() {
            request.client_id = client_id;
        }
        // 检查请求是否合法
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
        trace!("handle join group request: {:?}", request);

        let group = self.group_manager.get_group(&request.group_id);
        if let Some(group) = group {
            // 加入一个已经存在的组
            self.do_join_group(request, group, request_context).await
        } else if request.member_id.is_empty() {
            // 新组，新成员
            let group = GroupMetadata::new(&request.group_id);
            let group = self.group_manager.add_group(group);
            self.do_join_group(request, group, request_context).await
        } else {
            // 旧成员加入一个不存在的组
            Ok(self.join_error(request.member_id.clone(), ErrorCode::UnknownMemberId))
        }
    }
    async fn do_join_group(
        self: &Arc<Self>,
        request: JoinGroupRequest,
        group: Arc<RwLock<GroupMetadata>>,
        request_context: &RequestContext<'_>,
    ) -> KafkaResult<JoinGroupResult> {
        let locked_group = group.write().await;

        if !locked_group.is(GroupState::Empty)
            && (locked_group.protocol_type() != Some(&request.protocol_type)
                || !locked_group.is_support_protocols(
                    &request
                        .group_protocols
                        .iter()
                        .map(|p| p.name.clone())
                        .collect(),
                ))
        {
            // new member join group, but group is not empty, and protocol type or supported protocols are not match
            Ok(self.join_error(
                request.member_id.clone(),
                ErrorCode::InconsistentGroupProtocol,
            ))
        } else if !request.member_id.is_empty() && !locked_group.has_member(&request.member_id) {
            // member_id is not empty, but not in group
            Ok(self.join_error(request.member_id.clone(), ErrorCode::UnknownMemberId))
        } else {
            let coordinator_clone = self.clone();
            let result = match locked_group.state() {
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
                        self.add_member_and_rebalance(
                            request,
                            locked_group,
                            group.clone(),
                            request_context,
                        )
                        .await
                    } else {
                        // existing member join group
                        let (tx, rx) = oneshot::channel();
                        self.update_member_and_rebalance(
                            &request.member_id,
                            request.group_protocols,
                            tx,
                            locked_group,
                            group.clone(),
                        )
                        .await;
                        request_context.notify_processor_proceed().await;
                        Ok(rx.await.unwrap())
                    }
                }
                GroupState::AwaitingSync => {
                    if request.member_id.is_empty() {
                        // new member join group
                        self.add_member_and_rebalance(
                            request,
                            locked_group,
                            group.clone(),
                            request_context,
                        )
                        .await
                    } else {
                        let member_id = request.member_id.clone();
                        let leader_id = locked_group.leader_id().unwrap().to_string();
                        let member = locked_group.get_member(&member_id).unwrap();
                        if member.protocol_matches(&request.group_protocols) {
                            // member is joining with the same metadata (which could be because it failed to
                            // receive the initial JoinGroup response), so just return current group information
                            // for the current generation.
                            Ok(JoinGroupResult {
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
                            })
                        } else {
                            // member has changed protocol, need rebalance
                            let (tx, rx) = oneshot::channel();
                            self.update_member_and_rebalance(
                                &request.member_id,
                                request.group_protocols,
                                tx,
                                locked_group,
                                group.clone(),
                            )
                            .await;
                            request_context.notify_processor_proceed().await;
                            Ok(rx.await.unwrap())
                        }
                    }
                }
                GroupState::Empty | GroupState::Stable => {
                    if request.member_id.is_empty() {
                        // new member join group, and group is empty
                        self.add_member_and_rebalance(
                            request,
                            locked_group,
                            group.clone(),
                            request_context,
                        )
                        .await
                    } else {
                        // existing member join group
                        let member_id = request.member_id.clone();
                        let leader_id = locked_group.leader_id().unwrap().to_string();
                        let member = locked_group.get_member(&member_id).unwrap();
                        if member.id() == leader_id
                            || !member.protocol_matches(&request.group_protocols)
                        {
                            // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                            // The latter allows the leader to trigger rebalances for changes affecting assignment
                            // which do not affect the member metadata (such as topic metadata changes for the consumer)
                            let (tx, rx) = oneshot::channel();
                            self.update_member_and_rebalance(
                                &request.member_id,
                                request.group_protocols,
                                tx,
                                locked_group,
                                group.clone(),
                            )
                            .await;
                            request_context.notify_processor_proceed().await;
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
            // 再次获取锁，尝试完成延迟加入
            let locked_group = group.read().await;
            if locked_group.is(GroupState::PreparingRebalance) {
                coordinator_clone
                    .delayed_join_purgatory
                    .check_and_complete(locked_group.id())
                    .await;
                coordinator_clone
                    .initial_delayed_join_purgatory
                    .check_and_complete(locked_group.id())
                    .await;
            }
            result
        }
    }

    // do_join_group 的辅助函数-1，在获取写锁的情况下，添加成员，释放锁，并触发rebalance
    async fn add_member_and_rebalance(
        self: &Arc<Self>,
        request: JoinGroupRequest,
        mut locked_group: RwLockWriteGuard<'_, GroupMetadata>,
        group: Arc<RwLock<GroupMetadata>>,
        request_context: &RequestContext<'_>,
    ) -> KafkaResult<JoinGroupResult> {
        let member_id = format!("{}-{}", request.client_id, Uuid::new_v4());
        let mut member = MemberMetadata::new(
            &member_id,
            &request.client_id,
            &request.client_host,
            locked_group.id(),
            request.rebalance_timeout,
            request.session_timeout,
            &request.protocol_type,
            request
                .group_protocols
                .into_iter()
                .map(|p| (p.name.clone(), Bytes::copy_from_slice(&p.metadata)))
                .collect(),
        );
        let (tx, rx) = oneshot::channel();
        member.set_join_group_callback(tx);
        trace!("add member: {:?}", member);
        locked_group.add_member(member)?;

        // update the newMemberAdded flag to indicate that the join group can be further delayed
        if locked_group.is(GroupState::PreparingRebalance) && locked_group.generation_id() == 0 {
            locked_group.set_new_member_added();
        }

        // 释放锁，因为下边的调用会再次尝试获取锁，否则会造成死锁
        drop(locked_group);

        self.maybe_prepare_rebalance(group).await;

        // 通知request processor 可以处理下一个
        request_context.notify_processor_proceed().await;
        // 等待加入结果
        Ok(rx.await.unwrap())
    }

    // do_join_group 的辅助函数-2，在获取写锁的情况下，更新成员并触发rebalance，同时延迟的join操作需要一个独立的group
    async fn update_member_and_rebalance(
        self: &Arc<Self>,
        member_id: &str,
        protocols: Vec<ProtocolMetadata>,
        tx: oneshot::Sender<JoinGroupResult>,
        mut locked_group: RwLockWriteGuard<'_, GroupMetadata>,
        group: Arc<RwLock<GroupMetadata>>,
    ) {
        let member = locked_group.get_mut_member(member_id).unwrap();
        member.update_supported_protocols(protocols);
        member.set_join_group_callback(tx);

        // 释放锁，因为下边的调用会再次尝试获取锁，否则会造成死锁
        drop(locked_group);

        self.maybe_prepare_rebalance(group).await;
    }
    // 多个地方会调用这个函数，函数参数不能持有锁，否则会造成交叉获取锁操作，造成死锁
    async fn maybe_prepare_rebalance(self: &Arc<Self>, group: Arc<RwLock<GroupMetadata>>) {
        debug!("maybe prepare rebalance");
        let locked_group = group.read().await;
        if !locked_group.can_rebalance() {
            debug!(
                "maybe prepare rebalance: group can not rebalance, state: {:?}",
                locked_group.state()
            );
            return;
        }
        // 释放锁，因为下边的调用会再次尝试获取锁，否则会造成死锁
        drop(locked_group);
        self.prepare_rebalance(group).await;
    }
    async fn prepare_rebalance(self: &Arc<Self>, group: Arc<RwLock<GroupMetadata>>) {
        debug!("preparing  rebalance ");
        let group_clone = group.clone();
        let mut locked_group = group.write().await;
        let coordinator_clone = self.clone();
        let group_id = locked_group.id().to_string();
        if locked_group.is(GroupState::AwaitingSync) {
            debug!("preparing rebalance: group is awaiting sync");
            // consumer leader 的sync request还未上来，收到了新的join request，取消所有成员的assignment
            for member in locked_group.members() {
                member.set_assignment(Bytes::new());
            }

            // 释放锁，因为下边的调用会再次尝试获取锁，否则会造成死锁
            drop(locked_group);

            self.progate_assignment(
                group.clone(),
                &KafkaError::RebalanceInProgress(group_id.clone()),
            )
            .await;

            // 重新获取锁, 保持外部锁的使用状态
            locked_group = group.write().await;
        }

        // 初始化延迟加入

        if locked_group.is(GroupState::Empty) {
            // 初始化延迟加入
            debug!("preparing rebalance: group is empty");
            let initial_delayed_join = InitialDelayedJoin::new(
                coordinator_clone,
                group_clone,
                self.initial_delayed_join_purgatory.clone(),
                self.group_config.group_initial_rebalance_delay,
                self.group_config.group_initial_rebalance_delay,
                (locked_group.max_rebalance_timeout()
                    - self.group_config.group_initial_rebalance_delay)
                    .max(0),
            );
            locked_group.transition_to(GroupState::PreparingRebalance);

            // 释放锁, 因为下边的调用会再次尝试获取锁，否则会造成死锁
            drop(locked_group);

            let initial_delayed_join_clone = Arc::new(initial_delayed_join);

            self.initial_delayed_join_purgatory
                .try_complete_else_watch(initial_delayed_join_clone, vec![group_id])
                .await;
        } else {
            // 延迟加入
            debug!("preparing rebalance: group is not empty");
            let delayed_join = DelayedJoin::new(
                coordinator_clone,
                group_clone,
                locked_group.max_rebalance_timeout() as u64,
            );
            locked_group.transition_to(GroupState::PreparingRebalance);

            // 释放锁, 因为下边的调用会再次尝试获取锁，否则会造成死锁
            drop(locked_group);
            let delayed_join_clone = Arc::new(delayed_join);
            self.delayed_join_purgatory
                .try_complete_else_watch(delayed_join_clone, vec![group_id])
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
        trace!("on complete join");
        // remove all not yet rejoined members
        let mut locked_write_group = group.write().await;
        trace!(
            "acquired write lock in on complete join and group state is:{:?}",
            locked_write_group.state()
        );

        let not_yet_rejoined_members = locked_write_group.not_yet_rejoined_members();
        for member_id in not_yet_rejoined_members {
            locked_write_group.remove_member(&member_id);
        }

        trace!(
            "on complete join group state:{:?}",
            locked_write_group.state()
        );
        if !locked_write_group.is(GroupState::Dead) {
            trace!("on complete join init next generation");
            locked_write_group.init_next_generation();

            if locked_write_group.is(GroupState::Empty) {
                let result = self.group_manager.store_group(&locked_write_group);
                if result.is_err() {
                    error!("store group error: {}", result.unwrap_err());
                }
            } else {
                // 给所有等待加入的成员发送加入结果
                trace!("on complete join send join result");
                let leader_id = locked_write_group.leader_id().unwrap().to_string();
                let generation_id = locked_write_group.generation_id();
                let sub_protocol = locked_write_group.protocol().unwrap().to_string();
                let all_member_protocols = locked_write_group.current_member_metadata();

                for member in locked_write_group.members() {
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
                    let _ = member.take_join_group_callback().send(join_result);
                }
            }
        }
    }
    async fn complete_and_schedule_next_heartbeat_expiry(
        self: &Arc<Self>,
        group: Arc<RwLock<GroupMetadata>>,
        member_id: &str,
    ) {
        // trace!("complete  heartbeat expiry");
        let group_clone = group.clone();
        let mut group_lock = group.write().await;
        let group_id = group_lock.id().to_string();
        let member = group_lock.get_mut_member(member_id).unwrap();

        // complete current heartbeat expectation
        member.update_heartbeat();
        let delay_key = format!("{}-{}", group_id, member_id);

        let last_heartbeat = member.last_heartbeat();
        let session_timeout = member.session_timeout();
        // 释放锁，因为下边的调用会再次尝试获取锁，否则会造成死锁
        drop(group_lock);

        self.delayed_heartbeat_purgatory
            .check_and_complete(delay_key.as_str())
            .await;

        // reschedule the next heartbeat expiration deadline
        // trace!("reschedule the next heartbeat expiration deadline");
        let next_heartbeat_expiry = last_heartbeat + Duration::from_secs(session_timeout as u64);
        let delay_key = format!("{}-{}", group_id, member_id);
        let delay_heartbeat = DelayedHeartbeat::new(
            self.clone(),
            group_clone,
            next_heartbeat_expiry,
            member_id.to_string(),
            session_timeout as u64,
        );
        let delay_heartbeat_clone = Arc::new(delay_heartbeat);
        self.delayed_heartbeat_purgatory
            .try_complete_else_watch(delay_heartbeat_clone, vec![delay_key])
            .await;
    }
    pub async fn try_complete_heartbeat(
        &self,
        group: Arc<RwLock<GroupMetadata>>,
        member_id: &str,
        heartbeat_deadline: Instant,
    ) -> bool {
        // trace!(
        //     "try complete heartbeat for member: {} heartbeat_deadline: {}",
        //     member_id,
        //     heartbeat_deadline.elapsed().as_millis()
        // );
        let group_read = group.read().await;
        let member = group_read.get_member(member_id).unwrap();
        let ret = self.should_keep_member_alive(member, heartbeat_deadline) || member.is_leaving();
        // trace!("try complete heartbeat result: {}", ret);
        ret
    }

    pub async fn on_heartbeat_expiry(
        self: &Arc<Self>,
        group: Arc<RwLock<GroupMetadata>>,
        member_id: &str,
        heartbeat_deadline: Instant,
    ) {
        debug!("on heartbeat expiry for member: {}", member_id);
        let group_clone = group.clone();
        let group_read = group.read().await;
        let member = group_read.get_member(member_id).unwrap();
        if !self.should_keep_member_alive(member, heartbeat_deadline) {
            drop(group_read);
            self.remove_member_and_update_group(group_clone, member_id)
                .await;
        }
    }
    pub async fn on_heartbeat_complete(&self, group: Arc<RwLock<GroupMetadata>>, member_id: &str) {
        // debug!(
        //     "heartbeat complete for member: {} group: {}",
        //     member_id,
        //     group.read().await.id()
        // );
    }

    fn should_keep_member_alive(
        &self,
        member: &MemberMetadata,
        heartbeat_deadline: Instant,
    ) -> bool {
        member.is_awaiting_join()
            || member.is_awaiting_sync()
            || member.last_heartbeat() + Duration::from_secs(member.session_timeout() as u64)
                > heartbeat_deadline
    }
    async fn remove_member_and_update_group(
        self: &Arc<Self>,
        group: Arc<RwLock<GroupMetadata>>,
        member_id: &str,
    ) {
        debug!("remove expired member and update group");
        let group_clone = group.clone();
        let mut locked_write_group = group.write().await;
        locked_write_group.remove_member(member_id);
        let group_id = locked_write_group.id().to_string();
        match locked_write_group.state() {
            GroupState::Stable | GroupState::AwaitingSync => {
                // trace!(
                //     "remove member and update group state: {:?}",
                //     locked_write_group.state()
                // );
                drop(locked_write_group);
                self.maybe_prepare_rebalance(group_clone).await;
            }
            GroupState::Dead | GroupState::Empty => {}
            GroupState::PreparingRebalance => {
                // 延迟加入
                drop(locked_write_group);
                self.delayed_join_purgatory
                    .check_and_complete(group_id.as_str())
                    .await;
            }
        }
    }

    pub async fn handle_heartbeat(
        self: Arc<Self>,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> KafkaResult<()> {
        if !self.active.load() {
            Err(KafkaError::CoordinatorNotAvailable(group_id.to_string()))
        } else {
            let group = self.group_manager.get_group(group_id);

            if let Some(group) = group {
                let group_clone = group.clone();
                let locked_read_group = group.read().await;
                match locked_read_group.state() {
                    GroupState::Dead => {
                        // if the group is marked as dead, it means some other thread has just removed the group
                        // from the coordinator metadata; this is likely that the group has migrated to some other
                        // coordinator OR the group is in a transient unstable phase. Let the member retry
                        // joining without the specified member id,
                        Err(KafkaError::UnknownMemberId(member_id.to_string()))
                    }
                    GroupState::Empty => Err(KafkaError::UnknownMemberId(member_id.to_string())),
                    GroupState::AwaitingSync => {
                        if !locked_read_group.has_member(member_id) {
                            Err(KafkaError::UnknownMemberId(member_id.to_string()))
                        } else {
                            Err(KafkaError::RebalanceInProgress(group_id.to_string()))
                        }
                    }
                    GroupState::PreparingRebalance => {
                        if !locked_read_group.has_member(member_id) {
                            trace!("member not found in preparing rebalance");
                            Err(KafkaError::UnknownMemberId(member_id.to_string()))
                        } else if generation_id != locked_read_group.generation_id() {
                            trace!("generation id not match");
                            Err(KafkaError::IllegalGeneration(group_id.to_string()))
                        } else {
                            // 释放锁, 因为下边的调用会再次尝试获取锁，否则会造成死锁
                            drop(locked_read_group);
                            self.complete_and_schedule_next_heartbeat_expiry(
                                group_clone,
                                member_id,
                            )
                            .await;
                            debug!("heartbeat found group is in preparing rebalance");
                            Err(KafkaError::RebalanceInProgress(group_id.to_string()))
                        }
                    }
                    GroupState::Stable => {
                        if !locked_read_group.has_member(member_id) {
                            Err(KafkaError::UnknownMemberId(member_id.to_string()))
                        } else if generation_id != locked_read_group.generation_id() {
                            Err(KafkaError::IllegalGeneration(group_id.to_string()))
                        } else {
                            // 释放锁, 因为下边的调用会再次尝试获取锁，否则会造成死锁
                            drop(locked_read_group);
                            self.complete_and_schedule_next_heartbeat_expiry(
                                group_clone,
                                member_id,
                            )
                            .await;
                            Ok(())
                        }
                    }
                }
            } else {
                Err(KafkaError::UnknownMemberId(member_id.to_string()))
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
            Err(KafkaError::UnknownMemberId(member_id.to_string()))
        }
    }
    async fn do_sync_group(
        self: &Arc<Self>,
        group: Arc<RwLock<GroupMetadata>>,
        member_id: &str,
        generation_id: i32,
        mut group_assignment: HashMap<String, Bytes>,
    ) -> KafkaResult<SyncGroupResponse> {
        trace!("do sync group");

        let group_clone = group.clone();

        let mut write_lock = group.write().await;

        let group_id = write_lock.id().to_string();
        let leader_id = write_lock.leader_id().unwrap().to_string();

        if write_lock.get_member(member_id).is_none() {
            Err(KafkaError::UnknownMemberId(member_id.to_string()))
        } else if generation_id != write_lock.generation_id() {
            return Err(KafkaError::IllegalGeneration(group_id.to_string()));
        } else {
            let current_state = write_lock.state();
            debug!("do sync group current state: {:?}", current_state);

            return match current_state {
                GroupState::Empty | GroupState::Dead => {
                    Err(KafkaError::UnknownMemberId(member_id.to_string()))
                }
                GroupState::PreparingRebalance => {
                    return Err(KafkaError::RebalanceInProgress(group_id.to_string()));
                }
                GroupState::Stable => {
                    // if the group is stable, we just return the current assignment
                    let member = write_lock.get_member(member_id).unwrap();
                    let result =
                        SyncGroupResponse::new(ErrorCode::None, 0, member.assignment().unwrap());
                    drop(write_lock);
                    self.complete_and_schedule_next_heartbeat_expiry(group_clone, member_id)
                        .await;
                    Ok(result)
                }
                GroupState::AwaitingSync => {
                    let member = write_lock.get_mut_member(member_id).unwrap();
                    let (tx, rx) = oneshot::channel();
                    member.set_sync_group_callback(tx);
                    // if this is the leader, then we can attempt to persist state and transition to stable
                    if member.id() == leader_id {
                        info!(
                            "Assignment received from leader for group {} for generation {}",
                            group_id, generation_id
                        );
                        // fill any missing members with an empty assignment
                        let all_members = write_lock.members();
                        for member in all_members {
                            if !group_assignment.contains_key(member.id()) {
                                group_assignment.insert(member.id().to_string(), Bytes::new());
                            }
                        }

                        let store_result = self.group_manager.store_group(&write_lock);
                        trace!("store group result: {:?}", store_result);
                        // another member may have joined the group while we were awaiting this callback,
                        // so we must ensure we are still in the AwaitingSync state and the same generation
                        // when it gets invoked. if we have transitioned to another state, then do nothing
                        if write_lock.is(GroupState::AwaitingSync)
                            && generation_id == write_lock.generation_id()
                        {
                            trace!("store group result is ok, progate assignment");
                            if store_result.is_ok() {
                                for member in write_lock.members() {
                                    member.set_assignment(
                                        group_assignment.remove(member.id()).unwrap(),
                                    );
                                }

                                // 释放锁，因为下边的调用会再次尝试获取锁，否则会造成死锁
                                drop(write_lock);
                                self.progate_assignment(group_clone, &KafkaError::None)
                                    .await;

                                // 获取锁，改变状态
                                write_lock = group.write().await;
                                write_lock.transition_to(GroupState::Stable);
                                // 释放锁，因为下边的调用会再次尝试获取锁，否则会造成死锁
                                drop(write_lock);
                            } else {
                                // 设置错误并准备重新平衡
                                for member in write_lock.members() {
                                    member.set_assignment(Bytes::new());
                                }
                                // 释放锁，因为下边的调用会再次尝试获取锁，否则会造成死锁
                                drop(write_lock);
                                self.progate_assignment(group.clone(), &KafkaError::None)
                                    .await;

                                self.maybe_prepare_rebalance(group_clone).await;
                            }
                        }
                        trace!("awaitting sync group response");
                    } else {
                        debug!("not leader, wait for leader to send response");
                        drop(write_lock);
                    }
                    Ok(rx.await.unwrap())
                }
            };
        }
    }

    async fn progate_assignment(
        self: &Arc<Self>,
        group: Arc<RwLock<GroupMetadata>>,
        error: &KafkaError,
    ) {
        debug!("progate assignment entered");
        let mut locked_write_group = group.write().await;
        let mut member_ids = vec![];
        for member in locked_write_group.members() {
            // 有可能其他成员的sync group request还未上来
            if let Some(tx) = member.take_sync_group_callback() {
                let send_result = tx.send(SyncGroupResponse::new(
                    error.into(),
                    0,
                    member.assignment().unwrap(),
                ));
                if send_result.is_err() {
                    error!(
                        "send sync group response error: {:?}",
                        send_result.unwrap_err()
                    );
                }
            }
            member_ids.push(member.id().to_string());
        }
        // 释放锁，因为下边的调用会再次尝试获取锁，否则会造成死锁
        drop(locked_write_group);

        for member_id in member_ids {
            self.clone()
                .complete_and_schedule_next_heartbeat_expiry(group.clone(), &member_id)
                .await;
        }
    }

    pub async fn handle_group_leave(
        self: Arc<Self>,
        group_id: &str,
        member_id: &str,
    ) -> KafkaResult<()> {
        if !self.active.load() {
            return Err(KafkaError::CoordinatorNotAvailable(group_id.to_string()));
        }

        let group = self.group_manager.get_group(group_id);
        if let Some(group) = group {
            let group_clone = group.clone();
            let mut locked_write_group = group.write().await;
            if locked_write_group.has_member(member_id) {
                let member = locked_write_group.get_mut_member(member_id).unwrap();
                member.set_leaving();
                drop(locked_write_group);
                self.delayed_heartbeat_purgatory
                    .check_and_complete(format!("{}-{}", group_id, member_id).as_str())
                    .await;
                self.remove_member_and_update_group(group_clone, member_id)
                    .await;
            } else if locked_write_group.is(GroupState::Dead) {
                return Err(KafkaError::UnknownMemberId(member_id.to_string()));
            }
            Ok(())
        } else {
            Err(KafkaError::UnknownMemberId(member_id.to_string()))
        }
    }

    pub async fn handle_commit_offsets(
        self: Arc<Self>,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
        offsets: HashMap<TopicPartition, PartitionOffsetCommitData>,
    ) -> HashMap<TopicPartition, Vec<(i32, KafkaError)>> {
        let topic_partitions: Vec<TopicPartition> = offsets.keys().cloned().collect();
        let result = |error: KafkaError| {
            let mut topic_responses: HashMap<TopicPartition, Vec<(i32, KafkaError)>> =
                HashMap::new();
            for tp in topic_partitions {
                let topic_partitions = topic_responses.entry(tp.clone()).or_insert_with(Vec::new);
                topic_partitions.push((tp.partition, error.clone()));
            }
            topic_responses
        };
        if !self.active.load() {
            return result(KafkaError::CoordinatorNotAvailable(group_id.to_string()));
        }

        let group = self.group_manager.get_group(group_id);
        if let Some(group) = group {
            let group_clone = group.clone();
            self.do_commit_offsets(group_clone, member_id, generation_id, offsets)
                .await;
        } else if generation_id < 0 {
            // the group is not relying on Kafka for group management, so allow the commit
            let group_metadata = GroupMetadata::new(group_id);
            self.group_manager.add_group(group_metadata);
            let group_clone = self.group_manager.get_group(group_id).unwrap();
            self.do_commit_offsets(group_clone, member_id, generation_id, offsets)
                .await;
        } else {
            return result(KafkaError::IllegalGeneration(group_id.to_string()));
        }

        result(KafkaError::None)
    }
    pub async fn do_commit_offsets(
        self: &Arc<Self>,
        group: Arc<RwLock<GroupMetadata>>,
        member_id: &str,
        generation_id: i32,
        offsets: HashMap<TopicPartition, PartitionOffsetCommitData>,
    ) -> KafkaResult<()> {
        let group_clone = group.clone();
        let locked_read_group = group.read().await;
        let group_id = locked_read_group.id().to_string();
        if locked_read_group.is(GroupState::Dead) {
            return Err(KafkaError::UnknownMemberId(member_id.to_string()));
        } else if generation_id < 0 && locked_read_group.is(GroupState::Empty) {
            self.group_manager
                .store_offset(&group_id, member_id, offsets);
        } else if locked_read_group.is(GroupState::AwaitingSync) {
            return Err(KafkaError::RebalanceInProgress(group_id.to_string()));
        } else if !locked_read_group.has_member(member_id) {
            return Err(KafkaError::UnknownMemberId(member_id.to_string()));
        } else if generation_id != locked_read_group.generation_id() {
            return Err(KafkaError::IllegalGeneration(group_id.to_string()));
        } else {
            drop(locked_read_group);
            self.complete_and_schedule_next_heartbeat_expiry(group_clone, member_id)
                .await;
            self.group_manager
                .store_offset(&group_id, member_id, offsets);
        }
        Ok(())
    }
    pub fn handle_fetch_offsets(
        self: Arc<Self>,
        group_id: &str,
        partitions: Option<Vec<TopicPartition>>,
    ) -> KafkaResult<HashMap<TopicPartition, PartitionOffsetData>> {
        if !self.active.load() {
            return Err(KafkaError::CoordinatorNotAvailable(group_id.to_string()));
        }

        self.group_manager.get_offset(group_id, partitions)
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
    pub members: Option<BTreeMap<String, Bytes>>,
    pub member_id: String,
    pub generation_id: i32,
    pub sub_protocol: String,
    pub leader_id: String,
    pub error: Option<ErrorCode>,
}
