use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use crossbeam_utils::atomic::AtomicCell;
use uuid::Uuid;

use crate::{
    message::{
        delayed_operation::DelayedOperationPurgatory,
        kafka_consume::group::{GroupMetadata, GroupState, MemberMetadata},
    },
    protocol::api_schemas::consumer_protocol::ProtocolMetadata,
    request::{
        consumer_group::{
            FindCoordinatorRequest, FindCoordinatorResponse, JoinGroupRequest, JoinGroupResponse,
        },
        errors::{ErrorCode, KafkaError, KafkaResult},
    },
    service::{GroupConfig, Node},
    AppResult,
};

use super::{
    delayed_join::{DelayedJoin, InitialDelayedJoin},
    group::GroupMetadataManager,
};

pub struct GroupCoordinator {
    active: AtomicCell<bool>,
    node: Node,
    group_config: GroupConfig,
    group_manager: GroupMetadataManager,
    delayed_join_purgatory: Arc<DelayedOperationPurgatory<DelayedJoin>>,
    initial_delayed_join_purgatory: Arc<DelayedOperationPurgatory<InitialDelayedJoin>>,
}

impl GroupCoordinator {
    pub fn new(node: Node, group_manager: GroupMetadataManager, group_config: GroupConfig) -> Self {
        Self {
            active: AtomicCell::new(false),
            node,
            group_config,
            group_manager,
        }
    }

    pub fn find_coordinator(&self, group_id: &str) -> AppResult<FindCoordinatorResponse> {
        // 因为stonemq目前支持单机，所以coordinator就是自身
        let response: FindCoordinatorResponse = self.node.clone().into();
        Ok(response)
    }
    pub fn handle_join_group(&self, request: &JoinGroupRequest) -> KafkaResult<JoinGroupResult> {
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
        if let Some(group) = group {
            let _ = self.do_join_group(request, group);
        } else if request.member_id.is_empty() {
            let group_metadata = GroupMetadata::new(request.group_id.clone());
            let group_metadata = self.group_manager.add_group(group_metadata);
            let _ = self.do_join_group(request, group_metadata);
        } else {
            return Ok(self.join_error(request.member_id.clone(), ErrorCode::UnknownMemberId));
        }

        todo!()
    }
    fn do_join_group(
        &self,
        request: &JoinGroupRequest,
        group: GroupMetadata,
    ) -> KafkaResult<JoinGroupResult> {
        if !group.is(GroupState::Empty)
            && (!group.protocol_type_equals(&request.protocol_type)
                || !group.supports_protocols(
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
        } else if !request.member_id.is_empty() && !group.has_member(&request.member_id) {
            // member_id is not empty, but not in group
            return Ok(self.join_error(request.member_id.clone(), ErrorCode::UnknownMemberId));
        } else {
            match group.current_state() {
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
                        &self.add_member_and_rebalance();
                    } else {
                        // existing member join group
                        let member_metadata = group.get_member(&request.member_id);
                        &self.update_member_and_rebalance();
                    }
                }
                GroupState::AwaitingSync => {
                    if request.member_id.is_empty() {
                        // new member join group
                        &self.add_member_and_rebalance();
                    } else {
                        let member = group.get_member(&request.member_id);
                        if member.protocol_match(&request.group_protocols) {
                            // member is joining with the same metadata (which could be because it failed to
                            // receive the initial JoinGroup response), so just return current group information
                            // for the current generation.
                            return Ok(JoinGroupResult {
                                members: {
                                    if request.member_id == group.leader_id() {
                                        Some(group.members.clone())
                                    } else {
                                        None
                                    }
                                },
                                member_id: request.member_id.clone(),
                                generation_id: group.generation_id(),
                                sub_protocol: group.protocol(),
                                leader_id: group.leader_id(),
                                error: None,
                            });
                        } else {
                            // member has changed protocol, need rebalance
                            &self.update_member_and_rebalance();
                        }
                    }
                }
                GroupState::Empty | GroupState::Stable => {
                    if request.member_id.is_empty() {
                        // new member join group, and group is empty
                        &self.add_member_and_rebalance();
                    } else {
                        // existing member join group
                        let member = group.get_member(&request.member_id);
                        if member.id == group.leader_id()
                            || !member.match_protocol(&request.group_protocols)
                        {
                            // member is the leader or has changed protocol, need rebalance
                            &self.update_member_and_rebalance();
                        } else {
                            // member is not the leader and has not changed protocol, just return current group information
                            return Ok(JoinGroupResult {
                                members: None,
                                member_id: request.member_id.clone(),
                                generation_id: group.generation_id(),
                                sub_protocol: group.protocol(),
                                leader_id: group.leader_id().to_string(),
                                error: None,
                            });
                        }
                    }
                }
            }
            if group.is(GroupState::PreparingRebalance) {
                join_purgatory.checkAndComplete();
            }
        }

        todo!()
    }

    fn add_member_and_rebalance(
        &self,
        client_id: String,
        client_host: String,
        session_timeout: i32,
        rebalance_timeout: i32,
        protocol_type: String,
        group_protocols: Vec<ProtocolMetadata>,
        mut group: Arc<GroupMetadata>,
    ) -> KafkaResult<()> {
        let member_id = format!("{}-{}", client_id, Uuid::new_v4().to_string());
        let member_metadata = MemberMetadata::new(
            member_id,
            client_id,
            client_host,
            group.id().to_string(),
            session_timeout,
            rebalance_timeout,
            protocol_type,
            group_protocols,
        );
        group.add_member(member_metadata)?;
        self.prepare_rebalance(group);
        Ok(())
    }
    fn prepare_rebalance(&self, group: Arc<GroupMetadata>) {
        if !group.can_rebalance() {
            return;
        }
        if group.is(GroupState::AwaitingSync) {
            // cancel all member's assignment
            group.cancel_all_member_assignment();
            todo!()
        }
        if group.is(GroupState::Empty) {
            // 初始化延迟加入
            let initial_delayed_join = InitialDelayedJoin::new(
                self.clone(),
                group.clone(),
                self.initial_delayed_join_purgatory.clone(),
                self.group_config.group_rebalance_delay,
                0,
                0,
            );
        } else {
            // 延迟加入
            let delayed_join = DelayedJoin::new(
                self.clone(),
                group.clone(),
                self.delayed_join_purgatory.clone(),
            );
        }
    }
    pub fn can_complete_join(&self, group: GroupMetadata) -> bool {
        todo!()
    }
    pub fn on_complete_join(&self, group: Arc<Mutex<GroupMetadata>>) {
        // remove all not yet rejoined members
        let mut group = group.lock().unwrap();
        let not_yet_rejoined_members = group.not_yet_rejoined_members();
        for member_id in not_yet_rejoined_members {
            group.remove_member(&member_id);
        }
        drop(group); // unlock

        if !group.is(GroupState::Dead) {
            group.init_next_generation();

            if group.is(GroupState::Empty) {
                self.group_manager.store_group(group);
            } else {
                // 给所有等待加入的成员发送加入结果
                for member in group.all_members() {
                    let join_result = JoinGroupResult {
                        members: if member.id == group.leader_id() {
                            Some(group.members.clone())
                        } else {
                            None
                        },
                        member_id: member.id.clone(),
                        generation_id: group.generation_id(),
                        sub_protocol: group.protocol(),
                        leader_id: group.leader_id().to_string(),
                        error: None,
                    };

                    if member.is_awaiting_join_result() {
                        let _ = self.send_join_group_result(group, member);
                    }
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
                    GroupState::AwaitingSync{
                        if(group.)
                    }
                    _ => {} 
                }
            } else {
                    return Err(KafkaError::UnknownMemberId(member_id.to_string()));
                }
            }
        }

        todo!()
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

pub struct JoinGroupResult {
    pub members: Option<HashMap<String, Vec<u8>>>,
    pub member_id: String,
    pub generation_id: i32,
    pub sub_protocol: String,
    pub leader_id: String,
    pub error: Option<ErrorCode>,
}
