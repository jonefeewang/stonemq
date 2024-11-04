use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::oneshot;
use tracing::debug;

use crate::utils::{DelayedSyncOperation, DelayedSyncOperationPurgatory};

use super::{
    coordinator::GroupCoordinator,
    group::{GroupMetadata, MemberMetadata},
};

pub struct DelayedJoin {
    group_cordinator: Arc<GroupCoordinator>,
    group: Arc<RwLock<GroupMetadata>>,
    rebalance_timeout: u64,
}
impl DelayedJoin {
    pub fn new(
        group_cordinator: Arc<GroupCoordinator>,
        group: Arc<RwLock<GroupMetadata>>,
        rebalance_timeout: u64,
    ) -> Self {
        Self {
            group_cordinator,
            group,
            rebalance_timeout,
        }
    }
}
impl DelayedSyncOperation for DelayedJoin {
    fn delay_ms(&self) -> u64 {
        self.rebalance_timeout
    }

    fn try_complete(&self) -> bool {
        self.group_cordinator.can_complete_join(self.group.clone())
    }

    fn on_complete(&self) {
        self.group_cordinator.on_complete_join(self.group.clone())
    }

    fn on_expiration(&self) {
        debug!("delayed join expired");
    }
}
// 初始化延迟加入，第一个组的加入者
pub struct InitialDelayedJoin {
    group_cordinator: Arc<GroupCoordinator>,
    group: Arc<RwLock<GroupMetadata>>,
    purgatory: Arc<DelayedSyncOperationPurgatory<InitialDelayedJoin>>,
    configured_rebalance_delay: i32,
    delay_ms: i32,
    remaining_delay_ms: i32,
}
impl InitialDelayedJoin {
    pub fn new(
        group_cordinator: Arc<GroupCoordinator>,
        group: Arc<RwLock<GroupMetadata>>,
        purgatory: Arc<DelayedSyncOperationPurgatory<InitialDelayedJoin>>,
        configured_rebalance_delay: i32,
        delay_ms: i32,
        remaining_delay_ms: i32,
    ) -> Self {
        Self {
            group_cordinator,
            group,
            purgatory,
            configured_rebalance_delay,
            delay_ms,
            remaining_delay_ms,
        }
    }
}
impl DelayedSyncOperation for InitialDelayedJoin {
    fn delay_ms(&self) -> u64 {
        self.delay_ms as u64
    }

    fn try_complete(&self) -> bool {
        false
    }

    fn on_complete(&self) {
        if self.group.new_member_added && self.remaining_delay_ms != 0 {
            self.group.new_member_added = false;
            let delay = self.remaining_delay_ms.min(self.configured_rebalance_delay);
            let remaining = (self.remaining_delay_ms - delay).max(0);
            let new_join = InitialDelayedJoin {
                group_cordinator: self.group_cordinator.clone(),
                group: self.group.clone(),
                purgatory: self.purgatory.clone(),
                configured_rebalance_delay: self.configured_rebalance_delay,
                delay_ms: delay,
                remaining_delay_ms: remaining,
            };
            self.purgatory
                .try_complete_else_watch(new_join, vec![self.group.group_id.clone()]);
        } else {
            self.group_cordinator.on_complete_join(self.group.clone());
        }
    }

    fn on_expiration(&self) {
        debug!("initial delayed join expired");
    }
}

pub struct DelayedHeartbeat {
    group_cordinator: Arc<GroupCoordinator>,
    group: Arc<GroupMetadata>,
    member: MemberMetadata,
    heartbeat_deadline: u64,
    session_timeout: u64,
}

impl DelayedSyncOperation for DelayedHeartbeat {
    fn delay_ms(&self) -> u64 {
        self.session_timeout
    }

    fn try_complete(&self) -> bool {
        false
    }

    fn on_complete(&self) {
        todo!()
    }

    fn on_expiration(&self) {
        todo!()
    }
}
