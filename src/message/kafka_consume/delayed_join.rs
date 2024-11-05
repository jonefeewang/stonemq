use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::debug;

use crate::utils::{DelayedAsyncOperation, DelayedAsyncOperationPurgatory};

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
impl DelayedAsyncOperation for DelayedJoin {
    fn delay_ms(&self) -> u64 {
        self.rebalance_timeout
    }

    async fn try_complete(&self) -> bool {
        self.group_cordinator
            .can_complete_join(self.group.clone())
            .await
    }

    async fn on_complete(&self) {
        self.group_cordinator.on_complete_join(self.group.clone());
    }

    async fn on_expiration(&self) {
        debug!("delayed join expired");
    }
}
// 初始化延迟加入，第一个组的加入者
pub struct InitialDelayedJoin {
    group_cordinator: Arc<GroupCoordinator>,
    group: Arc<RwLock<GroupMetadata>>,
    purgatory: Arc<DelayedAsyncOperationPurgatory<InitialDelayedJoin>>,
    configured_rebalance_delay: i32,
    delay_ms: i32,
    remaining_delay_ms: i32,
}
impl InitialDelayedJoin {
    pub fn new(
        group_cordinator: Arc<GroupCoordinator>,
        group: Arc<RwLock<GroupMetadata>>,
        purgatory: Arc<DelayedAsyncOperationPurgatory<InitialDelayedJoin>>,
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
impl DelayedAsyncOperation for InitialDelayedJoin {
    fn delay_ms(&self) -> u64 {
        self.delay_ms as u64
    }

    async fn try_complete(&self) -> bool {
        false
    }

    async fn on_complete(&self) {
        let group_clone = self.group.clone();
        let mut locked_write_group = group_clone.write().await;
        let group_id = locked_write_group.id().to_string();
        if locked_write_group.new_member_added() && self.remaining_delay_ms != 0 {
            locked_write_group.reset_new_member_added();
            let delay = self.remaining_delay_ms.min(self.configured_rebalance_delay);
            let remaining = (self.remaining_delay_ms - delay).max(0);
            let new_delayed_join = InitialDelayedJoin {
                group_cordinator: self.group_cordinator.clone(),
                group: self.group.clone(),
                purgatory: self.purgatory.clone(),
                configured_rebalance_delay: self.configured_rebalance_delay,
                delay_ms: delay,
                remaining_delay_ms: remaining,
            };
            // 释放锁, 因为下边的调用会再次尝试获取锁
            drop(locked_write_group);

            self.purgatory
                .try_complete_else_watch(new_delayed_join, vec![group_id]);
        } else {
            self.group_cordinator.on_complete_join(self.group.clone());
        }
    }

    async fn on_expiration(&self) {
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

impl DelayedAsyncOperation for DelayedHeartbeat {
    fn delay_ms(&self) -> u64 {
        self.session_timeout
    }

    async fn try_complete(&self) -> bool {
        false
    }

    async fn on_complete(&self) {
        todo!()
    }

    async fn on_expiration(&self) {
        todo!()
    }
}
