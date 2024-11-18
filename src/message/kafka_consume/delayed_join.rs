use std::{future::Future, pin::Pin, sync::Arc};

use tokio::{sync::RwLock, time::Instant};
use tracing::{debug, trace};

use crate::utils::{DelayedAsyncOperation, DelayedAsyncOperationPurgatory};

use super::{coordinator::GroupCoordinator, group::GroupMetadata};

#[derive(Debug)]
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

    fn on_complete(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.group_cordinator
                .on_complete_join(self.group.clone())
                .await;
        })
    }

    async fn on_expiration(&self) {
        debug!("delayed join expired");
    }
}
// 初始化延迟加入，第一个组的加入者
#[derive(Debug)]
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

    fn on_complete(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        trace!("initial delayed join on-complete");
        Box::pin(async move {
            let group_clone = self.group.clone();
            let mut locked_write_group = group_clone.write().await;

            let group_id = locked_write_group.id().to_string();
            trace!(
                "initial delayed join reset condition {}:{}",
                locked_write_group.new_member_added(),
                self.remaining_delay_ms != 0
            );
            if locked_write_group.new_member_added() && self.remaining_delay_ms != 0 {
                trace!("initial delayed join reset new member added");
                locked_write_group.reset_new_member_added();
                let delay = self.remaining_delay_ms.min(self.configured_rebalance_delay);
                let remaining = (self.remaining_delay_ms - delay).max(0);
                drop(locked_write_group);

                let new_delayed_join = InitialDelayedJoin::new(
                    self.group_cordinator.clone(),
                    self.group.clone(),
                    self.purgatory.clone(),
                    self.configured_rebalance_delay,
                    delay,
                    remaining,
                );
                let new_delayed_join_clone = Arc::new(new_delayed_join);
                self.purgatory
                    .try_complete_else_watch(new_delayed_join_clone, vec![group_id])
                    .await;
            } else {
                drop(locked_write_group);
                self.group_cordinator
                    .on_complete_join(self.group.clone())
                    .await;
            }
            trace!("initial delayed join on-complete--ended");
        })
    }

    async fn on_expiration(&self) {
        trace!("initial delayed join expired");
    }
}

#[derive(Debug)]
pub struct DelayedHeartbeat {
    group_cordinator: Arc<GroupCoordinator>,
    group: Arc<RwLock<GroupMetadata>>,
    heartbeat_deadline: Instant,
    session_timeout: u64,
    member_id: String,
}
impl DelayedHeartbeat {
    pub fn new(
        group_cordinator: Arc<GroupCoordinator>,
        group: Arc<RwLock<GroupMetadata>>,
        heartbeat_deadline: Instant,
        member_id: String,
        session_timeout: u64,
    ) -> Self {
        trace!(
            "new delayed heartbeat member: {} session_timeout: {}",
            member_id,
            session_timeout
        );
        Self {
            group_cordinator,
            group,
            heartbeat_deadline,
            session_timeout,
            member_id,
        }
    }
}
impl DelayedAsyncOperation for DelayedHeartbeat {
    fn delay_ms(&self) -> u64 {
        self.session_timeout
    }

    async fn try_complete(&self) -> bool {
        self.group_cordinator
            .try_complete_heartbeat(self.group.clone(), &self.member_id, self.heartbeat_deadline)
            .await
    }

    fn on_complete(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        trace!("delayed heartbeat on-complete");
        Box::pin(async move {
            self.group_cordinator
                .on_heartbeat_complete(self.group.clone(), &self.member_id)
                .await;
        })
    }

    async fn on_expiration(&self) {
        trace!("delayed heartbeat on-expiration");
        self.group_cordinator
            .clone()
            .on_heartbeat_expiry(self.group.clone(), &self.member_id, self.heartbeat_deadline)
            .await;
    }
}
