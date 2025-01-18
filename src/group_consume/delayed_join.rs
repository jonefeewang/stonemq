/// Module for handling delayed operations in consumer group coordination.
/// This includes delayed join operations for new members and heartbeat monitoring
/// for existing members.
use std::{future::Future, pin::Pin, sync::Arc};

use tokio::{sync::RwLock, time::Instant};
use tracing::{debug, trace};

use crate::utils::{DelayedAsyncOperation, DelayedAsyncOperationPurgatory};

use super::{GroupCoordinator, GroupMetadata};

/// Represents a delayed join operation for a consumer group member.
/// This is used to handle the rebalance timeout when members join the group.
#[derive(Debug)]
pub struct DelayedJoin {
    /// Reference to the group coordinator that manages this operation
    group_cordinator: Arc<GroupCoordinator>,
    /// Reference to the group metadata
    group: Arc<RwLock<GroupMetadata>>,
    /// Maximum time to wait for all members to join
    rebalance_timeout: u64,
}

impl DelayedJoin {
    /// Creates a new delayed join operation
    ///
    /// # Arguments
    /// * `group_cordinator` - Reference to the group coordinator
    /// * `group` - Reference to the group metadata
    /// * `rebalance_timeout` - Maximum time to wait for all members to join
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

/// Represents an initial delayed join operation for the first member of a group.
/// This implements a special delay mechanism for the first rebalance to allow
/// additional members to join before completing the initial group formation.
#[derive(Debug)]
pub struct InitialDelayedJoin {
    /// Reference to the group coordinator
    group_cordinator: Arc<GroupCoordinator>,
    /// Reference to the group metadata
    group: Arc<RwLock<GroupMetadata>>,
    /// Reference to the purgatory that manages this operation
    purgatory: Arc<DelayedAsyncOperationPurgatory<InitialDelayedJoin>>,
    /// Configured delay for initial rebalance
    configured_rebalance_delay: i32,
    /// Current delay duration
    delay_ms: i32,
    /// Remaining delay time
    remaining_delay_ms: i32,
}

impl InitialDelayedJoin {
    /// Creates a new initial delayed join operation
    ///
    /// # Arguments
    /// * `group_cordinator` - Reference to the group coordinator
    /// * `group` - Reference to the group metadata
    /// * `purgatory` - Reference to the purgatory that manages this operation
    /// * `configured_rebalance_delay` - Configured delay for initial rebalance
    /// * `delay_ms` - Current delay duration
    /// * `remaining_delay_ms` - Remaining delay time
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

/// Represents a delayed heartbeat operation for monitoring member liveness.
/// This is used to detect member failures when heartbeats are not received
/// within the session timeout period.
#[derive(Debug)]
pub struct DelayedHeartbeat {
    /// Reference to the group coordinator
    group_cordinator: Arc<GroupCoordinator>,
    /// Reference to the group metadata
    group: Arc<RwLock<GroupMetadata>>,
    /// Deadline by which the next heartbeat must be received
    heartbeat_deadline: Instant,
    /// Session timeout period
    session_timeout: u64,
    /// ID of the member being monitored
    member_id: String,
}

impl DelayedHeartbeat {
    /// Creates a new delayed heartbeat operation
    ///
    /// # Arguments
    /// * `group_cordinator` - Reference to the group coordinator
    /// * `group` - Reference to the group metadata
    /// * `heartbeat_deadline` - Deadline by which the next heartbeat must be received
    /// * `member_id` - ID of the member being monitored
    /// * `session_timeout` - Session timeout period
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
