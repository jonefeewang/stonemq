/// Module defining the possible states of a consumer group and state transition rules.
/// The state machine governs how the group coordinator responds to various requests
/// and manages group membership changes.

/// Represents the current state of a consumer group.
/// The state determines how the coordinator handles various group operations
/// like member joins, heartbeats, and partition assignments.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GroupState {
    /// Group is preparing to rebalance
    ///
    /// Actions:
    /// - Respond to heartbeats with REBALANCE_IN_PROGRESS
    /// - Respond to sync group with REBALANCE_IN_PROGRESS
    /// - Remove member on leave group request
    /// - Park join group requests from new or existing members until all expected members have joined
    /// - Allow offset commits from previous generation
    /// - Allow offset fetch requests
    ///
    /// Transitions:
    /// - Some members have joined by the timeout => AwaitingSync
    /// - All members have left the group => Empty
    /// - Group is removed by partition emigration => Dead
    PreparingRebalance = 1,

    /// Group is awaiting state assignment from the leader
    ///
    /// Actions:
    /// - Respond to heartbeats with REBALANCE_IN_PROGRESS
    /// - Respond to offset commits with REBALANCE_IN_PROGRESS
    /// - Park sync group requests from followers until transition to Stable
    /// - Allow offset fetch requests
    ///
    /// Transitions:
    /// - Sync group with state assignment received from leader => Stable
    /// - Join group from new member or existing member with updated metadata => PreparingRebalance
    /// - Leave group from existing member => PreparingRebalance
    /// - Member failure detected => PreparingRebalance
    /// - Group is removed by partition emigration => Dead
    AwaitingSync = 2,

    /// Group is stable with all members active and partition assignments completed
    ///
    /// Actions:
    /// - Respond to member heartbeats normally
    /// - Respond to sync group from any member with current assignment
    /// - Respond to join group from followers with matching metadata with current group metadata
    /// - Allow offset commits from member of current generation
    /// - Allow offset fetch requests
    ///
    /// Transitions:
    /// - Member failure detected via heartbeat => PreparingRebalance
    /// - Leave group from existing member => PreparingRebalance
    /// - Leader join-group received => PreparingRebalance
    /// - Follower join-group with new metadata => PreparingRebalance
    /// - Group is removed by partition emigration => Dead
    Stable = 3,

    /// Group has no more members and its metadata is being removed
    ///
    /// Actions:
    /// - Respond to join group with UNKNOWN_MEMBER_ID
    /// - Respond to sync group with UNKNOWN_MEMBER_ID
    /// - Respond to heartbeat with UNKNOWN_MEMBER_ID
    /// - Respond to leave group with UNKNOWN_MEMBER_ID
    /// - Respond to offset commit with UNKNOWN_MEMBER_ID
    /// - Allow offset fetch requests
    ///
    /// Transitions:
    /// Dead is a final state before group metadata is cleaned up, so there are no transitions
    Dead = 4,

    /// Group has no members but retains metadata until offsets expire
    ///
    /// This state also represents groups which use Kafka only for offset commits and have no members.
    ///
    /// Actions:
    /// - Respond normally to join group from new members
    /// - Respond to sync group with UNKNOWN_MEMBER_ID
    /// - Respond to heartbeat with UNKNOWN_MEMBER_ID
    /// - Respond to leave group with UNKNOWN_MEMBER_ID
    /// - Respond to offset commit with UNKNOWN_MEMBER_ID
    /// - Allow offset fetch requests
    ///
    /// Transitions:
    /// - Last offsets removed in periodic expiration task => Dead
    /// - Join group from a new member => PreparingRebalance
    /// - Group is removed by partition emigration => Dead
    /// - Group is removed by expiration => Dead
    Empty = 5,
}

impl GroupState {
    /// Checks if a transition from the current state to the target state is valid
    ///
    /// # Arguments
    /// * `current` - The current state of the group
    /// * `target` - The target state to transition to
    ///
    /// # Returns
    /// * `true` if the transition is valid
    /// * `false` if the transition is not allowed
    pub const fn can_transition_to(current: GroupState, target: GroupState) -> bool {
        match (current, target) {
            (_, GroupState::Dead) => true,  // Any state can transition to Dead
            (GroupState::Dead, _) => false, // Dead state cannot transition to other states
            (GroupState::Empty, GroupState::PreparingRebalance) => true,
            (GroupState::Stable, GroupState::PreparingRebalance) => true,
            (GroupState::AwaitingSync, GroupState::PreparingRebalance) => true,
            (GroupState::PreparingRebalance, GroupState::AwaitingSync) => true,
            (GroupState::PreparingRebalance, GroupState::Empty) => true,
            (GroupState::AwaitingSync, GroupState::Stable) => true,
            _ => false,
        }
    }
}
