// 组状态
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GroupState {
    /// Group is preparing to rebalance
    ///
    /// action: respond to heartbeats with REBALANCE_IN_PROGRESS
    ///         respond to sync group with REBALANCE_IN_PROGRESS
    ///         remove member on leave group request
    ///         park join group requests from new or existing members until all expected members have joined
    ///         allow offset commits from previous generation
    ///         allow offset fetch requests
    /// transition: some members have joined by the timeout => AwaitingSync
    ///             all members have left the group => Empty
    ///             group is removed by partition emigration => Dead
    PreparingRebalance = 1,

    /// Group is awaiting state assignment from the leader
    ///
    /// action: respond to heartbeats with REBALANCE_IN_PROGRESS
    ///         respond to offset commits with REBALANCE_IN_PROGRESS
    ///         park sync group requests from followers until transition to Stable
    ///         allow offset fetch requests
    /// transition: sync group with state assignment received from leader => Stable
    ///             join group from new member or existing member with updated metadata => PreparingRebalance
    ///             leave group from existing member => PreparingRebalance
    ///             member failure detected => PreparingRebalance
    ///             group is removed by partition emigration => Dead
    AwaitingSync = 2,

    /// Group is stable
    ///
    /// action: respond to member heartbeats normally
    ///         respond to sync group from any member with current assignment
    ///         respond to join group from followers with matching metadata with current group metadata
    ///         allow offset commits from member of current generation
    ///         allow offset fetch requests
    /// transition: member failure detected via heartbeat => PreparingRebalance
    ///             leave group from existing member => PreparingRebalance
    ///             leader join-group received => PreparingRebalance
    ///             follower join-group with new metadata => PreparingRebalance
    ///             group is removed by partition emigration => Dead
    Stable = 3,

    /// Group has no more members and its metadata is being removed
    ///
    /// action: respond to join group with UNKNOWN_MEMBER_ID
    ///         respond to sync group with UNKNOWN_MEMBER_ID
    ///         respond to heartbeat with UNKNOWN_MEMBER_ID
    ///         respond to leave group with UNKNOWN_MEMBER_ID
    ///         respond to offset commit with UNKNOWN_MEMBER_ID
    ///         allow offset fetch requests
    /// transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
    Dead = 4,

    /// Group has no more members, but lingers until all offsets have expired. This state
    /// also represents groups which use Kafka only for offset commits and have no members.
    ///
    /// action: respond normally to join group from new members
    ///         respond to sync group with UNKNOWN_MEMBER_ID
    ///         respond to heartbeat with UNKNOWN_MEMBER_ID
    ///         respond to leave group with UNKNOWN_MEMBER_ID
    ///         respond to offset commit with UNKNOWN_MEMBER_ID
    ///         allow offset fetch requests
    /// transition: last offsets removed in periodic expiration task => Dead
    ///             join group from a new member => PreparingRebalance
    ///             group is removed by partition emigration => Dead
    ///             group is removed by expiration => Dead
    Empty = 5,
}
impl GroupState {
    /// 检查是否可以从当前状态转换到目标状态
    pub const fn can_transition_to(current: GroupState, target: GroupState) -> bool {
        match (current, target) {
            (_, GroupState::Dead) => true,  // 任何状态都可以转换到Dead
            (GroupState::Dead, _) => false, // Dead状态不能转换到其他状态
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
