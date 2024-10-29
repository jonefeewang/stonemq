pub use delayed_fetch::DelayedFetch;
pub use records::{MemoryRecordBuilder, MemoryRecords};
pub use replica::{JournalReplica, QueueReplica, ReplicaManager};
pub use topic_partition::{LogAppendInfo, PartitionData, TopicData, TopicPartition};

use crate::log::PositionInfo;
mod batch_records;
mod consume;
mod delayed_fetch;
mod delayed_operation;
mod records;
mod replica;
mod test;
mod topic_partition;

#[derive(Debug)]
pub struct LogFetchInfo {
    pub records: MemoryRecords,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub position_info: PositionInfo,
}
