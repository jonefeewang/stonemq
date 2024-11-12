pub use delayed_fetch::DelayedFetch;
pub use records::{MemoryRecordBuilder, MemoryRecords};
pub use replica::{JournalReplica, QueueReplica, ReplicaManager};
pub use topic_partition::{LogAppendInfo, PartitionMsgData, TopicData, TopicPartition};

use crate::log::PositionInfo;
mod batch_records;
mod delayed_fetch;
mod kafka_consume;
pub mod offset;
mod records;
mod replica;
mod topic_partition;

#[derive(Debug)]
pub struct LogFetchInfo {
    pub records: MemoryRecords,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub position_info: PositionInfo,
}
