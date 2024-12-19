mod batch_header;
mod constants;
mod memory_records;
mod record;
mod record_batch;
mod record_batch_test;
mod topic_partition;

pub use memory_records::MemoryRecords;
pub use record_batch::RecordBatch;
pub use topic_partition::{JournalPartition, QueuePartition};
pub use topic_partition::{PartitionMsgData, TopicData, TopicPartition};

use crate::log::PositionInfo;

#[derive(Debug)]
pub struct LogFetchInfo {
    pub records: MemoryRecords,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub position_info: PositionInfo,
}
