use std::sync::{Arc, LazyLock};

use crate::log::{PositionInfo, NO_POSITION_INFO};

mod batch_header;
mod constants;
mod delayed_fetch;
mod kafka_consume;
mod memory_records;
pub mod offset;
mod record;
mod record_batch;
mod replica;
mod tests;
mod topic_partition;

pub use batch_header::BatchHeader;
pub use constants::*;
pub use delayed_fetch::DelayedFetch;
pub use kafka_consume::GroupCoordinator;
pub use memory_records::MemoryRecords;
pub use record::{Record, RecordHeader};
pub use record_batch::{RecordBatch, RecordBatchBuilder};
pub use replica::{JournalReplica, QueueReplica, ReplicaManager};
pub use topic_partition::{PartitionMsgData, TopicData, TopicPartition};

#[derive(Debug)]
pub struct LogFetchInfo {
    pub records: MemoryRecords,
    pub log_start_offset: i64,
    pub log_end_offset: i64,
    pub position_info: PositionInfo,
}
