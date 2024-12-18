use tokio::sync::oneshot;

use crate::{message::TopicPartition, AppResult, MemoryRecords};

pub struct JournalLogWriteOp {
    pub journal_offset: i64,
    pub topic_partition: TopicPartition,
    pub queue_topic_partition: TopicPartition,
    pub first_batch_queue_base_offset: i64,
    pub last_batch_queue_base_offset: i64,
    pub records_count: u32,
    pub records: MemoryRecords,
    pub segment_base_offset: i64,
}

pub struct QueueLogWriteOp {
    pub journal_offset: i64,
    pub topic_partition: TopicPartition,
    pub first_batch_queue_base_offset: i64,
    pub last_batch_queue_base_offset: i64,
    pub records_count: u32,
    pub records: MemoryRecords,
    pub segment_base_offset: i64,
}

pub struct FlushRequest {
    pub topic_partition: TopicPartition,
    pub segment_base_offset: i64,
}

pub enum LogWriteRequest {
    AppendJournal {
        request: JournalLogWriteOp,
        reply: oneshot::Sender<AppResult<()>>,
    },
    AppendQueue {
        request: QueueLogWriteOp,
        reply: oneshot::Sender<AppResult<()>>,
    },
    Flush {
        request: FlushRequest,
        reply: oneshot::Sender<AppResult<u64>>,
    },
}
