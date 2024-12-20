use tokio::sync::oneshot;

use crate::{message::TopicPartition, AppResult, MemoryRecords};

#[derive(Debug)]
pub struct JournalFileWriteReq {
    pub journal_offset: i64,
    pub topic_partition: TopicPartition,
    pub queue_topic_partition: TopicPartition,
    pub first_batch_queue_base_offset: i64,
    pub last_batch_queue_base_offset: i64,
    pub records_count: u32,
    pub records: MemoryRecords,
}

#[derive(Debug)]
pub struct QueueFileWriteReq {
    pub topic_partition: TopicPartition,
    pub records: MemoryRecords,
}

#[derive(Debug)]
pub struct FlushRequest {
    pub topic_partition: TopicPartition,
}

#[derive(Debug)]
pub enum FileWriteRequest {
    AppendJournal {
        request: JournalFileWriteReq,
        reply: oneshot::Sender<AppResult<()>>,
    },
    AppendQueue {
        request: QueueFileWriteReq,
        reply: oneshot::Sender<AppResult<()>>,
    },
    Flush {
        request: FlushRequest,
        reply: oneshot::Sender<AppResult<u64>>,
    },
}
