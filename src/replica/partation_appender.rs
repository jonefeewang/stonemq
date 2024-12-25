use crate::log::LogAppendInfo;
use crate::message::{JournalPartition, MemoryRecords, TopicPartition};
use crate::utils::{MultipleChannelWorkerPool, PoolHandler, WorkerPoolConfig};
use crate::{AppError, AppResult};
use dashmap::DashMap;

use std::sync::Arc;
use tokio::sync::{broadcast, mpsc::Sender, oneshot};

use super::JOURNAL_PARTITION_APPENDER;

// 全局单例

#[derive(Debug)]
pub struct PartitionAppender {
    partitions: Arc<DashMap<TopicPartition, Arc<JournalPartition>>>,
    worker_pool: MultipleChannelWorkerPool<AppendRequest>,
}

#[derive(Debug)]
struct AppendRequest {
    journal_topic_partition: TopicPartition,
    queue_topic_partition: TopicPartition,
    records: MemoryRecords,
    reply: oneshot::Sender<AppResult<LogAppendInfo>>,
}

impl PartitionAppender {
    pub fn global_init(
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: Sender<()>,
        worker_pool_config: Option<WorkerPoolConfig>,
    ) -> &'static Arc<PartitionAppender> {
        JOURNAL_PARTITION_APPENDER.get_or_init(|| {
            Arc::new(Self::new(
                notify_shutdown,
                shutdown_complete_tx,
                worker_pool_config,
            ))
        })
    }

    fn new(
        notify_shutdown: broadcast::Sender<()>,
        shutdown_complete_tx: Sender<()>,
        worker_pool_config: Option<WorkerPoolConfig>,
    ) -> Self {
        let partitions = Arc::new(DashMap::new());
        let config = worker_pool_config.unwrap_or_default();

        let handler = AppendHandler {
            partitions: Arc::clone(&partitions),
        };

        let worker_pool =
            MultipleChannelWorkerPool::new(notify_shutdown, shutdown_complete_tx, handler, config);

        Self {
            partitions,
            worker_pool,
        }
    }

    pub fn register_partition(
        &self,
        topic_partition: TopicPartition,
        partition: Arc<JournalPartition>,
    ) {
        self.partitions.insert(topic_partition, partition);
    }

    pub async fn append_journal(
        &self,
        journal_topic_partition: &TopicPartition,
        queue_topic_partition: &TopicPartition,
        records: MemoryRecords,
    ) -> AppResult<LogAppendInfo> {
        if !self.partitions.contains_key(journal_topic_partition) {
            return Err(AppError::IllegalStateError(format!(
                "topic partition: {} not found",
                journal_topic_partition
            )));
        }

        let (tx, rx) = oneshot::channel();
        let channel_id = self.channel_id(journal_topic_partition);

        let request = AppendRequest {
            journal_topic_partition: journal_topic_partition.clone(),
            queue_topic_partition: queue_topic_partition.clone(),
            records,
            reply: tx,
        };

        self.worker_pool
            .send(request, channel_id)
            .await
            .map_err(|e| AppError::ChannelSendError(e.to_string()))?;

        rx.await
            .map_err(|e| AppError::ChannelRecvError(e.to_string()))?
    }

    fn channel_id(&self, topic_partition: &TopicPartition) -> i8 {
        (topic_partition.partition() % self.worker_pool.get_pool_config().num_channels as i32) as i8
    }
}

#[derive(Clone)]
struct AppendHandler {
    partitions: Arc<DashMap<TopicPartition, Arc<JournalPartition>>>,
}

impl PoolHandler<AppendRequest> for AppendHandler {
    async fn handle(&self, request: AppendRequest) {
        let result = if let Some(partition) = self.partitions.get(&request.journal_topic_partition)
        {
            partition
                .append_record_to_leader(request.records, &request.queue_topic_partition)
                .await
                .map_err(Into::into)
        } else {
            Err(AppError::IllegalStateError(format!(
                "topic partition: {} not found",
                request.journal_topic_partition
            )))
        };

        let _ = request.reply.send(result);
    }
}
