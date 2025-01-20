// Copyright 2025 jonefeewang@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::log::LogAppendInfo;
use crate::message::{JournalPartition, MemoryRecords, TopicPartition};
use crate::utils::{MultipleChannelWorkerPool, PoolHandler, WorkerPoolConfig};
use crate::{AppError, AppResult};
use dashmap::DashMap;
use tracing::debug;

use std::sync::Arc;
use tokio::sync::{broadcast, oneshot};

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
    pub fn new(
        notify_shutdown: broadcast::Sender<()>,
        worker_pool_config: Option<WorkerPoolConfig>,
    ) -> Self {
        let partitions = Arc::new(DashMap::new());
        let config = worker_pool_config.unwrap_or_default();

        let handler = AppendHandler {
            partitions: Arc::clone(&partitions),
        };

        let worker_pool = MultipleChannelWorkerPool::new(
            notify_shutdown,
            "partition_appender".to_string(),
            handler,
            config,
        );

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

impl Drop for PartitionAppender {
    fn drop(&mut self) {
        debug!("PartitionAppender dropped");
    }
}
