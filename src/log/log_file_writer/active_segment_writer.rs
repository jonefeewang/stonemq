use crate::message::TopicPartition;
use crate::utils::{MultipleChannelWorkerPool, PoolHandler, WorkerPoolConfig};
use crate::{AppError, AppResult};

use dashmap::DashMap;

use std::io;
use std::sync::Arc;
use tokio::sync::{broadcast, oneshot};

use super::log_request::{FlushRequest, JournalFileWriteReq, QueueFileWriteReq};
use super::{ActiveSegmentWriter, FileWriteRequest, SegmentLog, WriteConfig};

impl ActiveSegmentWriter {
    pub fn new(
        notify_shutdown: broadcast::Sender<()>,
        worker_pool_config: Option<WorkerPoolConfig>,
        write_config: Option<WriteConfig>,
    ) -> Self {
        let writers = Arc::new(DashMap::new());
        let config = worker_pool_config.unwrap_or_default();
        let write_config = write_config.unwrap_or_default();

        let handler = FileWriteHandler {
            writers: Arc::clone(&writers),
        };

        let worker_pool = MultipleChannelWorkerPool::new(
            notify_shutdown,
            "active_log_file_writer".to_string(),
            handler,
            config,
        );
        Self {
            writers,
            worker_pool,
            write_config,
        }
    }

    /// Open a new log file for the new base offset of the topic partition.
    /// Add the new segment to the mapping, allowing the previous segment to be automatically dropped.
    pub fn open_file(&self, topic_partition: &TopicPartition, base_offset: i64) -> io::Result<()> {
        let segment_log = SegmentLog::new(base_offset, topic_partition, &self.write_config);
        self.writers.insert(topic_partition.clone(), segment_log);
        Ok(())
    }
    pub async fn append_journal(&self, request: JournalFileWriteReq) -> AppResult<()> {
        let (tx, rx) = oneshot::channel();
        let channel_id = self.channel_id(&request.topic_partition);
        self.worker_pool
            .send(
                FileWriteRequest::AppendJournal { request, reply: tx },
                channel_id,
            )
            .await
            .map_err(|e| AppError::ChannelSendError(e.to_string()))?;
        rx.await
            .map_err(|e| AppError::ChannelRecvError(e.to_string()))??;
        Ok(())
    }

    pub async fn append_queue(&self, request: QueueFileWriteReq) -> AppResult<()> {
        let (tx, rx) = oneshot::channel();
        let channel_id = self.channel_id(&request.topic_partition);
        self.worker_pool
            .send(
                FileWriteRequest::AppendQueue { request, reply: tx },
                channel_id,
            )
            .await
            .map_err(|e| AppError::ChannelSendError(e.to_string()))?;
        rx.await
            .map_err(|e| AppError::ChannelRecvError(e.to_string()))??;
        Ok(())
    }

    pub async fn flush(&self, request: FlushRequest) -> AppResult<u64> {
        let (tx, rx) = oneshot::channel();
        let channel_id = self.channel_id(&request.topic_partition);
        self.worker_pool
            .send(FileWriteRequest::Flush { request, reply: tx }, channel_id)
            .await
            .map_err(|e| AppError::ChannelSendError(e.to_string()))?;
        let size = rx
            .await
            .map_err(|e| AppError::ChannelRecvError(e.to_string()))??;
        Ok(size)
    }
    fn channel_id(&self, topic_partition: &TopicPartition) -> i8 {
        (topic_partition.partition() % self.worker_pool.get_pool_config().num_channels as i32) as i8
    }

    pub fn active_segment_size(&self, topic_partition: &TopicPartition) -> u64 {
        self.writers.get(topic_partition).unwrap().get_size()
    }
}

// 处理器实现
#[derive(Clone)]
struct FileWriteHandler {
    writers: Arc<DashMap<TopicPartition, SegmentLog>>,
}

impl PoolHandler<FileWriteRequest> for FileWriteHandler {
    async fn handle(&self, request: FileWriteRequest) {
        match request {
            FileWriteRequest::AppendJournal { request, reply } => {
                if let Some(mut writer) = self.writers.get_mut(&request.topic_partition) {
                    let result = writer.write_journal(request).await;
                    let _ = reply.send(result.map_err(Into::into));
                }
            }
            FileWriteRequest::AppendQueue { request, reply } => {
                if let Some(mut writer) = self.writers.get_mut(&request.topic_partition) {
                    let result = writer.write_queue(request).await;
                    let _ = reply.send(result.map_err(Into::into));
                }
            }
            FileWriteRequest::Flush { request, reply } => {
                if let Some(mut writer) = self.writers.get_mut(&request.topic_partition) {
                    let result = writer.flush().await;
                    let _ = reply.send(result.map_err(Into::into));
                }
            }
        }
    }
}
