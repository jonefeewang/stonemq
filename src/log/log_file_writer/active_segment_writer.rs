//! Active Segment Writer Implementation
//!
//! This module implements the active segment writer functionality, which is responsible for
//! managing and coordinating write operations to active log segments across different topic
//! partitions. It uses a worker pool pattern to handle concurrent write operations efficiently.

use crate::message::TopicPartition;
use crate::utils::{MultipleChannelWorkerPool, PoolHandler, WorkerPoolConfig};
use crate::{AppError, AppResult};

use dashmap::DashMap;

use std::io;
use std::sync::Arc;
use tokio::sync::{broadcast, oneshot};

use super::log_request::{FlushRequest, JournalFileWriteReq, QueueFileWriteReq};
use super::{ActiveSegmentWriter, FileWriteRequest, SegmentLog, WriteConfig};

/// Manages active segment writers for different topic partitions.
///
/// The ActiveSegmentWriter is a central component that coordinates write operations
/// across multiple topic partitions. It maintains a pool of workers to handle
/// concurrent write operations and manages the lifecycle of segment logs.
///
/// # Architecture
///
/// - Uses a worker pool pattern for handling concurrent write operations
/// - Maintains a thread-safe map of active segment writers per topic partition
/// - Provides asynchronous write operations for both journal and queue logs
/// - Implements efficient channel-based communication for write requests
impl ActiveSegmentWriter {
    /// Creates a new ActiveSegmentWriter instance.
    ///
    /// Initializes a new writer with the specified configuration and sets up
    /// the worker pool for handling write requests.
    ///
    /// # Arguments
    /// * `notify_shutdown` - A broadcast channel sender for shutdown notifications
    /// * `worker_pool_config` - Optional configuration for the worker pool
    /// * `write_config` - Optional configuration for write operations
    ///
    /// # Returns
    /// * A new instance of ActiveSegmentWriter
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

    /// Opens a new log file for the specified topic partition.
    ///
    /// Creates and initializes a new segment log for the given topic partition
    /// with the specified base offset.
    ///
    /// # Arguments
    /// * `topic_partition` - The topic partition for which to open a new log file
    /// * `base_offset` - The base offset for the new segment
    ///
    /// # Returns
    /// * `io::Result<()>` - Success if the file was opened, error otherwise
    pub fn open_file(&self, topic_partition: &TopicPartition, base_offset: i64) -> io::Result<()> {
        let segment_log = SegmentLog::new(base_offset, topic_partition, &self.write_config);
        let tp_clone = topic_partition.clone();
        self.writers.insert(tp_clone, segment_log);
        Ok(())
    }

    /// Appends journal records to the log file.
    ///
    /// Asynchronously writes journal records to the appropriate segment log.
    /// The operation is handled by the worker pool to ensure efficient
    /// concurrent processing.
    ///
    /// # Arguments
    /// * `request` - The journal write request containing the records to append
    ///
    /// # Returns
    /// * `AppResult<()>` - Success if the records were appended, error otherwise
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

    /// Appends queue records to the log file.
    ///
    /// Asynchronously writes queue records to the appropriate segment log.
    /// Similar to append_journal, but specifically for queue records.
    ///
    /// # Arguments
    /// * `request` - The queue write request containing the records to append
    ///
    /// # Returns
    /// * `AppResult<()>` - Success if the records were appended, error otherwise
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

    /// Flushes the log file to disk.
    ///
    /// Forces any buffered data to be written to disk for the specified
    /// topic partition. Returns the number of bytes flushed.
    ///
    /// # Arguments
    /// * `request` - The flush request containing the topic partition to flush
    ///
    /// # Returns
    /// * `AppResult<u64>` - The size of the flushed data in bytes, or error if flush failed
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

    /// Calculates the channel ID for a given topic partition.
    ///
    /// Determines which worker pool channel should handle requests for a
    /// specific topic partition, ensuring even distribution of work.
    ///
    /// # Arguments
    /// * `topic_partition` - The topic partition to calculate the channel ID for
    ///
    /// # Returns
    /// * `i8` - The calculated channel ID
    fn channel_id(&self, topic_partition: &TopicPartition) -> i8 {
        (topic_partition.partition() % self.worker_pool.get_pool_config().num_channels as i32) as i8
    }

    /// Gets the current size of the active segment for a topic partition.
    ///
    /// Returns the total size of the active segment file for the specified
    /// topic partition.
    ///
    /// # Arguments
    /// * `topic_partition` - The topic partition to get the segment size for
    ///
    /// # Returns
    /// * `u64` - The size of the active segment in bytes
    pub fn active_segment_size(&self, topic_partition: &TopicPartition) -> u64 {
        self.writers.get(topic_partition).unwrap().file_size()
    }

    /// Gets the readable size of the segment for a topic partition.
    ///
    /// Returns the size of data that can be safely read from the segment
    /// for the specified topic partition.
    ///
    /// # Arguments
    /// * `topic_partition` - The topic partition to get the readable size for
    ///
    /// # Returns
    /// * `u64` - The readable size in bytes
    pub fn readable_size(&self, topic_partition: &TopicPartition) -> u64 {
        self.writers.get(topic_partition).unwrap().readable_size()
    }
}

/// Handler for processing file write requests in the worker pool.
///
/// Implements the actual processing of write requests, managing the interaction
/// with segment logs and ensuring thread-safe access to writers.
#[derive(Clone)]
struct FileWriteHandler {
    writers: Arc<DashMap<TopicPartition, SegmentLog>>,
}

impl PoolHandler<FileWriteRequest> for FileWriteHandler {
    /// Handles different types of file write requests.
    ///
    /// Processes three types of requests:
    /// * AppendJournal - Appends records to the journal log
    /// * AppendQueue - Appends records to the queue log
    /// * Flush - Flushes the log file to disk
    ///
    /// # Arguments
    /// * `request` - The file write request to handle
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
