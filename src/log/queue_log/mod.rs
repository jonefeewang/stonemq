mod queue_load;
mod queue_read;
mod queue_write;

use crossbeam::atomic::AtomicCell;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::log::log_segment::LogSegment;
use crate::message::TopicPartition;
use crate::{AppError, AppResult};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::path::Path;

#[derive(Debug)]
pub struct QueueLog {
    pub segments: RwLock<BTreeMap<i64, LogSegment>>,
    pub topic_partition: TopicPartition,
    pub log_start_offset: i64,
    pub recover_point: AtomicCell<i64>,
    pub last_offset: AtomicCell<i64>,
    pub index_file_max_size: u32,
}

impl Hash for QueueLog {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic_partition.hash(state);
    }
}

impl PartialEq for QueueLog {
    fn eq(&self, other: &Self) -> bool {
        self.topic_partition == other.topic_partition
    }
}

impl Eq for QueueLog {}

impl QueueLog {
    /// Creates a new QueueLog instance.
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition for this log.
    /// * `segments` - A BTreeMap of existing log segments.
    /// * `log_start_offset` - The starting offset for this log.
    /// * `recovery_offset` - The recovery point offset.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the new `QueueLog` instance on success.
    pub async fn new(
        topic_partition: &TopicPartition,
        mut segments: BTreeMap<i64, LogSegment>,
        log_start_offset: i64,
        recovery_offset: i64,
        next_offset: i64,
        index_file_max_size: u32,
    ) -> AppResult<Self> {
        let dir = topic_partition.queue_partition_dir();

        if !Path::new(&dir).exists() {
            info!("log dir does not exists, create queue log dir:{}", dir);
            std::fs::create_dir_all(&dir).map_err(|e| {
                AppError::DetailedIoError(format!("create queue log dir: {} error: {}", dir, e))
            })?;
        }

        if segments.is_empty() {
            warn!("no segment file found in queue log dir:{}", dir);
            let segment = LogSegment::new(topic_partition, &dir, 0, index_file_max_size).await?;
            segments.insert(0, segment);
        }

        Ok(QueueLog {
            topic_partition: topic_partition.clone(),
            segments: RwLock::new(segments),
            log_start_offset,
            recover_point: AtomicCell::new(recovery_offset),
            last_offset: AtomicCell::new(next_offset),
            index_file_max_size,
        })
    }
}
