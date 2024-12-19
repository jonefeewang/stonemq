use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Buf;

use crate::log::JournalLog;

use super::file_request::{JournalFileWriteReq, QueueFileWriteReq};

#[derive(Debug)]
pub struct FileInfo {
    path: PathBuf,
    size: AtomicU64,
}

impl FileInfo {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            size: AtomicU64::new(0),
        }
    }

    pub async fn write_journal(&self, request: JournalFileWriteReq) -> io::Result<()> {
        let msg = request.records.buffer.unwrap();
        let total_size = JournalLog::calculate_journal_log_overhead(&request.topic_partition)
            + msg.remaining() as u32;

        // 准备写入数据
        let mut buffer = Vec::with_capacity(total_size as usize);
        buffer.extend_from_slice(&total_size.to_be_bytes());
        buffer.extend_from_slice(&request.journal_offset.to_be_bytes());

        let tp_id = request.queue_topic_partition.id().to_string();
        let tp_id_bytes = tp_id.as_bytes();
        buffer.extend_from_slice(&(tp_id_bytes.len() as u32).to_be_bytes());
        buffer.extend_from_slice(tp_id_bytes);

        buffer.extend_from_slice(&request.first_batch_queue_base_offset.to_be_bytes());
        buffer.extend_from_slice(&request.last_batch_queue_base_offset.to_be_bytes());
        buffer.extend_from_slice(&request.records_count.to_be_bytes());
        buffer.extend_from_slice(msg.as_ref());

        let path = self.path.clone();

        let written_size = tokio::task::spawn_blocking(move || -> io::Result<u64> {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            file.write_all(&buffer)?;
            Ok(buffer.len() as u64)
        })
        .await??;

        self.size.fetch_add(written_size, Ordering::Release);
        Ok(())
    }

    pub async fn write_queue(&self, request: QueueFileWriteReq) -> io::Result<()> {
        let msg = request.records.buffer.unwrap();
        let total_write = msg.remaining();
        let path = self.path.clone();

        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            file.write_all(msg.as_ref())?;

            Ok(())
        })
        .await??;

        self.size.fetch_add(total_write as u64, Ordering::Release);
        Ok(())
    }

    pub async fn flush(&self) -> io::Result<u64> {
        let path = self.path.clone();

        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            file.sync_all()?;
            Ok(())
        })
        .await??;

        Ok(self.size.load(Ordering::Acquire))
    }

    pub fn get_size(&self) -> u64 {
        self.size.load(Ordering::Acquire)
    }
}
