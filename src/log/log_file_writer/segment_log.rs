use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Buf;

use crate::log::{JournalLog, LOG_FILE_SUFFIX};
use crate::message::TopicPartition;

use super::log_request::{JournalFileWriteReq, QueueFileWriteReq};
use super::WriteConfig;

#[derive(Debug)]
pub struct SegmentLog {
    path: PathBuf,
    size: AtomicU64,
    acc_buffer: WriteBuffer,
}

impl SegmentLog {
    pub fn new(
        base_offset: i64,
        topic_partition: &TopicPartition,
        write_config: &WriteConfig,
    ) -> Self {
        let path = format!(
            "{}/{}.{}",
            &topic_partition.partition_dir(),
            base_offset,
            LOG_FILE_SUFFIX
        );

        Self {
            path: path.into(),
            size: AtomicU64::new(0),
            acc_buffer: WriteBuffer::new(write_config),
        }
    }

    pub async fn write_journal(&mut self, request: JournalFileWriteReq) -> io::Result<()> {
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

        // 尝试写入，如果返回 Some 则需要刷盘
        if self.acc_buffer.try_write(&buffer) {
            // 异步执行刷盘操作
            let path = self.path.clone();
            let mut acc_buffer = self.acc_buffer.buffer.take().unwrap();
            let acc_buffer = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?;
                file.write_all(&acc_buffer)?;
                file.write_all(&buffer)?;
                acc_buffer.clear();
                Ok(acc_buffer)
            })
            .await??;
            self.acc_buffer.buffer = Some(acc_buffer);
        }
        self.size.fetch_add(total_size as u64, Ordering::Release);
        Ok(())
    }

    pub async fn write_queue(&mut self, request: QueueFileWriteReq) -> io::Result<()> {
        let msg = request.records.buffer.unwrap();
        let total_write = msg.remaining();

        if self.acc_buffer.try_write(msg.as_ref()) {
            let path = self.path.clone();
            let mut acc_buffer = self.acc_buffer.buffer.take().unwrap();
            let acc_buffer = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?;
                file.write_all(&acc_buffer)?;
                file.write_all(msg.as_ref())?;
                acc_buffer.clear();
                Ok(acc_buffer)
            })
            .await??;
            self.acc_buffer.buffer = Some(acc_buffer);
        }

        self.size.fetch_add(total_write as u64, Ordering::Release);
        Ok(())
    }

    pub async fn flush(&mut self) -> io::Result<u64> {
        let path = self.path.clone();

        let mut acc_buffer = self.acc_buffer.buffer.take().unwrap();
        let acc_buffer = tokio::task::spawn_blocking(move || -> io::Result<Vec<u8>> {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;

            file.write_all(&acc_buffer)?;
            file.sync_all()?;
            acc_buffer.clear();
            Ok(acc_buffer)
        })
        .await??;
        self.acc_buffer.buffer = Some(acc_buffer);

        Ok(self.size.load(Ordering::Acquire))
    }

    pub fn get_size(&self) -> u64 {
        self.size.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct WriteBuffer {
    buffer: Option<Vec<u8>>,
    last_flush: Instant,
    config: WriteConfig,
}

impl WriteBuffer {
    pub fn new(config: &WriteConfig) -> Self {
        Self {
            buffer: Some(Vec::with_capacity(config.buffer_capacity)),
            last_flush: Instant::now(),
            config: config.clone(),
        }
    }

    pub fn try_write(&mut self, data: &[u8]) -> bool {
        if self.should_flush(data.len()) {
            self.last_flush = Instant::now();
            self.buffer.as_mut().unwrap().extend_from_slice(data);
            true
        } else {
            self.buffer.as_mut().unwrap().extend_from_slice(data);
            false
        }
    }

    fn should_flush(&self, incoming_size: usize) -> bool {
        self.buffer.as_ref().unwrap().len() + incoming_size >= self.config.buffer_capacity
            || self.last_flush.elapsed() >= self.config.flush_interval
    }
}
