use crate::message::TopicPartition;
use crate::{AppError, AppResult};
use async_channel::{self, Receiver};
use dashmap::DashMap;
use std::io;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::error;

use super::file_request::{FlushRequest, JournalFileWriteReq, QueueFileWriteReq};
use super::{ActiveLogFileWriter, FileInfo, LogWriteRequest};
impl ActiveLogFileWriter {
    pub fn new() -> Self {
        let (tx, rx) = async_channel::bounded(1024);
        let writer = Self {
            writers: Arc::new(DashMap::new()),
            request_tx: tx,
        };
        writer.spawn_workers(rx);
        writer
    }
    pub async fn append_journal(&self, request: JournalFileWriteReq) -> AppResult<()> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(LogWriteRequest::AppendJournal { request, reply: tx })
            .await
            .map_err(|e| AppError::ChannelSendError(e.to_string()))?;
        rx.await
            .map_err(|e| AppError::ChannelRecvError(e.to_string()))??;
        Ok(())
    }

    pub async fn append_queue(&self, request: QueueFileWriteReq) -> AppResult<()> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(LogWriteRequest::AppendQueue { request, reply: tx })
            .await
            .map_err(|e| AppError::ChannelSendError(e.to_string()))?;
        rx.await
            .map_err(|e| AppError::ChannelRecvError(e.to_string()))??;
        Ok(())
    }

    pub async fn flush(&self, request: FlushRequest) -> AppResult<u64> {
        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(LogWriteRequest::Flush { request, reply: tx })
            .await
            .map_err(|e| AppError::ChannelSendError(e.to_string()))?;
        let size = rx
            .await
            .map_err(|e| AppError::ChannelRecvError(e.to_string()))??;
        Ok(size)
    }

    pub fn active_segment_size(&self, topic_partition: &TopicPartition) -> u64 {
        self.writers.get(topic_partition).unwrap().get_size()
    }

    fn spawn_workers(&self, rx: Receiver<LogWriteRequest>) {
        const WORKER_COUNT: usize = 10;
        let writers = self.writers.clone();

        for id in 0..WORKER_COUNT {
            let rx = rx.clone();
            let writers = writers.clone();

            tokio::spawn(async move {
                Self::worker_loop(id, rx, writers).await;
            });
        }
    }

    async fn worker_loop(
        id: usize,
        rx: Receiver<LogWriteRequest>,
        writers: Arc<DashMap<TopicPartition, FileInfo>>,
    ) {
        while let Ok(request) = rx.recv().await {
            if let Err(e) = Self::handle_request(request, &writers).await {
                error!("Worker {}: Error handling request: {}", id, e);
            }
        }
    }

    async fn handle_request(
        request: LogWriteRequest,
        writers: &DashMap<TopicPartition, FileInfo>,
    ) -> io::Result<()> {
        match request {
            LogWriteRequest::AppendJournal { request, reply } => {
                if let Some(writer) = writers.get(&request.topic_partition) {
                    let result = writer.write_journal(request).await;
                    let _ = reply.send(result.map_err(Into::into));
                }
            }
            LogWriteRequest::AppendQueue { request, reply } => {
                if let Some(writer) = writers.get(&request.topic_partition) {
                    let result = writer.write_queue(request).await;
                    let _ = reply.send(result.map_err(Into::into));
                }
            }
            LogWriteRequest::Flush { request, reply } => {
                if let Some(writer) = writers.get(&request.topic_partition) {
                    let result = writer.flush().await;
                    let _ = reply.send(result.map_err(Into::into));
                }
            }
        }
        Ok(())
    }

    pub fn open_file(&self, topic_partition: &TopicPartition, base_offset: i64) -> io::Result<()> {
        let file_path = format!("{}/{}.log", topic_partition.partition_dir(), base_offset);
        let file_info = FileInfo::new(file_path);
        self.writers.insert(topic_partition.clone(), file_info);
        Ok(())
    }
}
