use crate::log::FileOp;
use crate::message::MemoryRecords;
use crate::message::TopicPartition;
use crate::AppError::InvalidValue;
use crate::AppResult;
use bytes::Buf;
use crossbeam_utils::atomic::AtomicCell;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, trace};

#[derive(Debug)]
pub struct FileRecords {
    pub tx: Sender<FileOp>,
    size: Arc<AtomicCell<usize>>,
}

impl FileRecords {
    pub async fn open<P: AsRef<Path>>(file_name: P) -> AppResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file_name)
            .await?;

        let metadata = file.metadata().await?;
        let (tx, rx) = mpsc::channel(100);

        let file_records = Self {
            tx,
            size: Arc::new(AtomicCell::new(metadata.len() as usize)),
        };

        let buf_writer = BufWriter::new(file);
        let file_name = file_name.as_ref().to_string_lossy().into_owned();

        file_records.start_job_task(rx, buf_writer, file_records.size.clone(), file_name);

        Ok(file_records)
    }
    pub fn start_job_task(
        &self,
        mut rx: Receiver<FileOp>,
        mut buf_writer: BufWriter<File>,
        total_size: Arc<AtomicCell<usize>>,
        file_name: String,
    ) {
        tokio::spawn(async move {
            let writer = &mut buf_writer;
            while let Some(message) = rx.recv().await {
                match message {
                    FileOp::AppendRecords((topic_partition, records, resp_tx)) => {
                        match Self::append(writer, (topic_partition, records)).await {
                            Ok(total_write) => {
                                trace!("{} file append finished .", &file_name);
                                total_size.fetch_add(total_write);
                                resp_tx.send(Ok(())).unwrap_or_else(|_| {
                                    error!("send success  response error");
                                });
                            }
                            Err(error) => {
                                error!("append record error:{:?}", error);
                                resp_tx.send(Err(error)).unwrap_or_else(|_| {
                                    error!("send error response error");
                                });
                            }
                        }
                    }
                    FileOp::Flush(sender) => match writer.get_ref().sync_all().await {
                        Ok(_) => {
                            sender
                                .send(Ok(total_size.load() as u64))
                                .unwrap_or_else(|_| {
                                    error!("send flush success response error");
                                });
                            trace!("{} file flush finished .", &file_name);
                        }
                        Err(error) => {
                            error!("flush file error:{:?}", error);
                        }
                    },
                    FileOp::FetchRecords => {}
                }
            }
            trace!("{} file records append thread exit", &file_name)
        });
    }
    pub async fn stop_job_task(&self) {
        self.tx.closed().await;
    }

    pub async fn append(
        buf_writer: &mut BufWriter<File>,
        records: (TopicPartition, MemoryRecords),
    ) -> AppResult<usize> {
        trace!("append log to file ..");
        let topic_partition_id = records.0.id();
        let mut total_write = 0;
        buf_writer
            .write_u32(topic_partition_id.len() as u32)
            .await?;
        total_write += 4;
        let tp_id_bytes = topic_partition_id.as_bytes();
        buf_writer.write_all(tp_id_bytes).await?;
        total_write += tp_id_bytes.len();
        let msg = records.1.buffer.ok_or(InvalidValue(
            "empty message when append to file ",
            topic_partition_id,
        ))?;
        let msg_len = msg.remaining();
        buf_writer.write_all(msg.as_ref()).await?;
        total_write += msg_len;
        buf_writer.flush().await?;
        Ok(total_write)
    }
    pub fn size(&self) -> usize {
        self.size.load()
    }
}
