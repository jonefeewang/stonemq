use crate::log::log_segment::PositionInfo;
use crate::log::FileOp;
use crate::message::MemoryRecords;
use crate::message::TopicPartition;
use crate::AppError::InvalidValue;
use crate::AppResult;
use bytes::{Buf, BytesMut};
use crossbeam_utils::atomic::AtomicCell;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, trace};

#[derive(Debug)]
pub struct FileRecords {
    pub tx: Sender<FileOp>,
    size: Arc<AtomicCell<usize>>,
    file_name: String,
}

/// Splitter的读取和消费的读取还不太一样
/// 1.Splitter的读取是一个读取者，而且连续的读取，所以针对一个journal log 最好每个splitter任务自己维护一个ReadBuffer
/// 而不需要通过FileRecord来读取，自己只要知道从哪个segment开始读取即可，然后读取到哪个位置，然后读取下一个segment
/// 2.消费的读取是多个并发读取者，而且不连续的数据.可以维护一个BufReader pool，然后读取者从pool中获取一个BufReader
/// .比如最热active segment, 可能有多个BufReader，而其他segment可能只有一个BufReader
impl FileRecords {
    // 未完成，这里应该是消费者读取的位置
    pub async fn read(&self, pos: usize, read_exact_byte: usize) -> AppResult<BytesMut> {
        let file = OpenOptions::new()
            .read(true)
            .open(&self.file_name)
            .await?;
        let mut buf = BytesMut::with_capacity(read_exact_byte);
        let mut reader = BufReader::new(file);
        reader.seek(std::io::SeekFrom::Start(pos as u64)).await?;
        reader.read(&mut buf).await?;
        todo!()
    }
    pub async fn open<P: AsRef<Path>>(file_name: P) -> AppResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file_name)
            .await?;

        let metadata = file.metadata().await?;
        let (tx, rx) = mpsc::channel(100);

        let file_name = file_name.as_ref().to_string_lossy().into_owned();
        let file_records = Self {
            tx,
            size: Arc::new(AtomicCell::new(metadata.len() as usize)),
            file_name,
        };


        file_records.start_job_task(file, rx, file_records.size.clone());

        Ok(file_records)
    }
    pub fn start_job_task(
        &self,
        file: File,
        mut rx: Receiver<FileOp>,
        total_size: Arc<AtomicCell<usize>>,
    ) {
        let file_name = self.file_name.clone();
        tokio::spawn(async move {
            let mut writer = BufWriter::new(file);
            while let Some(message) = rx.recv().await {
                match message {
                    FileOp::AppendRecords((topic_partition, records, resp_tx)) => {
                        match Self::append(&mut writer, (topic_partition, records)).await {
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
    /// 将文件指针移动到指定的offset位置
    pub async fn seek(mut file: File, target_offset: u64, ref_pos: &PositionInfo) -> AppResult<File> {
        file.seek(SeekFrom::Start(ref_pos.position as u64)).await?;
        let mut cur_offset = ref_pos.offset;
        if cur_offset == target_offset {
            return Ok(file);
        }
        loop {

            // skip current offset
            file.seek(SeekFrom::Current(8)).await?;
            let batch_size = file.read_u32().await?;
            // skip current batch
            file.seek(SeekFrom::Current(batch_size as i64)).await?;
            cur_offset = file.read_u64().await?;

            if cur_offset == target_offset {
                file.seek(SeekFrom::Current(-8)).await?;
                break;
            }
        }

        Ok(file)
    }
}
