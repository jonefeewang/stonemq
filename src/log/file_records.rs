use crate::log::calculate_journal_log_overhead;

use super::LogType;
use crate::log::log_segment::PositionInfo;
use crate::log::FileOp;
use crate::message::MemoryRecords;
use crate::message::TopicPartition;
use crate::AppError::InvalidValue;
use crate::AppResult;
use bytes::buf;
use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use crossbeam_utils::atomic::AtomicCell;
use std::io::{Error, ErrorKind, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Duration;
use tokio_util::codec::length_delimited;
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
    // pub async fn read(&self, pos: usize, read_exact_byte: usize) -> AppResult<BytesMut> {
    //     let file = OpenOptions::new()
    //         .read(true)
    //         .open(&self.file_name)
    //         .await?;
    //     let mut buf = BytesMut::with_capacity(read_exact_byte);
    //     let mut reader = BufReader::new(file);
    //     reader.seek(std::io::SeekFrom::Start(pos as u64)).await?;
    //     reader.read(&mut buf).await?;
    //     todo!()
    // }
    pub async fn open<P: AsRef<Path>>(file_name: P) -> AppResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
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
        // 这里使用消息通知模式，是为了避免使用锁

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
                    FileOp::AppendJournal((offset, topic_partition, records, resp_tx)) => {
                        match Self::append_journal_recordbatch(
                            &mut writer,
                            (offset, topic_partition, records),
                        )
                        .await
                        {
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
                    FileOp::AppendQueue((offset, topic_partition, records, resp_tx)) => {
                        match Self::append_queue_recordbatch(
                            &mut writer,
                            (offset, topic_partition, records),
                        )
                        .await
                        {
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

    /// 将日志记录批次追加到日志文件中
    ///
    /// 日志记录格式：
    /// 4字节：total_size
    /// 8字节：offset
    /// 4字节：topic_partition 字符串长度
    /// 字节：topic_partition 字符串
    /// 剩余字节：消息内容(MemoryRecords)
    ///
    /// # 参数
    ///
    /// * `buf_writer` - 文件的缓冲写入器
    /// * `offset` - 日志记录的偏移量
    /// * `topic_partition` - 主题分区信息
    /// * `records` - 要追加的内存记录
    ///
    /// # 返回值
    ///
    /// 返回写入的总字节数，如果成功的话
    ///
    /// # 错误
    ///
    /// 如果写入过程中发生错误，将返回一个 `AppError`
    pub async fn append_journal_recordbatch(
        buf_writer: &mut BufWriter<File>,
        (offset, topic_partition, records): (i64, TopicPartition, MemoryRecords),
    ) -> AppResult<usize> {
        trace!("正在将日志追加到文件...");

        let topic_partition_id = topic_partition.id();
        let tp_id_bytes = topic_partition_id.as_bytes();

        let msg = records
            .buffer
            .ok_or_else(|| InvalidValue("追加到文件时消息为空", topic_partition_id.to_string()))?;

        let total_size = calculate_journal_log_overhead(&topic_partition) + msg.remaining() as u32;

        buf_writer.write_u32(total_size).await?;
        buf_writer.write_i64(offset).await?;
        buf_writer.write_u32(tp_id_bytes.len() as u32).await?;
        buf_writer.write_all(tp_id_bytes).await?;
        buf_writer.write_all(msg.as_ref()).await?;
        buf_writer.flush().await?;

        Ok(total_size as usize)
    }

    pub async fn append_queue_recordbatch(
        buf_writer: &mut BufWriter<File>,
        (_, topic_partition, records): (i64, TopicPartition, MemoryRecords),
    ) -> AppResult<usize> {
        let topic_partition_id = topic_partition.id();
        let total_write = records.size();

        let msg = records
            .buffer
            .ok_or_else(|| InvalidValue("追加到文件时消息为空", topic_partition_id.to_string()))?;
        buf_writer.write_all(msg.as_ref()).await?;
        buf_writer.flush().await?;
        Ok(total_write)
    }

    pub fn size(&self) -> usize {
        self.size.load()
    }
    /// 将文件指针移动到指定的offset位置
    /// 如果报错，很有可能是当前内容还未落盘，所以读取不到，可以sleep一会再读取
    pub async fn seek(
        mut file: File,
        target_offset: i64,
        ref_pos: &PositionInfo,
        log_type: LogType,
    ) -> std::io::Result<(File, i64)> {
        let PositionInfo {
            offset, position, ..
        } = ref_pos;

        if *offset == target_offset {
            file.seek(SeekFrom::Start(*position as u64)).await?;
            return Ok((file, *position as i64));
        }

        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY: Duration = Duration::from_millis(100);

        for retry in 0..MAX_RETRIES {
            match file.seek(SeekFrom::Start(*position as u64)).await {
                Ok(_) => match log_type {
                    LogType::Journal => {
                        match Self::seek_to_target_offset_journal(&mut file, target_offset).await {
                            Ok(position) => {
                                if position > 0 {
                                    return Ok((file, position));
                                } else {
                                    //当前offset已经大于目标offset，则退出，（不应该出现的情况）
                                    error!("查找offset,当前offset已经大于目标offset,不应该出现的情况，退出");
                                    break;
                                }
                            }
                            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                                if retry == MAX_RETRIES - 1 {
                                    return Err(e);
                                }
                                tokio::time::sleep(RETRY_DELAY).await;
                                continue;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    LogType::Queue => {
                        match Self::seek_to_target_offset_queue(&mut file, target_offset).await {
                            Ok(position) => {
                                if position > 0 {
                                    return Ok((file, position));
                                } else {
                                    //当前offset已经大于目标offset，则退出，（不应该出现的情况）
                                    error!("查找offset,当前offset已经大于目标offset,不应该出现的情况，退出");
                                    break;
                                }
                            }
                            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                                if retry == MAX_RETRIES - 1 {
                                    return Err(e);
                                }
                                tokio::time::sleep(RETRY_DELAY).await;
                                continue;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                },

                Err(e) => {
                    if retry == MAX_RETRIES - 1 {
                        return Err(e);
                    }
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }
            }
        }
        Err(Error::new(
            ErrorKind::NotFound,
            format!("目标偏移量 {} 未找到", target_offset),
        ))
    }

    async fn seek_to_target_offset_journal(
        file: &mut File,
        target_offset: i64,
    ) -> std::io::Result<i64> {
        let mut buffer = [0u8; 12];

        loop {
            file.read_exact(&mut buffer).await?;
            let batch_size = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
            let current_offset = i64::from_be_bytes([
                buffer[4], buffer[5], buffer[6], buffer[7], buffer[8], buffer[9], buffer[10],
                buffer[11],
            ]);

            match current_offset.cmp(&target_offset) {
                std::cmp::Ordering::Equal => {
                    let current_position = file.seek(SeekFrom::Current(-12)).await?;
                    return Ok(current_position as i64);
                }
                std::cmp::Ordering::Greater => return Ok(-1),
                std::cmp::Ordering::Less => {
                    file.seek(SeekFrom::Current(batch_size as i64 - 12)).await?;
                }
            }
        }
    }
    async fn seek_to_target_offset_queue(
        file: &mut File,
        target_offset: i64,
    ) -> std::io::Result<i64> {
        let mut buffer = [0u8; 12];

        loop {
            file.read_exact(&mut buffer).await?;
            let length = u32::from_be_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]);
            let current_offset = i64::from_be_bytes([
                buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6],
                buffer[7],
            ]);

            match current_offset.cmp(&target_offset) {
                std::cmp::Ordering::Equal => {
                    let current_position = file.seek(SeekFrom::Current(-8)).await?;
                    return Ok(current_position as i64);
                }
                std::cmp::Ordering::Greater => return Ok(-1),
                std::cmp::Ordering::Less => {
                    file.seek(SeekFrom::Current(length as i64)).await?;
                }
            }
        }
    }
}
