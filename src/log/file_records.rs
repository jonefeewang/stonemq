use super::LogType;
use crate::log::journal_log::JournalLog;
use crate::log::log_segment::PositionInfo;
use crate::log::FileOp;
use crate::message::MemoryRecords;
use crate::message::TopicPartition;
use crate::AppError;
use crate::AppResult;

use bytes::Buf;
use crossbeam::atomic::AtomicCell;
use std::io::{Error, ErrorKind, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Duration;
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
            .append(true)
            .write(true)
            .open(&file_name)
            .await
            .map_err(|e| {
                AppError::DetailedIoError(format!(
                    "open file: {} error: {} while open file records",
                    file_name.as_ref().to_string_lossy(),
                    e
                ))
            })?;

        let metadata = file.metadata().await.map_err(|e| {
            AppError::DetailedIoError(format!(
                "get file: {} metadata error: {} while open file records",
                file_name.as_ref().to_string_lossy(),
                e
            ))
        })?;
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
        size: Arc<AtomicCell<usize>>,
    ) {
        let file_name = self.file_name.clone();
        tokio::spawn(async move {
            let mut writer = BufWriter::new(file);
            while let Some(message) = rx.recv().await {
                match message {
                    FileOp::AppendJournal((
                                              journal_offset,
                                              topic_partition,
                                              first_batch_queue_base_offset,
                                              last_batch_queue_base_offset,
                                              records_count,
                                              records,
                                              resp_tx,
                                          )) => {
                        match Self::append_journal_recordbatch(
                            &mut writer,
                            (
                                journal_offset,
                                topic_partition,
                                first_batch_queue_base_offset,
                                last_batch_queue_base_offset,
                                records_count,
                                records,
                            ),
                        )
                            .await
                        {
                            Ok(total_write) => {
                                trace!("{} file append finished .", &file_name);
                                size.fetch_add(total_write);
                                resp_tx.send(Ok(())).unwrap_or_else(|_| {
                                    error!("send success  response error");
                                });
                            }
                            Err(error) => {
                                error!("append record error:{:?}", error);
                                resp_tx
                                    .send(Err(AppError::DetailedIoError(format!(
                                        "append record error:{:?}",
                                        error
                                    ))))
                                    .unwrap_or_else(|_| {
                                        error!("send error response error");
                                    });
                            }
                        }
                    }
                    FileOp::AppendQueue((
                                            journal_offset,
                                            topic_partition,
                                            first_batch_queue_base_offset,
                                            last_batch_queue_base_offset,
                                            records_count,
                                            records,
                                            resp_tx,
                                        )) => {
                        match Self::append_queue_recordbatch(
                            &mut writer,
                            (
                                journal_offset,
                                topic_partition,
                                first_batch_queue_base_offset,
                                last_batch_queue_base_offset,
                                records_count,
                                records,
                            ),
                        )
                            .await
                        {
                            Ok(total_write) => {
                                trace!("{} file append finished .", &file_name);
                                size.fetch_add(total_write);
                                resp_tx.send(Ok(())).unwrap_or_else(|_| {
                                    error!("send success  response error");
                                });
                            }
                            Err(error) => {
                                error!("append record error:{:?}", error);
                                resp_tx
                                    .send(Err(AppError::DetailedIoError(format!(
                                        "append record error:{:?}",
                                        error
                                    ))))
                                    .unwrap_or_else(|_| {
                                        error!("send error response error");
                                    });
                            }
                        }
                    }

                    FileOp::Flush(sender) => match writer.get_ref().sync_all().await {
                        Ok(_) => {
                            sender.send(Ok(size.load() as u64)).unwrap_or_else(|_| {
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
    /// * `first_batch_queue_base_offset` - 第一个批次的偏移量
    /// * `last_batch_queue_base_offset` - 最后一个批次的偏移量
    /// * `records_count` - 记录的个数
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
        (
            journal_offset,
            topic_partition,
            first_batch_queue_base_offset,
            last_batch_queue_base_offset,
            records_count,
            records,
        ): (i64, TopicPartition, i64, i64, u32, MemoryRecords),
    ) -> std::io::Result<usize> {
        trace!("正在将日志追加到文件...");

        let topic_partition_id = topic_partition.id();
        let tp_id_bytes = topic_partition_id.as_bytes();

        let msg = records.buffer.unwrap();

        let total_size =
            JournalLog::calculate_journal_log_overhead(&topic_partition) + msg.remaining() as u32;

        buf_writer.write_u32(total_size).await?;
        buf_writer.write_i64(journal_offset).await?;
        buf_writer.write_u32(tp_id_bytes.len() as u32).await?;
        buf_writer.write_all(tp_id_bytes).await?;
        buf_writer.write_i64(first_batch_queue_base_offset).await?;
        buf_writer.write_i64(last_batch_queue_base_offset).await?;
        buf_writer.write_u32(records_count).await?;
        buf_writer.write_all(msg.as_ref()).await?;
        buf_writer.flush().await?;

        // total write size = total_size + 4
        Ok(total_size as usize + 4)
    }

    pub async fn append_queue_recordbatch(
        buf_writer: &mut BufWriter<File>,
        (_, _, _, _, _, records): (i64, TopicPartition, i64, i64, u32, MemoryRecords),
    ) -> std::io::Result<usize> {
        let total_write = records.size();

        let msg = records.buffer.unwrap();
        buf_writer.write_all(msg.as_ref()).await?;
        buf_writer.flush().await?;
        Ok(total_write)
    }

    pub fn size(&self) -> usize {
        self.size.load()
    }
    /// 通过index 文件找目标offset所在的segment，然后移动文件指针到segment的offset位置
    /// 如果报错，很有可能是当前内容还未落盘，所以读取不到，可以sleep一会再读取
    pub async fn seek(
        mut file: File,
        target_offset: i64,
        ref_pos: PositionInfo,
        log_type: LogType,
    ) -> std::io::Result<(File, PositionInfo)> {
        let PositionInfo {
            offset, position, ..
        } = ref_pos;

        if offset == target_offset {
            file.seek(SeekFrom::Start(position as u64)).await?;
            return Ok((file, ref_pos));
        }

        const MAX_RETRIES: u32 = 5;
        const RETRY_DELAY: Duration = Duration::from_millis(100);

        for retry in 0..MAX_RETRIES {
            match file.seek(SeekFrom::Start(position as u64)).await {
                Ok(_) => match log_type {
                    LogType::Journal => {
                        match Self::seek_to_target_offset_journal(&mut file, target_offset).await {
                            Ok(position) => {
                                let mut new_pos = ref_pos;
                                new_pos.position = position as i64;
                                new_pos.offset = target_offset;
                                return Ok((file, new_pos));
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
                                    let mut new_pos = ref_pos;
                                    new_pos.position = position;
                                    new_pos.offset = target_offset;
                                    return Ok((file, new_pos));
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
    ) -> std::io::Result<u64> {
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
                    return Ok(current_position);
                }
                // 当前offset大于目标offset，则返回-1，表示找不到,splitter 在读取下一个offset时，这个offset还未生产出来，或者还未落盘
                std::cmp::Ordering::Greater => {
                    return Err(Error::new(ErrorKind::NotFound, "目标偏移量未找到"))
                }
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
            let length = i32::from_be_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]);
            let current_offset = i64::from_be_bytes([
                buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6],
                buffer[7],
            ]);

            match current_offset.cmp(&target_offset) {
                std::cmp::Ordering::Equal => {
                    let current_position = file.seek(SeekFrom::Current(-12)).await?;
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use tempfile::tempdir;
    use tokio::fs::File;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_seek_journal() -> std::io::Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.log");
        let mut file = File::create(&file_path).await?;

        // 写入3条测试记录
        // 记录1: batch_size=20, offset=1
        file.write_all(&[0, 0, 0, 20]).await?; // batch size
        file.write_all(&[0, 0, 0, 0, 0, 0, 0, 1]).await?; // offset
        file.write_all(&[1; 20]).await?; // data

        // 记录2: batch_size=30, offset=2
        file.write_all(&[0, 0, 0, 30]).await?;
        file.write_all(&[0, 0, 0, 0, 0, 0, 0, 2]).await?;
        file.write_all(&[2; 30]).await?;

        // 记录3: batch_size=40, offset=3
        file.write_all(&[0, 0, 0, 40]).await?;
        file.write_all(&[0, 0, 0, 0, 0, 0, 0, 3]).await?;
        file.write_all(&[3; 40]).await?;

        let mut file = File::open(&file_path).await?;

        // 测试查找存在的offset
        let position = FileRecords::seek_to_target_offset_journal(&mut file, 2).await?;
        assert_eq!(position, 32);

        // 测试查找不存在的offset
        let result = FileRecords::seek_to_target_offset_journal(&mut file, 4).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::NotFound);

        Ok(())
    }

    #[tokio::test]
    async fn test_seek_queue() -> std::io::Result<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test.log");
        let mut file = File::create(&file_path).await?;

        // 写入3条测试记录
        // 记录1: offset=1, length=20
        file.write_all(&[0, 0, 0, 0, 0, 0, 0, 1]).await?; // offset
        file.write_all(&[0, 0, 0, 20]).await?; // length
        file.write_all(&[1; 20]).await?; // data

        // 记录2: offset=2, length=30
        file.write_all(&[0, 0, 0, 0, 0, 0, 0, 2]).await?;
        file.write_all(&[0, 0, 0, 30]).await?;
        file.write_all(&[2; 30]).await?;

        // 记录3: offset=3, length=40
        file.write_all(&[0, 0, 0, 0, 0, 0, 0, 3]).await?;
        file.write_all(&[0, 0, 0, 40]).await?;
        file.write_all(&[3; 40]).await?;

        let mut file = File::open(&file_path).await?;

        // 测试查找存在的offset
        let position = FileRecords::seek_to_target_offset_queue(&mut file, 2).await?;
        assert_eq!(position, 32);

        // 测试查找不存在的offset
        let position = FileRecords::seek_to_target_offset_queue(&mut file, 4).await?;
        assert_eq!(position, -1);

        Ok(())
    }

    #[tokio::test]
    async fn test_seek_queue_seg() -> std::io::Result<()> {
        let file_path =
            PathBuf::from("/Users/wangjunfei/projects/stonemq/test_data/queue/topic_a-0/0.log");
        let file = File::open(&file_path).await?;

        // 测试查找存在的offset
        let ref_pos = PositionInfo {
            base_offset: 0,
            offset: 164,
            position: 1883,
        };
        let (_, position) = FileRecords::seek(file, 200, ref_pos, LogType::Queue).await?;
        assert_eq!(position.position, 2268);

        Ok(())
    }
    #[tokio::test]
    async fn test_seek_journal_seg() -> std::io::Result<()> {
        let file_path =
            PathBuf::from("/Users/wangjunfei/projects/stonemq/test_data/journal/journal-0/0.log");
        let file = File::open(&file_path).await?;

        // 测试查找存在的offset
        let ref_pos = PositionInfo {
            base_offset: 0,
            offset: 3,
            position: 882,
        };
        let (_, position) = FileRecords::seek(file, 4, ref_pos, LogType::Journal).await?;
        assert_eq!(position.position, 1000);

        Ok(())
    }
}
