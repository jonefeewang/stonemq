use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};

use tokio::fs;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tracing::{info, warn};

use crate::{AppResult, BROKER_CONFIG};
use crate::AppError::{IllegalStateError, InvalidValue};
use crate::message::MemoryRecords;
use crate::topic_partition::TopicPartition;

pub enum LogType {
    Journal,
    Queue,
}
struct TimeIndex {}
struct OffsetIndex {}

/// 每个Log代表一个topic-partition的目录及下边的日志文件
/// 日志文件以segment为单位，每个segment文件大小固定

pub struct Log {
    pub segments: BTreeMap<i64, LogSegment>,
    pub dir: String,
    pub log_start_offset: i64,
    pub recover_point: i64,
}
pub struct FileRecords {
    buf_reader: BufReader<File>,
    buf_writer: BufWriter<File>,
}
#[derive(Default)]
pub struct LogManager {
    journal_logs: HashMap<TopicPartition, Log>,
    queue_logs: HashMap<TopicPartition, Log>,
}
struct LogSegment {
    log: FileRecords,
    base_offset: i64,
    time_index: Option<TimeIndex>,
    offset_index: Option<OffsetIndex>,
}
impl FileRecords {
    pub async fn open(file_name: String, create: bool) -> AppResult<FileRecords> {
        let read_file = OpenOptions::new()
            .read(true)
            .create(create)
            .open(&file_name)
            .await?;
        let write_file = OpenOptions::new().write(true).open(&file_name).await?;
        let file_records = FileRecords {
            buf_reader: BufReader::new(read_file),
            buf_writer: BufWriter::new(write_file),
        };
        Ok(file_records)
    }

    pub async fn append(
        &mut self,
        topic_partition: TopicPartition,
        records: MemoryRecords,
    ) -> AppResult<()> {
        let topic_partition_id = topic_partition.string_id();
        self.buf_writer
            .write_u32(topic_partition_id.len() as u32)
            .await?;
        self.buf_writer
            .write_all(topic_partition_id.as_bytes())
            .await?;
        self.buf_writer
            .write_all(
                records
                    .buffer
                    .ok_or(InvalidValue(
                        "invalid message when append to file ",
                        topic_partition_id,
                    ))?
                    .as_ref(),
            )
            .await?;
        self.buf_writer.flush().await?;
        Ok(())
    }
}

impl LogManager {
    pub async fn startup(&self) -> AppResult<()> {
        todo!()
    }
    pub async fn load_logs(&mut self, log_dir: &String, log_type: &LogType) -> AppResult<()> {
        if !fs::metadata(log_dir)
            .await
            .map(|meta| meta.is_dir())
            .unwrap_or(false)
        {
            fs::create_dir_all(log_dir).await?;
            info!("create log directory path: {}", log_dir);
        }

        while let Some(dir) = fs::read_dir(log_dir).await?.next_entry().await? {
            if dir.metadata().await?.file_type().is_dir() {
                info!("load log from disk, found dir:{:?}", dir.file_name());
                let topic_partition =
                    TopicPartition::from_string(dir.file_name().to_string_lossy())?;
                self.get_or_create_log(&topic_partition, log_type).await?;
            }
        }
        Ok(())
    }
    pub async fn get_or_create_log(
        &mut self,
        topic_partition: &TopicPartition,
        log_type: &LogType,
    ) -> AppResult<&Log> {
        let log_config = &BROKER_CONFIG.get().unwrap().log_config;
        let base_dir = match log_type {
            LogType::Journal => &log_config.journal_base_dir,
            LogType::Queue => &log_config.queue_base_dir,
        };
        let logs = match log_type {
            LogType::Journal => &mut self.journal_logs,
            LogType::Queue => &mut self.queue_logs,
        };
        let partition_path = format!("{}/{}", base_dir, topic_partition.string_id());
        // 判断目录是否存在
        if !fs::metadata(&partition_path)
            .await
            .map(|meta| meta.is_dir())
            .unwrap_or(false)
        {
            fs::create_dir_all(&partition_path).await?;
            info!("create partition path: {}", partition_path);
        }
        let log = Log::new(Cow::Owned(partition_path), 0, 0).await?;
        if logs.insert(topic_partition.clone(), log).is_some() {
            warn!("log already exists, partition:{}", topic_partition);
            return Err(IllegalStateError(Cow::Owned(format!(
                "topic partition log dir duplicated:{} ",
                topic_partition.string_id()
            ))));
        } else {
            info!("load log from disk, partition:{}", topic_partition);
        }
        Ok(logs.get(topic_partition).unwrap())
    }
}

impl Log {
    pub async fn new(
        dir: Cow<'static, str>,
        log_start_offset: i64,
        log_recovery_point: i64,
    ) -> AppResult<Self> {
        let mut log = Log {
            segments: BTreeMap::new(),
            dir: dir.to_string(),
            log_start_offset,
            recover_point: log_recovery_point,
        };
        log.load_segments().await?;
        Ok(log)
    }
    ///
    /// 1. validate memory records
    /// 2. assign offset to records
    /// 2. may be rolled the segment file
    /// 3. append to active segment file
    pub async fn append_records(
        &mut self,
        topic_partition: TopicPartition,
        records: MemoryRecords,
    ) -> AppResult<()> {
        todo!()
    }
    pub async fn load_segments(&mut self) -> AppResult<()> {
        while let Some(file) = fs::read_dir(&self.dir).await?.next_entry().await? {
            if file.metadata().await?.file_type().is_file() {
                let file_name_osstr = file.file_name();
                let file_name = file_name_osstr.to_str().ok_or(InvalidValue(
                    "segment file name",
                    file_name_osstr.to_string_lossy().to_string(),
                ))?;
                info!("load segment :{:?}", file_name);
                let dot_index = file_name
                    .rfind('.')
                    .ok_or(InvalidValue("segment file name", file_name.to_string()))?;
                let start_offset: i64 = file_name[0..dot_index]
                    .parse()
                    .map_err(|_| InvalidValue("segment file name", file_name.to_string()))?;
                let segment = LogSegment::new_journal_seg(start_offset).await?;
                self.segments.insert(start_offset, segment);
            }
        }

        Ok(())
    }
}

impl LogSegment {
    pub async fn new_journal_seg(base_offset: i64) -> AppResult<Self> {
        let log_config = &BROKER_CONFIG.get().unwrap().log_config;
        let file_name = format!("{}/{}.log", log_config.journal_base_dir, base_offset);
        let file_records = FileRecords::open(file_name, false).await?;
        Ok(LogSegment {
            log: file_records,
            base_offset,
            time_index: None,
            offset_index: None,
        })
    }
}
