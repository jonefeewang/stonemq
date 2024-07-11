use std::any::type_name;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::fs as sync_fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Buf;
use crossbeam_utils::atomic::AtomicCell;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use tokio::fs;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Interval;
use tracing::{debug, error, info, trace, warn};

use crate::{AppError, AppResult, BROKER_CONFIG};
use crate::AppError::{IllegalStateError, InvalidValue};
use crate::checkpoint::CheckPointFile;
use crate::message::MemoryRecords;
use crate::topic_partition::{LogAppendInfo, TopicPartition};

#[derive(Debug)]
struct TimeIndex {}
#[derive(Debug)]
struct OffsetIndex {}

pub(crate) trait Log: Debug {
    async fn append_records(
        &self,
        records: (TopicPartition, MemoryRecords),
    ) -> AppResult<LogAppendInfo>;
    fn load_segments(dir: &str, rt: &Runtime) -> AppResult<BTreeMap<u64, LogSegment>>;
    async fn new(
        dir: String,
        segments: BTreeMap<u64, LogSegment>,
        log_start_offset: u64,
        log_recovery_point: u64,
    ) -> AppResult<Self>
    where
        Self: Sized;
    async fn flush(&self) -> AppResult<()>;
    fn no_active_segment_error(&self, dir: String) -> AppError {
        IllegalStateError(Cow::Owned(format!("no active segment found log:{}", dir,)))
    }
}

/// 每个Log代表一个topic-partition的目录及下边的日志文件
/// 日志文件以segment为单位，每个segment文件大小固定
/// 这里使用tokio的读写锁来保护并发读写：
/// 1. 写场景：roll log或delete segment时，需要修改segments，需要写锁，频率相对较低
/// 2. append message, 读取message时， 都使用的是读锁，频率相对较高
#[derive(Debug)]
pub struct QueueLog {
    pub segments: RwLock<BTreeMap<u64, LogSegment>>,
    pub dir: String,
    pub log_start_offset: u64,
    pub recover_point: u64,
}
/// 1. 写场景：roll log或delete segment时，需要修改segments，需要写锁，频率相对较低
/// 2. append message, 读取message时， 都使用的是读锁，频率相对较高
#[derive(Debug)]
pub struct JournalLog {
    pub segments: RwLock<BTreeMap<u64, LogSegment>>,
    // queue log的topicPartition对应的下一个offset值
    pub next_offset_info: DashMap<TopicPartition, i64>,
    pub dir: String,
    pub log_start_offset: u64,
    pub recover_point: AtomicCell<u64>,
}
pub enum LogMessage {
    AppendRecords(
        (
            TopicPartition,
            MemoryRecords,
            oneshot::Sender<AppResult<()>>,
        ),
    ),
    FetchRecords,
    Flush(oneshot::Sender<AppResult<(u64)>>),
}
impl CheckPointFile for LogManager {}
impl Log for JournalLog {
    ///
    /// 1. validate memory records
    /// 2. assign offset to records
    /// 2. may be rolled the segment file
    /// 3. append to active segment file
    /// 注意：这里的records(TopicPartition, MemoryRecords)是queue log的topicPartition
    async fn append_records(
        &self,
        records: (TopicPartition, MemoryRecords),
    ) -> AppResult<LogAppendInfo> {
        let (topic_partition, mut memory_records) = records;
        // assign offset for the record batch, configure the base offset,
        // set the maximum timestamp, and retain the offset of the maximum timestamp.
        // for the mvp version, we just assume the last record of the batch has the max timestamp
        let mut next_offset_value = 0i64;
        let batch_count = memory_records.records_count();
        // 修改next offset值，快速获取和释放锁
        {
            let mut next_offset = self
                .next_offset_info
                .entry(topic_partition.clone())
                .or_insert(0);
            *next_offset += batch_count as i64;
            next_offset_value = *next_offset;
        }
        // assign offset
        memory_records.set_base_offset(next_offset_value)?;
        let max_timestamp = memory_records.get_max_timestamp();
        let max_timestamp_offset = next_offset_value - 1;

        let mut active_seg_size = 0u64;
        let mut active_base_offset = 0u64;
        {
            // get active segment info
            let segments = self.segments.read().await;
            let last_seg = segments
                .iter()
                .next_back()
                .ok_or(self.no_active_segment_error(self.dir.clone()))?;
            active_seg_size = last_seg.1.size() as u64;
            active_base_offset = *last_seg.0;
            // 使用这个作用域释放读锁
        }

        let buffer = memory_records.buffer.as_ref().ok_or(InvalidValue(
            "empty message when append to file",
            topic_partition.id(),
        ))?;
        let msg_len = buffer.remaining();

        // check active segment size, if exceed the segment size, roll the segment
        if active_seg_size + msg_len as u64 >= BROKER_CONFIG.get().unwrap().log.journal_segment_size
        {
            // 确定需要滚动日之后，进入新的作用域，获取写锁，准备滚动日志
            let mut segments = self.segments.write().await;
            let (write_active_base, write_active_seg) = segments
                .iter()
                .next_back()
                .ok_or(self.no_active_segment_error(self.dir.clone()))?;
            // check again to make sure other request has not rolled it already
            if *write_active_base == active_base_offset {
                // other request has not rolled it yet
                let new_base_offset = write_active_seg.size() as u64;
                self.recover_point.store(new_base_offset);
                write_active_seg.flush().await?;
                write_active_seg.close_write().await;
                // create new segment file
                let new_seg =
                    LogSegment::new_queue_seg(self.dir.clone(), new_base_offset, true).await?;
                segments.insert(new_base_offset, new_seg);
                trace!(
                    "roll journal log segment to new base offset:{}",
                    new_base_offset
                );
            }
        }

        let segments = self.segments.read().await;
        let (_, active_seg) = segments
            .iter()
            .next_back()
            .ok_or(self.no_active_segment_error(self.dir.clone()))?;
        let (tx, rx) = oneshot::channel::<AppResult<()>>();

        active_seg
            .append_record((topic_partition, memory_records, tx))
            .await?;
        rx.await??;
        trace!(
            "insert {} records to journal log with offset {}",
            batch_count,
            next_offset_value
        );
        Ok(LogAppendInfo {
            base_offset: next_offset_value,
            log_append_time: 0,
        })
    }
    /// 加载`dir`下的所有segment文件
    /// journal log没有index文件
    /// queue log有time index和offset index文件
    fn load_segments(dir: &str, rt: &Runtime) -> AppResult<BTreeMap<u64, LogSegment>> {
        let mut segments = BTreeMap::new();
        info!("load segment files from dir:{}", dir);

        if sync_fs::read_dir(dir)?.next().is_none() {
            info!("journal log directory is empty: {}", dir);
            return Ok(segments);
        }
        let mut read_dir = sync_fs::read_dir(dir)?;
        while let Some(file) = read_dir.next().transpose()? {
            if file.metadata()?.file_type().is_file() {
                let file_name = file.file_name().to_string_lossy().to_string();
                let dot_index = file_name.rfind('.');
                match dot_index {
                    None => {
                        warn!("invalid segment file name:{}", file_name);
                        continue;
                    }
                    Some(dot_index) => {
                        let file_prefix = &file_name[..dot_index];
                        let file_suffix = &file_name[dot_index..];
                        match file_suffix {
                            ".timeindex" => {
                                warn!("journal log should not have time index file");
                                continue;
                            }
                            ".index" => {
                                warn!("journal log should not have offset index file");
                                continue;
                            }
                            ".log" => {
                                let base_offset = file_prefix.parse::<u64>();
                                match base_offset {
                                    Ok(base_offset) => {
                                        let segment = rt.block_on(LogSegment::new_journal_seg(
                                            dir.to_string(),
                                            0,
                                            false,
                                        ))?;
                                        segments.insert(base_offset, segment);
                                    }
                                    Err(_) => {
                                        warn!("invalid segment file name:{}", file_prefix);
                                        continue;
                                    }
                                }
                            }
                            other => {
                                warn!("invalid segment file name:{}", other);
                                continue;
                            }
                        }
                    }
                }
            }
        }
        // 初始化

        Ok(segments)
    }

    async fn new(
        dir: String,
        mut segments: BTreeMap<u64, LogSegment>,
        log_start_offset: u64,
        log_recovery_point: u64,
    ) -> AppResult<Self> {
        // 如果segments是空的, 默认创建一个
        // 运行时候，创建topic 不管是journal还是queue，都需要在异步运行时内，因为要做同步，在不同运行时内无法做同步
        if segments.is_empty() {
            info!("no segment file found in journal log dir:{}", dir);
            //初始化一个空的segment
            let segment = LogSegment::new_journal_seg(dir.clone(), 0, true).await?;
            segments.insert(0, segment);
        }
        // 如果log目录不存在，先创建它
        if !Path::new::<Path>(dir.as_ref()).exists() {
            info!("log dir does not exists, create journal log dir:{}", dir);
            fs::create_dir_all(&dir).await?;
        }
        Ok(JournalLog {
            dir: dir.clone(),
            segments: RwLock::new(segments),
            log_start_offset,
            recover_point: AtomicCell::new(log_recovery_point),
            next_offset_info: DashMap::new(),
        })
    }

    async fn flush(&self) -> AppResult<()> {
        let segments = self.segments.read().await;
        let (_, active_seg) = segments.iter().next_back().ok_or_else(|| {
            IllegalStateError(Cow::Owned(format!(
                "no active segment found in journal log:{}",
                self.dir.clone(),
            )))
        })?;
        let size = active_seg.flush().await?;
        self.recover_point.store(size);
        Ok(())
    }
}

impl Log for QueueLog {
    async fn append_records(
        &self,
        records: (TopicPartition, MemoryRecords),
    ) -> AppResult<LogAppendInfo> {
        let (topic_partition, memory_records) = records;
        let mut active_seg_size = 0u64;
        let mut active_base_offset = 0u64;
        {
            // get active segment info
            let segments = self.segments.read().await;
            let last_seg = segments
                .iter()
                .next_back()
                .ok_or(self.no_active_segment_error(self.dir.clone()))?;
            active_seg_size = last_seg.1.size() as u64;
            active_base_offset = *last_seg.0;
            // 使用这个作用域释放读锁
        }

        let buffer = memory_records.buffer.as_ref().ok_or(InvalidValue(
            "empty message when append to file",
            topic_partition.id(),
        ))?;
        let msg_len = buffer.remaining();

        // check active segment size, if exceed the segment size, roll the segment
        if active_seg_size + msg_len as u64 >= BROKER_CONFIG.get().unwrap().log.journal_segment_size
        {
            // 确定需要滚动日之后，进入新的作用域，获取写锁，准备滚动日志
            // check again to make sure other request has rolled it already
            let mut segments = self.segments.write().await;
            let last_seg = segments.iter().next_back().unwrap();
            if *last_seg.0 == active_base_offset {
                // other request has not rolled it yet
                let new_base_offset = last_seg.1.size() as u64;
                let new_seg =
                    LogSegment::new_queue_seg(self.dir.clone(), new_base_offset, true).await?;
                // close old segment write, release its pipe resource
                last_seg.1.flush().await?;
                last_seg.1.close_write().await;
                segments.insert(new_base_offset, new_seg);
            }
        }

        let segments = self.segments.read().await;
        let (active_base_offset, active_seg) = segments.iter().next_back().unwrap();
        let (tx, rx) = oneshot::channel::<AppResult<()>>();

        active_seg
            .append_record((topic_partition, memory_records, tx))
            .await?;
        rx.await??;
        Ok(LogAppendInfo {
            base_offset: *active_base_offset as i64,
            log_append_time: 0,
        })
    }
    /// 加载`dir`下的所有segment文件
    /// journal log没有index文件
    /// queue log有time index和offset index文件
    fn load_segments(dir: &str, rt: &Runtime) -> AppResult<BTreeMap<u64, LogSegment>> {
        let mut segments = BTreeMap::new();
        info!("load segment files from dir:{}", dir);

        if sync_fs::read_dir(dir)?.next().is_none() {
            info!("queue logs directory is empty: {}", dir);
            return Ok(segments);
        }
        let mut read_dir = sync_fs::read_dir(dir)?;
        while let Some(file) = read_dir.next().transpose()? {
            if file.metadata()?.file_type().is_file() {
                let file_name = file.file_name().to_string_lossy().to_string();
                let dot_index = file_name.rfind('.');
                match dot_index {
                    None => {
                        warn!("invalid segment file name:{}", file_name);
                        continue;
                    }
                    Some(dot_index) => {
                        let file_prefix = &file_name[..dot_index];
                        let file_suffix = &file_name[dot_index..];
                        match file_suffix {
                            ".timeindex" => {}
                            ".index" => {}
                            ".log" => {
                                let base_offset = file_prefix.parse::<u64>();
                                match base_offset {
                                    Ok(base_offset) => {
                                        let segment = rt.block_on(LogSegment::new_queue_seg(
                                            dir.to_string(),
                                            base_offset,
                                            false,
                                        ))?;
                                        segments.insert(base_offset, segment);
                                    }
                                    Err(_) => {
                                        warn!("invalid segment file name:{}", file_prefix);
                                        continue;
                                    }
                                }
                            }
                            other => {
                                warn!("invalid segment file name:{}", other);
                                continue;
                            }
                        }
                    }
                }
            }
        }

        Ok(segments)
    }

    async fn new(
        dir: String,
        mut segments: BTreeMap<u64, LogSegment>,
        log_start_offset: u64,
        log_recovery_point: u64,
    ) -> AppResult<Self> {
        // 如果segments是空的, 默认创建一个
        // 运行时候，创建topic 不管是journal还是queue，都需要在异步运行时内，因为要做同步，在不同运行时内无法做同步
        if segments.is_empty() {
            warn!("no segment file found in queue log dir:{}", dir);
            //初始化一个空的segment
            let segment = LogSegment::new_queue_seg(dir.clone(), 0, true).await?;
            segments.insert(0, segment);
        }
        // 如果log目录不存在，先创建它
        if !Path::new::<Path>(dir.as_ref()).exists() {
            info!("log dir does not exists, create queue log dir:{}", dir);
            fs::create_dir_all(&dir).await?;
        }

        Ok(QueueLog {
            dir: dir.to_string(),
            segments: RwLock::new(segments),
            log_start_offset,
            recover_point: log_recovery_point,
        })
    }

    async fn flush(&self) -> AppResult<()> {
        todo!()
    }
}

#[derive(Debug)]
pub struct FileRecords {
    tx: Sender<LogMessage>,
    size: Arc<AtomicCell<usize>>,
}

const JOURNAL_CHECK_POINT_FILE_NAME: &str = ".journal_recovery_checkpoints";
///
/// 这里使用DashMap来保障并发安全，但是安全仅限于对map entry的增加或删除。对于log的读写操作，则需要tokio RwLock
/// 来保护。
/// 1. 对于partition的增加或减少，这种操作相对低频，这里的DashMap保障读写锁，锁争抢的概率较低，代价是可以接受的
/// 2. 对于log的读写操作，这里的RwLock保障并发读写。读操作直接使用log的不可变
///
///
#[derive(Default, Debug)]
pub struct LogManager {
    journal_logs: DashMap<TopicPartition, Arc<JournalLog>>,
    queue_logs: DashMap<TopicPartition, Arc<QueueLog>>,
}
#[derive(Debug)]
pub struct LogSegment {
    log_dir: String,
    log: FileRecords,
    base_offset: u64,
    time_index: Option<TimeIndex>,
    offset_index: Option<OffsetIndex>,
    index_interval_bytes: i32,
}

impl FileRecords {
    pub async fn open(file_name: String) -> AppResult<FileRecords> {
        let write_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&file_name)
            .await?;
        let (tx, rx) = mpsc::channel(100);
        let file_records = FileRecords {
            tx,
            size: Arc::new(AtomicCell::new(0)),
        };
        file_records.start_append_thread(
            rx,
            BufWriter::new(write_file),
            file_records.size.clone(),
            file_name,
        );
        Ok(file_records)
    }
    pub fn start_append_thread(
        &self,
        mut rx: Receiver<LogMessage>,
        mut buf_writer: BufWriter<File>,
        size: Arc<AtomicCell<usize>>,
        file_name: String,
    ) {
        tokio::spawn(async move {
            let writer = &mut buf_writer;
            let total_size = size.clone();
            while let Some(message) = rx.recv().await {
                match message {
                    LogMessage::AppendRecords((topic_partition, records, resp_tx)) => {
                        match Self::append(writer, (topic_partition, records)).await {
                            Ok(size) => {
                                trace!("{} file append finished .", &file_name);
                                total_size.fetch_add(size);
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
                    LogMessage::Flush(sender) => match writer.get_ref().sync_all().await {
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
                    LogMessage::FetchRecords => {}
                }
            }
            trace!("{} file records append thread exit", &file_name)
        });
    }
    pub async fn close_write(&self) {
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

impl LogManager {
    pub fn new() -> Self {
        LogManager {
            journal_logs: DashMap::new(),
            queue_logs: DashMap::new(),
        }
    }

    ///
    /// 在broker启动的时候，从硬盘加载所有的日志文件，包括journal和queue日志
    /// 预期在broker启动前加载
    pub fn startup(&mut self, rt: &Runtime) -> AppResult<()> {
        info!("log manager startup ...");
        let log_config = &BROKER_CONFIG.get().unwrap().log;
        let journal_logs = self.load_logs::<JournalLog>(&log_config.journal_base_dir, rt)?;
        self.journal_logs.extend(journal_logs);
        let queue_logs = self.load_logs::<QueueLog>(&log_config.queue_base_dir, rt)?;
        self.queue_logs.extend(queue_logs);
        info!("log manager startup completed.");
        Ok(())
    }

    pub(crate) fn load_logs<T: Log>(
        &mut self,
        logs_dir: &str,
        rt: &Runtime,
    ) -> AppResult<Vec<(TopicPartition, Arc<T>)>> {
        info!("load logs from dir:{} for {}", logs_dir, type_name::<T>());
        if !sync_fs::metadata(logs_dir)
            .map(|meta| meta.is_dir())
            .unwrap_or(false)
        {
            let (msg, arg) = (
                "logs directory does not exist, lacks the necessary permissions, or is a file.: {}",
                logs_dir,
            );
            error!("{} {}", msg, arg);
            return Err(InvalidValue(msg, arg.to_string()));
        }
        let mut logs = vec![];

        let mut dir = sync_fs::read_dir(logs_dir)?;

        while let Some(dir) = dir.next().transpose()? {
            if dir.metadata()?.file_type().is_dir() {
                let log = LogManager::load_log(dir.path().to_string_lossy().as_ref(), rt)?;
                let tp = TopicPartition::from_string(dir.file_name().to_string_lossy())?;
                trace!("found log:{:}", &tp.id());
                logs.push((tp, Arc::new(log)));
            } else {
                warn!("invalid log dir:{:?}", dir.path().to_string_lossy());
            }
        }
        info!("load {} logs from dir:{} finished", logs.len(), logs_dir);
        Ok(logs)
    }
    ///
    /// 加载单个topic-partition的日志目录,并加载其中的segment文件
    fn load_log<T: Log>(log_dir: &str, rt: &Runtime) -> AppResult<T> {
        // 加载log目录下的segment文件
        let log_segments = T::load_segments(log_dir, rt)?;
        // 构建Log
        let log_start_offset = log_segments.first_key_value().map(|(k, _)| *k).unwrap_or(0);
        let log = rt.block_on(T::new(
            log_dir.to_string(),
            log_segments,
            log_start_offset,
            0,
        ))?;
        Ok(log)
    }
    pub async fn get_or_create_journal_log(
        &self,
        topic_partition: &TopicPartition,
    ) -> AppResult<Arc<JournalLog>> {
        let log = self.journal_logs.entry(topic_partition.clone());
        match log {
            Entry::Occupied(occupied) => Ok(occupied.get().clone()),
            Entry::Vacant(vacant) => {
                warn!(
                    "journal log for topic-partition:{} not found",
                    topic_partition.id()
                );
                let journal_log_path = format!(
                    "{}/{}",
                    BROKER_CONFIG.get().unwrap().log.journal_base_dir,
                    topic_partition.id()
                );
                let journal_log = JournalLog::new(journal_log_path, BTreeMap::new(), 0, 0).await?;
                let log = Arc::new(journal_log);
                vacant.insert(log.clone());
                Ok(log)
            }
        }
    }
    pub fn get_or_create_queue_log(
        &self,
        topic_partition: &TopicPartition,
        rt: &Runtime,
    ) -> AppResult<Arc<QueueLog>> {
        let log = self.queue_logs.entry(topic_partition.clone());
        match log {
            Entry::Occupied(occupied) => Ok(occupied.get().clone()),
            Entry::Vacant(vacant) => {
                let queue_log_path = format!(
                    "{}/{}",
                    BROKER_CONFIG.get().unwrap().log.queue_base_dir,
                    topic_partition.id()
                );
                let log =
                    Arc::new(rt.block_on(QueueLog::new(queue_log_path, BTreeMap::new(), 0, 0))?);
                vacant.insert(log.clone());
                Ok(log)
            }
        }
    }
    async fn recovery_checkpoint_task(&self, mut interval: Interval) -> AppResult<()> {
        loop {
            interval.tick().await;
            let check_points: HashMap<TopicPartition, u64> = self
                .journal_logs
                .iter()
                .map(|entry| {
                    let tp = entry.key();
                    let log = entry.value();
                    (tp.clone(), log.recover_point.load())
                })
                .collect();
            self.checkpoints(JOURNAL_CHECK_POINT_FILE_NAME, check_points, 0)
                .await?;
        }
    }
    pub(crate) async fn start_task(self: Arc<Self>) -> AppResult<()> {
        let recovery_check_interval = BROKER_CONFIG
            .get()
            .unwrap()
            .log
            .recovery_checkpoint_interval;
        let interval = tokio::time::interval(Duration::from_secs(recovery_check_interval));
        tokio::spawn(async move {
            {
                let result = self.recovery_checkpoint_task(interval).await;
                match result {
                    Ok(_) => {
                        debug!("journal log recovery checkpoint task finished");
                    }
                    Err(error) => {
                        error!("recovery checkpoint task error:{:?}", error);
                    }
                }
            }
        });
        Ok(())
    }
}

impl LogSegment {
    pub fn size(&self) -> usize {
        self.log.size()
    }
    pub async fn new_journal_seg(dir: String, base_offset: u64, create: bool) -> AppResult<Self> {
        let file_name = format!("{}/{}.log", dir, base_offset);
        trace!("new segment file:{}", file_name);
        if create {
            trace!(
                "file dose not exist, create one journal segment file:{}",
                file_name
            );
        }
        let file_records = FileRecords::open(file_name).await?;
        let segment = LogSegment {
            log_dir: dir.to_string(),
            log: file_records,
            base_offset,
            time_index: None,
            offset_index: None,
            index_interval_bytes: 0,
        };
        Ok(segment)
    }
    pub async fn new_queue_seg(dir: String, base_offset: u64, create: bool) -> AppResult<Self> {
        let file_name = format!("{}/{}.log", dir, base_offset);
        if create {
            trace!("create queue segment file:{}", file_name);
        }
        let file_records = FileRecords::open(file_name).await?;
        let segment = LogSegment {
            log_dir: dir,
            log: file_records,
            base_offset,
            time_index: None,
            offset_index: None,
            index_interval_bytes: 0,
        };
        Ok(segment)
    }
    pub async fn append_record(
        &self,
        records: (
            TopicPartition,
            MemoryRecords,
            oneshot::Sender<AppResult<()>>,
        ),
    ) -> AppResult<()> {
        self.log.tx.send(LogMessage::AppendRecords(records)).await?;
        Ok(())
    }
    pub(crate) async fn close_write(&self) {
        self.log.close_write().await;
    }
    pub(crate) async fn flush(&self) -> AppResult<(u64)> {
        let (tx, rx) = oneshot::channel::<AppResult<(u64)>>();
        self.log.tx.send(LogMessage::Flush(tx)).await?;
        let size = rx.await??;
        Ok(size)
    }
}
