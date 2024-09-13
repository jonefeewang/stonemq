use crate::log::file_records::FileRecords;
use crate::log::log_segment::PositionInfo;
use crate::log::{JournalLog, QueueLog};
use crate::message::TopicPartition;
use crate::{global_config, AppResult};
use bytes::BytesMut;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncReadExt, BufReader};

/// Splitter的读取和消费的读取还不太一样
/// 1.Splitter的读取是一个读取者，而且连续的读取，所以针对一个journal log 最好每个splitter任务自己维护一个ReadBuffer
/// 而不需要通过FileRecord来读取，自己只要知道从哪个segment开始读取即可，然后读取到哪个位置，然后读取下一个segment
/// 2.消费的读取是多个并发读取者，而且不连续的数据.可以维护一个BufReader pool，然后读取者从pool中获取一个BufReader
/// .比如最热active segment, 可能有多个BufReader，而其他segment可能只有一个BufReader
pub struct SplitterTask {
    journal_log: Arc<JournalLog>,
    queue_logs: HashSet<Arc<QueueLog>>,
    topic_partition: TopicPartition,
}
impl SplitterTask {
    pub fn new(
        journal_log: Arc<JournalLog>,
        queue_logs: HashSet<Arc<QueueLog>>,
        topic_partition: TopicPartition,
    ) -> Self {
        SplitterTask {
            journal_log,
            queue_logs,
            topic_partition,
        }
    }
    pub async fn run(&self) -> AppResult<()> {
        let mut target_offset = self.journal_log.split_offset.load();
        // 循环读取，理论上这个任务不会停止，最多在读取不到active segment的最新消息时暂停一会
        loop {
            //定位到segment内的近似位置
            let position_info = self.journal_log
                .get_position_info(target_offset).await?;
            self.read_seg(target_offset, &position_info).await?;
            target_offset = self.journal_log.next_base_offset(position_info.base_offset).await;
        }
    }
    /// 从这个offset开始读取，直到本段读取结束
    async fn read_seg(&self, target_offset: u64, position_info: &PositionInfo) -> AppResult<()> {
        let journal_topic_dir = PathBuf::from(global_config().log.journal_base_dir.clone())
            .join(self.topic_partition.id());


        let segment_path = journal_topic_dir.join(format!("{}.log", position_info.base_offset));
        let seg_file = fs::File::open(segment_path).await?;


        // 定位到segment内的确切位置
        let journal_seg = FileRecords::seek(seg_file, target_offset, &position_info).await?;

        //开始读取
        let mut reader = BufReader::new(journal_seg);
        let reader_buffer_size = global_config().log.splitter_read_buffer_size;
        let mut buf = BytesMut::with_capacity(reader_buffer_size as usize);
        let mut total_read = position_info.position;
        let current_seg_base_offset = position_info.base_offset;
        loop {
            let bytes_read = reader.read(&mut buf).await? as u32;
            if bytes_read == 0 {
                //读不出东西来了, 而且当前段不是active segment
                if current_seg_base_offset != self.journal_log.current_active_seg_offset().await {
                    //结束读取，返回，等待读取下一段
                } else {
                    //当前段是active的，但是读不出东西，表明已经到最后位置，等待一会继续读取
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
            total_read += bytes_read;
            self.write_queue_log(&buf).await?;
            buf.clear();
        }
    }

    async fn write_queue_log(&self, buffer: &BytesMut) -> AppResult<()> {
        todo!()
    }
}