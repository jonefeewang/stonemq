use std::collections::HashMap;

use crate::{AppResult, Log, LogManager};
use crate::topic_partition::{Partition, TopicData, TopicPartition};

pub struct Replica {
    broker_id: i32,
    topic_partition: TopicPartition,
    log: Option<Log>,
}
/// replica manager 持有一个all partitions的集合，这个集合是从controller发送的
/// leaderAndIsrRequest命令里获取的, 所有的replica信息都在partition里。Log里的
/// topic partition 和 这里的partition没有做一致性的合并，各自管理各自的。replica manager
/// 通过log manager来管理存储层
pub struct ReplicaManager {
    all_partitions: HashMap<TopicPartition, Partition>,
    log_manager: LogManager,
}

impl ReplicaManager {
    pub fn new(log_manager: LogManager) -> Self {
        ReplicaManager {
            log_manager,
            all_partitions: HashMap::new(),
        }
    }
    pub async fn append_records(&self, topic_data: TopicData) -> AppResult<()> {
        //在这里调用partition 的append log 方法
        todo!()
    }
    ///
    /// 因为当前StoneMQ是单机生产和消费，还没有集群的概念，这里从kv db里获取所有的topic partition信息，
    /// 模拟从controller 发出的leader and isr request上获取
    ///
    pub async fn startup(&self) -> AppResult<()> {
        todo!()
    }
}
