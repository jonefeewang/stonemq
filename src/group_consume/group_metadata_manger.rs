use std::{collections::HashMap, sync::Arc};

use dashmap::DashMap;
use rocksdb::{IteratorMode, Options, SliceTransform, DB};
use tokio::sync::RwLock;
use tokio::sync::RwLockWriteGuard;
use tracing::{error, info, trace};

use crate::global_config;
use crate::request::KafkaError;
use crate::request::PartitionOffsetCommitData;
use crate::request::PartitionOffsetData;
use crate::{message::TopicPartition, request::KafkaResult};

use super::GroupMetadata;
use super::GroupMetadataManager;

impl GroupMetadataManager {
    const GROUP_PREFIX: &str = "group";
    const OFFSET_PREFIX: &str = "offset";
    pub fn group_db_key(group_id: &str) -> String {
        format!("{}:{}", Self::GROUP_PREFIX, group_id)
    }
    pub fn offset_db_key(group_id: &str, topic_partition: &TopicPartition) -> String {
        format!("{}:{}:{}", Self::OFFSET_PREFIX, group_id, topic_partition)
    }
    pub fn new(groups: DashMap<String, Arc<RwLock<GroupMetadata>>>) -> Self {
        Self { groups }
    }

    pub fn add_group(&self, group_metadata: GroupMetadata) -> Arc<RwLock<GroupMetadata>> {
        self.groups
            .entry(group_metadata.id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(group_metadata)))
            .value()
            .clone()
    }
    pub fn get_group(&self, group_id: &str) -> Option<Arc<RwLock<GroupMetadata>>> {
        self.groups.get(group_id).map(|g| g.clone())
    }
    pub fn store_group(&self, write_lock: &RwLockWriteGuard<GroupMetadata>) -> KafkaResult<()> {
        let group_id = write_lock.id.clone();
        let group_data = write_lock.serialize()?;

        let db_path = &global_config().general.local_db_path;
        let db = DB::open_default(db_path).unwrap();
        let result = db.put(Self::group_db_key(&group_id), group_data);
        if result.is_err() {
            return Err(KafkaError::CoordinatorNotAvailable(group_id.clone()));
        }
        Ok(())
    }
    pub fn store_offset(
        &self,
        group_id: &str,
        member_id: &str,
        offsets: HashMap<TopicPartition, PartitionOffsetCommitData>,
    ) -> KafkaResult<()> {
        let db_path = &global_config().general.local_db_path;
        let db = DB::open_default(db_path).unwrap();
        for (topic_partition, offset_and_metadata) in offsets {
            let key = Self::offset_db_key(group_id, &topic_partition);
            let value = offset_and_metadata.serialize();
            if let Err(e) = value {
                error!("序列化offset失败: {}", e);
                return Err(KafkaError::Unknown(format!(
                    "group id:{}  member id:{} 序列化offset失败: {}",
                    group_id, member_id, e
                )));
            } else {
                let value = value.unwrap();
                let result = db.put(&key, &value);
                if result.is_err() {
                    let error_msg = format!(
                        "group id:{}  member id:{} 存储offset失败: {:?}",
                        group_id,
                        member_id,
                        result.err().unwrap()
                    );
                    error!("{}", error_msg);
                    return Err(KafkaError::Unknown(error_msg));
                } else {
                    trace!("存储offset成功: {}, value: {:?}", key, value);
                }
            }
        }
        Ok(())
    }
    pub fn get_offset(
        &self,
        group_id: &str,
        partitions: Option<Vec<TopicPartition>>,
    ) -> KafkaResult<HashMap<TopicPartition, PartitionOffsetData>> {
        let db_path = &global_config().general.local_db_path;
        let db = DB::open_default(db_path).unwrap();
        let mut offsets = HashMap::new();
        for partition in partitions.unwrap_or_default() {
            let key = Self::offset_db_key(group_id, &partition);
            let value = db.get(key);
            let partition_id = partition.partition();
            if let Ok(Some(value)) = value {
                // 如果offset存在，则返回offset
                let partition_offset_data = PartitionOffsetCommitData::deserialize(&value);
                if let Ok(partition_offset_data) = partition_offset_data {
                    offsets.insert(
                        partition,
                        PartitionOffsetData {
                            partition_id,
                            offset: partition_offset_data.offset,
                            metadata: partition_offset_data.metadata,
                            error: KafkaError::None,
                        },
                    );
                } else {
                    return Err(KafkaError::Unknown(format!(
                        "group id:{} 反序列化offset失败: {}",
                        group_id,
                        partition_offset_data.err().unwrap()
                    )));
                }
            } else {
                // 如果offset不存在，则返回0
                offsets.insert(
                    partition,
                    PartitionOffsetData {
                        partition_id,
                        offset: 0,
                        metadata: None,
                        error: KafkaError::None,
                    },
                );
            }
        }
        trace!("获取offset成功: {:?}", offsets);
        Ok(offsets)
    }
    pub fn load() -> Self {
        // 配置 RocksDB 选项
        let mut opts = Options::default();
        opts.create_if_missing(true);

        // 假设所有 Group ID 前缀为 "group:"
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(6));

        // 打开数据库
        let db = DB::open(&opts, &global_config().general.local_db_path).expect("无法打开 RocksDB");

        // 设置迭代器模式
        let mode = IteratorMode::From(Self::GROUP_PREFIX.as_bytes(), rocksdb::Direction::Forward);
        let iter = db.iterator(mode);

        // 执行前缀扫描
        let groups = DashMap::new();
        for result in iter {
            if let Ok((key, value)) = result {
                if key.starts_with(Self::GROUP_PREFIX.as_bytes()) {
                    // 处理键值对
                    let group_id = String::from_utf8_lossy(&key).to_string();
                    let group_metadata = GroupMetadata::deserialize(&value, &group_id).unwrap();
                    info!("加载组元数据: {} {:#?}", group_id, group_metadata);
                    groups.insert(group_id, Arc::new(RwLock::new(group_metadata)));
                } else {
                    // 超过前缀范围，停止扫描
                    break;
                }
            } else {
                error!("加载组元数据失败: {}", result.err().unwrap());
            }
        }
        Self::new(groups)
    }
}
