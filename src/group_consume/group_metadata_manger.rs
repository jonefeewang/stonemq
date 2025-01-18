/// Module for managing consumer group metadata persistence and retrieval.
/// Provides functionality for storing and loading group metadata and offsets using RocksDB.
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
    /// Prefix for group metadata keys in RocksDB
    const GROUP_PREFIX: &str = "group";
    /// Prefix for offset keys in RocksDB
    const OFFSET_PREFIX: &str = "offset";

    /// Generates a RocksDB key for storing group metadata
    ///
    /// # Arguments
    /// * `group_id` - ID of the consumer group
    pub fn group_db_key(group_id: &str) -> String {
        format!("{}:{}", Self::GROUP_PREFIX, group_id)
    }

    /// Generates a RocksDB key for storing partition offsets
    ///
    /// # Arguments
    /// * `group_id` - ID of the consumer group
    /// * `topic_partition` - Topic partition information
    pub fn offset_db_key(group_id: &str, topic_partition: &TopicPartition) -> String {
        format!("{}:{}:{}", Self::OFFSET_PREFIX, group_id, topic_partition)
    }

    /// Creates a new GroupMetadataManager instance
    ///
    /// # Arguments
    /// * `groups` - Map of group IDs to their metadata
    /// * `db` - RocksDB instance for persistence
    pub fn new(groups: DashMap<String, Arc<RwLock<GroupMetadata>>>, db: DB) -> Self {
        Self { groups, db }
    }

    /// Adds a new group to the manager
    ///
    /// # Arguments
    /// * `group_metadata` - Metadata for the new group
    ///
    /// # Returns
    /// Reference to the stored group metadata
    pub fn add_group(&self, group_metadata: GroupMetadata) -> Arc<RwLock<GroupMetadata>> {
        self.groups
            .entry(group_metadata.id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(group_metadata)))
            .value()
            .clone()
    }

    /// Retrieves a group's metadata by ID
    ///
    /// # Arguments
    /// * `group_id` - ID of the group to retrieve
    ///
    /// # Returns
    /// Optional reference to the group metadata
    pub fn get_group(&self, group_id: &str) -> Option<Arc<RwLock<GroupMetadata>>> {
        self.groups.get(group_id).map(|g| g.clone())
    }

    /// Stores group metadata in RocksDB
    ///
    /// # Arguments
    /// * `write_lock` - Write lock containing the group metadata to store
    ///
    /// # Returns
    /// * `Ok(())` - If storage succeeds
    /// * `Err(KafkaError)` - If storage fails
    pub fn store_group(&self, write_lock: &RwLockWriteGuard<GroupMetadata>) -> KafkaResult<()> {
        let group_id = write_lock.id.clone();
        let group_data = write_lock.serialize()?;

        let result = self.db.put(Self::group_db_key(&group_id), group_data);
        if result.is_err() {
            return Err(KafkaError::CoordinatorNotAvailable(group_id.clone()));
        }
        Ok(())
    }

    /// Stores partition offsets for a group member
    ///
    /// # Arguments
    /// * `group_id` - ID of the consumer group
    /// * `member_id` - ID of the group member
    /// * `offsets` - Map of topic partitions to their offsets
    ///
    /// # Returns
    /// * `Ok(())` - If storage succeeds
    /// * `Err(KafkaError)` - If storage fails
    pub fn store_offset(
        &self,
        group_id: &str,
        member_id: &str,
        offsets: HashMap<TopicPartition, PartitionOffsetCommitData>,
    ) -> KafkaResult<()> {
        for (topic_partition, offset_and_metadata) in offsets {
            let key = Self::offset_db_key(group_id, &topic_partition);
            let value = offset_and_metadata.serialize();
            if let Err(e) = value {
                error!("Failed to serialize offset: {}", e);
                return Err(KafkaError::Unknown(format!(
                    "Failed to serialize offset for group:{} member:{}: {}",
                    group_id, member_id, e
                )));
            } else {
                let value = value.unwrap();
                let result = self.db.put(&key, &value);
                if result.is_err() {
                    let error_msg = format!(
                        "Failed to store offset for group:{} member:{}: {:?}",
                        group_id,
                        member_id,
                        result.err().unwrap()
                    );
                    error!("{}", error_msg);
                    return Err(KafkaError::Unknown(error_msg));
                } else {
                    trace!("Successfully stored offset: {}, value: {:?}", key, value);
                }
            }
        }
        Ok(())
    }

    /// Retrieves partition offsets for a group
    ///
    /// # Arguments
    /// * `group_id` - ID of the consumer group
    /// * `partitions` - Optional list of partitions to retrieve offsets for
    ///
    /// # Returns
    /// * `Ok(HashMap)` - Map of topic partitions to their offset data
    /// * `Err(KafkaError)` - If retrieval fails
    pub fn get_offset(
        &self,
        group_id: &str,
        partitions: Option<Vec<TopicPartition>>,
    ) -> KafkaResult<HashMap<TopicPartition, PartitionOffsetData>> {
        let mut offsets = HashMap::new();
        for partition in partitions.unwrap_or_default() {
            let key = Self::offset_db_key(group_id, &partition);
            let value = self.db.get(key);
            let partition_id = partition.partition();
            if let Ok(Some(value)) = value {
                // Return stored offset if it exists
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
                        "Failed to deserialize offset for group:{}: {}",
                        group_id,
                        partition_offset_data.err().unwrap()
                    )));
                }
            } else {
                // Return 0 if offset doesn't exist
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
        trace!("Successfully retrieved offsets: {:?}", offsets);
        Ok(offsets)
    }

    /// Loads all group metadata from RocksDB
    ///
    /// Initializes RocksDB with appropriate options and loads all stored
    /// group metadata into memory.
    ///
    /// # Returns
    /// New GroupMetadataManager instance with loaded data
    pub fn load() -> Self {
        // Configure RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true);

        // Set prefix extractor for group ID prefix
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(6));

        // Open database
        let db = DB::open(&opts, &global_config().general.local_db_path)
            .expect("Failed to open RocksDB");

        // Set iterator mode
        let mode = IteratorMode::From(Self::GROUP_PREFIX.as_bytes(), rocksdb::Direction::Forward);
        let iter = db.iterator(mode);

        // Scan for group metadata
        let groups = DashMap::new();
        for result in iter {
            if let Ok((key, value)) = result {
                if key.starts_with(Self::GROUP_PREFIX.as_bytes()) {
                    // Process key-value pair
                    let group_id = String::from_utf8_lossy(&key).to_string();
                    let group_metadata = GroupMetadata::deserialize(&value, &group_id).unwrap();
                    info!("Loaded group metadata: {} {:#?}", group_id, group_metadata);
                    groups.insert(group_id, Arc::new(RwLock::new(group_metadata)));
                } else {
                    // Stop scanning when prefix no longer matches
                    break;
                }
            } else {
                error!("Failed to load group metadata: {}", result.err().unwrap());
            }
        }
        Self::new(groups, db)
    }
}
