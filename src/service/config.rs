extern crate config as _;

use std::path::Path;

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

use super::{AppError, AppResult};

pub static GLOBAL_CONFIG: OnceCell<BrokerConfig> = OnceCell::new();
pub fn global_config() -> &'static BrokerConfig {
    GLOBAL_CONFIG.get().unwrap()
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct GeneralConfig {
    pub id: i32,
    pub max_msg_size: i32,
    pub local_db_path: String,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct NetworkConfig {
    pub ip: String,
    pub port: u16,
    pub max_connection: usize,
    pub max_package_size: usize,
}
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct GroupConsumeConfig {
    pub group_min_session_timeout: i32,
    pub group_max_session_timeout: i32,
    pub group_initial_rebalance_delay: i32,
}
/// Represents the configuration for a log.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LogConfig {
    pub journal_topic_count: u32,
    pub queue_topic_count: u32,
    /// The interval at which recovery checkpoints are written.
    pub recovery_checkpoint_interval: u64,
    pub splitter_read_buffer_size: u32,
    pub splitter_wait_interval: u32,
    pub file_records_comm_channel_size: usize,

    /// The base directory for the journal.
    pub journal_base_dir: String,
    /// The size of each journal segment.
    pub journal_segment_size: u64,
    /// The size of the journal index file.
    pub journal_index_file_size: usize,
    /// The interval at which journal index entries are written.
    pub journal_index_interval_bytes: usize,

    /// The base directory for the queue.
    pub queue_base_dir: String,
    /// The size of each queue segment.
    pub queue_segment_size: u64,
    /// The size of the queue index file.
    pub queue_index_file_size: usize,
    /// The interval at which queue index entries are written.
    pub queue_index_interval_bytes: usize,

    /// The path to the key-value store.
    pub kv_store_path: String,
}
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct ActiveSegmentWriterPool {
    pub channel_capacity: usize,
    pub num_channels: i8,
    pub monitor_interval: u64,
    pub worker_check_timeout: u64,
}
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct RequestHandlerPool {
    pub channel_capacity: usize,
    pub num_channels: i8,
    pub monitor_interval: u64,
    pub worker_check_timeout: u64,
}
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct PartitionAppenderPool {
    pub channel_capacity: usize,
    pub num_channels: i8,
    pub monitor_interval: u64,
    pub worker_check_timeout: u64,
}
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct ActiveSegmentWriter {
    pub buffer_capacity: usize,
    pub flush_interval: u64,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct BrokerConfig {
    pub general: GeneralConfig,
    pub network: NetworkConfig,
    pub log: LogConfig,
    pub group_consume: GroupConsumeConfig,
    pub active_segment_writer: ActiveSegmentWriter,
    pub active_segment_writer_pool: ActiveSegmentWriterPool,
    pub request_handler_pool: RequestHandlerPool,
    pub partition_appender_pool: PartitionAppenderPool,
}

impl BrokerConfig {
    pub fn set_up_config<P: AsRef<Path>>(path: P) -> AppResult<BrokerConfig> {
        let path_str = path
            .as_ref()
            .to_str()
            .ok_or(AppError::InvalidValue(format!(
                "config file path: {}",
                path.as_ref().to_string_lossy()
            )))?;
        let config = config::Config::builder()
            .add_source(config::File::with_name(path_str))
            .build()
            // .expect("error in reading config files:");
            .unwrap_or_else(|err| {
                eprintln!("error in reading config files: {:?}", err);
                // io::stderr().flush().unwrap();
                std::process::exit(1);
            });

        // println!("raw config {:?}",config);

        let server_config: BrokerConfig = config.try_deserialize()?;

        Ok(server_config)
    }
}
