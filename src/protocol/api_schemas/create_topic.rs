use std::collections::HashMap;
use std::sync::Arc;

use once_cell::sync::Lazy;

use crate::protocol::schema::Schema;
use crate::protocol::types::DataType;
use crate::protocol::value_set::ValueSet;
use crate::AppResult;

const REQUESTS_KEY_NAME: &str = "create_topic_requests";
const TIMEOUT_KEY_NAME: &str = "timeout";
const VALIDATE_ONLY_KEY_NAME: &str = "validate_only";
const TOPIC_KEY_NAME: &str = "topic";
const NUM_PARTITIONS_KEY_NAME: &str = "num_partitions";
const REPLICATION_FACTOR_KEY_NAME: &str = "replication_factor";
const REPLICA_ASSIGNMENT_KEY_NAME: &str = "replica_assignment";
const REPLICA_ASSIGNMENT_PARTITION_ID_KEY_NAME: &str = "partition_id";
const REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME: &str = "replicas";
const CONFIG_KEY_KEY_NAME: &str = "config_name";
const CONFIG_VALUE_KEY_NAME: &str = "config_value";
const CONFIGS_KEY_NAME: &str = "config_entries";

pub struct CreateTopicRequest {
    topics: HashMap<String, TopicDetails>,
    timeout: u32,
    validate_only: bool,
}

pub struct TopicDetails {
    num_partitions: i32,
    replication_factor: i16,
    config: HashMap<String, String>,
}
impl CreateTopicRequest {
    pub fn create_from_value_set(mut _value_set: ValueSet) -> AppResult<CreateTopicRequest> {
        todo!()
    }
}

pub static SINGLE_CREATE_TOPIC_REQUEST_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    // config array schema
    let config_schema = Arc::new(Schema::from_fields_desc_vec(vec![
        (0, CONFIG_KEY_KEY_NAME, String::default().into()),
        (1, CONFIG_VALUE_KEY_NAME, Some(String::default()).into()),
    ]));
    let config_ary = DataType::array_of_schema(config_schema);

    // partition replica assignment schema
    let replica_assignment_schema = Arc::new(Schema::from_fields_desc_vec(vec![
        (0, REPLICA_ASSIGNMENT_PARTITION_ID_KEY_NAME, 0i32.into()),
        (
            1,
            REPLICA_ASSIGNMENT_REPLICAS_KEY_NAME,
            DataType::array_of_i32_type(0i32),
        ),
    ]));
    let replica_assignment_ary = DataType::array_of_schema(replica_assignment_schema);

    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, TOPIC_KEY_NAME, String::default().into()),
        (1, NUM_PARTITIONS_KEY_NAME, 0i32.into()),
        (2, REPLICATION_FACTOR_KEY_NAME, 0i16.into()),
        (3, REPLICA_ASSIGNMENT_KEY_NAME, replica_assignment_ary),
        (4, CONFIGS_KEY_NAME, config_ary),
    ];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});

pub static CREATE_TOPIC_REQUEST_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (
            0,
            REQUESTS_KEY_NAME,
            DataType::array_of_schema(SINGLE_CREATE_TOPIC_REQUEST_V0.clone()),
        ),
        (1, TIMEOUT_KEY_NAME, 0u32.into()),
    ];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});
pub static CREATE_TOPIC_REQUEST_V1: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (
            0,
            REQUESTS_KEY_NAME,
            DataType::array_of_schema(SINGLE_CREATE_TOPIC_REQUEST_V0.clone()),
        ),
        (1, TIMEOUT_KEY_NAME, 0u32.into()),
        (2, VALIDATE_ONLY_KEY_NAME, false.into()),
    ];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});
