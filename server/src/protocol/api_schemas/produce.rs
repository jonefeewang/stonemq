use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::Arc;

use once_cell::sync::Lazy;

use crate::message::MemoryRecord;
use crate::protocol::array::Array;
use crate::protocol::field::Field;
use crate::protocol::primary_types::{PString, I32, I8};
use crate::protocol::protocol::Protocol;
use crate::protocol::schema::Schema;
use crate::protocol::types::FieldTypeEnum;
use crate::protocol::{Acks, ApiKey, ApiVersion};
use crate::request::{PartitionData, ProduceRequest, RequestHeader, TopicData};

pub const TRANSACTIONAL_ID_KEY_NAME: &str = "transactional_id";
pub const ACKS_KEY_NAME: &str = "acks";
pub const TIMEOUT_KEY_NAME: &str = "timeout";
pub const TOPIC_DATA_KEY_NAME: &str = "topic_data";

// Topic level field names
pub const TOPIC_KEY_NAME: &str = "topic";
pub const PARTITION_DATA_KEY_NAME: &str = "data";

// Partition level field names
pub const PARTITION_KEY_NAME: &str = "partition";
pub const RECORD_SET_KEY_NAME: &str = "record_set";

///
/// protocol format: `TopicName [Partition MessageSetSize MessageSet]`
/// Schema format:  
///  `Schema(
///          topic name field,
///          Array(
///                Schema(
///                       partition field,
///                       record set field
///                       )
///                )
///         )
/// `
///
pub static TOPIC_PRODUCE_DATA_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    // inner schema: [Partition MessageSetSize MessageSet]

    //Partition
    let partition_field = Field {
        index: 0,
        name: PARTITION_DATA_KEY_NAME,
        p_type: FieldTypeEnum::I32E(I32::default()),
    };
    //MessageSetSize MessageSet
    let record_set_field = Field {
        index: 1,
        name: RECORD_SET_KEY_NAME,
        p_type: FieldTypeEnum::RecordsE(MemoryRecord::default()),
    };

    let mut fields_by_name: HashMap<&str, Field> = HashMap::new();
    fields_by_name.insert(PARTITION_KEY_NAME, partition_field.clone());
    fields_by_name.insert(RECORD_SET_KEY_NAME, record_set_field.clone());
    let inner_schema = Schema {
        fields_by_name,
        fields: vec![partition_field, record_set_field],
    };

    // outer schema: TopicName inner_schema
    let data_ary = FieldTypeEnum::ArrayE(Array {
        can_be_empty: false,
        p_type: Arc::new(FieldTypeEnum::SchemaE(Arc::new(inner_schema))),
        values: None,
    });
    let mut fields_by_name: HashMap<&str, Field> = HashMap::new();
    //TopicName
    let topic_filed = Field {
        index: 0,
        name: TOPIC_KEY_NAME,
        p_type: FieldTypeEnum::PStringE(PString::default()),
    };
    let data_field = Field {
        index: 1,
        name: PARTITION_DATA_KEY_NAME,
        p_type: data_ary,
    };
    fields_by_name.insert(TOPIC_KEY_NAME, topic_filed.clone());
    fields_by_name.insert(PARTITION_DATA_KEY_NAME, data_field.clone());
    let schema = Schema {
        fields_by_name,
        fields: vec![topic_filed, data_field],
    };
    Arc::new(schema)
});

///
/// protocol format: `RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]`
/// Schema format:
/// `Schema(
///         acks field,
///         timeout field,
///         Array(
///               Schema(
///                      topic name field,
///                      Array(
///                            Schema(
///                                   partition field,
///                                   record set field
///                                   )
///                            )
///                      )
///                )
///         )
/// `
///
///          
///
///
pub static PRODUCE_REQUEST_SCHEMA_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let mut fields_by_name = HashMap::new();
    let acks_field = Field {
        index: 0,
        name: ACKS_KEY_NAME,
        p_type: FieldTypeEnum::I8E(I8::default()),
    };
    let timeout_filed = Field {
        index: 1,
        name: TIMEOUT_KEY_NAME,
        p_type: FieldTypeEnum::I32E(I32::default()),
    };
    let topic_data_field = Field {
        index: 2,
        name: TOPIC_DATA_KEY_NAME,
        p_type: FieldTypeEnum::ArrayE(Array {
            can_be_empty: false,
            p_type: Arc::new(FieldTypeEnum::SchemaE(Arc::clone(&TOPIC_PRODUCE_DATA_V0))),
            values: None,
        }),
    };
    fields_by_name.insert(ACKS_KEY_NAME, acks_field.clone());
    fields_by_name.insert(TIMEOUT_KEY_NAME, timeout_filed.clone());
    fields_by_name.insert(TOPIC_DATA_KEY_NAME, topic_data_field.clone());

    let schema = Schema {
        fields_by_name,
        fields: vec![acks_field, timeout_filed, topic_data_field],
    };
    Arc::new(schema)
});

#[test]
fn test_produce_request_to_struct() {
    // Create a ProduceRequest instance
    let produce_request = ProduceRequest {
        request_header: RequestHeader {
            len: 10,
            api_key: ApiKey::Produce,
            api_version: ApiVersion::V0,
            correlation_id: 1,
        },
        required_acks: Acks::default(),
        timeout: 1000,
        topic_data: vec![
            TopicData::new(
                "test_topic1",
                vec![PartitionData::new(1), PartitionData::new(2)],
            ),
            TopicData::new(
                "test_topic2",
                vec![PartitionData::new(1), PartitionData::new(2)],
            ),
        ],
    };

    // Call to_struct method
    let result = Protocol::encode_produce_request_to_struct(&produce_request).unwrap();

    // Check the returned Struct instance
    assert_eq!(
        result.get_field_value(ACKS_KEY_NAME).unwrap(),
        &FieldTypeEnum::from_acks(&Acks::default())
    );
    assert_eq!(
        result.get_field_value(TIMEOUT_KEY_NAME).unwrap(),
        &FieldTypeEnum::from_i32(1000)
    );

    // Check topic_data field
    let topic_data_field = result.get_field_value(TOPIC_DATA_KEY_NAME).unwrap();
    if let FieldTypeEnum::ArrayE(array) = topic_data_field {
        assert!(
            !array.values().as_ref().unwrap().is_empty(),
            "Topic data should not be empty"
        );
        for (index, item) in array.values().as_ref().unwrap().iter().enumerate() {
            if let FieldTypeEnum::StructE(structure) = item {
                assert_eq!(
                    structure.get_field_value(TOPIC_KEY_NAME).unwrap(),
                    &FieldTypeEnum::from_string(&format!("test_topic{}", index + 1))
                );
                if let FieldTypeEnum::ArrayE(array) =
                    structure.get_field_value(PARTITION_DATA_KEY_NAME).unwrap()
                {
                    assert!(
                        !array.values().as_ref().unwrap().is_empty(),
                        "Partition data should not be empty"
                    );
                    for (index, item) in array.values().as_ref().unwrap().iter().enumerate() {
                        if let FieldTypeEnum::StructE(structure) = item {
                            assert_eq!(
                                structure.get_field_value(PARTITION_KEY_NAME).unwrap(),
                                &FieldTypeEnum::from_i32(index as i32 + 1)
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Schema format:
/// `Schema(
///         acks field,
///         timeout field,
///         Array(
///               Schema(
///                      topic name field,
///                      Array(
///                            Schema(
///                                   partition field,
///                                   record set field
///                                   )
///                            )
///                      )
///                )
///         )
/// `
///

#[test]
fn test_produce_request_read_write() {
    // 设置订阅者
    tracing_subscriber::fmt()
        .with_env_filter("debug")
        .with_test_writer()
        .init();

    let produce_request = ProduceRequest {
        request_header: RequestHeader {
            len: 10,
            api_key: ApiKey::Produce,
            api_version: ApiVersion::V0,
            correlation_id: 1,
        },
        required_acks: Acks::default(),
        timeout: 1000,
        topic_data: vec![
            TopicData::new(
                "test_topic1",
                vec![PartitionData::new(1), PartitionData::new(2)],
            ),
            TopicData::new(
                "test_topic2",
                vec![PartitionData::new(1), PartitionData::new(2)],
            ),
        ],
    };
    let produce_structure = Protocol::encode_produce_request_to_struct(&produce_request).unwrap();
    let mut buffer = BytesMut::new();

    produce_structure.write_to(&mut buffer).unwrap();
    let read_produce_structure = Arc::clone(&PRODUCE_REQUEST_SCHEMA_V0)
        .read_struct_from_buffer(&mut buffer.freeze())
        .unwrap();
    assert_eq!(read_produce_structure, produce_structure);
}
