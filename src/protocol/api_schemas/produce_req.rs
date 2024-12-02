use std::borrow::Cow;
use std::sync::Arc;

use bytes::BytesMut;
use once_cell::sync::Lazy;
use tracing::trace;

use crate::message::TopicData;
use crate::message::{MemoryRecords, PartitionMsgData};
use crate::protocol::array::ArrayType;
use crate::protocol::primary_types::{NPString, PString, I16, I32};
use crate::protocol::schema::Schema;
use crate::protocol::types::DataType;
use crate::protocol::value_set::ValueSet;
use crate::protocol::{Acks, ApiKey, ApiVersion, ProtocolCodec};
use crate::request::produce::ProduceRequest;
use crate::AppError::ProtocolError;
use crate::AppResult;

pub const TRANSACTIONAL_ID_KEY_NAME: &str = "transactional_id";
pub const ACKS_KEY_NAME: &str = "acks";
pub const TIMEOUT_KEY_NAME: &str = "timeout";
pub const TOPIC_DATA_KEY_NAME: &str = "topic_data";
pub const TOPIC_KEY_NAME: &str = "topic";
pub const PARTITION_DATA_KEY_NAME: &str = "data";
pub const PARTITION_KEY_NAME: &str = "partition";
pub const RECORD_SET_KEY_NAME: &str = "record_set";

impl ProtocolCodec<ProduceRequest> for ProduceRequest {
    fn encode(self, api_version: &ApiVersion, _correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::Produce);
        let mut produce_req_value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut produce_req_value_set)
            .unwrap();
        let mut buffer = BytesMut::new();
        produce_req_value_set.write_to(&mut buffer);
        buffer
    }

    fn decode(buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<ProduceRequest> {
        trace!("ProduceRequest read start");
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::Produce);
        trace!("ProduceRequest schema read start ----{:?}", schema);
        let produce_req_value_set = schema.read_from(buffer)?;
        let produce_request = ProduceRequest::decode_from_value_set(produce_req_value_set)?;
        Ok(produce_request)
    }
}
///
/// decoding implementation
///
impl ProduceRequest {
    fn decode_from_value_set(mut produce_req_value_set: ValueSet) -> AppResult<ProduceRequest> {
        let ack_field_value: i16 = produce_req_value_set.get_field_value(ACKS_KEY_NAME).into();
        let required_acks = Acks::try_from(ack_field_value)?;

        let timeout = produce_req_value_set
            .get_field_value(TIMEOUT_KEY_NAME)
            .into();

        let topic_ary_field = produce_req_value_set.get_field_value(TOPIC_DATA_KEY_NAME);
        let topic_ary_type: ArrayType = topic_ary_field.into();
        let topic_ary = topic_ary_type
            .values
            .ok_or(ProtocolError(Cow::Borrowed("topic data is empty")))?;
        let mut topic_array = Vec::with_capacity(topic_ary.len());
        for topic_value in topic_ary {
            let mut topic_data_value_set: ValueSet = topic_value.into();
            // "topic" field
            let topic_name = topic_data_value_set.get_field_value(TOPIC_KEY_NAME).into();

            // "data" field
            let partition_data_field =
                topic_data_value_set.get_field_value(PARTITION_DATA_KEY_NAME);
            let partition_array_type: ArrayType = partition_data_field.into();

            let partition_data = partition_array_type
                .values
                .ok_or(ProtocolError(Cow::Borrowed("partition data is empty")))?;
            let mut partition_ary = Vec::with_capacity(partition_data.len());
            for partition in partition_data {
                if let DataType::ValueSet(mut value_set) = partition {
                    // "partition" field
                    let partition_num: i32 = value_set.get_field_value(PARTITION_KEY_NAME).into();
                    // "record_set" field
                    let records: MemoryRecords =
                        value_set.get_field_value(RECORD_SET_KEY_NAME).into();
                    partition_ary.push(PartitionMsgData::new(partition_num, records));
                }
            }
            topic_array.push(TopicData::new(topic_name, partition_ary));
        }
        Ok(ProduceRequest::new(
            None,
            required_acks,
            timeout,
            topic_array,
        ))
    }
}
///
/// encoding implementation
///
impl ProduceRequest {
    ///
    /// protocol format: `RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]`
    /// Schema format:
    /// `schema(
    ///          acks,
    ///          timeout,
    ///          topic_data:
    ///                     Array(
    ///                           Schema(
    ///                                  topic name field,
    ///                                  Array(
    ///                                        Schema(
    ///                                               partition field,
    ///                                               record set field
    ///                                               )
    ///                                        )
    ///                                 )
    ///                      )
    ///  )`
    fn encode_to_value_set(self, produce_req_value_set: &mut ValueSet) -> AppResult<()> {
        produce_req_value_set
            .append_field_value(TRANSACTIONAL_ID_KEY_NAME, self.transactional_id.into());

        produce_req_value_set.append_field_value(ACKS_KEY_NAME, self.required_acks.into());
        produce_req_value_set.append_field_value(TIMEOUT_KEY_NAME, DataType::from(self.timeout));

        let topic_data_ary =
            Self::generate_topic_data_array(self.topic_data, produce_req_value_set);
        let topic_data_schema = Schema::sub_schema_of_ary_field(
            produce_req_value_set.schema.clone(),
            TOPIC_DATA_KEY_NAME,
        );

        let array = DataType::array_of_value_set(topic_data_ary, topic_data_schema);
        produce_req_value_set.append_field_value(TOPIC_DATA_KEY_NAME, array);
        Ok(())
    }

    ///
    /// topic data `[TopicName [Partition MessageSetSize MessageSet]]`
    /// partition data `[Partition MessageSetSize MessageSet]`
    /// Schema format:
    ///                     `Array(
    ///                           Schema(
    ///                                  topic name field,
    ///                                  Array(
    ///                                        Schema(
    ///                                               partition field,
    ///                                               record set field
    ///                                               )
    ///                                        )
    ///                                 )
    ///                      )`

    fn generate_topic_data_array(
        topic_data: Vec<TopicData>,
        produce_req_value_set: &mut ValueSet,
    ) -> Vec<DataType> {
        let mut topic_data_ary = Vec::with_capacity(topic_data.len());

        for data in topic_data {
            let mut topic_data_value_set =
                produce_req_value_set.sub_valueset_of_ary_field(TOPIC_DATA_KEY_NAME);
            topic_data_value_set
                .append_field_value(TOPIC_KEY_NAME, DataType::from(data.topic_name.as_str()));

            let partition_data_ary =
                Self::generate_partition_data_array(data.partition_data, &topic_data_value_set);
            let partition_ary_schema = Schema::sub_schema_of_ary_field(
                topic_data_value_set.schema.clone(),
                PARTITION_DATA_KEY_NAME,
            );
            let partition_array =
                DataType::array_of_value_set(partition_data_ary, partition_ary_schema);
            topic_data_value_set.append_field_value(PARTITION_DATA_KEY_NAME, partition_array);
            topic_data_ary.push(DataType::ValueSet(topic_data_value_set));
        }

        topic_data_ary
    }

    ///
    /// topic data `[TopicName [Partition MessageSetSize MessageSet]]`
    /// partition data `[Partition MessageSetSize MessageSet]`
    fn generate_partition_data_array(
        partition_data_vec: Vec<PartitionMsgData>,
        topic_data_value_set: &ValueSet,
    ) -> Vec<DataType> {
        let mut partition_data_ary = Vec::with_capacity(partition_data_vec.len());

        for partition_data in partition_data_vec {
            let mut partition_value_set =
                topic_data_value_set.sub_valueset_of_ary_field(PARTITION_DATA_KEY_NAME);
            partition_value_set
                .append_field_value(PARTITION_KEY_NAME, partition_data.partition.into());
            partition_value_set.append_field_value(
                RECORD_SET_KEY_NAME,
                DataType::Records(partition_data.message_set),
            );
            partition_data_ary.push(DataType::ValueSet(partition_value_set));
        }
        partition_data_ary
    }
}

///
/// protocol format: `TopicName [Partition MessageSetSize MessageSet]`
/// Schema format:
/// `Schema(
///      topic name field,
///      Array(
///            Schema(
///                   partition field,
///                   record set field
///                   )
///            )
///  )`
pub static TOPIC_PRODUCE_DATA_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    // inner schema: [Partition MessageSetSize MessageSet]

    let inner_fields_desc_vec = vec![
        //Partition
        (0, PARTITION_KEY_NAME, DataType::I32(I32::default())),
        //MessageSetSize MessageSet
        (
            1,
            RECORD_SET_KEY_NAME,
            DataType::Records(MemoryRecords::empty()),
        ),
    ];
    let inner_schema = Schema::from_fields_desc_vec(inner_fields_desc_vec);

    // outer schema: TopicName inner_schema
    let data_ary = DataType::Array(ArrayType {
        can_be_empty: false,
        p_type: Arc::new(DataType::Schema(Arc::new(inner_schema))),
        values: None,
    });
    let outer_fields_desc_vec = vec![
        (0, TOPIC_KEY_NAME, DataType::PString(PString::default())),
        (1, PARTITION_DATA_KEY_NAME, data_ary),
    ];

    let schema = Schema::from_fields_desc_vec(outer_fields_desc_vec);
    Arc::new(schema)
});

///
/// protocol format: `RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]`
/// Schema format:
/// `schema(
///          acks,
///          timeout,
///          topic_data:
///           Array(
///               Schema(
///                      topic name field,
///                      Array(
///                            Schema(
///                                   partition field,
///                                   record set field
///                                   )
///                            )
///                     )
///          )
///  )`
pub static PRODUCE_REQUEST_SCHEMA_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, ACKS_KEY_NAME, DataType::I16(I16::default())),
        (1, TIMEOUT_KEY_NAME, DataType::I32(I32::default())),
        (
            2,
            TOPIC_DATA_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(Arc::clone(&TOPIC_PRODUCE_DATA_V0))),
                values: None,
            }),
        ),
    ]);
    Arc::new(schema)
});
///
/// protocol format: `Transactional_id RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]`
/// Schema format:
/// `schema(
///          transactional_id,
///          acks,
///          timeout,
///          topic_data:
///           Array(
///               Schema(
///                      topic name field,
///                      Array(
///                            Schema(
///                                   partition field,
///                                   record set field
///                                   )
///                            )
///                     )
///          )
///  )`
pub static PRODUCE_REQUEST_SCHEMA_V3: Lazy<Arc<Schema>> = Lazy::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (
            0,
            TRANSACTIONAL_ID_KEY_NAME,
            DataType::NPString(NPString::default()),
        ),
        (1, ACKS_KEY_NAME, DataType::I16(I16::default())),
        (2, TIMEOUT_KEY_NAME, DataType::I32(I32::default())),
        (
            3,
            TOPIC_DATA_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(Arc::clone(&TOPIC_PRODUCE_DATA_V0))),
                values: None,
            }),
        ),
    ]);
    Arc::new(schema)
});
#[cfg(test)]
mod tests {
    use crate::protocol::ProtocolCodec;

    use super::*;

    fn create_test_produce_request() -> ProduceRequest {
        let partition_data = vec![1, 2]
            .into_iter()
            .map(|partition| PartitionMsgData::new(partition, MemoryRecords::empty()))
            .collect::<Vec<_>>();
        let topic_data = vec!["test_topic1".to_string(), "test_topic2".to_string()]
            .into_iter()
            .map(|topic| TopicData::new(topic, partition_data.clone()))
            .collect::<Vec<_>>();

        ProduceRequest::new(None, Acks::default(), 1000, topic_data)
    }
    #[test]
    fn test_produce_request_to_value_set() {
        let produce_request = create_test_produce_request();
        let api_version = ApiVersion::default();
        let schema = match api_version {
            ApiVersion::V0 | ApiVersion::V1 | ApiVersion::V2 => {
                Arc::clone(&PRODUCE_REQUEST_SCHEMA_V0)
            }
            ApiVersion::V3 | ApiVersion::V4 => Arc::clone(&PRODUCE_REQUEST_SCHEMA_V3),
            ApiVersion::V5 => {
                //not implemented
                todo!()
            }
        };
        let mut produce_req_value_set = ValueSet::new(schema);

        // Call to_struct method
        produce_request
            .encode_to_value_set(&mut produce_req_value_set)
            .unwrap();

        // Check the returned Struct instance
        let acks_field_value = produce_req_value_set.get_field_value(ACKS_KEY_NAME);
        assert_eq!(acks_field_value, Acks::default().into());

        let timeout_field_value = produce_req_value_set.get_field_value(TIMEOUT_KEY_NAME);
        assert_eq!(timeout_field_value, 1000.into());

        // Check topic_data field
        let topic_data_field = produce_req_value_set
            .get_field_value(TOPIC_DATA_KEY_NAME)
            .into();
        if let DataType::Array(array) = topic_data_field {
            assert!(array.values.is_some(), "Topic data should not be empty");
            for (index, item) in array.values.unwrap().into_iter().enumerate() {
                if let DataType::ValueSet(mut topic_value_set) = item {
                    let topic_name_field_value = topic_value_set.get_field_value(TOPIC_KEY_NAME);
                    assert_eq!(
                        topic_name_field_value,
                        format!("test_topic{}", index + 1).into()
                    );
                    let partition_data_field_value = topic_value_set.get_field_value(PARTITION_DATA_KEY_NAME);
                    if let DataType::Array(array) = partition_data_field_value {
                        assert!(array.values.is_some(), "Partition data should not be empty");
                        for (index, item) in array.values.unwrap().into_iter().enumerate() {
                            if let DataType::ValueSet(mut partition_value_set) = item {
                                let partition_num_field_value =
                                    partition_value_set.get_field_value(PARTITION_KEY_NAME);
                                assert_eq!(
                                    partition_num_field_value,
                                    (index as i32 + 1).into()
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
    #[tokio::test]
    async fn test_produce_request_read_write() {
        tracing_subscriber::fmt()
            .with_env_filter("debug")
            .with_test_writer()
            .init();

        let produce_request = create_test_produce_request();
        let api_version = ApiVersion::default();
        let produce_request_clone = produce_request.clone();
        let buffer = produce_request.encode(&api_version, 0);
        let mut buffer = BytesMut::from(&buffer[..]);
        let target_produce_request = ProduceRequest::decode(&mut buffer, &api_version).unwrap();
        assert_eq!(produce_request_clone, target_produce_request);
    }
}
