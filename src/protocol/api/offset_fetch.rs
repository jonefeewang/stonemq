use crate::{
    protocol::{
        base::{NPString, PString, ProtocolType, I16, I32, I64},
        schema_base::{Schema, ValueSet},
        types::ArrayType,
        ApiKey, ApiVersion, ProtocolCodec,
    },
    request::{
        consumer_group::{FetchOffsetsRequest, FetchOffsetsResponse, PartitionOffsetData},
        errors::ErrorCode,
    },
    AppError::ProtocolError,
};
use std::{borrow::Cow, collections::HashMap, sync::Arc};

use crate::{message::TopicPartition, AppResult};
use bytes::{BufMut, BytesMut};
use once_cell::sync::Lazy;

const GROUP_ID_KEY_NAME: &str = "group_id";

const PARTITION_KEY_NAME: &str = "partition";
const PARTITIONS_KEY_NAME: &str = "partitions";
const TOPIC_KEY_NAME: &str = "topic";

const OFFSET_KEY_NAME: &str = "offset";

const PARTITION_RESPONSES_KEY_NAME: &str = "partition_responses";
const METADATA_KEY_NAME: &str = "metadata";

const TOPICS_KEY_NAME: &str = "topics";
const ERROR_CODE_KEY_NAME: &str = "error_code";

const THROTTLE_TIME_KEY_NAME: &str = "throttle_time_ms";
const RESPONSES_KEY_NAME: &str = "responses";

impl ProtocolCodec<FetchOffsetsRequest> for FetchOffsetsRequest {
    fn decode(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<FetchOffsetsRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::OffsetFetch);
        let fetch_offsets_req_value_set = schema.read_from(buffer)?;
        let fetch_offsets_request =
            FetchOffsetsRequest::decode_from_value_set(fetch_offsets_req_value_set)?;
        Ok(fetch_offsets_request)
    }

    fn encode(self, _api_version: &ApiVersion, _correlation_id: i32) -> BytesMut {
        todo!()
    }
}

impl FetchOffsetsRequest {
    /// Decodes a FetchOffsetsRequest from a ValueSet
    ///
    /// # Arguments
    /// * `value_set` - The ValueSet to decode from
    ///
    /// # Returns
    /// * `AppResult<FetchOffsetsRequest>` - The decoded request on success
    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<FetchOffsetsRequest> {
        // Extract the consumer group ID
        let group_id = value_set.get_field_value(GROUP_ID_KEY_NAME).into();

        // Get the topics array
        let topics_array: ArrayType = value_set.get_field_value(TOPICS_KEY_NAME).into();

        // Return early if no topics specified
        let topics = match topics_array.values {
            None => {
                return Ok(FetchOffsetsRequest {
                    group_id,
                    partitions: None,
                })
            }
            Some(topics) => topics,
        };

        // Process each topic and its partitions
        let mut topic_partitions = Vec::new();
        for topic_value in topics {
            let mut topic_value_set: ValueSet = topic_value.into();

            // Get topic name
            let topic_name: String = topic_value_set.get_field_value(TOPIC_KEY_NAME).into();

            // Get partitions for this topic
            let partitions_array: ArrayType =
                topic_value_set.get_field_value(PARTITIONS_KEY_NAME).into();

            let partitions = partitions_array
                .values
                .ok_or(ProtocolError(Cow::Borrowed("partitions is empty")))?;

            // Process each partition
            for partition_value in partitions {
                let mut partition_value_set: ValueSet = partition_value.into();
                let partition_id: i32 = partition_value_set
                    .get_field_value(PARTITION_KEY_NAME)
                    .into();

                topic_partitions.push(TopicPartition::new(topic_name.clone(), partition_id));
            }
        }

        Ok(FetchOffsetsRequest {
            group_id,
            partitions: Some(topic_partitions),
        })
    }
}

impl ProtocolCodec<FetchOffsetsResponse> for FetchOffsetsResponse {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::OffsetFetch);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set);
        let body_size = value_set.size();
        let response_total_size = 4 + body_size;
        let mut writer = BytesMut::with_capacity(response_total_size);
        writer.put_i32(response_total_size as i32);
        writer.put_i32(correlation_id);
        value_set.write_to(&mut writer);
        writer
    }

    fn decode(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<FetchOffsetsResponse> {
        let _ = api_version;
        let _ = buffer;
        todo!()
    }
}

impl FetchOffsetsResponse {
    /// Encodes the FetchOffsetsResponse into a ValueSet for network transmission
    ///
    /// # Arguments
    /// * `response_value_set` - The ValueSet to encode the response into
    ///
    /// # Returns
    /// * `AppResult<()>` - Ok if encoding succeeds, Err otherwise
    fn encode_to_value_set(self, response_value_set: &mut ValueSet) {
        // Encode throttle time
        response_value_set.append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into());

        // Group offsets by topic for encoding
        let topic_offset_map = self.offsets.into_iter().fold(
            HashMap::<String, Vec<(i32, PartitionOffsetData)>>::new(),
            |mut acc, (topic_partition, partition_data)| {
                acc.entry(topic_partition.topic)
                    .or_default()
                    .push((topic_partition.partition, partition_data));
                acc
            },
        );

        // Build topic responses array
        let mut topic_response_array = Vec::new();
        for (topic_name, partitions) in topic_offset_map {
            let mut topic_value_set =
                response_value_set.sub_valueset_of_ary_field(RESPONSES_KEY_NAME);
            topic_value_set.append_field_value(TOPIC_KEY_NAME, topic_name.into());

            // Build partition responses for this topic
            let mut partition_response_array = Vec::new();
            for (partition_id, partition_data) in partitions {
                let mut partition_value_set =
                    topic_value_set.sub_valueset_of_ary_field(PARTITION_RESPONSES_KEY_NAME);
                partition_value_set.append_field_value(PARTITION_KEY_NAME, partition_id.into());
                partition_value_set
                    .append_field_value(OFFSET_KEY_NAME, partition_data.offset.into());
                partition_value_set
                    .append_field_value(METADATA_KEY_NAME, partition_data.metadata.into());
                let error_code = ErrorCode::from(&partition_data.error);
                partition_value_set
                    .append_field_value(ERROR_CODE_KEY_NAME, (error_code as i16).into());
                partition_response_array.push(ProtocolType::ValueSet(partition_value_set));
            }

            // Add partition array to topic value set
            let partition_schema = topic_value_set
                .schema
                .clone()
                .sub_schema_of_ary_field(PARTITION_RESPONSES_KEY_NAME);
            topic_value_set.append_field_value(
                PARTITION_RESPONSES_KEY_NAME,
                ProtocolType::array_of_value_set(partition_response_array, partition_schema),
            );
            topic_response_array.push(ProtocolType::ValueSet(topic_value_set));
        }

        // Add topic array to response
        let topic_schema = response_value_set
            .schema
            .clone()
            .sub_schema_of_ary_field(RESPONSES_KEY_NAME);
        response_value_set.append_field_value(
            RESPONSES_KEY_NAME,
            ProtocolType::array_of_value_set(topic_response_array, topic_schema),
        );
        let error_code = ErrorCode::from(&self.error_code);
        response_value_set.append_field_value(ERROR_CODE_KEY_NAME, (error_code as i16).into());
    }
}

pub static OFFSET_FETCH_REQUEST_PARTITION_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> =
        vec![(0, PARTITION_KEY_NAME, ProtocolType::I32(I32::default()))];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_FETCH_REQUEST_TOPIC_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, TOPIC_KEY_NAME, ProtocolType::PString(PString::default())),
        (
            1,
            PARTITIONS_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::Schema(
                    OFFSET_FETCH_REQUEST_PARTITION_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];

    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_FETCH_REQUEST_V3_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (
            0,
            GROUP_ID_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            1,
            TOPICS_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: true,
                p_type: Arc::new(ProtocolType::Schema(
                    OFFSET_FETCH_REQUEST_TOPIC_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_FETCH_RESPONSE_PARTITION_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, PARTITION_KEY_NAME, ProtocolType::I32(I32::default())),
        (1, OFFSET_KEY_NAME, ProtocolType::I64(I64::default())),
        (
            2,
            METADATA_KEY_NAME,
            ProtocolType::NPString(NPString::default()),
        ),
        (3, ERROR_CODE_KEY_NAME, ProtocolType::I16(I16::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_FETCH_RESPONSE_TOPIC_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, TOPIC_KEY_NAME, ProtocolType::PString(PString::default())),
        (
            1,
            PARTITION_RESPONSES_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::Schema(
                    OFFSET_FETCH_RESPONSE_PARTITION_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_FETCH_RESPONSE_V3_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, ProtocolType::I32(I32::default())),
        (
            1,
            RESPONSES_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::Schema(
                    OFFSET_FETCH_RESPONSE_TOPIC_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
        (2, ERROR_CODE_KEY_NAME, ProtocolType::I16(I16::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
