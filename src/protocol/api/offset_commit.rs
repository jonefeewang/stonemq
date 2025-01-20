// Copyright 2025 jonefeewang@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::{
    log::LogType,
    protocol::{
        base::{NPString, PString, ProtocolType, I16, I32, I64},
        schema_base::{Schema, ValueSet},
        types::ArrayType,
        ApiKey, ApiVersion, ProtocolCodec,
    },
    request::{ErrorCode, OffsetCommitRequest, OffsetCommitResponse, PartitionOffsetCommitData},
    AppError,
};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use crate::{message::TopicPartition, AppResult};
use bytes::{BufMut, BytesMut};

const GROUP_ID_KEY_NAME: &str = "group_id";
const GROUP_GENERATION_ID_KEY_NAME: &str = "group_generation_id";
const RESPONSES_KEY_NAME: &str = "responses";
const RETENTION_TIME_KEY_NAME: &str = "retention_time";
const PARTITION_KEY_NAME: &str = "partition";
const PARTITIONS_KEY_NAME: &str = "partitions";
const TOPIC_KEY_NAME: &str = "topic";

const OFFSET_KEY_NAME: &str = "offset";

const PARTITION_RESPONSES_KEY_NAME: &str = "partition_responses";
const METADATA_KEY_NAME: &str = "metadata";

const TOPICS_KEY_NAME: &str = "topics";
const ERROR_CODE_KEY_NAME: &str = "error_code";
const THROTTLE_TIME_KEY_NAME: &str = "throttle_time_ms";
const MEMBER_ID_KEY_NAME: &str = "member_id";

impl ProtocolCodec<OffsetCommitRequest> for OffsetCommitRequest {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema =
            Self::fetch_request_schema_for_api(api_version, &ApiKey::OffsetCommit).unwrap();
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set);
        let body_size = value_set.size();
        let request_total_size = 4 + body_size;
        let mut writer = BytesMut::with_capacity(request_total_size);
        writer.put_i32(request_total_size as i32);
        writer.put_i32(correlation_id);
        value_set.write_to(&mut writer);
        writer
    }

    fn decode(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<OffsetCommitRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::OffsetCommit)?;
        let value_set = schema.read_from(buffer)?;
        let offset_commit_request = OffsetCommitRequest::decode_from_value_set(value_set)?;
        Ok(offset_commit_request)
    }
}

impl OffsetCommitRequest {
    /// Decodes an OffsetCommitRequest from a ValueSet
    ///
    /// # Arguments
    /// * `value_set` - The ValueSet containing the encoded request
    ///
    /// # Returns
    /// * `AppResult<OffsetCommitRequest>` - The decoded request on success
    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<OffsetCommitRequest> {
        // Parse basic fields
        let group_id = value_set.get_field_value(GROUP_ID_KEY_NAME).into();
        let generation_id = value_set
            .get_field_value(GROUP_GENERATION_ID_KEY_NAME)
            .into();
        let member_id = value_set.get_field_value(MEMBER_ID_KEY_NAME).into();
        let retention_time = value_set.get_field_value(RETENTION_TIME_KEY_NAME).into();

        // Parse topics array
        let topic_array: ArrayType = value_set.get_field_value(TOPICS_KEY_NAME).into();
        let topic_values = topic_array
            .values
            .ok_or_else(|| AppError::MalformedProtocol("topics array is empty".to_string()))?;

        // Build offset data map
        let offset_data = topic_values.into_iter().try_fold(
            HashMap::new(),
            |mut acc,
             topic_value|
             -> AppResult<HashMap<TopicPartition, PartitionOffsetCommitData>> {
                let mut topic_value_set: ValueSet = topic_value.into();
                let topic_name: String = topic_value_set.get_field_value(TOPIC_KEY_NAME).into();

                // Parse partitions array for this topic
                let partition_array: ArrayType =
                    topic_value_set.get_field_value(PARTITIONS_KEY_NAME).into();
                let partition_values = partition_array.values.ok_or_else(|| {
                    AppError::MalformedProtocol("partitions array is empty".to_string())
                })?;

                // Process each partition
                for partition_value in partition_values {
                    let mut partition_value_set: ValueSet = partition_value.into();
                    let partition_id: i32 = partition_value_set
                        .get_field_value(PARTITION_KEY_NAME)
                        .into();
                    let offset: i64 = partition_value_set.get_field_value(OFFSET_KEY_NAME).into();
                    let metadata: Option<String> = partition_value_set
                        .get_field_value(METADATA_KEY_NAME)
                        .into();

                    let partition_data = PartitionOffsetCommitData {
                        partition_id,
                        offset,
                        metadata,
                    };

                    acc.insert(
                        TopicPartition::new(topic_name.clone(), partition_id, LogType::Queue),
                        partition_data,
                    );
                }
                Ok(acc)
            },
        )?;

        Ok(OffsetCommitRequest {
            group_id,
            generation_id,
            member_id,
            retention_time,
            offset_data,
        })
    }

    // 客户端会调用这个方法，服务端目前用不到，先不实现
    fn encode_to_value_set(self, _: &mut ValueSet) {
        todo!()
    }
}

impl ProtocolCodec<OffsetCommitResponse> for OffsetCommitResponse {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::OffsetCommit);
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
    ) -> AppResult<OffsetCommitResponse> {
        let _ = buffer;
        let _ = api_version;
        todo!()
    }
}

impl OffsetCommitResponse {
    /// Encodes an OffsetCommitResponse into a ValueSet format
    /// The encoding structure is:
    /// - throttle_time_ms: throttling time
    /// - responses: array
    ///   - topic: topic name
    ///   - partition_responses: array
    ///     - partition: partition id
    ///     - error_code: error code
    fn encode_to_value_set(self, value_set: &mut ValueSet) {
        // Encode throttle time
        value_set.append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into());

        // Build topic responses array
        let mut topic_responses = Vec::with_capacity(self.responses.len());
        for (topic_partition, partition_errors) in self.responses {
            // Create a sub ValueSet for each topic
            let mut topic_value_set = value_set.sub_valueset_of_ary_field(RESPONSES_KEY_NAME);
            topic_value_set.append_field_value(TOPIC_KEY_NAME, topic_partition.topic().into());

            // Build partition responses array
            let mut partition_responses = Vec::with_capacity(partition_errors.len());
            for (partition_id, error) in partition_errors {
                // Create a sub ValueSet for each partition
                let mut partition_value_set =
                    topic_value_set.sub_valueset_of_ary_field(PARTITION_RESPONSES_KEY_NAME);
                partition_value_set.append_field_value(PARTITION_KEY_NAME, partition_id.into());

                // Convert error to protocol format
                let error_code = ErrorCode::from(&error);
                partition_value_set
                    .append_field_value(ERROR_CODE_KEY_NAME, (error_code as i16).into());
                partition_responses.push(ProtocolType::ValueSet(partition_value_set));
            }

            // Get schema for partition responses
            let partition_schema = topic_value_set
                .schema
                .clone()
                .sub_schema_of_ary_field(PARTITION_RESPONSES_KEY_NAME);

            // Add partition responses array to topic ValueSet
            topic_value_set.append_field_value(
                PARTITION_RESPONSES_KEY_NAME,
                ProtocolType::array_of_value_set(partition_responses, partition_schema),
            );

            topic_responses.push(ProtocolType::ValueSet(topic_value_set));
        }

        // Get schema for topic responses
        let topic_schema = value_set
            .schema
            .clone()
            .sub_schema_of_ary_field(RESPONSES_KEY_NAME);

        // Add topic responses array to root ValueSet
        value_set.append_field_value(
            RESPONSES_KEY_NAME,
            ProtocolType::array_of_value_set(topic_responses, topic_schema),
        );
    }
}

pub static OFFSET_COMMIT_REQUEST_PARTITION_V2_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, PARTITION_KEY_NAME, ProtocolType::I32(I32::default())),
        (1, OFFSET_KEY_NAME, ProtocolType::I64(I64::default())),
        (
            2,
            METADATA_KEY_NAME,
            ProtocolType::NPString(NPString::default()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_COMMIT_REQUEST_TOPIC_V2_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, TOPIC_KEY_NAME, ProtocolType::PString(PString::default())),
        (
            1,
            PARTITIONS_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::Schema(
                    OFFSET_COMMIT_REQUEST_PARTITION_V2_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_COMMIT_REQUEST_V3_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (
            0,
            GROUP_ID_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            1,
            GROUP_GENERATION_ID_KEY_NAME,
            ProtocolType::I32(I32::default()),
        ),
        (
            2,
            MEMBER_ID_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            3,
            RETENTION_TIME_KEY_NAME,
            ProtocolType::I64(I64::default()),
        ),
        (
            4,
            TOPICS_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::Schema(
                    OFFSET_COMMIT_REQUEST_TOPIC_V2_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_COMMIT_RESPONSE_PARTITION_V0_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, PARTITION_KEY_NAME, ProtocolType::I32(I32::default())),
        (1, ERROR_CODE_KEY_NAME, ProtocolType::I16(I16::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_COMMIT_RESPONSE_TOPIC_V0_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, TOPIC_KEY_NAME, ProtocolType::PString(PString::default())),
        (
            1,
            PARTITION_RESPONSES_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::Schema(
                    OFFSET_COMMIT_RESPONSE_PARTITION_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static OFFSET_COMMIT_RESPONSE_V3_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, ProtocolType::I32(I32::default())),
        (
            1,
            RESPONSES_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::Schema(
                    OFFSET_COMMIT_RESPONSE_TOPIC_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
