use std::collections::HashMap;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use once_cell::sync::Lazy;
use tracing::trace;

use crate::protocol::base::ProtocolType;
use crate::protocol::schema_base::{Schema, ValueSet};
use crate::protocol::types::{ApiKey, ApiVersion};
use crate::protocol::ProtocolCodec;
use crate::request::{PartitionResponse, ProduceResponse};
use crate::AppResult;

// 定义全局常量
const RESPONSES_KEY_NAME: &str = "responses";

// topic level field names
const TOPIC_KEY_NAME: &str = "topic";
const PARTITION_RESPONSES_KEY_NAME: &str = "partition_responses";

// partition level field names
const PARTITION_KEY_NAME: &str = "partition";
const ERROR_CODE_KEY_NAME: &str = "error_code";
const THROTTLE_TIME_MS_KEY_NAME: &str = "throttle_time_ms";

// CORRUPT_MESSAGE (2)
// UNKNOWN_TOPIC_OR_PARTITION (3)
// NOT_LEADER_FOR_PARTITION (6)
// MESSAGE_TOO_LARGE (10)
// INVALID_TOPIC (17)
// RECORD_LIST_TOO_LARGE (18)
// NOT_ENOUGH_REPLICAS (19)
// NOT_ENOUGH_REPLICAS_AFTER_APPEND (20)
// INVALID_REQUIRED_ACKS (21)
// TOPIC_AUTHORIZATION_FAILED (29)
// UNSUPPORTED_FOR_MESSAGE_FORMAT (43)
// INVALID_PRODUCER_EPOCH (47)
// CLUSTER_AUTHORIZATION_FAILED (31)
// TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)

const BASE_OFFSET_KEY_NAME: &str = "base_offset";
const LOG_APPEND_TIME_KEY_NAME: &str = "log_append_time";

pub static PRODUCE_RESPONSE_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let partition_reps_schema = Schema::from_fields_desc_vec(vec![
        (0, PARTITION_KEY_NAME, 0i32.into()),
        (1, ERROR_CODE_KEY_NAME, 0i16.into()),
        (2, BASE_OFFSET_KEY_NAME, 0i64.into()),
    ]);
    let topic_reps_schema = Schema::from_fields_desc_vec(vec![
        (0, TOPIC_KEY_NAME, "".into()),
        (
            1,
            PARTITION_RESPONSES_KEY_NAME,
            ProtocolType::array_of_schema(Arc::new(partition_reps_schema)),
        ),
    ]);
    let responses_schema = Schema::from_fields_desc_vec(vec![(
        0,
        RESPONSES_KEY_NAME,
        ProtocolType::array_of_schema(Arc::new(topic_reps_schema)),
    )]);

    Arc::new(responses_schema)
});
pub static PRODUCE_RESPONSE_V1: Lazy<Arc<Schema>> = Lazy::new(|| {
    let partition_reps_schema = Schema::from_fields_desc_vec(vec![
        (0, PARTITION_KEY_NAME, 0i32.into()),
        (1, ERROR_CODE_KEY_NAME, 0i16.into()),
        (2, BASE_OFFSET_KEY_NAME, 0i64.into()),
    ]);
    let topic_reps_schema = Schema::from_fields_desc_vec(vec![
        (0, TOPIC_KEY_NAME, "".into()),
        (
            1,
            PARTITION_RESPONSES_KEY_NAME,
            ProtocolType::array_of_schema(Arc::new(partition_reps_schema)),
        ),
    ]);
    let responses_schema = Schema::from_fields_desc_vec(vec![
        (
            0,
            RESPONSES_KEY_NAME,
            ProtocolType::array_of_schema(Arc::new(topic_reps_schema)),
        ),
        (1, THROTTLE_TIME_MS_KEY_NAME, 0i32.into()),
    ]);

    Arc::new(responses_schema)
});
pub static PRODUCE_RESPONSE_V2: Lazy<Arc<Schema>> = Lazy::new(|| {
    let partition_reps_schema = Schema::from_fields_desc_vec(vec![
        (0, PARTITION_KEY_NAME, 0i32.into()),
        (1, ERROR_CODE_KEY_NAME, 0i16.into()),
        (2, BASE_OFFSET_KEY_NAME, 0i64.into()),
        (3, LOG_APPEND_TIME_KEY_NAME, 0i64.into()),
    ]);
    let topic_reps_schema = Schema::from_fields_desc_vec(vec![
        (0, TOPIC_KEY_NAME, "".into()),
        (
            1,
            PARTITION_RESPONSES_KEY_NAME,
            ProtocolType::array_of_schema(Arc::new(partition_reps_schema)),
        ),
    ]);
    let responses_schema = Schema::from_fields_desc_vec(vec![
        (
            0,
            RESPONSES_KEY_NAME,
            ProtocolType::array_of_schema(Arc::new(topic_reps_schema)),
        ),
        (1, THROTTLE_TIME_MS_KEY_NAME, 0i32.into()),
    ]);

    Arc::new(responses_schema)
});

impl ProduceResponse {
    pub(crate) fn encode_to_value_set(self, produce_reps_valueset: &mut ValueSet) -> AppResult<()> {
        let mut topic_responses_map: HashMap<String, Vec<PartitionResponse>> = HashMap::new();
        for (key, value) in self.responses.into_iter() {
            topic_responses_map
                .entry(key.topic)
                .or_default()
                .push(value);
        }
        let mut topic_response_ary = Vec::with_capacity(topic_responses_map.len());

        for (topic, partition_response) in topic_responses_map {
            let mut topic_response =
                produce_reps_valueset.sub_valueset_of_ary_field(RESPONSES_KEY_NAME);
            topic_response.append_field_value(TOPIC_KEY_NAME, topic.into());

            let mut partition_response_ary = Vec::with_capacity(partition_response.len());
            for partition_response in partition_response {
                let mut partition_response_valueset =
                    topic_response.sub_valueset_of_ary_field(PARTITION_RESPONSES_KEY_NAME);
                partition_response_valueset
                    .append_field_value(PARTITION_KEY_NAME, partition_response.partition.into());
                partition_response_valueset
                    .append_field_value(ERROR_CODE_KEY_NAME, partition_response.error_code.into());
                partition_response_valueset.append_field_value(
                    BASE_OFFSET_KEY_NAME,
                    partition_response.base_offset.into(),
                );

                if partition_response_valueset
                    .schema
                    .has_field(LOG_APPEND_TIME_KEY_NAME)
                {
                    partition_response_valueset.append_field_value(
                        LOG_APPEND_TIME_KEY_NAME,
                        partition_response.log_append_time.into(),
                    );
                }

                partition_response_ary.push(ProtocolType::ValueSet(partition_response_valueset));
            }
            let partition_schema = topic_response
                .schema
                .clone()
                .sub_schema_of_ary_field(PARTITION_RESPONSES_KEY_NAME);
            topic_response.append_field_value(
                PARTITION_RESPONSES_KEY_NAME,
                ProtocolType::array_of_value_set(partition_response_ary, partition_schema),
            );

            topic_response_ary.push(ProtocolType::ValueSet(topic_response));
        }
        let topic_schema = produce_reps_valueset
            .schema
            .clone()
            .sub_schema_of_ary_field(RESPONSES_KEY_NAME);
        produce_reps_valueset.append_field_value(
            RESPONSES_KEY_NAME,
            ProtocolType::array_of_value_set(topic_response_ary, topic_schema),
        );
        if produce_reps_valueset
            .schema
            .has_field(THROTTLE_TIME_MS_KEY_NAME)
        {
            produce_reps_valueset.append_field_value(
                THROTTLE_TIME_MS_KEY_NAME,
                self.throttle_time.unwrap_or(0).into(),
            );
        }
        Ok(())
    }
}

impl ProtocolCodec<ProduceResponse> for ProduceResponse {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::Produce);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set).unwrap();
        let body_size = value_set.size();
        let mut writer = BytesMut::with_capacity(4 + body_size);

        // correlation_id + response_total_size
        let response_total_size = 4 + body_size;
        writer.put_i32(response_total_size as i32);
        writer.put_i32(correlation_id);
        value_set.write_to(&mut writer);
        trace!(
            "write response total size:{} with correlation_id:{}",
            response_total_size,
            correlation_id
        );
        writer
    }

    fn decode(_buffer: &mut BytesMut, _api_version: &ApiVersion) -> AppResult<ProduceResponse> {
        todo!()
    }
}
