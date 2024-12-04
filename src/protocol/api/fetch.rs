use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use once_cell::sync::Lazy;

use crate::{
    message::{MemoryRecords, TopicPartition},
    protocol::{
        base::{PString, ProtocolType, I16, I32, I64, I8},
        schema_base::{Schema, ValueSet},
        types::ArrayType,
        ApiKey, ApiVersion, ProtocolCodec,
    },
    request::{FetchRequest, FetchResponse, IsolationLevel, PartitionDataRep, PartitionDataReq},
    AppError, AppResult,
};

use bytes::{BufMut, BytesMut};

impl ProtocolCodec<FetchRequest> for FetchRequest {
    fn encode(self, _api_version: &ApiVersion, _correlation_id: i32) -> BytesMut {
        todo!()
    }

    fn decode(buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<FetchRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::Fetch);
        let value_set = schema.read_from(buffer)?;
        FetchRequest::decode_from_value_set(value_set)
    }
}

impl ProtocolCodec<FetchResponse> for FetchResponse {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::Fetch);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set);
        let body_size = value_set.size();
        let total_size = 4 + body_size;
        let mut writer = BytesMut::with_capacity(total_size);
        writer.put_i32(total_size as i32);
        writer.put_i32(correlation_id);
        value_set.write_to(&mut writer);
        writer
    }

    fn decode(buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<FetchResponse> {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::Fetch);
        let value_set = schema.read_from(buffer)?;
        FetchResponse::decode_from_value_set(value_set)
    }
}

impl FetchRequest {
    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<FetchRequest> {
        let replica_id = value_set.get_field_value("replica_id").into();
        let max_wait_ms = value_set.get_field_value("max_wait_time").into();
        let min_bytes = value_set.get_field_value("min_bytes").into();
        let max_bytes = value_set.get_field_value("max_bytes").into();
        let isolation_level_i8: i8 = value_set.get_field_value("isolation_level").into();
        let isolation_level = IsolationLevel::try_from(isolation_level_i8)?;

        let topics_array: ArrayType = value_set.get_field_value("topics").into();
        let topics_values = topics_array.values.ok_or(AppError::MalformedProtocol(
            "topics array is empty".to_string(),
        ))?;

        let mut topics = Vec::new();
        for topic_value in topics_values {
            let mut topic_value_set: ValueSet = topic_value.into();
            let topic: String = topic_value_set.get_field_value("topic").into();

            let partitions_array: ArrayType = topic_value_set.get_field_value("partitions").into();
            let partitions_values = partitions_array.values.ok_or(AppError::MalformedProtocol(
                "partitions array is empty".to_string(),
            ))?;

            let mut partitions = Vec::new();
            for partition_value in partitions_values {
                let mut partition_value_set: ValueSet = partition_value.into();
                let partition = partition_value_set.get_field_value("partition").into();
                let fetch_offset = partition_value_set.get_field_value("fetch_offset").into();
                let log_start_offset = partition_value_set
                    .get_field_value("log_start_offset")
                    .into();
                let max_bytes = partition_value_set.get_field_value("max_bytes").into();

                partitions.push((partition, fetch_offset, log_start_offset, max_bytes));
            }

            topics.push((topic, partitions));
        }
        let mut fetch_data = BTreeMap::new();
        for (topic, partitions) in topics {
            for (partition, fetch_offset, log_start_offset, max_bytes) in partitions {
                fetch_data.insert(
                    TopicPartition::new(topic.clone(), partition),
                    PartitionDataReq::new(fetch_offset, log_start_offset, max_bytes),
                );
            }
        }

        Ok(FetchRequest {
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            fetch_data,
        })
    }
}

impl FetchResponse {
    fn encode_to_value_set(self, response_value_set: &mut ValueSet) {
        // 按topic分组responses
        let mut topic_responses: HashMap<String, Vec<(TopicPartition, PartitionDataRep)>> =
            HashMap::new();
        for (topic_partition, partition_data) in self.responses.into_iter() {
            topic_responses
                .entry(topic_partition.topic.clone())
                .or_default()
                .push((topic_partition, partition_data));
        }
        response_value_set.append_field_value("throttle_time_ms", self.throttle_time.into());

        // 构建topic responses数组
        let mut topic_responses_array = Vec::new();
        for (topic, partition_data_list) in topic_responses {
            let mut topic_value_set = response_value_set.sub_valueset_of_ary_field("responses");
            topic_value_set.append_field_value("topic", topic.into());

            // 构建partition responses数组
            let mut partition_responses = Vec::new();
            for (topic_partition, partition_data) in partition_data_list {
                let mut partition_value_set =
                    topic_value_set.sub_valueset_of_ary_field("partition_responses");

                let mut header_value_set =
                    partition_value_set.sub_valueset_of_schema_field("partition_header");

                header_value_set.append_field_value("partition", topic_partition.partition.into());
                header_value_set.append_field_value("error_code", partition_data.error_code.into());
                header_value_set
                    .append_field_value("high_watermark", partition_data.high_watermark.into());
                header_value_set.append_field_value(
                    "last_stable_offset",
                    partition_data.last_stable_offset.into(),
                );
                header_value_set
                    .append_field_value("log_start_offset", partition_data.log_start_offset.into());

                // 处理aborted transactions, 因为当前版本不支持transaction, 所以aborted_transactions为空
                let aborted_txns_schema = header_value_set
                    .schema
                    .clone()
                    .sub_schema_of_ary_field("aborted_transactions");

                let aborted_txns_array = ProtocolType::Array(ArrayType {
                    can_be_empty: false,
                    p_type: Arc::new(ProtocolType::ValueSet(ValueSet::new(aborted_txns_schema))),
                    values: None,
                });
                header_value_set.append_field_value("aborted_transactions", aborted_txns_array);

                // 追加header
                partition_value_set.append_field_value("partition_header", header_value_set.into());
                partition_value_set.append_field_value("record_set", partition_data.records.into());

                partition_responses.push(partition_value_set.into());
            }
            // 追加partition_responses
            let partition_responses_schema = topic_value_set
                .schema
                .clone()
                .sub_schema_of_ary_field("partition_responses");
            topic_value_set.append_field_value(
                "partition_responses",
                ProtocolType::array_of_value_set(partition_responses, partition_responses_schema),
            );
            topic_responses_array.push(topic_value_set.into());
        }

        // 追加topic_responses 数组
        let topic_responses_schema = response_value_set
            .schema
            .clone()
            .sub_schema_of_ary_field("responses");
        response_value_set.append_field_value(
            "responses",
            ProtocolType::array_of_value_set(topic_responses_array, topic_responses_schema),
        );
    }

    fn decode_from_value_set(_value_set: ValueSet) -> AppResult<FetchResponse> {
        todo!()
    }
}

pub static FETCH_PARTITION_REQUEST_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc = vec![
        (0, "partition", ProtocolType::I32(I32::default())),
        (1, "fetch_offset", ProtocolType::I64(I64::default())),
        (2, "log_start_offset", ProtocolType::I64(I64::default())),
        (3, "max_bytes", ProtocolType::I32(I32::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FETCH_TOPIC_REQUEST_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc = vec![
        (0, "topic", ProtocolType::PString(PString::default())),
        (
            1,
            "partitions",
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::Schema(
                    FETCH_PARTITION_REQUEST_V5_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FETCH_REQUEST_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, "replica_id", ProtocolType::I32(I32::default())),
        (1, "max_wait_time", ProtocolType::I32(I32::default())),
        (2, "min_bytes", ProtocolType::I32(I32::default())),
        (3, "max_bytes", ProtocolType::I32(I32::default())),
        (4, "isolation_level", ProtocolType::I8(I8::default())),
        (
            5,
            "topics",
            ProtocolType::Array(ArrayType {
                can_be_empty: true,
                p_type: Arc::new(ProtocolType::Schema(FETCH_TOPIC_REQUEST_V5_SCHEMA.clone())),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FETCH_ABORTED_TRANSACTION_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc = vec![
        (0, "producer_id", ProtocolType::I64(I64::default())),
        (1, "first_offset", ProtocolType::I64(I64::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
pub static FETCH_PARTITION_RESPONSE_HEADER_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc = vec![
        (0, "partition", ProtocolType::I32(I32::default())),
        (1, "error_code", ProtocolType::I16(I16::default())),
        (2, "high_watermark", ProtocolType::I64(I64::default())),
        (3, "last_stable_offset", ProtocolType::I64(I64::default())),
        (4, "log_start_offset", ProtocolType::I64(I64::default())),
        (
            5,
            "aborted_transactions",
            ProtocolType::Array(ArrayType {
                can_be_empty: true,
                p_type: Arc::new(ProtocolType::Schema(
                    FETCH_ABORTED_TRANSACTION_V5_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FETCH_PARTITION_RESPONSE_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc = vec![
        (
            0,
            "partition_header",
            ProtocolType::Schema(FETCH_PARTITION_RESPONSE_HEADER_V5_SCHEMA.clone()),
        ),
        (
            1,
            "record_set",
            ProtocolType::Records(MemoryRecords::empty()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FETCH_TOPIC_RESPONSE_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc = vec![
        (0, "topic", ProtocolType::PString(PString::default())),
        (
            1,
            "partition_responses",
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::Schema(
                    FETCH_PARTITION_RESPONSE_V5_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FETCH_RESPONSE_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, "throttle_time_ms", ProtocolType::I32(I32::default())),
        (
            1,
            "responses",
            ProtocolType::Array(ArrayType {
                can_be_empty: true,
                p_type: Arc::new(ProtocolType::Schema(FETCH_TOPIC_RESPONSE_V5_SCHEMA.clone())),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
