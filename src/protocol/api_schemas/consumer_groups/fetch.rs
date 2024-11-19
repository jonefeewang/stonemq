use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use once_cell::sync::Lazy;

use crate::{
    message::TopicPartition,
    protocol::{
        array::ArrayType,
        primary_types::{PBytes, PString, I16, I32, I64, I8},
        schema::Schema,
        types::DataType,
        value_set::ValueSet,
        ApiKey, ApiVersion, ProtocolCodec,
    },
    request::fetch::{
        FetchRequest, FetchResponse, IsolationLevel, PartitionDataRep, PartitionDataReq,
    },
    AppError, AppResult,
};

pub static FETCH_PARTITION_REQUEST_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc = vec![
        (0, "partition", DataType::I32(I32::default())),
        (1, "fetch_offset", DataType::I64(I64::default())),
        (2, "log_start_offset", DataType::I64(I64::default())),
        (3, "max_bytes", DataType::I32(I32::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FETCH_TOPIC_REQUEST_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc = vec![
        (0, "topic", DataType::PString(PString::default())),
        (
            1,
            "partitions",
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(FETCH_PARTITION_REQUEST_V5_SCHEMA.clone())),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FETCH_REQUEST_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, "replica_id", DataType::I32(I32::default())),
        (1, "max_wait_time", DataType::I32(I32::default())),
        (2, "min_bytes", DataType::I32(I32::default())),
        (3, "max_bytes", DataType::I32(I32::default())),
        (4, "isolation_level", DataType::I8(I8::default())),
        (
            5,
            "topics",
            DataType::Array(ArrayType {
                can_be_empty: true,
                p_type: Arc::new(DataType::Schema(FETCH_TOPIC_REQUEST_V5_SCHEMA.clone())),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FETCH_ABORTED_TRANSACTION_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc = vec![
        (0, "producer_id", DataType::I64(I64::default())),
        (1, "first_offset", DataType::I64(I64::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
pub static FETCH_PARTITION_RESPONSE_HEADER_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc = vec![
        (0, "partition", DataType::I32(I32::default())),
        (1, "error_code", DataType::I16(I16::default())),
        (2, "high_watermark", DataType::I64(I64::default())),
        (3, "last_stable_offset", DataType::I64(I64::default())),
        (4, "log_start_offset", DataType::I64(I64::default())),
        (
            5,
            "aborted_transactions",
            DataType::Array(ArrayType {
                can_be_empty: true,
                p_type: Arc::new(DataType::Schema(
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
            "header",
            DataType::Schema(FETCH_PARTITION_RESPONSE_HEADER_V5_SCHEMA.clone()),
        ),
        (1, "records", DataType::PBytes(PBytes::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FETCH_TOPIC_RESPONSE_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc = vec![
        (0, "topic", DataType::PString(PString::default())),
        (
            1,
            "partition_responses",
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(FETCH_PARTITION_RESPONSE_V5_SCHEMA.clone())),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FETCH_RESPONSE_V5_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, "throttle_time_ms", DataType::I32(I32::default())),
        (
            1,
            "responses",
            DataType::Array(ArrayType {
                can_be_empty: true,
                p_type: Arc::new(DataType::Schema(FETCH_TOPIC_RESPONSE_V5_SCHEMA.clone())),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

use bytes::BytesMut;
use std::borrow::Cow;
use tokio::io::AsyncWriteExt;

impl ProtocolCodec<FetchRequest> for FetchRequest {
    async fn write_to<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        todo!()
    }

    fn read_from(buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<FetchRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::Fetch);
        let value_set = schema.read_from(buffer)?;
        FetchRequest::decode_from_value_set(value_set)
    }
}

impl ProtocolCodec<FetchResponse> for FetchResponse {
    async fn write_to<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::Fetch);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set)?;
        let body_size = value_set.size()?;
        let total_size = 4 + body_size;
        writer.write_i32(total_size as i32).await?;
        writer.write_i32(correlation_id).await?;
        value_set.write_to(writer).await?;
        writer.flush().await?;
        Ok(())
    }

    fn read_from(buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<FetchResponse> {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::Fetch);
        let value_set = schema.read_from(buffer)?;
        FetchResponse::decode_from_value_set(value_set)
    }
}

impl FetchRequest {
    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<FetchRequest> {
        let replica_id = value_set.get_field_value("replica_id")?.try_into()?;
        let max_wait_ms = value_set.get_field_value("max_wait_time")?.try_into()?;
        let min_bytes = value_set.get_field_value("min_bytes")?.try_into()?;
        let max_bytes = value_set.get_field_value("max_bytes")?.try_into()?;
        let isolation_level_i8: i8 = value_set.get_field_value("isolation_level")?.try_into()?;
        let isolation_level = IsolationLevel::try_from(isolation_level_i8)?;

        let topics_array: ArrayType = value_set.get_field_value("topics")?.try_into()?;
        let topics_values = topics_array
            .values
            .ok_or(AppError::ProtocolError(Cow::Borrowed(
                "topics array is empty",
            )))?;

        let mut topics = Vec::new();
        for topic_value in topics_values {
            let mut topic_value_set: ValueSet = topic_value.try_into()?;
            let topic: String = topic_value_set.get_field_value("topic")?.try_into()?;

            let partitions_array: ArrayType =
                topic_value_set.get_field_value("partitions")?.try_into()?;
            let partitions_values =
                partitions_array
                    .values
                    .ok_or(AppError::ProtocolError(Cow::Borrowed(
                        "partitions array is empty",
                    )))?;

            let mut partitions = Vec::new();
            for partition_value in partitions_values {
                let mut partition_value_set: ValueSet = partition_value.try_into()?;
                let partition = partition_value_set
                    .get_field_value("partition")?
                    .try_into()?;
                let fetch_offset = partition_value_set
                    .get_field_value("fetch_offset")?
                    .try_into()?;
                let log_start_offset = partition_value_set
                    .get_field_value("log_start_offset")?
                    .try_into()?;
                let max_bytes = partition_value_set
                    .get_field_value("max_bytes")?
                    .try_into()?;

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
    fn encode_to_value_set(mut self, response_value_set: &mut ValueSet) -> AppResult<()> {
        // 按topic分组responses
        let mut topic_responses: HashMap<String, Vec<(TopicPartition, PartitionDataRep)>> =
            HashMap::new();
        for (topic_partition, partition_data) in self.responses.into_iter() {
            topic_responses
                .entry(topic_partition.topic.clone())
                .or_default()
                .push((topic_partition, partition_data));
        }
        response_value_set.append_field_value("throttle_time_ms", self.throttle_time.into())?;

        // 构建topic responses数组
        let mut topic_responses_array = Vec::new();
        for (topic, partition_data_list) in topic_responses {
            let mut topic_value_set = response_value_set.sub_valueset_of_ary_field("responses")?;
            topic_value_set.append_field_value("topic", topic.into())?;

            // 构建partition responses数组
            let mut partition_responses = Vec::new();
            for (topic_partition, partition_data) in partition_data_list {
                let mut partition_value_set =
                    topic_value_set.sub_valueset_of_ary_field("partition_responses")?;

                let mut header_value_set =
                    partition_value_set.sub_valueset_of_schema_field("header")?;

                header_value_set
                    .append_field_value("partition", topic_partition.partition.into())?;
                header_value_set
                    .append_field_value("error_code", partition_data.error_code.into())?;
                header_value_set
                    .append_field_value("high_watermark", partition_data.high_watermark.into())?;
                header_value_set.append_field_value(
                    "last_stable_offset",
                    partition_data.last_stable_offset.into(),
                )?;
                header_value_set.append_field_value(
                    "log_start_offset",
                    partition_data.log_start_offset.into(),
                )?;

                // 处理aborted transactions, 因为当前版本不支持transaction, 所以aborted_transactions为空
                let aborted_txns_schema = header_value_set
                    .schema
                    .clone()
                    .sub_schema_of_ary_field("aborted_transactions")?;

                let aborted_txns_array = DataType::Array(ArrayType {
                    can_be_empty: false,
                    p_type: Arc::new(DataType::ValueSet(ValueSet::new(aborted_txns_schema))),
                    values: None,
                });
                header_value_set.append_field_value("aborted_transactions", aborted_txns_array)?;

                // 追加header
                partition_value_set.append_field_value("header", header_value_set.into())?;
                partition_value_set.append_field_value("records", partition_data.records.into())?;

                partition_responses.push(partition_value_set.into());
            }
            // 追加partition_responses
            let partition_responses_schema = topic_value_set
                .schema
                .clone()
                .sub_schema_of_ary_field("partition_responses")?;
            topic_value_set.append_field_value(
                "partition_responses",
                DataType::array_of_value_set(partition_responses, partition_responses_schema),
            )?;
            topic_responses_array.push(topic_value_set.into());
        }

        // 追加topic_responses 数组
        let topic_responses_schema = response_value_set
            .schema
            .clone()
            .sub_schema_of_ary_field("responses")?;
        response_value_set.append_field_value(
            "responses",
            DataType::array_of_value_set(topic_responses_array, topic_responses_schema),
        )?;

        Ok(())
    }

    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<FetchResponse> {
        todo!()
    }
}
