use crate::{
    protocol::types::DataType,
    request::consumer_group::{
        OffsetCommitRequest, OffsetCommitResponse, PartitionOffsetCommitData,
    },
    AppError::ProtocolError,
};
use std::{borrow::Cow, collections::HashMap};

use crate::{
    message::TopicPartition,
    protocol::{
        api_schemas::{
            consumer_protocol::TOPIC_KEY_NAME, ERROR_CODE_KEY_NAME, THROTTLE_TIME_KEY_NAME,
        },
        array::ArrayType,
        value_set::ValueSet,
        ApiKey, ApiVersion, ProtocolCodec,
    },
    request::{
        consumer_group::{FetchOffsetsRequest, FetchOffsetsResponse, PartitionOffsetData},
        errors::ErrorCode,
    },
    AppResult,
};
use tokio::io::AsyncWriteExt;

use super::{
    GROUP_GENERATION_ID_KEY_NAME, GROUP_ID_KEY_NAME, MEMBER_ID_KEY_NAME, METADATA_KEY_NAME,
    OFFSET_KEY_NAME, PARTITIONS_KEY_NAME, PARTITION_KEY_NAME, PARTITION_RESPONSES_KEY_NAME,
    RESPONSES_KEY_NAME, RETENTION_TIME_KEY_NAME, TOPICS_KEY_NAME,
};

impl ProtocolCodec<FetchOffsetsRequest> for FetchOffsetsRequest {
    fn read_from(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<FetchOffsetsRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::OffsetFetch);
        let fetch_offsets_req_value_set = schema.read_from(buffer)?;
        let fetch_offsets_request =
            FetchOffsetsRequest::decode_from_value_set(fetch_offsets_req_value_set)?;
        Ok(fetch_offsets_request)
    }

    async fn write_to<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        let _ = correlation_id;
        let _ = api_version;
        let _ = writer;
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
        let group_id = value_set.get_field_value(GROUP_ID_KEY_NAME)?.try_into()?;

        // Get the topics array
        let topics_array: ArrayType = value_set.get_field_value(TOPICS_KEY_NAME)?.try_into()?;

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
            let mut topic_value_set: ValueSet = topic_value.try_into()?;

            // Get topic name
            let topic_name: String = topic_value_set
                .get_field_value(TOPIC_KEY_NAME)?
                .try_into()?;

            // Get partitions for this topic
            let partitions_array: ArrayType = topic_value_set
                .get_field_value(PARTITIONS_KEY_NAME)?
                .try_into()?;

            let partitions = partitions_array
                .values
                .ok_or(ProtocolError(Cow::Borrowed("partitions is empty")))?;

            // Process each partition
            for partition_value in partitions {
                let mut partition_value_set: ValueSet = partition_value.try_into()?;
                let partition_id: i32 = partition_value_set
                    .get_field_value(PARTITION_KEY_NAME)?
                    .try_into()?;

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
    async fn write_to<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::OffsetFetch);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set)?;
        let body_size = value_set.size()?;
        let response_total_size = 4 + body_size;
        writer.write_i32(response_total_size as i32).await?;
        writer.write_i32(correlation_id).await?;
        value_set.write_to(writer).await?;
        Ok(())
    }

    fn read_from(
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
    fn encode_to_value_set(self, response_value_set: &mut ValueSet) -> AppResult<()> {
        // Encode throttle time
        response_value_set
            .append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into())?;

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
                response_value_set.sub_valueset_of_ary_field(RESPONSES_KEY_NAME)?;
            topic_value_set.append_field_value(TOPIC_KEY_NAME, topic_name.into())?;

            // Build partition responses for this topic
            let mut partition_response_array = Vec::new();
            for (partition_id, partition_data) in partitions {
                let mut partition_value_set =
                    topic_value_set.sub_valueset_of_ary_field(PARTITION_RESPONSES_KEY_NAME)?;
                partition_value_set.append_field_value(PARTITION_KEY_NAME, partition_id.into())?;
                partition_value_set
                    .append_field_value(OFFSET_KEY_NAME, partition_data.offset.into())?;
                partition_value_set
                    .append_field_value(METADATA_KEY_NAME, partition_data.metadata.into())?;
                let error_code = ErrorCode::from(&partition_data.error);
                partition_value_set
                    .append_field_value(ERROR_CODE_KEY_NAME, (error_code as i16).into())?;
                partition_response_array.push(DataType::ValueSet(partition_value_set));
            }

            // Add partition array to topic value set
            let partition_schema = topic_value_set
                .schema
                .clone()
                .sub_schema_of_ary_field(PARTITION_RESPONSES_KEY_NAME)?;
            topic_value_set.append_field_value(
                PARTITION_RESPONSES_KEY_NAME,
                DataType::array_of_value_set(partition_response_array, partition_schema),
            )?;
            topic_response_array.push(DataType::ValueSet(topic_value_set));
        }

        // Add topic array to response
        let topic_schema = response_value_set
            .schema
            .clone()
            .sub_schema_of_ary_field(RESPONSES_KEY_NAME)?;
        response_value_set.append_field_value(
            RESPONSES_KEY_NAME,
            DataType::array_of_value_set(topic_response_array, topic_schema),
        )?;
        Ok(())
    }
}

impl ProtocolCodec<OffsetCommitRequest> for OffsetCommitRequest {
    async fn write_to<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::OffsetCommit);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set)?;
        let body_size = value_set.size()?;
        let request_total_size = 4 + body_size;
        writer.write_i32(request_total_size as i32).await?;
        writer.write_i32(correlation_id).await?;
        value_set.write_to(writer).await?;
        Ok(())
    }

    fn read_from(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<OffsetCommitRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::OffsetCommit);
        let mut value_set = schema.read_from(buffer)?;
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
        let group_id = value_set.get_field_value(GROUP_ID_KEY_NAME)?.try_into()?;
        let generation_id = value_set
            .get_field_value(GROUP_GENERATION_ID_KEY_NAME)?
            .try_into()?;
        let member_id = value_set.get_field_value(MEMBER_ID_KEY_NAME)?.try_into()?;
        let retention_time = value_set
            .get_field_value(RETENTION_TIME_KEY_NAME)?
            .try_into()?;

        // Parse topics array
        let topic_array: ArrayType = value_set.get_field_value(TOPICS_KEY_NAME)?.try_into()?;
        let topic_values = topic_array
            .values
            .ok_or_else(|| ProtocolError(Cow::Borrowed("topics array is empty")))?;

        // Build offset data map
        let offset_data = topic_values.into_iter().try_fold(
            HashMap::new(),
            |mut acc,
             topic_value|
             -> AppResult<HashMap<TopicPartition, PartitionOffsetCommitData>> {
                let mut topic_value_set: ValueSet = topic_value.try_into()?;
                let topic_name: String = topic_value_set
                    .get_field_value(TOPIC_KEY_NAME)?
                    .try_into()?;

                // Parse partitions array for this topic
                let partition_array: ArrayType = topic_value_set
                    .get_field_value(PARTITIONS_KEY_NAME)?
                    .try_into()?;
                let partition_values = partition_array
                    .values
                    .ok_or_else(|| ProtocolError(Cow::Borrowed("partitions array is empty")))?;

                // Process each partition
                for partition_value in partition_values {
                    let mut partition_value_set: ValueSet = partition_value.try_into()?;
                    let partition_id: i32 = partition_value_set
                        .get_field_value(PARTITION_KEY_NAME)?
                        .try_into()?;
                    let offset: i64 = partition_value_set
                        .get_field_value(OFFSET_KEY_NAME)?
                        .try_into()?;
                    let metadata: String = partition_value_set
                        .get_field_value(METADATA_KEY_NAME)?
                        .try_into()?;

                    let partition_data = PartitionOffsetCommitData {
                        partition_id,
                        offset,
                        metadata,
                    };

                    acc.insert(
                        TopicPartition::new(topic_name.clone(), partition_id),
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
    fn encode_to_value_set(self, _: &mut ValueSet) -> AppResult<()> {
        todo!()
    }
}

impl ProtocolCodec<OffsetCommitResponse> for OffsetCommitResponse {
    async fn write_to<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::OffsetCommit);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set)?;
        let body_size = value_set.size()?;
        let response_total_size = 4 + body_size;
        writer.write_i32(response_total_size as i32).await?;
        writer.write_i32(correlation_id).await?;
        value_set.write_to(writer).await?;
        Ok(())
    }

    fn read_from(
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
    fn encode_to_value_set(self, value_set: &mut ValueSet) -> AppResult<()> {
        // Encode throttle time
        value_set.append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into())?;

        // Build topic responses array
        let mut topic_responses = Vec::with_capacity(self.responses.len());
        for (topic_partition, partition_errors) in self.responses {
            // Create a sub ValueSet for each topic
            let mut topic_value_set = value_set.sub_valueset_of_ary_field(RESPONSES_KEY_NAME)?;
            topic_value_set.append_field_value(TOPIC_KEY_NAME, topic_partition.topic.into())?;

            // Build partition responses array
            let mut partition_responses = Vec::with_capacity(partition_errors.len());
            for (partition_id, error) in partition_errors {
                // Create a sub ValueSet for each partition
                let mut partition_value_set =
                    topic_value_set.sub_valueset_of_ary_field(PARTITION_RESPONSES_KEY_NAME)?;
                partition_value_set.append_field_value(PARTITION_KEY_NAME, partition_id.into())?;

                // Convert error to protocol format
                let error_code = ErrorCode::from(&error);
                partition_value_set
                    .append_field_value(ERROR_CODE_KEY_NAME, (error_code as i16).into())?;
                partition_responses.push(DataType::ValueSet(partition_value_set));
            }

            // Get schema for partition responses
            let partition_schema = topic_value_set
                .schema
                .clone()
                .sub_schema_of_ary_field(PARTITION_RESPONSES_KEY_NAME)?;

            // Add partition responses array to topic ValueSet
            topic_value_set.append_field_value(
                PARTITION_RESPONSES_KEY_NAME,
                DataType::array_of_value_set(partition_responses, partition_schema),
            )?;

            topic_responses.push(DataType::ValueSet(topic_value_set));
        }

        // Get schema for topic responses
        let topic_schema = value_set
            .schema
            .clone()
            .sub_schema_of_ary_field(RESPONSES_KEY_NAME)?;

        // Add topic responses array to root ValueSet
        value_set.append_field_value(
            RESPONSES_KEY_NAME,
            DataType::array_of_value_set(topic_responses, topic_schema),
        )?;

        Ok(())
    }

    fn decode_from_value_set(value_set: ValueSet) -> AppResult<OffsetCommitResponse> {
        let _ = value_set;
        todo!()
    }
}
