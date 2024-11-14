use once_cell::sync::Lazy;
use std::{borrow::Cow, sync::Arc};

use crate::{
    protocol::{
        api_schemas::{ERROR_CODE_KEY_NAME, THROTTLE_TIME_KEY_NAME},
        primary_types::{NPBytes, PString, I16, I32},
        schema::Schema,
        types::DataType,
        value_set::ValueSet,
        ApiKey, ApiVersion, ProtocolCodec,
    },
    request::{
        consumer_group::{HeartbeatRequest, HeartbeatResponse},
        errors::ErrorCode,
    },
    AppResult,
};
use tokio::io::AsyncWriteExt;

use super::{GROUP_GENERATION_ID_KEY_NAME, GROUP_ID_KEY_NAME, MEMBER_ID_KEY_NAME};

pub static HEARTBEAT_REQUEST_V1_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, GROUP_ID_KEY_NAME, DataType::PString(PString::default())),
        (
            1,
            GROUP_GENERATION_ID_KEY_NAME,
            DataType::I32(I32::default()),
        ),
        (2, MEMBER_ID_KEY_NAME, DataType::PString(PString::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static HEARTBEAT_RESPONSE_V1_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, DataType::I32(I32::default())),
        (1, ERROR_CODE_KEY_NAME, DataType::I16(I16::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

impl ProtocolCodec<HeartbeatRequest> for HeartbeatRequest {
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

    fn read_from(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<HeartbeatRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::Heartbeat);
        let mut value_set = schema.read_from(buffer)?;
        let heartbeat_request = HeartbeatRequest::decode_from_value_set(value_set)?;
        Ok(heartbeat_request)
    }
}

impl HeartbeatRequest {
    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<HeartbeatRequest> {
        let group_id = value_set.get_field_value(GROUP_ID_KEY_NAME)?.try_into()?;
        let generation_id = value_set
            .get_field_value(GROUP_GENERATION_ID_KEY_NAME)?
            .try_into()?;
        let member_id = value_set.get_field_value(MEMBER_ID_KEY_NAME)?.try_into()?;

        Ok(HeartbeatRequest {
            group_id,
            member_id,
            generation_id,
        })
    }
}

impl ProtocolCodec<HeartbeatResponse> for HeartbeatResponse {
    async fn write_to<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::Heartbeat);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set)?;
        let body_size = value_set.size();
        // correlation_id + response_total_size
        let response_total_size = 4 + body_size?;
        writer.write_i32(response_total_size as i32).await?;
        writer.write_i32(correlation_id).await?;
        value_set.write_to(writer).await?;
        writer.flush().await?;
        Ok(())
    }

    fn read_from(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<HeartbeatResponse> {
        todo!()
    }
}

impl HeartbeatResponse {
    fn encode_to_value_set(self, response_valueset: &mut ValueSet) -> AppResult<()> {
        response_valueset
            .append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into())?;
        response_valueset.append_field_value(ERROR_CODE_KEY_NAME, self.error_code.into())?;
        Ok(())
    }
}
