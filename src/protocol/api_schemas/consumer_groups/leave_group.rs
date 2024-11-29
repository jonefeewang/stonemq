use crate::{
    protocol::{
        api_schemas::{ERROR_CODE_KEY_NAME, THROTTLE_TIME_KEY_NAME},
        value_set::ValueSet,
        ApiKey, ApiVersion, ProtocolCodec,
    },
    request::{
        consumer_group::{LeaveGroupRequest, LeaveGroupResponse},
        errors::ErrorCode,
    },
    AppResult,
};
use tokio::io::AsyncWriteExt;

use super::{GROUP_ID_KEY_NAME, MEMBER_ID_KEY_NAME};

impl ProtocolCodec<LeaveGroupRequest> for LeaveGroupRequest {
    fn decode(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<LeaveGroupRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::LeaveGroup);
        let leave_group_req_value_set = schema.read_from(buffer)?;
        let leave_group_request =
            LeaveGroupRequest::decode_from_value_set(leave_group_req_value_set)?;
        Ok(leave_group_request)
    }

    async fn encode<W>(
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
}

impl LeaveGroupRequest {
    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<LeaveGroupRequest> {
        let group_id = value_set.get_field_value(GROUP_ID_KEY_NAME)?.try_into()?;
        let member_id = value_set.get_field_value(MEMBER_ID_KEY_NAME)?.try_into()?;

        Ok(LeaveGroupRequest {
            group_id,
            member_id,
        })
    }
}

impl ProtocolCodec<LeaveGroupResponse> for LeaveGroupResponse {
    async fn encode<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::LeaveGroup);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set)?;
        let body_size = value_set.size()?;
        let response_total_size = 4 + body_size;
        writer.write_i32(response_total_size as i32).await?;
        writer.write_i32(correlation_id).await?;
        value_set.write_to(writer).await?;
        writer.flush().await?;
        Ok(())
    }

    fn decode(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<LeaveGroupResponse> {
        todo!()
    }
}

impl LeaveGroupResponse {
    fn encode_to_value_set(self, response_valueset: &mut ValueSet) -> AppResult<()> {
        response_valueset
            .append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into())?;
        let error_code = ErrorCode::from(&self.error);
        response_valueset.append_field_value(ERROR_CODE_KEY_NAME, (error_code as i16).into())?;
        Ok(())
    }
}
