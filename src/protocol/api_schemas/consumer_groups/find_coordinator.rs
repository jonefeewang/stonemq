use bytes::BytesMut;
use tokio::io::AsyncWriteExt;

use crate::{
    protocol::{types::DataType, value_set::ValueSet, ApiKey, ApiVersion, ProtocolCodec},
    AppResult,
};

use crate::protocol::api_schemas::consumer_groups::{
    COORDINATOR_KEY_NAME, ERROR_CODE_KEY_NAME, ERROR_MESSAGE_KEY_NAME, GROUP_ID_KEY_NAME,
    NODE_HOST_KEY_NAME, NODE_ID_KEY_NAME, NODE_PORT_KEY_NAME, THROTTLE_TIME_KEY_NAME,
};

use crate::request::consumer_group::{FindCoordinatorRequest, FindCoordinatorResponse};

use super::{COORDINATOR_KEY_KEY_NAME, COORDINATOR_TYPE_KEY_NAME};

impl ProtocolCodec<FindCoordinatorRequest> for FindCoordinatorRequest {
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

    fn decode(
        buffer: &mut BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<FindCoordinatorRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::FindCoordinator);
        let mut value_set = schema.read_from(buffer)?;
        let coordinator_key = value_set
            .get_field_value(COORDINATOR_KEY_KEY_NAME)?
            .try_into()?;
        let coordinator_type = value_set
            .get_field_value(COORDINATOR_TYPE_KEY_NAME)?
            .try_into()?;
        Ok(FindCoordinatorRequest {
            coordinator_key,
            coordinator_type,
        })
    }
}

impl ProtocolCodec<FindCoordinatorResponse> for FindCoordinatorResponse {
    async fn encode<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::FindCoordinator);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set)?;
        let body_size = value_set.size()?;
        // correlation_id + response_total_size
        let response_total_size = 4 + body_size;
        writer.write_i32(response_total_size as i32).await?;
        writer.write_i32(correlation_id).await?;
        value_set.write_to(writer).await?;
        writer.flush().await?;
        Ok(())
    }

    fn decode(
        buffer: &mut BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<FindCoordinatorResponse> {
        todo!()
    }
}

impl FindCoordinatorResponse {
    fn encode_to_value_set(self, response_valueset: &mut ValueSet) -> AppResult<()> {
        // outer value set
        response_valueset
            .append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into())?;
        response_valueset.append_field_value(ERROR_CODE_KEY_NAME, self.error.into())?;
        response_valueset.append_field_value(ERROR_MESSAGE_KEY_NAME, self.error_message.into())?;

        // nested value set
        let mut coordinator_valueset =
            response_valueset.sub_valueset_of_schema_field(COORDINATOR_KEY_NAME)?;
        coordinator_valueset.append_field_value(NODE_ID_KEY_NAME, self.node.node_id.into())?;
        coordinator_valueset.append_field_value(NODE_HOST_KEY_NAME, self.node.host.into())?;
        coordinator_valueset.append_field_value(NODE_PORT_KEY_NAME, self.node.port.into())?;

        // 将嵌套的value set 添加到response的value set中
        response_valueset.append_field_value(
            COORDINATOR_KEY_NAME,
            DataType::ValueSet(coordinator_valueset),
        )?;
        Ok(())
    }
}
