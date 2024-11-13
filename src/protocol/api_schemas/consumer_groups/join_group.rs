use crate::protocol::api_schemas::consumer_groups::{
    GENERATION_ID_KEY_NAME, GROUP_ID_KEY_NAME, GROUP_PROTOCOLS_KEY_NAME, GROUP_PROTOCOL_KEY_NAME,
    LEADER_ID_KEY_NAME, MEMBERS_KEY_NAME, MEMBER_ID_KEY_NAME, MEMBER_METADATA_KEY_NAME,
    PROTOCOL_METADATA_KEY_NAME, PROTOCOL_NAME_KEY_NAME, PROTOCOL_TYPE_KEY_NAME,
    REBALANCE_TIMEOUT_KEY_NAME, SESSION_TIMEOUT_KEY_NAME,
};
use crate::protocol::api_schemas::consumer_protocol::ProtocolMetadata;
use crate::protocol::api_schemas::{ERROR_CODE_KEY_NAME, THROTTLE_TIME_KEY_NAME};
use crate::protocol::array::ArrayType;
use crate::protocol::primary_types::{NPBytes, PBytes, PString, I16, I32};
use crate::protocol::schema::Schema;
use crate::protocol::types::DataType;
use crate::protocol::value_set::ValueSet;
use crate::protocol::{ApiKey, ApiVersion, ProtocolCodec};
use crate::request::consumer_group::{JoinGroupRequest, JoinGroupResponse};
use crate::{AppError, AppResult};
use bytes::BytesMut;
use once_cell::sync::Lazy;
use std::borrow::Cow;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

pub static JOIN_GROUP_REQUEST_PROTOCOL_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (
            0,
            PROTOCOL_NAME_KEY_NAME,
            DataType::PString(PString::default()),
        ),
        (
            1,
            PROTOCOL_METADATA_KEY_NAME,
            DataType::PBytes(PBytes::default()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
pub static JOIN_GROUP_REQUEST_V2_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, GROUP_ID_KEY_NAME, DataType::PString(PString::default())),
        (1, SESSION_TIMEOUT_KEY_NAME, DataType::I32(I32::default())),
        (2, REBALANCE_TIMEOUT_KEY_NAME, DataType::I32(I32::default())),
        (3, MEMBER_ID_KEY_NAME, DataType::PString(PString::default())),
        (
            4,
            PROTOCOL_TYPE_KEY_NAME,
            DataType::PString(PString::default()),
        ),
        (
            5,
            GROUP_PROTOCOLS_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(
                    JOIN_GROUP_REQUEST_PROTOCOL_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
pub static JOIN_GROUP_RESPONSE_MEMBER_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, MEMBER_ID_KEY_NAME, DataType::PString(PString::default())),
        (
            1,
            MEMBER_METADATA_KEY_NAME,
            DataType::PBytes(PBytes::default()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
pub static JOIN_GROUP_RESPONSE_V2_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, DataType::I32(I32::default())),
        (1, ERROR_CODE_KEY_NAME, DataType::I16(I16::default())),
        (2, GENERATION_ID_KEY_NAME, DataType::I32(I32::default())),
        (
            3,
            GROUP_PROTOCOL_KEY_NAME,
            DataType::PString(PString::default()),
        ),
        (4, LEADER_ID_KEY_NAME, DataType::PString(PString::default())),
        (5, MEMBER_ID_KEY_NAME, DataType::PString(PString::default())),
        (
            6,
            MEMBERS_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(
                    JOIN_GROUP_RESPONSE_MEMBER_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

impl ProtocolCodec<JoinGroupRequest> for JoinGroupRequest {
    fn read_from(
        buffer: &mut bytes::BytesMut,
        api_version: &crate::protocol::ApiVersion,
    ) -> AppResult<JoinGroupRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::JoinGroup);
        let join_group_req_value_set = schema.read_from(buffer)?;
        let join_group_request = JoinGroupRequest::decode_from_value_set(join_group_req_value_set)?;
        Ok(join_group_request)
    }

    async fn write_to<W>(
        self,
        buffer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        todo!()
    }
}

impl JoinGroupRequest {
    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<JoinGroupRequest> {
        let group_id = value_set.get_field_value(GROUP_ID_KEY_NAME)?.try_into()?;
        let session_timeout = value_set
            .get_field_value(SESSION_TIMEOUT_KEY_NAME)?
            .try_into()?;
        let rebalance_timeout = value_set
            .get_field_value(REBALANCE_TIMEOUT_KEY_NAME)?
            .try_into()?;
        let member_id = value_set.get_field_value(MEMBER_ID_KEY_NAME)?.try_into()?;
        let protocol_type = value_set
            .get_field_value(PROTOCOL_TYPE_KEY_NAME)?
            .try_into()?;
        let group_protocols: ArrayType = value_set
            .get_field_value(GROUP_PROTOCOLS_KEY_NAME)?
            .try_into()?;
        let protocol_ary = group_protocols
            .values
            .ok_or(AppError::ProtocolError(Cow::Borrowed(
                "group protocols is empty",
            )))?;
        let mut group_protocols = Vec::with_capacity(protocol_ary.len());
        for protocol in protocol_ary {
            let mut protocol_metadata: ValueSet = protocol.try_into()?;
            let protocol_name = protocol_metadata
                .get_field_value(PROTOCOL_NAME_KEY_NAME)?
                .try_into()?;
            let protocol_metadata: PBytes = protocol_metadata
                .get_field_value(PROTOCOL_METADATA_KEY_NAME)?
                .try_into()?;

            group_protocols.push(ProtocolMetadata {
                name: protocol_name,
                metadata: protocol_metadata.value,
            });
        }

        Ok(JoinGroupRequest {
            client_id: "".to_string(),
            client_host: "".to_string(),
            group_id,
            session_timeout,
            rebalance_timeout,
            member_id,
            protocol_type,
            group_protocols,
        })
    }
}

impl ProtocolCodec<JoinGroupResponse> for JoinGroupResponse {
    async fn write_to<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::JoinGroup);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set)?;
        let body_size = value_set.size();
        // correlation_id + response_total_size
        let response_total_size = 4 + body_size?;
        writer.write_i32(response_total_size as i32).await?;
        writer.write_i32(correlation_id).await?;
        value_set.write_to(writer).await?;
        Ok(())
    }

    fn read_from(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<JoinGroupResponse> {
        todo!()
    }
}

impl JoinGroupResponse {
    fn encode_to_value_set(self, response_valueset: &mut ValueSet) -> AppResult<()> {
        response_valueset.append_field_value(
            THROTTLE_TIME_KEY_NAME,
            self.throttle_time.unwrap_or(0).into(),
        )?;
        response_valueset.append_field_value(ERROR_CODE_KEY_NAME, self.error_code.into())?;
        response_valueset.append_field_value(GENERATION_ID_KEY_NAME, self.generation_id.into())?;
        response_valueset
            .append_field_value(GROUP_PROTOCOL_KEY_NAME, self.group_protocol.into())?;
        response_valueset.append_field_value(LEADER_ID_KEY_NAME, self.leader_id.into())?;
        response_valueset.append_field_value(MEMBER_ID_KEY_NAME, self.member_id.into())?;

        let mut member_ary = Vec::with_capacity(self.members.len());
        for (member_id, member_metadata) in self.members {
            let mut member_valueset =
                response_valueset.sub_valueset_of_ary_field(MEMBERS_KEY_NAME)?;
            member_valueset.append_field_value(MEMBER_ID_KEY_NAME, member_id.into())?;
            member_valueset.append_field_value(
                MEMBER_METADATA_KEY_NAME,
                BytesMut::from(member_metadata).into(),
            )?;
            member_ary.push(DataType::ValueSet(member_valueset.clone()));
        }

        let member_schema = response_valueset
            .schema
            .clone()
            .sub_schema_of_ary_field(MEMBERS_KEY_NAME)?;
        response_valueset.append_field_value(
            MEMBERS_KEY_NAME,
            DataType::array_of_value_set(member_ary, member_schema),
        )?;
        Ok(())
    }
}
