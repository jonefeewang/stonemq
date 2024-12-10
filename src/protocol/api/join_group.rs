use crate::protocol::base::{PBytes, PString, ProtocolType, I16, I32};
use crate::protocol::schema_base::{Schema, ValueSet};
use crate::protocol::types::ArrayType;
use crate::protocol::{ApiKey, ApiVersion, ProtocolCodec};
use crate::request::{JoinGroupRequest, JoinGroupResponse, ProtocolMetadata};
use crate::{AppError, AppResult};
use bytes::{BufMut, BytesMut};
use once_cell::sync::Lazy;

use std::sync::Arc;

const GROUP_ID_KEY_NAME: &str = "group_id";
const SESSION_TIMEOUT_KEY_NAME: &str = "session_timeout";
const REBALANCE_TIMEOUT_KEY_NAME: &str = "rebalance_timeout";
const MEMBER_ID_KEY_NAME: &str = "member_id";
const PROTOCOL_TYPE_KEY_NAME: &str = "protocol_type";
const GROUP_PROTOCOLS_KEY_NAME: &str = "group_protocols";

const PROTOCOL_NAME_KEY_NAME: &str = "protocol_name";
const PROTOCOL_METADATA_KEY_NAME: &str = "protocol_metadata";

const GENERATION_ID_KEY_NAME: &str = "generation_id";
const GROUP_PROTOCOL_KEY_NAME: &str = "group_protocol";
const LEADER_ID_KEY_NAME: &str = "leader_id";
const MEMBERS_KEY_NAME: &str = "members";
const MEMBER_METADATA_KEY_NAME: &str = "member_metadata";
const THROTTLE_TIME_KEY_NAME: &str = "throttle_time_ms";
const ERROR_CODE_KEY_NAME: &str = "error_code";

impl ProtocolCodec<JoinGroupRequest> for JoinGroupRequest {
    fn decode(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<JoinGroupRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::JoinGroup)?;
        let join_group_req_value_set = schema.read_from(buffer)?;
        let join_group_request = JoinGroupRequest::decode_from_value_set(join_group_req_value_set)?;
        Ok(join_group_request)
    }

    fn encode(self, _api_version: &ApiVersion, _correlation_id: i32) -> BytesMut {
        todo!()
    }
}

impl JoinGroupRequest {
    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<JoinGroupRequest> {
        let group_id = value_set.get_field_value(GROUP_ID_KEY_NAME).into();
        let session_timeout = value_set.get_field_value(SESSION_TIMEOUT_KEY_NAME).into();
        let rebalance_timeout = value_set.get_field_value(REBALANCE_TIMEOUT_KEY_NAME).into();
        let member_id = value_set.get_field_value(MEMBER_ID_KEY_NAME).into();
        let protocol_type = value_set.get_field_value(PROTOCOL_TYPE_KEY_NAME).into();
        let group_protocols: ArrayType = value_set.get_field_value(GROUP_PROTOCOLS_KEY_NAME).into();
        let protocol_ary = group_protocols.values.ok_or(AppError::MalformedProtocol(
            "group protocols is empty".to_string(),
        ))?;
        let mut group_protocols = Vec::with_capacity(protocol_ary.len());
        for protocol in protocol_ary {
            let mut protocol_metadata: ValueSet = protocol.into();
            let protocol_name = protocol_metadata
                .get_field_value(PROTOCOL_NAME_KEY_NAME)
                .into();
            let protocol_metadata: PBytes = protocol_metadata
                .get_field_value(PROTOCOL_METADATA_KEY_NAME)
                .into();

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
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::JoinGroup);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set).unwrap();
        let body_size = value_set.size();
        // correlation_id + response_total_size
        let response_total_size = 4 + body_size;
        let mut writer = BytesMut::with_capacity(response_total_size);
        writer.put_i32(response_total_size as i32);
        writer.put_i32(correlation_id);
        value_set.write_to(&mut writer);
        writer
    }

    fn decode(
        _buffer: &mut bytes::BytesMut,
        _api_version: &ApiVersion,
    ) -> AppResult<JoinGroupResponse> {
        todo!()
    }
}

impl JoinGroupResponse {
    fn encode_to_value_set(self, response_valueset: &mut ValueSet) -> AppResult<()> {
        response_valueset.append_field_value(
            THROTTLE_TIME_KEY_NAME,
            self.throttle_time.unwrap_or(0).into(),
        );
        response_valueset.append_field_value(ERROR_CODE_KEY_NAME, self.error_code.into());
        response_valueset.append_field_value(GENERATION_ID_KEY_NAME, self.generation_id.into());
        response_valueset.append_field_value(GROUP_PROTOCOL_KEY_NAME, self.group_protocol.into());
        response_valueset.append_field_value(LEADER_ID_KEY_NAME, self.leader_id.into());
        response_valueset.append_field_value(MEMBER_ID_KEY_NAME, self.member_id.into());

        let mut member_ary = Vec::with_capacity(self.members.len());
        for (member_id, member_metadata) in self.members {
            let mut member_valueset = response_valueset.sub_valueset_of_ary_field(MEMBERS_KEY_NAME);
            member_valueset.append_field_value(MEMBER_ID_KEY_NAME, member_id.into());
            member_valueset.append_field_value(
                MEMBER_METADATA_KEY_NAME,
                BytesMut::from(member_metadata).into(),
            );
            member_ary.push(ProtocolType::ValueSet(member_valueset.clone()));
        }

        let member_schema = response_valueset
            .schema
            .clone()
            .sub_schema_of_ary_field(MEMBERS_KEY_NAME);
        response_valueset.append_field_value(
            MEMBERS_KEY_NAME,
            ProtocolType::array_of_value_set(member_ary, member_schema),
        );
        Ok(())
    }
}

static JOIN_GROUP_REQUEST_PROTOCOL_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (
            0,
            PROTOCOL_NAME_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            1,
            PROTOCOL_METADATA_KEY_NAME,
            ProtocolType::PBytes(PBytes::default()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
pub static JOIN_GROUP_REQUEST_V2_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (
            0,
            GROUP_ID_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            1,
            SESSION_TIMEOUT_KEY_NAME,
            ProtocolType::I32(I32::default()),
        ),
        (
            2,
            REBALANCE_TIMEOUT_KEY_NAME,
            ProtocolType::I32(I32::default()),
        ),
        (
            3,
            MEMBER_ID_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            4,
            PROTOCOL_TYPE_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            5,
            GROUP_PROTOCOLS_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::Schema(
                    JOIN_GROUP_REQUEST_PROTOCOL_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
static JOIN_GROUP_RESPONSE_MEMBER_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (
            0,
            MEMBER_ID_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            1,
            MEMBER_METADATA_KEY_NAME,
            ProtocolType::PBytes(PBytes::default()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
pub static JOIN_GROUP_RESPONSE_V2_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, ProtocolType::I32(I32::default())),
        (1, ERROR_CODE_KEY_NAME, ProtocolType::I16(I16::default())),
        (2, GENERATION_ID_KEY_NAME, ProtocolType::I32(I32::default())),
        (
            3,
            GROUP_PROTOCOL_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            4,
            LEADER_ID_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            5,
            MEMBER_ID_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            6,
            MEMBERS_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::Schema(
                    JOIN_GROUP_RESPONSE_MEMBER_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
