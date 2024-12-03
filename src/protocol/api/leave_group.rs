use std::sync::Arc;

use crate::{
    protocol::{
        base::{NPBytes, PString, ProtocolType, I16, I32},
        schema::{Schema, ValueSet},
        ApiKey, ApiVersion, ProtocolCodec,
    },
    request::{
        consumer_group::{LeaveGroupRequest, LeaveGroupResponse},
        errors::ErrorCode,
    },
    AppResult,
};
use bytes::{BufMut, BytesMut};
use once_cell::sync::Lazy;

const GROUP_ID_KEY_NAME: &str = "group_id";
const MEMBER_ID_KEY_NAME: &str = "member_id";
const THROTTLE_TIME_KEY_NAME: &str = "throttle_time_ms";
const ERROR_CODE_KEY_NAME: &str = "error_code";

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

    fn encode(self, _api_version: &ApiVersion, _correlation_id: i32) -> BytesMut {
        todo!()
    }
}

impl LeaveGroupRequest {
    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<LeaveGroupRequest> {
        let group_id = value_set.get_field_value(GROUP_ID_KEY_NAME).into();
        let member_id = value_set.get_field_value(MEMBER_ID_KEY_NAME).into();

        Ok(LeaveGroupRequest {
            group_id,
            member_id,
        })
    }
}

impl ProtocolCodec<LeaveGroupResponse> for LeaveGroupResponse {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::LeaveGroup);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set).unwrap();
        let body_size = value_set.size();
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
    ) -> AppResult<LeaveGroupResponse> {
        todo!()
    }
}

impl LeaveGroupResponse {
    fn encode_to_value_set(self, response_valueset: &mut ValueSet) -> AppResult<()> {
        response_valueset.append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into());
        let error_code = ErrorCode::from(&self.error);
        response_valueset.append_field_value(ERROR_CODE_KEY_NAME, (error_code as i16).into());
        Ok(())
    }
}

pub static LEAVE_GROUP_REQUEST_V1_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (
            0,
            GROUP_ID_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            1,
            MEMBER_ID_KEY_NAME,
            ProtocolType::NPBytes(NPBytes::default()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static LEAVE_GROUP_RESPONSE_V1_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, ProtocolType::I32(I32::default())),
        (1, ERROR_CODE_KEY_NAME, ProtocolType::I16(I16::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
