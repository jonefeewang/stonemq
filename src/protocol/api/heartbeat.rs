use bytes::{BufMut, BytesMut};
use once_cell::sync::Lazy;
use std::sync::Arc;

use crate::{
    protocol::{
        base::{PString, ProtocolType, I16, I32},
        schema_base::{Schema, ValueSet},
        ApiKey, ApiVersion, ProtocolCodec,
    },
    request::{HeartbeatRequest, HeartbeatResponse},
    AppResult,
};

const GROUP_GENERATION_ID_KEY_NAME: &str = "group_generation_id";
const GROUP_ID_KEY_NAME: &str = "group_id";
const MEMBER_ID_KEY_NAME: &str = "member_id";
const THROTTLE_TIME_KEY_NAME: &str = "throttle_time_ms";
const ERROR_CODE_KEY_NAME: &str = "error_code";

pub static HEARTBEAT_REQUEST_V1_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (
            0,
            GROUP_ID_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            1,
            GROUP_GENERATION_ID_KEY_NAME,
            ProtocolType::I32(I32::default()),
        ),
        (
            2,
            MEMBER_ID_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static HEARTBEAT_RESPONSE_V1_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, ProtocolType::I32(I32::default())),
        (1, ERROR_CODE_KEY_NAME, ProtocolType::I16(I16::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

impl ProtocolCodec<HeartbeatRequest> for HeartbeatRequest {
    fn encode(self, _api_version: &ApiVersion, _correlation_id: i32) -> BytesMut {
        todo!()
    }

    fn decode(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<HeartbeatRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::Heartbeat)?;
        let value_set = schema.read_from(buffer)?;
        HeartbeatRequest::decode_from_value_set(value_set)
    }
}

impl HeartbeatRequest {
    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<HeartbeatRequest> {
        let group_id = value_set.get_field_value(GROUP_ID_KEY_NAME).into();
        let generation_id = value_set
            .get_field_value(GROUP_GENERATION_ID_KEY_NAME)
            .into();
        let member_id = value_set.get_field_value(MEMBER_ID_KEY_NAME).into();

        Ok(HeartbeatRequest {
            group_id,
            member_id,
            generation_id,
        })
    }
}

impl ProtocolCodec<HeartbeatResponse> for HeartbeatResponse {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::Heartbeat);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set);
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
    ) -> AppResult<HeartbeatResponse> {
        todo!()
    }
}

impl HeartbeatResponse {
    fn encode_to_value_set(self, response_valueset: &mut ValueSet) {
        response_valueset.append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into());
        response_valueset.append_field_value(ERROR_CODE_KEY_NAME, self.error_code.into());
    }
}
