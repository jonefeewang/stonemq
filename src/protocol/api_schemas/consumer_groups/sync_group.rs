use std::{borrow::Cow, collections::HashMap, sync::Arc};

use bytes::{BufMut, BytesMut};
use once_cell::sync::Lazy;

use crate::{
    protocol::{
        api_schemas::{ERROR_CODE_KEY_NAME, THROTTLE_TIME_KEY_NAME},
        array::ArrayType,
        primary_types::{PBytes, PString, I16, I32},
        schema::Schema,
        types::DataType,
        value_set::ValueSet,
        ApiKey, ApiVersion, ProtocolCodec,
    },
    request::consumer_group::{SyncGroupRequest, SyncGroupResponse},
    AppError, AppResult,
};

use super::{
    GENERATION_ID_KEY_NAME, GROUP_ASSIGNMENT_KEY_NAME, GROUP_ID_KEY_NAME,
    MEMBER_ASSIGNMENT_KEY_NAME, MEMBER_ID_KEY_NAME,
};

pub static SYNC_GROUP_REQUEST_MEMBER_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, MEMBER_ID_KEY_NAME, DataType::PString(PString::default())),
        (
            1,
            MEMBER_ASSIGNMENT_KEY_NAME,
            DataType::PBytes(PBytes::default()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static SYNC_GROUP_REQUEST_V1_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, GROUP_ID_KEY_NAME, DataType::PString(PString::default())),
        (1, GENERATION_ID_KEY_NAME, DataType::I32(I32::default())),
        (2, MEMBER_ID_KEY_NAME, DataType::PString(PString::default())),
        (
            3,
            GROUP_ASSIGNMENT_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(
                    SYNC_GROUP_REQUEST_MEMBER_V0_SCHEMA.clone(),
                )),
                values: None,
            }),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static SYNC_GROUP_RESPONSE_V1_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, DataType::I32(I32::default())),
        (1, ERROR_CODE_KEY_NAME, DataType::I16(I16::default())),
        (
            2,
            MEMBER_ASSIGNMENT_KEY_NAME,
            DataType::PBytes(PBytes::default()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

impl ProtocolCodec<SyncGroupRequest> for SyncGroupRequest {
    fn encode(self, _api_version: &ApiVersion, _correlation_id: i32) -> BytesMut {
        todo!()
    }

    fn decode(buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<SyncGroupRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::SyncGroup);
        let value_set = schema.read_from(buffer)?;
        let sync_group_request = SyncGroupRequest::decode_from_value_set(value_set)?;
        Ok(sync_group_request)
    }
}
impl SyncGroupRequest {
    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<SyncGroupRequest> {
        let group_id = value_set.get_field_value(GROUP_ID_KEY_NAME).into();
        let generation_id = value_set.get_field_value(GENERATION_ID_KEY_NAME).into();
        let member_id = value_set.get_field_value(MEMBER_ID_KEY_NAME).into();
        let group_assignment: ArrayType =
            value_set.get_field_value(GROUP_ASSIGNMENT_KEY_NAME).into();
        let group_assignment_ary =
            group_assignment
                .values
                .ok_or(AppError::ProtocolError(Cow::Borrowed(
                    "group assignment is empty",
                )))?;
        let mut group_assignment = HashMap::with_capacity(group_assignment_ary.len());
        for member_assignment in group_assignment_ary {
            let mut member_assignment_value_set: ValueSet = member_assignment.into();
            let member_id = member_assignment_value_set
                .get_field_value(MEMBER_ID_KEY_NAME)
                .into();
            let member_assignment: PBytes = member_assignment_value_set
                .get_field_value(MEMBER_ASSIGNMENT_KEY_NAME)
                .into();
            group_assignment.insert(member_id, member_assignment.value);
        }

        Ok(SyncGroupRequest {
            group_id,
            generation_id,
            member_id,
            group_assignment,
        })
    }
}

impl ProtocolCodec<SyncGroupResponse> for SyncGroupResponse {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::SyncGroup);
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

    fn decode(_buffer: &mut BytesMut, _api_version: &ApiVersion) -> AppResult<SyncGroupResponse> {
        todo!()
    }
}
impl SyncGroupResponse {
    fn encode_to_value_set(self, response_valueset: &mut ValueSet) -> AppResult<()> {
        response_valueset.append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into());
        response_valueset.append_field_value(ERROR_CODE_KEY_NAME, (self.error_code as i16).into());
        response_valueset.append_field_value(
            MEMBER_ASSIGNMENT_KEY_NAME,
            BytesMut::from(self.member_assignment).into(),
        );
        Ok(())
    }
}
