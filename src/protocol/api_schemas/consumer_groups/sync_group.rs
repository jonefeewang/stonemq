use std::{borrow::Cow, collections::HashMap, sync::Arc};

use bytes::BytesMut;
use once_cell::sync::Lazy;

use crate::{
    protocol::{
        api_schemas::{ERROR_CODE_KEY_NAME, THROTTLE_TIME_KEY_NAME},
        array::ArrayType,
        primary_types::{NPBytes, PBytes, PString, I16, I32},
        schema::Schema,
        types::DataType,
        value_set::ValueSet,
        ApiKey, ApiVersion, ProtocolCodec,
    },
    request::consumer_group::{SyncGroupRequest, SyncGroupResponse},
    AppError, AppResult,
};
use tokio::io::AsyncWriteExt;

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
    async fn encode<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()> {
        todo!()
    }

    fn decode(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<SyncGroupRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::SyncGroup);
        let mut value_set = schema.read_from(buffer)?;
        let sync_group_request = SyncGroupRequest::decode_from_value_set(value_set)?;
        Ok(sync_group_request)
    }
}
impl SyncGroupRequest {
    fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<SyncGroupRequest> {
        let group_id = value_set.get_field_value(GROUP_ID_KEY_NAME)?.try_into()?;
        let generation_id = value_set
            .get_field_value(GENERATION_ID_KEY_NAME)?
            .try_into()?;
        let member_id = value_set.get_field_value(MEMBER_ID_KEY_NAME)?.try_into()?;
        let group_assignment: ArrayType = value_set
            .get_field_value(GROUP_ASSIGNMENT_KEY_NAME)?
            .try_into()?;
        let group_assignment_ary =
            group_assignment
                .values
                .ok_or(AppError::ProtocolError(Cow::Borrowed(
                    "group assignment is empty",
                )))?;
        let mut group_assignment = HashMap::with_capacity(group_assignment_ary.len());
        for member_assignment in group_assignment_ary {
            let mut member_assignment_value_set: ValueSet = member_assignment.try_into()?;
            let member_id = member_assignment_value_set
                .get_field_value(MEMBER_ID_KEY_NAME)?
                .try_into()?;
            let member_assignment: PBytes = member_assignment_value_set
                .get_field_value(MEMBER_ASSIGNMENT_KEY_NAME)?
                .try_into()?;
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
    async fn encode<W>(
        self,
        writer: &mut W,
        api_version: &ApiVersion,
        correlation_id: i32,
    ) -> AppResult<()>
    where
        W: AsyncWriteExt + Unpin + Send,
    {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::SyncGroup);
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

    fn decode(
        buffer: &mut bytes::BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<SyncGroupResponse> {
        todo!()
    }
}
impl SyncGroupResponse {
    fn encode_to_value_set(self, response_valueset: &mut ValueSet) -> AppResult<()> {
        response_valueset
            .append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into())?;
        response_valueset
            .append_field_value(ERROR_CODE_KEY_NAME, (self.error_code as i16).into())?;
        response_valueset.append_field_value(
            MEMBER_ASSIGNMENT_KEY_NAME,
            BytesMut::from(self.member_assignment).into(),
        )?;
        Ok(())
    }
}
