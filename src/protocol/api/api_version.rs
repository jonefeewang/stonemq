use std::sync::{Arc, LazyLock};

use bytes::{BufMut, BytesMut};
use tracing::trace;

use crate::protocol::base::ProtocolType;
use crate::protocol::schema_base::{Schema, ValueSet};
use crate::protocol::types::{ApiKey, ApiVersion};
use crate::protocol::ProtocolCodec;
use crate::request::{ApiVersionRequest, ApiVersionResponse};
use crate::AppResult;

pub const THROTTLE_TIME_KEY_NAME: &str = "throttle_time_ms";
pub const ERROR_CODE_KEY_NAME: &str = "error_code";
pub const API_VERSIONS_KEY_NAME: &str = "api_versions";
pub const API_KEY_NAME: &str = "api_key";
pub const MIN_VERSION_KEY_NAME: &str = "min_version";
pub const MAX_VERSION_KEY_NAME: &str = "max_version";

impl ProtocolCodec<ApiVersionRequest> for ApiVersionRequest {
    fn encode(self, _api_version: &ApiVersion, _correlation_id: i32) -> BytesMut {
        todo!()
    }

    fn decode(_buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<ApiVersionRequest> {
        Ok(ApiVersionRequest::new(*api_version))
    }
}
impl ProtocolCodec<ApiVersionResponse> for ApiVersionResponse {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::ApiVersionKey);
        let mut apiversion_reps_value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut apiversion_reps_value_set);
        // correlation_id + response_total_size
        let response_total_size = 4 + apiversion_reps_value_set.size();
        let mut writer = BytesMut::with_capacity(response_total_size);
        writer.put_i32(response_total_size as i32);
        writer.put_i32(correlation_id);
        apiversion_reps_value_set.write_to(&mut writer);
        trace!(
            "write response total size:{} with correlation_id:{}",
            response_total_size,
            correlation_id
        );
        writer
    }

    fn decode(_buffer: &mut BytesMut, _api_version: &ApiVersion) -> AppResult<ApiVersionResponse> {
        todo!()
    }
}

impl ApiVersionResponse {
    pub(crate) fn encode_to_value_set(self, apiversion_reps_value_set: &mut ValueSet) {
        apiversion_reps_value_set.append_field_value(ERROR_CODE_KEY_NAME, self.error_code.into());
        let mut versions_ary = Vec::with_capacity(self.api_versions.len());
        for (api_code, supported_versions) in self.api_versions {
            let mut version_value_set =
                apiversion_reps_value_set.sub_valueset_of_ary_field(API_VERSIONS_KEY_NAME);
            version_value_set.append_field_value(API_KEY_NAME, api_code.into());
            version_value_set.append_field_value(MIN_VERSION_KEY_NAME, supported_versions.0.into());
            version_value_set.append_field_value(MAX_VERSION_KEY_NAME, supported_versions.1.into());
            versions_ary.push(ProtocolType::ValueSet(version_value_set));
        }
        let versions_ary_schema = apiversion_reps_value_set
            .schema
            .clone()
            .sub_schema_of_ary_field(API_VERSIONS_KEY_NAME);
        apiversion_reps_value_set.append_field_value(
            API_VERSIONS_KEY_NAME,
            ProtocolType::array_of_value_set(versions_ary, versions_ary_schema),
        );

        if apiversion_reps_value_set
            .schema
            .fields_index_by_name
            .contains_key(THROTTLE_TIME_KEY_NAME)
        {
            apiversion_reps_value_set
                .append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into());
        }
    }
}

pub static API_VERSIONS_REQUEST_V0: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![]);
    Arc::new(schema)
});
pub static API_VERSIONS_V0: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, API_KEY_NAME, 0i16.into()),
        (1, MIN_VERSION_KEY_NAME, 0i16.into()),
        (2, MAX_VERSION_KEY_NAME, 0i16.into()),
    ]);
    Arc::new(schema)
});
pub static API_VERSIONS_RESPONSE_V0: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, ERROR_CODE_KEY_NAME, 0i16.into()),
        (
            1,
            API_VERSIONS_KEY_NAME,
            ProtocolType::array_of_schema(Arc::clone(&API_VERSIONS_V0)),
        ),
    ]);
    Arc::new(schema)
});
pub static API_VERSIONS_RESPONSE_V1: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, ERROR_CODE_KEY_NAME, 0i16.into()),
        (
            1,
            API_VERSIONS_KEY_NAME,
            ProtocolType::array_of_schema(Arc::clone(&API_VERSIONS_V0)),
        ),
        (2, THROTTLE_TIME_KEY_NAME, 0i32.into()),
    ]);
    Arc::new(schema)
});
