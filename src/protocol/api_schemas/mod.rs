use std::collections::HashMap;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use once_cell::sync::Lazy;
use tracing::trace;

use crate::protocol::schema::Schema;
use crate::protocol::types::DataType;
use crate::protocol::value_set::ValueSet;
use crate::protocol::{ApiKey, ApiVersion, ProtocolCodec};
use crate::request::{ApiVersionRequest, ApiVersionResponse, RequestHeader};
use crate::AppResult;

pub mod consumer_groups;
pub mod consumer_protocol;
pub mod metadata_reps;
pub mod metadata_req;
pub mod produce_reps;
pub mod produce_req;

// Constants for key names used in the request header
static API_KEY: &str = "api_key";
static API_VERSION_KEY_NAME: &str = "api_version";

static CORRELATION_ID_KEY_NAME: &str = "correlation_id";
static CLIENT_ID_KEY_NAME: &str = "client_id";
// Lazy static reference to the request header schema
pub static REQUEST_HEADER_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let tuple: Vec<(i32, &str, DataType)> = vec![
        (0, API_KEY, 0i16.into()),
        (1, API_VERSION_KEY_NAME, 0i16.into()),
        (2, CORRELATION_ID_KEY_NAME, 0i32.into()),
        (3, CLIENT_ID_KEY_NAME, Some(String::new()).into()),
    ];
    Arc::new(Schema::from_fields_desc_vec(tuple))
});

/// Implementation of methods for the RequestHeader struct
impl RequestHeader {
    /// Writes the request header to a byte stream
    ///
    /// # Arguments
    ///
    /// * `stream` - A mutable reference to a BytesMut instance where the request header will be written
    ///
    /// # Returns
    ///
    /// * `AppResult<()>` - An application-specific result type
    pub async fn write_to(self, writer: &mut BytesMut) {
        let schema = Arc::clone(&REQUEST_HEADER_SCHEMA);
        let mut schema_data = ValueSet::new(schema);
        schema_data.append_field_value(API_KEY, self.api_key.into());
        schema_data.append_field_value(API_VERSION_KEY_NAME, self.api_version.into());
        schema_data.append_field_value(CORRELATION_ID_KEY_NAME, self.correlation_id.into());
        schema_data.append_field_value(CLIENT_ID_KEY_NAME, self.client_id.clone().into());
        schema_data.write_to(writer)
    }

    /// Reads a request header from a byte stream
    ///
    /// # Arguments
    ///
    /// * `stream` - A mutable reference to a BytesMut instance from which the request header will be read
    ///
    /// # Returns
    ///
    /// * `AppResult<RequestHeader>` - An application-specific result type containing a RequestHeader instance
    pub fn read_from(stream: &mut BytesMut) -> AppResult<RequestHeader> {
        let schema = Arc::clone(&REQUEST_HEADER_SCHEMA);
        let mut schema_data: ValueSet = schema.read_from(stream)?;

        let api_key_field_value: i16 = schema_data.get_field_value(API_KEY).into();
        let api_key = api_key_field_value.try_into()?;

        let api_version_field_value: i16 = schema_data.get_field_value(API_VERSION_KEY_NAME).into();
        let api_version = api_version_field_value.try_into()?;

        let correlation_id = schema_data.get_field_value(CORRELATION_ID_KEY_NAME).into();
        let client_id = schema_data.get_field_value(CLIENT_ID_KEY_NAME).into();

        Ok(RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }
}

pub static SUPPORTED_API_VERSIONS: Lazy<Arc<HashMap<i16, Vec<ApiVersion>>>> = Lazy::new(|| {
    let mut map = HashMap::new();
    map.insert(
        ApiKey::ApiVersionKey.into(),
        vec![ApiVersion::V0, ApiVersion::V1],
    );
    map.insert(
        ApiKey::Metadata.into(),
        vec![
            ApiVersion::V0,
            ApiVersion::V1,
            ApiVersion::V2,
            ApiVersion::V3,
            ApiVersion::V4,
        ],
    );
    map.insert(
        ApiKey::Produce.into(),
        vec![
            ApiVersion::V0,
            ApiVersion::V1,
            ApiVersion::V2,
            ApiVersion::V3,
            ApiVersion::V4,
        ],
    );
    map.insert(
        ApiKey::Fetch.into(),
        vec![
            ApiVersion::V0,
            ApiVersion::V1,
            ApiVersion::V2,
            ApiVersion::V3,
            ApiVersion::V4,
            ApiVersion::V5,
        ],
    );
    map.insert(
        ApiKey::JoinGroup.into(),
        vec![ApiVersion::V0, ApiVersion::V1, ApiVersion::V2],
    );
    map.insert(
        ApiKey::SyncGroup.into(),
        vec![ApiVersion::V0, ApiVersion::V1],
    );
    map.insert(
        ApiKey::Heartbeat.into(),
        vec![ApiVersion::V0, ApiVersion::V1],
    );
    map.insert(
        ApiKey::LeaveGroup.into(),
        vec![ApiVersion::V0, ApiVersion::V1],
    );
    map.insert(
        ApiKey::OffsetFetch.into(),
        vec![
            ApiVersion::V0,
            ApiVersion::V1,
            ApiVersion::V2,
            ApiVersion::V3,
        ],
    );
    map.insert(
        ApiKey::OffsetCommit.into(),
        vec![
            ApiVersion::V0,
            ApiVersion::V1,
            ApiVersion::V2,
            ApiVersion::V3,
        ],
    );
    map.insert(
        ApiKey::FindCoordinator.into(),
        vec![ApiVersion::V0, ApiVersion::V1],
    );
    Arc::new(map)
});

pub const THROTTLE_TIME_KEY_NAME: &str = "throttle_time_ms";
pub const ERROR_CODE_KEY_NAME: &str = "error_code";
pub const API_VERSIONS_KEY_NAME: &str = "api_versions";
pub const API_KEY_NAME: &str = "api_key";
pub const MIN_VERSION_KEY_NAME: &str = "min_version";
pub const MAX_VERSION_KEY_NAME: &str = "max_version";

pub static API_VERSIONS_REQUEST_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![]);
    Arc::new(schema)
});
pub static API_VERSIONS_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, API_KEY_NAME, 0i16.into()),
        (1, MIN_VERSION_KEY_NAME, 0i16.into()),
        (2, MAX_VERSION_KEY_NAME, 0i16.into()),
    ]);
    Arc::new(schema)
});
pub static API_VERSIONS_RESPONSE_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, ERROR_CODE_KEY_NAME, 0i16.into()),
        (
            1,
            API_VERSIONS_KEY_NAME,
            DataType::array_of_schema(Arc::clone(&API_VERSIONS_V0)),
        ),
    ]);
    Arc::new(schema)
});
pub static API_VERSIONS_RESPONSE_V1: Lazy<Arc<Schema>> = Lazy::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, ERROR_CODE_KEY_NAME, 0i16.into()),
        (
            1,
            API_VERSIONS_KEY_NAME,
            DataType::array_of_schema(Arc::clone(&API_VERSIONS_V0)),
        ),
        (2, THROTTLE_TIME_KEY_NAME, 0i32.into()),
    ]);
    Arc::new(schema)
});

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
            versions_ary.push(DataType::ValueSet(version_value_set));
        }
        let versions_ary_schema = apiversion_reps_value_set
            .schema
            .clone()
            .sub_schema_of_ary_field(API_VERSIONS_KEY_NAME);
        apiversion_reps_value_set.append_field_value(
            API_VERSIONS_KEY_NAME,
            DataType::array_of_value_set(versions_ary, versions_ary_schema),
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
