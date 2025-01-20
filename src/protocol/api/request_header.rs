use std::sync::{Arc, LazyLock};

use bytes::BytesMut;

use crate::{
    protocol::{
        base::ProtocolType,
        schema_base::{Schema, ValueSet},
        ApiKey, ApiVersion,
    },
    request::RequestHeader,
    AppResult,
};

// Constants for key names used in the request header
static API_KEY: &str = "api_key";
static API_VERSION_KEY_NAME: &str = "api_version";
static CORRELATION_ID_KEY_NAME: &str = "correlation_id";
static CLIENT_ID_KEY_NAME: &str = "client_id";

// Lazy static reference to the request header schema
pub static REQUEST_HEADER_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let tuple: Vec<(i32, &str, ProtocolType)> = vec![
        (0, API_KEY, 0i16.into()),
        (1, API_VERSION_KEY_NAME, 0i16.into()),
        (2, CORRELATION_ID_KEY_NAME, 0i32.into()),
        (3, CLIENT_ID_KEY_NAME, Some(String::new()).into()),
    ];
    Arc::new(Schema::from_fields_desc_vec(tuple))
});

/// Implementation of methods for the RequestHeader struct
impl RequestHeader {
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
        let api_key = ApiKey::from_i16(api_key_field_value)?;

        let api_version_field_value: i16 = schema_data.get_field_value(API_VERSION_KEY_NAME).into();
        let api_version = ApiVersion::from_i16(api_version_field_value)?;

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
