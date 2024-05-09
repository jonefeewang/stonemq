use std::sync::Arc;

use bytes::BytesMut;
use once_cell::sync::Lazy;

use crate::AppResult;
use crate::protocol::schema::Schema;
use crate::protocol::types::DataType;
use crate::protocol::value_set::ValueSet;
use crate::request::RequestHeader;

mod create_topic;
pub mod produce;

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
    pub fn write_to(&self, stream: &mut BytesMut) -> AppResult<()> {
        let schema = Arc::clone(&REQUEST_HEADER_SCHEMA);
        let mut schema_data = ValueSet::new(schema);
        schema_data.append_field_value(API_KEY, (&self.api_key).into())?;
        schema_data.append_field_value(API_VERSION_KEY_NAME, (&self.api_version).into())?;
        schema_data.append_field_value(CORRELATION_ID_KEY_NAME, self.correlation_id.into())?;
        schema_data.append_field_value(CLIENT_ID_KEY_NAME, self.client_id.clone().into())?;
        schema_data.write_to(stream)
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
        let mut request_header = RequestHeader::new_empty();
        request_header.api_key = schema_data.get_field_value(API_KEY)?.try_into_i16_type()?;
        request_header.api_version = schema_data
            .get_field_value(API_VERSION_KEY_NAME)?
            .try_into_i16_type()?;
        request_header.correlation_id = schema_data
            .get_field_value(CORRELATION_ID_KEY_NAME)?
            .try_into()?;
        request_header.client_id = schema_data
            .get_field_value(CLIENT_ID_KEY_NAME)?
            .try_into()?;
        Ok(request_header)
    }
}
