use crate::protocol::{ApiKey, ApiVersion};

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}
