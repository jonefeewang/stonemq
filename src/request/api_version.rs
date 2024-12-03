use std::collections::HashMap;

use crate::protocol::{ApiVersion, SUPPORTED_API_VERSIONS};

#[derive(Debug)]
pub struct ApiVersionRequest {
    _version: ApiVersion,
}

impl ApiVersionRequest {
    pub fn new(_version: ApiVersion) -> Self {
        ApiVersionRequest { _version }
    }
    pub fn process(&self) -> ApiVersionResponse {
        let mut api_versions = HashMap::new();
        for (key, value) in SUPPORTED_API_VERSIONS.iter() {
            api_versions.insert(
                *key,
                (
                    *value.first().unwrap() as i16,
                    *value.last().unwrap() as i16,
                ),
            );
        }
        ApiVersionResponse {
            error_code: 0,
            throttle_time_ms: 0,
            api_versions,
        }
    }
}

#[derive(Debug)]
pub struct ApiVersionResponse {
    pub error_code: i16,
    pub throttle_time_ms: i32,
    pub api_versions: HashMap<i16, (i16, i16)>,
}
