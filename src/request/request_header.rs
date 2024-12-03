use crate::protocol::{ApiKey, ApiVersion};

#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    // for test use
    pub fn new(
        api_key: ApiKey,
        api_version: ApiVersion,
        correlation_id: i32,
        client_id: Option<String>,
    ) -> Self {
        RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        }
    }
    pub fn size(&self) -> usize {
        match &self.client_id {
            Some(client_id) => {
                let client_id_size = client_id.len();
                2 + 2 + 4 + 4 + client_id_size
            }
            None => 2 + 2 + 4 + 4,
        }
    }
}
