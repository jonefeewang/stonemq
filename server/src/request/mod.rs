use crate::{AppError, Connection};
use crate::protocol::{ApiKey, ApiVersion};
use crate::request::produce::{MetaDataRequest, ProduceRequest};

mod create_topic;
pub mod produce;

pub enum ApiRequest {
    Produce(ProduceRequest),
    Metadata(MetaDataRequest),
}

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
    // create a new empty RequestHeader to read data from socket
    pub fn new_empty() -> RequestHeader {
        RequestHeader {
            api_key: ApiKey::Produce(0, "Produce"),
            api_version: ApiVersion::V0(0),
            correlation_id: 0,
            client_id: None,
        }
    }
    pub fn size(&self) -> usize {
        match &self.client_id {
            Some(client_id) => {
                let client_id_size = client_id.len();
                let size = 2 + 2 + 4 + 4 + client_id_size;
                size
            }
            None => {
                let size = 2 + 2 + 4 + 4;
                size
            }
        }
    }
}

pub struct RequestContext<'c> {
    pub conn: &'c mut Connection,
    pub request_header: &'c RequestHeader,
}
impl<'c> RequestContext<'c> {
    pub fn new(conn: &'c mut Connection, request_header: &'c RequestHeader) -> Self {
        RequestContext {
            conn,
            request_header,
        }
    }
}

#[derive(Debug)]
pub struct RequestProcessor {}

impl RequestProcessor {
    pub fn process_request(request: ApiRequest, request_context: &RequestContext) {
        match request {
            ApiRequest::Produce(request) => Self::handle_produce_request(request_context, request),

            ApiRequest::Metadata(request) => {
                Self::handle_metadata_request(request_context, request)
            }
        }
    }

    pub fn handle_produce_request(request_context: &RequestContext, request: ProduceRequest) {
        todo!()
    }

    pub fn handle_metadata_request(request_context: &RequestContext, request: MetaDataRequest) {
        todo!()
    }
    pub(crate) fn respond_invalid_request(error: AppError, request_context: &RequestContext) {
        todo!()
    }
}
