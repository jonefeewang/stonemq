use std::sync::Arc;

use crate::message::MemoryRecord;
use crate::protocol::{Acks, ApiKey, ApiVersion};
use crate::{AppError, BrokerConfig, Connection};

pub enum RequestEnum {
    ProduceRequestV0E(ProduceRequest),
    ProduceRequestV1(ProduceRequest),
    ProduceRequestV2(ProduceRequest),
    ProduceRequestV3(ProduceRequestV3),
    MetadataRequest(MetaDataRequest),
}

#[derive(Default)]
pub struct RequestHeader {
    pub len: u32,
    pub api_key: ApiKey,
    pub api_version: ApiVersion,
    pub correlation_id: i32,
}

#[derive(Clone, Default)]
pub struct PartitionData {
    pub partition: i32,
    pub message_set: MemoryRecord,
}

impl PartitionData {
    pub fn new(partition: i32) -> PartitionData {
        PartitionData {
            partition,
            message_set: MemoryRecord::default(),
        }
    }
}
#[derive(Clone, Default)]
pub struct TopicData {
    pub topic_name: String,
    pub partition_data: Vec<PartitionData>,
}

impl TopicData {
    pub fn new(topic_name: &str, partition_data: Vec<PartitionData>) -> TopicData {
        TopicData {
            topic_name: topic_name.to_string(),
            partition_data,
        }
    }
}

pub struct ProduceRequest {
    pub request_header: RequestHeader,
    pub required_acks: Acks,
    pub timeout: i32,
    pub topic_data: Vec<TopicData>,
}

impl ProduceRequest {}

pub struct ProduceRequestV3 {
    correlation_id: i32,
    required_acks: Acks,
    timeout: i32,
    transactional_id: String,
    topic_data: Vec<TopicData>,
}
pub struct MetaDataRequest {
    b: i16,
}
pub struct RequestContext<'c> {
    conn: &'c Connection,
    broker_config: &'c Arc<BrokerConfig>,
}
impl<'c> RequestContext<'c> {
    pub fn new(conn: &'c Connection, broker_config: &'c Arc<BrokerConfig>) -> Self {
        RequestContext {
            conn,
            broker_config,
        }
    }
}

#[derive(Debug)]
pub struct RequestProcessor {}

impl RequestProcessor {
    pub fn process_request(request: RequestEnum, request_context: &RequestContext) {
        match request {
            RequestEnum::ProduceRequestV0E(request)
            | RequestEnum::ProduceRequestV1(request)
            | RequestEnum::ProduceRequestV2(request) => {
                Self::handle_produce_request(request_context, request)
            }
            RequestEnum::ProduceRequestV3(request) => {
                Self::handle_produce_request_v3(request_context, request)
            }
            RequestEnum::MetadataRequest(request) => {
                Self::handle_metadata_request(request_context, request)
            }
        }
    }

    pub fn handle_produce_request(request_context: &RequestContext, request: ProduceRequest) {
        todo!()
    }
    pub fn handle_produce_request_v3(request_context: &RequestContext, request: ProduceRequestV3) {
        todo!()
    }
    pub fn handle_metadata_request(request_context: &RequestContext, request: MetaDataRequest) {
        todo!()
    }
    pub(crate) fn respond_invalid_request(error: AppError, request_context: &RequestContext) {
        todo!()
    }
}
