use std::sync::Arc;

use crate::{AppError, BrokerConfig, Connection};
use crate::message::MemoryRecordBatch;
use crate::protocol::Acks;

pub enum RequestEnum {
    ProduceRequestV0(ProduceRequestV0),
    ProduceRequestV1(ProduceRequestV0),
    ProduceRequestV2(ProduceRequestV0),
    ProduceRequestV3(ProduceRequestV3),
    MetadataRequest(MetaDataRequest),
}

pub struct PartitionData {
    partition: i32,
    message_set: MemoryRecordBatch,
}
pub struct TopicData {
    topic_name: String,
    partition_data: Vec<PartitionData>,
}

pub struct ProduceRequestV0 {
    correlation_id: i32,
    required_acks: Acks,
    timeout: i32,
    topic_data: Vec<TopicData>,
}
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
            RequestEnum::ProduceRequestV0(request)
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

    pub fn handle_produce_request(request_context: &RequestContext, request: ProduceRequestV0) {
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
