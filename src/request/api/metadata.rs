use crate::service::Node;
use crate::AppError;

use crate::{
    request::errors::{ErrorCode, KafkaError},
    request::RequestContext,
};

use super::ApiHandler;

pub struct MetadataRequestHandler;
impl ApiHandler for MetadataRequestHandler {
    type Request = MetaDataRequest;
    type Response = MetadataResponse;

    async fn handle_request(
        &self,
        request: MetaDataRequest,
        context: &RequestContext,
    ) -> MetadataResponse {
        let metadata = context.replica_manager.get_queue_metadata(
            request
                .topics
                .as_ref()
                .map(|v| v.iter().map(|s| s.as_str()).collect()),
        );
        // send metadata to client
        MetadataResponse::new(metadata, Node::new_localhost())
    }
}

#[derive(Debug, Clone)]
pub struct MetaDataRequest {
    pub topics: Option<Vec<String>>,
    pub allow_auto_topic_creation: bool,
}
#[derive(Debug)]
pub struct MetadataResponse {
    pub throttle_time_ms: i32,
    pub brokers: Vec<Node>,
    pub controller: Node,
    pub topic_metadata: Vec<TopicMetadata>,
    pub cluster_id: Option<String>,
}
impl MetadataResponse {
    pub fn new(
        topic_metadata: Vec<(String, Option<Vec<i32>>, Option<AppError>)>,
        broker: Node,
    ) -> MetadataResponse {
        let topic_metadata = topic_metadata
            .into_iter()
            .map(|(topic, partitions, protocol_error)| {
                let topic_error_code = 0;
                if let Some(protocol_error) = protocol_error {
                    let kafka_error = KafkaError::from(protocol_error);
                    let error_code = ErrorCode::from(&kafka_error);
                    TopicMetadata {
                        error_code: error_code as i16,
                        topic,
                        is_internal: false,
                        partition_metadata: vec![],
                    }
                } else if let Some(partitions) = partitions {
                    let partition_metadata = partitions
                        .into_iter()
                        .map(|partition_id| PartitionMetadata {
                            partition_error_code: 0,
                            partition_id,
                            leader: broker.clone(),
                            replicas: vec![broker.clone()],
                            isr: vec![broker.clone()],
                        })
                        .collect();
                    TopicMetadata {
                        error_code: topic_error_code,
                        topic,
                        is_internal: false,
                        partition_metadata,
                    }
                } else {
                    TopicMetadata {
                        error_code: topic_error_code,
                        topic,
                        is_internal: false,
                        partition_metadata: vec![],
                    }
                }
            })
            .collect();

        MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![broker.clone()],
            controller: broker.clone(),
            topic_metadata,
            cluster_id: None,
        }
    }
}

#[derive(Debug)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub topic: String,
    pub is_internal: bool,
    pub partition_metadata: Vec<PartitionMetadata>,
}
#[derive(Debug)]
pub struct PartitionMetadata {
    pub partition_error_code: i16,
    pub partition_id: i32,
    pub leader: Node,
    pub replicas: Vec<Node>,
    pub isr: Vec<Node>,
}
