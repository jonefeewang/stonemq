use crate::broker::Node;
use crate::protocol::ProtocolError;

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
        topic_metadata: Vec<(String, Option<Vec<i32>>, Option<ProtocolError>)>,
        broker: Node,
    ) -> MetadataResponse {
        let topic_metadata = topic_metadata
            .into_iter()
            .map(|(topic, partitions, protocol_error)| {
                let mut topic_error_code = 0;
                if let Some(protocol_error) = protocol_error {
                    match protocol_error {
                        ProtocolError::InvalidTopic(detail) => {
                            topic_error_code = detail.code;
                        }
                    }

                    TopicMetadata {
                        topic_error_code,
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
                        topic_error_code,
                        topic,
                        is_internal: false,
                        partition_metadata,
                    }
                } else {
                    TopicMetadata {
                        topic_error_code,
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
    pub topic_error_code: i16,
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
