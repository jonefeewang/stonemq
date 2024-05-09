use std::borrow::Cow;

use crate::{AppError, AppResult};
use crate::protocol::Acks;
use crate::topic_partition::TopicData;

#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub transactional_id: Option<String>,
    pub required_acks: Acks,
    pub timeout: i32,
    pub topic_data: Vec<TopicData>,
}

impl ProduceRequest {
    pub fn new(
        transactional_id: Option<String>,
        required_acks: Acks,
        timeout: i32,
        topic_data: Vec<TopicData>,
    ) -> ProduceRequest {
        ProduceRequest {
            transactional_id,
            required_acks,
            timeout,
            topic_data,
        }
    }
    ///
    /// Create an empty ProduceRequest to accept data from the client
    pub(crate) fn new_empty() -> ProduceRequest {
        ProduceRequest {
            transactional_id: None,
            required_acks: Acks::All(-1, "All"),
            timeout: 0,
            topic_data: vec![],
        }
    }
    pub fn validate(&self) -> AppResult<()> {
        if self.timeout < 0 {
            return Err(AppError::RequestError(Cow::Borrowed(
                "timeout must be >= 0",
            )));
        }
        Ok(())
    }
}
impl PartialEq for ProduceRequest {
    fn eq(&self, other: &Self) -> bool {
        self.transactional_id == other.transactional_id
            && self.required_acks == other.required_acks
            && self.timeout == other.timeout
            && self.topic_data == other.topic_data
    }
}
impl Eq for ProduceRequest {}

pub struct MetaDataRequest {
    b: i16,
}
