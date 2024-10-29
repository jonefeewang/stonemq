use std::borrow::Cow;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use once_cell::sync::Lazy;
use tracing::debug;

use crate::message::TopicPartition;
use crate::protocol::array::ArrayType;
use crate::protocol::primary_types::{NPBytes, PString, I16, I32};
use crate::protocol::schema::Schema;
use crate::protocol::types::DataType;
use crate::protocol::value_set::ValueSet;

use crate::{AppError, AppResult};

pub const PROTOCOL_TYPE: &str = "consumer";

pub const VERSION_KEY_NAME: &str = "version";
pub const TOPICS_KEY_NAME: &str = "topics";
pub const TOPIC_KEY_NAME: &str = "topic";
pub const PARTITIONS_KEY_NAME: &str = "partitions";
pub const TOPIC_PARTITIONS_KEY_NAME: &str = "topic_partitions";
pub const USER_DATA_KEY_NAME: &str = "user_data";

pub const CONSUMER_PROTOCOL_V0: i16 = 0;

pub static CONSUMER_PROTOCOL_V0_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> =
        vec![(0, VERSION_KEY_NAME, DataType::I16(I16::default()))];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});
pub static CONSUMER_PROTOCOL_HEADER_V0: Lazy<ValueSet> = Lazy::new(|| {
    let mut value_set = ValueSet::new(CONSUMER_PROTOCOL_V0_SCHEMA.clone());
    value_set
        .append_field_value(VERSION_KEY_NAME, DataType::I16(I16::default()))
        .unwrap();
    value_set
});
pub static SUBSCRIPTION_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (
            0,
            TOPIC_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::PString(PString::default())),
                values: None,
            }),
        ),
        (1, USER_DATA_KEY_NAME, DataType::NPBytes(NPBytes::default())),
    ];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});
pub static TOPIC_ASSIGNMENT_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (0, TOPIC_KEY_NAME, DataType::PString(PString::default())),
        (
            1,
            PARTITIONS_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::I32(I32::default())),
                values: None,
            }),
        ),
    ];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});

pub static ASSIGNMENT_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, DataType)> = vec![
        (
            0,
            TOPIC_PARTITIONS_KEY_NAME,
            DataType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(DataType::Schema(Arc::clone(&TOPIC_ASSIGNMENT_V0))),
                values: None,
            }),
        ),
        (1, USER_DATA_KEY_NAME, DataType::NPBytes(NPBytes::default())),
    ];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});

pub struct ConsumerProtocol {}
pub struct Subscription {
    pub topics: Vec<String>,
    pub user_data: BytesMut,
}
pub struct TopicAssignment {
    pub partitions: Vec<TopicPartition>,
    pub user_data: BytesMut,
}

impl Subscription {
    pub fn read_from(mut buf: BytesMut) -> AppResult<Self> {
        let mut value_set = CONSUMER_PROTOCOL_V0_SCHEMA.clone().read_from(&mut buf)?;
        let version: i16 = value_set.get_field_value(VERSION_KEY_NAME)?.try_into()?;
        debug!("version:{}", version);

        let mut subscription = SUBSCRIPTION_V0.clone().read_from(&mut buf)?;
        let topics: ArrayType = subscription.get_field_value(TOPIC_KEY_NAME)?.try_into()?;
        let user_data: NPBytes = subscription
            .get_field_value(USER_DATA_KEY_NAME)?
            .try_into()?;
        let topics = topics
            .values
            .ok_or(AppError::ProtocolError(Cow::Borrowed("topics is empty")))?;
        let mut topics_vec: Vec<String> = Vec::with_capacity(topics.len());
        for topic in topics {
            topics_vec.push(topic.try_into()?);
        }
        if let Some(user_data) = user_data.value {
            Ok(Self {
                topics: topics_vec,
                user_data,
            })
        } else {
            Ok(Self {
                topics: topics_vec,
                user_data: BytesMut::new(),
            })
        }
    }
}

impl TopicAssignment {
    pub fn read_from(mut buf: BytesMut) -> AppResult<Self> {
        let mut value_set = CONSUMER_PROTOCOL_V0_SCHEMA.clone().read_from(&mut buf)?;
        let version: i16 = value_set.get_field_value(VERSION_KEY_NAME)?.try_into()?;
        debug!("version:{}", version);

        let user_data: NPBytes = value_set.get_field_value(USER_DATA_KEY_NAME)?.try_into()?;
        let mut topic_assignment = ASSIGNMENT_V0.clone().read_from(&mut buf)?;
        let topic_partitions: ArrayType = topic_assignment
            .get_field_value(TOPIC_PARTITIONS_KEY_NAME)?
            .try_into()?;
        let topic_partitions =
            topic_partitions
                .values
                .ok_or(AppError::ProtocolError(Cow::Borrowed(
                    "topic_partitions is empty",
                )))?;
        let mut topic_assignment_vec = Vec::with_capacity(topic_partitions.len());
        for topic_partition in topic_partitions {
            let mut topic_partition: ValueSet = topic_partition.try_into()?;
            let topic: String = topic_partition
                .get_field_value(TOPIC_KEY_NAME)?
                .try_into()?;
            let partitions: ArrayType = topic_partition
                .get_field_value(PARTITIONS_KEY_NAME)?
                .try_into()?;
            let partitions = partitions
                .values
                .ok_or(AppError::ProtocolError(Cow::Borrowed(
                    "partitions is empty",
                )))?;

            for partition in partitions {
                let partition: i32 = partition.try_into()?;
                topic_assignment_vec.push(TopicPartition::new(topic.clone(), partition));
            }
        }
        if let Some(user_data) = user_data.value {
            Ok(Self {
                partitions: topic_assignment_vec,
                user_data,
            })
        } else {
            Ok(Self {
                partitions: topic_assignment_vec,
                user_data: BytesMut::new(),
            })
        }
    }
}
