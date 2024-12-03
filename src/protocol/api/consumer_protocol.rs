use std::borrow::Cow;
use std::sync::Arc;

use bytes::BytesMut;
use once_cell::sync::Lazy;
use tracing::debug;

use crate::message::TopicPartition;
use crate::protocol::types::ArrayType;
use crate::protocol::base::{NPBytes, PString, ProtocolType, I16, I32};
use crate::protocol::schema::Schema;
use crate::protocol::schema::ValueSet;

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
    let fields_desc: Vec<(i32, &str, ProtocolType)> =
        vec![(0, VERSION_KEY_NAME, ProtocolType::I16(I16::default()))];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});
pub static CONSUMER_PROTOCOL_V0_HEADER: Lazy<ValueSet> = Lazy::new(|| {
    let mut value_set = ValueSet::new(CONSUMER_PROTOCOL_V0_SCHEMA.clone());
    value_set.append_field_value(VERSION_KEY_NAME, ProtocolType::I16(I16::default()));
    value_set
});
pub static SUBSCRIPTION_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (
            0,
            TOPIC_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::PString(PString::default())),
                values: None,
            }),
        ),
        (
            1,
            USER_DATA_KEY_NAME,
            ProtocolType::NPBytes(NPBytes::default()),
        ),
    ];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});
pub static TOPIC_ASSIGNMENT_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, TOPIC_KEY_NAME, ProtocolType::PString(PString::default())),
        (
            1,
            PARTITIONS_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::I32(I32::default())),
                values: None,
            }),
        ),
    ];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});

pub static ASSIGNMENT_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (
            0,
            TOPIC_PARTITIONS_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::Schema(Arc::clone(&TOPIC_ASSIGNMENT_V0))),
                values: None,
            }),
        ),
        (
            1,
            USER_DATA_KEY_NAME,
            ProtocolType::NPBytes(NPBytes::default()),
        ),
    ];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});

/////////////////---sticky assignor---/////////////////
pub static PREVIOUS_ASSIGNMENT_KEY_NAME: &str = "previous_assignment";

pub static TOPIC_ASSIGNMENT_STICKY: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, TOPIC_KEY_NAME, ProtocolType::PString(PString::default())),
        (
            1,
            PARTITIONS_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: false,
                p_type: Arc::new(ProtocolType::I32(I32::default())),
                values: None,
            }),
        ),
    ];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});
pub static STICKY_ASSIGNOR_USER_DATA: Lazy<Arc<Schema>> = Lazy::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![(
        0,
        PREVIOUS_ASSIGNMENT_KEY_NAME,
        ProtocolType::Array(ArrayType {
            can_be_empty: false,
            p_type: Arc::new(ProtocolType::Schema(Arc::clone(&TOPIC_ASSIGNMENT_STICKY))),
            values: None,
        }),
    )];
    let schema = Schema::from_fields_desc_vec(fields_desc);
    Arc::new(schema)
});

/////////////////---consumer protocol---/////////////////

pub struct ConsumerProtocol {}
pub struct TopicAssignment {
    pub partitions: Vec<TopicPartition>,
    pub user_data: BytesMut,
}


pub struct Subscription {
    pub topics: Vec<String>,
    // sticky assignor's subscripted topics list
    // serialized by STICKY_ASSIGNOR_USER_DATA
    pub user_data: BytesMut,
}

impl Subscription {
    pub fn read_from(mut buf: BytesMut) -> AppResult<Self> {
        let mut value_set = CONSUMER_PROTOCOL_V0_SCHEMA.clone().read_from(&mut buf)?;
        let version: i16 = value_set.get_field_value(VERSION_KEY_NAME).into();
        debug!("version:{}", version);

        let mut subscription = SUBSCRIPTION_V0.clone().read_from(&mut buf)?;
        let topics: ArrayType = subscription.get_field_value(TOPIC_KEY_NAME).into();
        let user_data: NPBytes = subscription.get_field_value(USER_DATA_KEY_NAME).into();
        let topics = topics
            .values
            .ok_or(AppError::ProtocolError(Cow::Borrowed("topics is empty")))?;
        let mut topics_vec: Vec<String> = Vec::with_capacity(topics.len());
        for topic in topics {
            topics_vec.push(topic.into());
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
        let version: i16 = value_set.get_field_value(VERSION_KEY_NAME).into();
        debug!("version:{}", version);

        let user_data: NPBytes = value_set.get_field_value(USER_DATA_KEY_NAME).into();
        let mut topic_assignment = ASSIGNMENT_V0.clone().read_from(&mut buf)?;
        let topic_partitions: ArrayType = topic_assignment
            .get_field_value(TOPIC_PARTITIONS_KEY_NAME)
            .into();
        let topic_partitions =
            topic_partitions
                .values
                .ok_or(AppError::ProtocolError(Cow::Borrowed(
                    "topic_partitions is empty",
                )))?;
        let mut topic_assignment_vec = Vec::with_capacity(topic_partitions.len());
        for topic_partition in topic_partitions {
            let mut topic_partition: ValueSet = topic_partition.into();
            let topic: String = topic_partition.get_field_value(TOPIC_KEY_NAME).into();
            let partitions: ArrayType = topic_partition.get_field_value(PARTITIONS_KEY_NAME).into();
            let partitions = partitions
                .values
                .ok_or(AppError::ProtocolError(Cow::Borrowed(
                    "partitions is empty",
                )))?;

            for partition in partitions {
                topic_assignment_vec.push(TopicPartition::new(topic.clone(), partition.into()));
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
