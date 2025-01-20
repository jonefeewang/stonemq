use std::clone::Clone;
use std::sync::{Arc, LazyLock};

use bytes::{BufMut, BytesMut};
use tracing::trace;

use crate::protocol::base::ProtocolType;
use crate::protocol::schema_base::{Schema, ValueSet};
use crate::protocol::types::{ApiKey, ApiVersion};
use crate::protocol::ProtocolCodec;
use crate::request::MetadataResponse;
use crate::AppResult;

const BROKERS_KEY_NAME: &str = "brokers";
const TOPIC_METADATA_KEY_NAME: &str = "topic_metadata";

// broker 级别字段名称
const NODE_ID_KEY_NAME: &str = "node_id";
const HOST_KEY_NAME: &str = "host";
const PORT_KEY_NAME: &str = "port";
const RACK_KEY_NAME: &str = "rack";

const CONTROLLER_ID_KEY_NAME: &str = "controller_id";

const CLUSTER_ID_KEY_NAME: &str = "cluster_id";

// topic 级别字段名称
const TOPIC_ERROR_CODE_KEY_NAME: &str = "topic_error_code";

// 可能的错误码
// UnknownTopic (3)
// LeaderNotAvailable (5)
// InvalidTopic (17)
// TopicAuthorizationFailed (29)

const TOPIC_KEY_NAME: &str = "topic";
const IS_INTERNAL_KEY_NAME: &str = "is_internal";
const PARTITION_METADATA_KEY_NAME: &str = "partition_metadata";

// partition 级别字段名称
const PARTITION_ERROR_CODE_KEY_NAME: &str = "partition_error_code";

// 可能的错误码
// LeaderNotAvailable (5)
// ReplicaNotAvailable (9)

const PARTITION_KEY_NAME: &str = "partition_id";
const LEADER_KEY_NAME: &str = "leader";
const REPLICAS_KEY_NAME: &str = "replicas";
const ISR_KEY_NAME: &str = "isr";
const THROTTLE_TIME_MS_KEY_NAME: &str = "throttle_time_ms";

pub static PARTITION_METADATA_V0: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, PARTITION_ERROR_CODE_KEY_NAME, 0i16.into()),
        (1, PARTITION_KEY_NAME, 0i32.into()),
        (2, LEADER_KEY_NAME, 0i32.into()),
        (3, REPLICAS_KEY_NAME, ProtocolType::array_of_i32_type(0i32)),
        (4, ISR_KEY_NAME, ProtocolType::array_of_i32_type(0i32)),
    ]);
    Arc::new(schema)
});

pub static TOPIC_METADATA_V0: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, TOPIC_ERROR_CODE_KEY_NAME, 0i16.into()),
        (1, TOPIC_KEY_NAME, "".into()),
        (
            2,
            PARTITION_METADATA_KEY_NAME,
            ProtocolType::array_of_schema(PARTITION_METADATA_V0.clone()),
        ),
    ]);
    Arc::new(schema)
});
pub static TOPIC_METADATA_V1: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, TOPIC_ERROR_CODE_KEY_NAME, 0i16.into()),
        (1, TOPIC_KEY_NAME, "".into()),
        (2, IS_INTERNAL_KEY_NAME, false.into()),
        (
            3,
            PARTITION_METADATA_KEY_NAME,
            ProtocolType::array_of_schema(PARTITION_METADATA_V0.clone()),
        ),
    ]);
    Arc::new(schema)
});
pub static METADATA_BROKER_V0: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, NODE_ID_KEY_NAME, 0i32.into()),
        (1, HOST_KEY_NAME, "".into()),
        (2, PORT_KEY_NAME, 0i32.into()),
    ]);
    Arc::new(schema)
});
pub static METADATA_BROKER_V1: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, NODE_ID_KEY_NAME, 0i32.into()),
        (1, HOST_KEY_NAME, "".into()),
        (2, PORT_KEY_NAME, 0i32.into()),
        (3, RACK_KEY_NAME, Option::<String>::default().into()),
    ]);
    Arc::new(schema)
});

pub static METADATA_RESPONSE_V0: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (
            0,
            BROKERS_KEY_NAME,
            ProtocolType::array_of_schema(METADATA_BROKER_V0.clone()),
        ),
        (
            1,
            TOPIC_METADATA_KEY_NAME,
            ProtocolType::array_of_schema(TOPIC_METADATA_V0.clone()),
        ),
    ]);
    Arc::new(schema)
});
pub static METADATA_RESPONSE_V1: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (
            0,
            BROKERS_KEY_NAME,
            ProtocolType::array_of_schema(METADATA_BROKER_V1.clone()),
        ),
        (
            1,
            TOPIC_METADATA_KEY_NAME,
            ProtocolType::array_of_schema(TOPIC_METADATA_V1.clone()),
        ),
    ]);
    Arc::new(schema)
});
pub static METADATA_RESPONSE_V2: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (
            0,
            BROKERS_KEY_NAME,
            ProtocolType::array_of_schema(METADATA_BROKER_V1.clone()),
        ),
        (1, CLUSTER_ID_KEY_NAME, Option::<String>::default().into()),
        (2, CONTROLLER_ID_KEY_NAME, 0i32.into()),
        (
            3,
            TOPIC_METADATA_KEY_NAME,
            ProtocolType::array_of_schema(TOPIC_METADATA_V1.clone()),
        ),
    ]);
    Arc::new(schema)
});
pub static METADATA_RESPONSE_V3: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (0, THROTTLE_TIME_MS_KEY_NAME, 0i32.into()),
        (
            1,
            BROKERS_KEY_NAME,
            ProtocolType::array_of_schema(METADATA_BROKER_V1.clone()),
        ),
        (2, CLUSTER_ID_KEY_NAME, Option::<String>::default().into()),
        (3, CONTROLLER_ID_KEY_NAME, 0i32.into()),
        (
            4,
            TOPIC_METADATA_KEY_NAME,
            ProtocolType::array_of_schema(TOPIC_METADATA_V1.clone()),
        ),
    ]);
    Arc::new(schema)
});

impl MetadataResponse {
    pub(crate) fn encode_to_value_set(self, metadata_rsp_valueset: &mut ValueSet) {
        // throttle_time_ms
        if metadata_rsp_valueset
            .schema
            .has_field(THROTTLE_TIME_MS_KEY_NAME)
        {
            metadata_rsp_valueset
                .append_field_value(THROTTLE_TIME_MS_KEY_NAME, self.throttle_time_ms.into());
        }
        // brokers

        let mut broker_ary: Vec<ProtocolType> = Vec::with_capacity(self.brokers.len());
        for broker in self.brokers {
            let mut broker_valueset =
                metadata_rsp_valueset.sub_valueset_of_ary_field(BROKERS_KEY_NAME);
            broker_valueset.append_field_value(NODE_ID_KEY_NAME, broker.node_id.into());
            broker_valueset.append_field_value(HOST_KEY_NAME, broker.host.into());
            broker_valueset.append_field_value(PORT_KEY_NAME, broker.port.into());
            if broker_valueset.schema.has_field(RACK_KEY_NAME) {
                broker_valueset.append_field_value(RACK_KEY_NAME, broker.rack.into());
            }
            broker_ary.push(ProtocolType::ValueSet(broker_valueset));
        }
        let broker_ary_schema = metadata_rsp_valueset
            .schema
            .clone()
            .sub_schema_of_ary_field(BROKERS_KEY_NAME);
        metadata_rsp_valueset.append_field_value(
            BROKERS_KEY_NAME,
            ProtocolType::array_of_value_set(broker_ary, broker_ary_schema),
        );

        // cluster_id
        if metadata_rsp_valueset.schema.has_field(CLUSTER_ID_KEY_NAME) {
            metadata_rsp_valueset.append_field_value(CLUSTER_ID_KEY_NAME, self.cluster_id.into());
        }

        // controller_id
        if metadata_rsp_valueset
            .schema
            .has_field(CONTROLLER_ID_KEY_NAME)
        {
            metadata_rsp_valueset
                .append_field_value(CONTROLLER_ID_KEY_NAME, self.controller.node_id.into());
        }

        // topic_metadata
        let mut topic_metadata_ary: Vec<ProtocolType> =
            Vec::with_capacity(self.topic_metadata.len());
        for topic_metadata in self.topic_metadata {
            // create valueset for each topic
            let mut topic_metadata_valueset =
                metadata_rsp_valueset.sub_valueset_of_ary_field(TOPIC_METADATA_KEY_NAME);
            // error code
            topic_metadata_valueset
                .append_field_value(TOPIC_ERROR_CODE_KEY_NAME, topic_metadata.error_code.into());
            // internal
            topic_metadata_valueset.append_field_value(TOPIC_KEY_NAME, topic_metadata.topic.into());
            if topic_metadata_valueset
                .schema
                .has_field(IS_INTERNAL_KEY_NAME)
            {
                topic_metadata_valueset
                    .append_field_value(IS_INTERNAL_KEY_NAME, topic_metadata.is_internal.into());
            }
            // partition_metadata
            let mut partition_metadata_ary: Vec<ProtocolType> =
                Vec::with_capacity(topic_metadata.partition_metadata.len());
            for partition_metadata in topic_metadata.partition_metadata {
                let mut partition_metadata_valueset =
                    topic_metadata_valueset.sub_valueset_of_ary_field(PARTITION_METADATA_KEY_NAME);
                partition_metadata_valueset.append_field_value(
                    PARTITION_ERROR_CODE_KEY_NAME,
                    partition_metadata.partition_error_code.into(),
                );
                partition_metadata_valueset
                    .append_field_value(PARTITION_KEY_NAME, partition_metadata.partition_id.into());
                partition_metadata_valueset
                    .append_field_value(LEADER_KEY_NAME, partition_metadata.leader.node_id.into());
                let replicas_ary = partition_metadata
                    .replicas
                    .iter()
                    .map(|node| node.node_id)
                    .collect();
                partition_metadata_valueset.append_field_value(
                    REPLICAS_KEY_NAME,
                    ProtocolType::array_of(Some(replicas_ary)),
                );
                let isr_ary = partition_metadata
                    .isr
                    .iter()
                    .map(|node| node.node_id)
                    .collect();
                partition_metadata_valueset
                    .append_field_value(ISR_KEY_NAME, ProtocolType::array_of(Some(isr_ary)));
                partition_metadata_ary.push(ProtocolType::ValueSet(partition_metadata_valueset));
            }
            let partition_metadata_schema = topic_metadata_valueset
                .schema
                .clone()
                .sub_schema_of_ary_field(PARTITION_METADATA_KEY_NAME);
            topic_metadata_valueset.append_field_value(
                PARTITION_METADATA_KEY_NAME,
                ProtocolType::array_of_value_set(partition_metadata_ary, partition_metadata_schema),
            );
            topic_metadata_ary.push(ProtocolType::ValueSet(topic_metadata_valueset));
        }
        let topic_metadata_schema = metadata_rsp_valueset
            .schema
            .clone()
            .sub_schema_of_ary_field(TOPIC_METADATA_KEY_NAME);
        metadata_rsp_valueset.append_field_value(
            TOPIC_METADATA_KEY_NAME,
            ProtocolType::array_of_value_set(topic_metadata_ary, topic_metadata_schema),
        );
    }
}

impl ProtocolCodec<MetadataResponse> for MetadataResponse {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::Metadata);
        let mut metadata_reps_value_set = ValueSet::new(schema);
        trace!("write response:{:?}", &self);
        self.encode_to_value_set(&mut metadata_reps_value_set);
        // correlation_id + response_total_size
        let response_total_size = 4 + metadata_reps_value_set.size();
        let mut writer = BytesMut::with_capacity(response_total_size);
        writer.put_i32(response_total_size as i32);
        writer.put_i32(correlation_id);
        metadata_reps_value_set.write_to(&mut writer);
        trace!(
            "write response total size:{} with correlation_id:{}",
            response_total_size,
            correlation_id
        );

        writer
    }

    fn decode(_buffer: &mut BytesMut, _api_version: &ApiVersion) -> AppResult<MetadataResponse> {
        todo!()
    }
}
