use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use once_cell::sync::Lazy;

use crate::protocol::base::{Bool, PString, ProtocolType};
use crate::protocol::schema_base::{Schema, ValueSet};
use crate::protocol::types::ArrayType;
use crate::protocol::{ApiKey, ApiVersion, ProtocolCodec};
use crate::request::MetaDataRequest;
use crate::AppResult;

impl ProtocolCodec<MetaDataRequest> for MetaDataRequest {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::Metadata);
        let mut metadata_req_value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut metadata_req_value_set);
        let mut writer = BytesMut::with_capacity(metadata_req_value_set.size());
        metadata_req_value_set.write_to(&mut writer);
        writer.put_i32(correlation_id);
        writer
    }

    fn decode(buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<MetaDataRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::Metadata);
        let metadata_req_value_set = schema.read_from(buffer)?;
        let produce_request = MetaDataRequest::decode_from_value_set(metadata_req_value_set)?;
        Ok(produce_request)
    }
}
impl MetaDataRequest {
    pub fn decode_from_value_set(mut value_set: ValueSet) -> AppResult<MetaDataRequest> {
        let topics_data_type = value_set.get_field_value(TOPICS_KEY_NAME);
        let allow_auto_topic_creation: bool = value_set
            .get_field_value(ALLOW_AUTO_TOPIC_CREATION_KEY_NAME)
            .into();
        let topics_ary_type: ArrayType = topics_data_type.into();
        let topics_ary = topics_ary_type.values;

        let topics = match topics_ary {
            None => None,
            Some(ary) => {
                let mut topics = Vec::with_capacity(ary.len());
                for topic in ary {
                    if let ProtocolType::PString(pstring) = topic {
                        topics.push(pstring.value);
                    }
                }
                Some(topics)
            }
        };

        Ok(MetaDataRequest {
            topics,
            allow_auto_topic_creation,
        })
    }
    fn encode_to_value_set(self, value_set: &mut ValueSet) {
        let ary_datatype = ProtocolType::array_of(self.topics);
        value_set.append_field_value(TOPICS_KEY_NAME, ary_datatype);
        value_set.append_field_value(
            ALLOW_AUTO_TOPIC_CREATION_KEY_NAME,
            self.allow_auto_topic_creation.into(),
        );
    }
}

const TOPICS_KEY_NAME: &str = "topics";
const ALLOW_AUTO_TOPIC_CREATION_KEY_NAME: &str = "allow_auto_topic_creation";
pub static METADATA_REQUEST_V0: Lazy<Arc<Schema>> = Lazy::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![(
        0,
        TOPICS_KEY_NAME,
        ProtocolType::Array(ArrayType {
            can_be_empty: false,
            p_type: Arc::new(ProtocolType::PString(PString::default())),
            values: None,
        }),
    )]);
    Arc::new(schema)
});
pub static METADATA_REQUEST_V1: Lazy<Arc<Schema>> = Lazy::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![(
        0,
        TOPICS_KEY_NAME,
        ProtocolType::Array(ArrayType {
            can_be_empty: true,
            p_type: Arc::new(ProtocolType::PString(PString::default())),
            values: None,
        }),
    )]);
    Arc::new(schema)
});
pub static METADATA_REQUEST_V4: Lazy<Arc<Schema>> = Lazy::new(|| {
    let schema = Schema::from_fields_desc_vec(vec![
        (
            0,
            TOPICS_KEY_NAME,
            ProtocolType::Array(ArrayType {
                can_be_empty: true,
                p_type: Arc::new(ProtocolType::PString(PString::default())),
                values: None,
            }),
        ),
        (
            1,
            ALLOW_AUTO_TOPIC_CREATION_KEY_NAME,
            ProtocolType::Bool(Bool::default()),
        ),
    ]);
    Arc::new(schema)
});
