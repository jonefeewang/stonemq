use crate::protocol::api_schemas::produce::{
    ACKS_KEY_NAME, PARTITION_DATA_KEY_NAME, PARTITION_KEY_NAME, RECORD_SET_KEY_NAME,
    TIMEOUT_KEY_NAME, TOPIC_DATA_KEY_NAME, TOPIC_KEY_NAME,
};
use crate::protocol::schema::Schema;
use crate::protocol::structure::Struct;
use crate::protocol::types::FieldTypeEnum;
use crate::request::{PartitionData, ProduceRequest, TopicData};
use crate::AppResult;

pub struct Protocol {}

impl Protocol {
    ///
    /// format: `RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]`
    pub fn encode_produce_request_to_struct(produce_request: &ProduceRequest) -> AppResult<Struct> {
        let mut produce_structure =
            Struct::new_produce_request_struct(&produce_request.request_header);

        produce_structure.append_field_value(
            ACKS_KEY_NAME,
            FieldTypeEnum::from_acks(&produce_request.required_acks),
        )?;
        produce_structure.append_field_value(
            TIMEOUT_KEY_NAME,
            FieldTypeEnum::from_i32(produce_request.timeout),
        )?;

        let topic_data_ary =
            Self::generate_topic_data_array(&produce_request.topic_data, &produce_structure)?;
        let topic_data_schema =
            Schema::get_array_field_schema(produce_structure.get_schema(), TOPIC_DATA_KEY_NAME)?;

        let array = FieldTypeEnum::array_enum_from_structure_vec(topic_data_ary, topic_data_schema);
        produce_structure.append_field_value(TOPIC_DATA_KEY_NAME, array)?;

        Ok(produce_structure)
    }

    ///
    /// topic data `[TopicName [Partition MessageSetSize MessageSet]]`
    /// partition data `[Partition MessageSetSize MessageSet]`
    fn generate_topic_data_array(
        topic_data: &[TopicData],
        produce_structure: &Struct,
    ) -> AppResult<Vec<FieldTypeEnum>> {
        let mut topic_data_ary = Vec::with_capacity(topic_data.len());

        for data in topic_data {
            let mut topic_data_structure =
                produce_structure.new_struct_from_schema_field(TOPIC_DATA_KEY_NAME)?;
            topic_data_structure
                .append_field_value(TOPIC_KEY_NAME, FieldTypeEnum::from_string(&data.topic_name))?;

            let partition_data_ary =
                Self::generate_partition_data_array(&data.partition_data, &topic_data_structure)?;
            let schema = Schema::get_array_field_schema(
                topic_data_structure.get_schema(),
                PARTITION_DATA_KEY_NAME,
            )?;
            let array = FieldTypeEnum::array_enum_from_structure_vec(partition_data_ary, schema);
            topic_data_structure.append_field_value(PARTITION_DATA_KEY_NAME, array)?;
            topic_data_ary.push(FieldTypeEnum::StructE(topic_data_structure));
        }

        Ok(topic_data_ary)
    }

    ///
    /// topic data `[TopicName [Partition MessageSetSize MessageSet]]`
    /// partition data `[Partition MessageSetSize MessageSet]`
    fn generate_partition_data_array(
        partition_data_slice: &[PartitionData],
        topic_data_structure: &Struct,
    ) -> AppResult<Vec<FieldTypeEnum>> {
        let mut partition_data_ary = Vec::with_capacity(partition_data_slice.len());

        for partition_data in partition_data_slice {
            let mut partition_struct =
                topic_data_structure.new_struct_from_schema_field(PARTITION_DATA_KEY_NAME)?;
            partition_struct.append_field_value(
                PARTITION_KEY_NAME,
                FieldTypeEnum::from_i32(partition_data.partition),
            )?;
            partition_struct.append_field_value(
                RECORD_SET_KEY_NAME,
                // clone is ok here.
                // because message_set is backed by  a `Bytes`, it is cheap to clone it.
                FieldTypeEnum::RecordsE(partition_data.message_set.clone()),
            )?;

            partition_data_ary.push(FieldTypeEnum::StructE(partition_struct));
        }

        Ok(partition_data_ary)
    }
}
