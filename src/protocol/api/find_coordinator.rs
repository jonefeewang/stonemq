// Copyright 2025 jonefeewang@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, LazyLock};

use bytes::{BufMut, BytesMut};

use crate::{
    protocol::{
        base::{NPString, PString, ProtocolType, I16, I32, I8},
        schema_base::{Schema, ValueSet},
        ApiKey, ApiVersion, ProtocolCodec,
    },
    AppResult,
};

const COORDINATOR_KEY_KEY_NAME: &str = "coordinator_key";
const COORDINATOR_TYPE_KEY_NAME: &str = "coordinator_type";

const NODE_ID_KEY_NAME: &str = "node_id";
const NODE_HOST_KEY_NAME: &str = "host";
const NODE_PORT_KEY_NAME: &str = "port";

const ERROR_MESSAGE_KEY_NAME: &str = "error_message";
const COORDINATOR_KEY_NAME: &str = "coordinator";

const ERROR_CODE_KEY_NAME: &str = "error_code";
const THROTTLE_TIME_KEY_NAME: &str = "throttle_time_ms";

use crate::request::{FindCoordinatorRequest, FindCoordinatorResponse};

impl ProtocolCodec<FindCoordinatorRequest> for FindCoordinatorRequest {
    fn encode(self, _api_version: &ApiVersion, _correlation_id: i32) -> BytesMut {
        todo!()
    }

    fn decode(
        buffer: &mut BytesMut,
        api_version: &ApiVersion,
    ) -> AppResult<FindCoordinatorRequest> {
        let schema = Self::fetch_request_schema_for_api(api_version, &ApiKey::FindCoordinator)?;
        let mut value_set = schema.read_from(buffer)?;
        let coordinator_key = value_set.get_field_value(COORDINATOR_KEY_KEY_NAME).into();
        let coordinator_type = value_set.get_field_value(COORDINATOR_TYPE_KEY_NAME).into();
        Ok(FindCoordinatorRequest {
            coordinator_key,
            coordinator_type,
        })
    }
}

impl ProtocolCodec<FindCoordinatorResponse> for FindCoordinatorResponse {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut {
        let schema = Self::fetch_response_schema_for_api(api_version, &ApiKey::FindCoordinator);
        let mut value_set = ValueSet::new(schema);
        self.encode_to_value_set(&mut value_set);
        let body_size = value_set.size();
        // correlation_id + response_total_size
        let response_total_size = 4 + body_size;
        let mut writer = BytesMut::with_capacity(response_total_size);
        writer.put_i32(response_total_size as i32);
        writer.put_i32(correlation_id);
        value_set.write_to(&mut writer);
        writer
    }

    fn decode(
        _buffer: &mut BytesMut,
        _api_version: &ApiVersion,
    ) -> AppResult<FindCoordinatorResponse> {
        todo!()
    }
}

impl FindCoordinatorResponse {
    fn encode_to_value_set(self, response_valueset: &mut ValueSet) {
        // outer value set
        response_valueset.append_field_value(THROTTLE_TIME_KEY_NAME, self.throttle_time_ms.into());
        response_valueset.append_field_value(ERROR_CODE_KEY_NAME, self.error.into());
        response_valueset.append_field_value(ERROR_MESSAGE_KEY_NAME, self.error_message.into());

        // nested value set
        let mut coordinator_valueset =
            response_valueset.sub_valueset_of_schema_field(COORDINATOR_KEY_NAME);
        coordinator_valueset.append_field_value(NODE_ID_KEY_NAME, self.node.node_id.into());
        coordinator_valueset.append_field_value(NODE_HOST_KEY_NAME, self.node.host.into());
        coordinator_valueset.append_field_value(NODE_PORT_KEY_NAME, self.node.port.into());

        // 将嵌套的value set 添加到response的value set中
        response_valueset.append_field_value(
            COORDINATOR_KEY_NAME,
            ProtocolType::ValueSet(coordinator_valueset),
        );
    }
}

pub static FIND_COORDINATOR_REQUEST_V1_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (
            0,
            COORDINATOR_KEY_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (
            1,
            COORDINATOR_TYPE_KEY_NAME,
            ProtocolType::I8(I8::default()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FIND_COORDINATOR_BROKER_V0_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, NODE_ID_KEY_NAME, ProtocolType::I32(I32::default())),
        (
            1,
            NODE_HOST_KEY_NAME,
            ProtocolType::PString(PString::default()),
        ),
        (2, NODE_PORT_KEY_NAME, ProtocolType::I32(I32::default())),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});

pub static FIND_COORDINATOR_RESPONSE_V1_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    let fields_desc: Vec<(i32, &str, ProtocolType)> = vec![
        (0, THROTTLE_TIME_KEY_NAME, ProtocolType::I32(I32::default())),
        (1, ERROR_CODE_KEY_NAME, ProtocolType::I16(I16::default())),
        (
            2,
            ERROR_MESSAGE_KEY_NAME,
            ProtocolType::NPString(NPString::default()),
        ),
        (
            3,
            COORDINATOR_KEY_NAME,
            ProtocolType::Schema(FIND_COORDINATOR_BROKER_V0_SCHEMA.clone()),
        ),
    ];
    Arc::new(Schema::from_fields_desc_vec(fields_desc))
});
