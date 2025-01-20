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

use crate::protocol::api::{
    FETCH_REQUEST_V5_SCHEMA, FETCH_RESPONSE_V5_SCHEMA, LEAVE_GROUP_REQUEST_V1_SCHEMA,
};
use crate::protocol::api::{
    FIND_COORDINATOR_REQUEST_V1_SCHEMA, FIND_COORDINATOR_RESPONSE_V1_SCHEMA,
    LEAVE_GROUP_RESPONSE_V1_SCHEMA, OFFSET_COMMIT_REQUEST_V3_SCHEMA,
    OFFSET_COMMIT_RESPONSE_V3_SCHEMA, OFFSET_FETCH_REQUEST_V3_SCHEMA,
    OFFSET_FETCH_RESPONSE_V3_SCHEMA,
};
use crate::protocol::api::{HEARTBEAT_REQUEST_V1_SCHEMA, HEARTBEAT_RESPONSE_V1_SCHEMA};
use crate::protocol::api::{JOIN_GROUP_REQUEST_V2_SCHEMA, JOIN_GROUP_RESPONSE_V2_SCHEMA};
use crate::protocol::api::{SYNC_GROUP_REQUEST_V1_SCHEMA, SYNC_GROUP_RESPONSE_V1_SCHEMA};
use bytes::BytesMut;
use std::sync::Arc;

use crate::protocol::api::{
    API_VERSIONS_REQUEST_V0, API_VERSIONS_RESPONSE_V0, API_VERSIONS_RESPONSE_V1,
};
use crate::protocol::api::{METADATA_REQUEST_V0, METADATA_REQUEST_V1, METADATA_REQUEST_V4};
use crate::protocol::api::{
    METADATA_RESPONSE_V0, METADATA_RESPONSE_V1, METADATA_RESPONSE_V2, METADATA_RESPONSE_V3,
};
use crate::protocol::api::{PRODUCE_REQUEST_SCHEMA_V0, PRODUCE_REQUEST_SCHEMA_V3};
use crate::protocol::api::{PRODUCE_RESPONSE_V0, PRODUCE_RESPONSE_V1, PRODUCE_RESPONSE_V2};
use crate::protocol::schema_base::Schema;
use crate::{AppError, AppResult};

use crate::protocol::types::ApiVersion;
use crate::protocol::types::ApiVersion::{V0, V1, V2, V3, V4, V5};
use crate::protocol::types::{
    ApiKey, ApiVersionKey, Fetch, FindCoordinator, Heartbeat, JoinGroup, LeaveGroup, Metadata,
    OffsetCommit, OffsetFetch, Produce, SyncGroup,
};

pub trait ProtocolCodec<T> {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut;

    fn decode(buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<T>;

    fn fetch_request_schema_for_api(
        api_version: &ApiVersion,
        api_key: &ApiKey,
    ) -> AppResult<Arc<Schema>> {
        match (api_key, api_version) {
            // Produce API
            (Produce, V0 | V1 | V2) => Ok(Arc::clone(&PRODUCE_REQUEST_SCHEMA_V0)),
            (Produce, V3) => Ok(Arc::clone(&PRODUCE_REQUEST_SCHEMA_V3)),
            (Produce, V4 | V5) => Err(AppError::UnsupportedVersion(api_version.as_i16())),

            // Fetch API
            (Fetch, V0 | V1 | V2 | V3 | V4) => {
                Err(AppError::UnsupportedVersion(api_version.as_i16()))
            }
            (Fetch, V5) => Ok(Arc::clone(&FETCH_REQUEST_V5_SCHEMA)),

            // Metadata API
            (Metadata, V0) => Ok(Arc::clone(&METADATA_REQUEST_V0)),
            (Metadata, V1 | V2 | V3) => Ok(Arc::clone(&METADATA_REQUEST_V1)),
            (Metadata, V4) => Ok(Arc::clone(&METADATA_REQUEST_V4)),
            (Metadata, V5) => Err(AppError::UnsupportedVersion(api_version.as_i16())),

            // ApiVersion API
            (ApiVersionKey, V0 | V1) => Ok(Arc::clone(&API_VERSIONS_REQUEST_V0)),
            (ApiVersionKey, V2 | V3 | V4 | V5) => {
                Err(AppError::UnsupportedVersion(api_version.as_i16()))
            }

            // FindCoordinator API
            (FindCoordinator, V0) => todo!("too old, not support"),
            (FindCoordinator, V1) => Ok(Arc::clone(&FIND_COORDINATOR_REQUEST_V1_SCHEMA)),
            (FindCoordinator, V2 | V3 | V4 | V5) => {
                Err(AppError::UnsupportedVersion(api_version.as_i16()))
            }

            // JoinGroup API
            (JoinGroup, V0) => todo!("too old, not support"),
            (JoinGroup, V1 | V2) => Ok(Arc::clone(&JOIN_GROUP_REQUEST_V2_SCHEMA)),
            (JoinGroup, V3 | V4 | V5) => Err(AppError::UnsupportedVersion(api_version.as_i16())),

            // SyncGroup API
            (SyncGroup, V0 | V1 | V2) => Ok(Arc::clone(&SYNC_GROUP_REQUEST_V1_SCHEMA)),
            (SyncGroup, V3 | V4 | V5) => Err(AppError::UnsupportedVersion(api_version.as_i16())),

            // LeaveGroup API
            (LeaveGroup, V0 | V1) => Ok(Arc::clone(&LEAVE_GROUP_REQUEST_V1_SCHEMA)),
            (LeaveGroup, V2 | V3 | V4 | V5) => {
                Err(AppError::UnsupportedVersion(api_version.as_i16()))
            }

            // Heartbeat API
            (Heartbeat, V0) => todo!("too old, not support"),
            (Heartbeat, V1) => Ok(Arc::clone(&HEARTBEAT_REQUEST_V1_SCHEMA)),
            (Heartbeat, V2 | V3 | V4 | V5) => {
                Err(AppError::UnsupportedVersion(api_version.as_i16()))
            }

            // OffsetCommit API
            (OffsetCommit, V0 | V1) => todo!("too old, not support"),
            (OffsetCommit, V2 | V3) => Ok(Arc::clone(&OFFSET_COMMIT_REQUEST_V3_SCHEMA)),
            (OffsetCommit, V4 | V5) => Err(AppError::UnsupportedVersion(api_version.as_i16())),

            // OffsetFetch API
            (OffsetFetch, V0 | V1) => todo!("too old, not support"),
            (OffsetFetch, V2 | V3) => Ok(Arc::clone(&OFFSET_FETCH_REQUEST_V3_SCHEMA)),
            (OffsetFetch, V4 | V5) => Err(AppError::UnsupportedVersion(api_version.as_i16())),
        }
    }
    fn fetch_response_schema_for_api(api_version: &ApiVersion, api_key: &ApiKey) -> Arc<Schema> {
        match (api_key, api_version) {
            // Produce API
            (Produce, V0) => Arc::clone(&PRODUCE_RESPONSE_V0),
            (Produce, V1) => Arc::clone(&PRODUCE_RESPONSE_V1),
            (Produce, V2 | V3 | V4) => Arc::clone(&PRODUCE_RESPONSE_V2),
            (Produce, V5) => todo!("not exist"),

            // Fetch API
            (Fetch, V0 | V1 | V2 | V3 | V4) => todo!("too old, not support"),
            (Fetch, V5) => Arc::clone(&FETCH_RESPONSE_V5_SCHEMA),

            // Metadata API
            (Metadata, V0) => Arc::clone(&METADATA_RESPONSE_V0),
            (Metadata, V1) => Arc::clone(&METADATA_RESPONSE_V1),
            (Metadata, V2) => Arc::clone(&METADATA_RESPONSE_V2),
            (Metadata, V3 | V4) => Arc::clone(&METADATA_RESPONSE_V3),
            (Metadata, V5) => todo!("not exist"),

            // ApiVersion API
            (ApiVersionKey, V0) => Arc::clone(&API_VERSIONS_RESPONSE_V0),
            (ApiVersionKey, V1) => Arc::clone(&API_VERSIONS_RESPONSE_V1),
            (ApiVersionKey, _) => Arc::clone(&API_VERSIONS_RESPONSE_V1),

            // JoinGroup API
            (JoinGroup, V0 | V1) => todo!("too old, not support"),
            (JoinGroup, V2) => Arc::clone(&JOIN_GROUP_RESPONSE_V2_SCHEMA),
            (JoinGroup, V3 | V4 | V5) => todo!("not exist"),

            // SyncGroup API
            (SyncGroup, V0) => todo!("too old, not support"),
            (SyncGroup, V1) => Arc::clone(&SYNC_GROUP_RESPONSE_V1_SCHEMA),
            (SyncGroup, V2 | V3 | V4 | V5) => todo!("not exist"),

            // Heartbeat API
            (Heartbeat, V0) => todo!("too old, not support"),
            (Heartbeat, V1) => Arc::clone(&HEARTBEAT_RESPONSE_V1_SCHEMA),
            (Heartbeat, V2 | V3 | V4 | V5) => todo!("not exist"),

            // LeaveGroup API
            (LeaveGroup, V0 | V1) => Arc::clone(&LEAVE_GROUP_RESPONSE_V1_SCHEMA),
            (LeaveGroup, V2 | V3 | V4 | V5) => todo!("not exist"),

            // OffsetFetch API
            (OffsetFetch, V0 | V1 | V2) => todo!("too old, not support"),
            (OffsetFetch, V3) => Arc::clone(&OFFSET_FETCH_RESPONSE_V3_SCHEMA),
            (OffsetFetch, V4 | V5) => todo!("not exist"),

            // OffsetCommit API
            (OffsetCommit, V0 | V1 | V2) => todo!("too old, not support"),
            (OffsetCommit, V3) => Arc::clone(&OFFSET_COMMIT_RESPONSE_V3_SCHEMA),
            (OffsetCommit, V4 | V5) => todo!("not exist"),

            // FindCoordinator API
            (FindCoordinator, V0) => todo!("too old, not support"),
            (FindCoordinator, V1) => Arc::clone(&FIND_COORDINATOR_RESPONSE_V1_SCHEMA),
            (FindCoordinator, V2 | V3 | V4 | V5) => todo!("not exist"),
        }
    }
}
