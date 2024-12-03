use crate::protocol::api::consumer_groups::fetch::{
    FETCH_REQUEST_V5_SCHEMA, FETCH_RESPONSE_V5_SCHEMA,
};
use crate::protocol::api::consumer_groups::heartbeat::{
    HEARTBEAT_REQUEST_V1_SCHEMA, HEARTBEAT_RESPONSE_V1_SCHEMA,
};
use crate::protocol::api::consumer_groups::join_group::{
    JOIN_GROUP_REQUEST_V2_SCHEMA, JOIN_GROUP_RESPONSE_V2_SCHEMA,
};
use crate::protocol::api::consumer_groups::sync_group::{
    SYNC_GROUP_REQUEST_V1_SCHEMA, SYNC_GROUP_RESPONSE_V1_SCHEMA,
};
use crate::protocol::api::consumer_groups::{
    FIND_COORDINATOR_REQUEST_V1_SCHEMA, FIND_COORDINATOR_RESPONSE_V1_SCHEMA,
    LEAVE_GROUP_RESPONSE_V1_SCHEMA, OFFSET_COMMIT_REQUEST_V3_SCHEMA,
    OFFSET_COMMIT_RESPONSE_V3_SCHEMA, OFFSET_FETCH_REQUEST_V3_SCHEMA,
    OFFSET_FETCH_RESPONSE_V3_SCHEMA,
};
use bytes::BytesMut;
use std::sync::Arc;

use crate::protocol::api::metadata_reps::{
    METADATA_RESPONSE_V0, METADATA_RESPONSE_V1, METADATA_RESPONSE_V2, METADATA_RESPONSE_V3,
};
use crate::protocol::api::metadata_req::{
    METADATA_REQUEST_V0, METADATA_REQUEST_V1, METADATA_REQUEST_V4,
};
use crate::protocol::api::produce_reps::{
    PRODUCE_RESPONSE_V0, PRODUCE_RESPONSE_V1, PRODUCE_RESPONSE_V2,
};
use crate::protocol::api::produce_req::{
    PRODUCE_REQUEST_SCHEMA_V0, PRODUCE_REQUEST_SCHEMA_V3,
};
use crate::protocol::api::{
    API_VERSIONS_REQUEST_V0, API_VERSIONS_RESPONSE_V0, API_VERSIONS_RESPONSE_V1,
};
use crate::protocol::schema::Schema;
use crate::AppResult;

use super::api_key::ApiKey::{
    self, ApiVersionKey, Fetch, FindCoordinator, Heartbeat, JoinGroup, LeaveGroup, Metadata,
    OffsetCommit, OffsetFetch, Produce, SyncGroup,
};
use super::api_versions::ApiVersion;
use ApiVersion::{V0, V1, V2, V3, V4, V5};

pub trait ProtocolCodec<T> {
    fn encode(self, api_version: &ApiVersion, correlation_id: i32) -> BytesMut;

    fn decode(buffer: &mut BytesMut, api_version: &ApiVersion) -> AppResult<T>;

    fn fetch_request_schema_for_api(api_version: &ApiVersion, api_key: &ApiKey) -> Arc<Schema> {
        match (api_key, api_version) {
            // Produce API
            (Produce, V0 | V1 | V2) => Arc::clone(&PRODUCE_REQUEST_SCHEMA_V0),
            (Produce, V3) => Arc::clone(&PRODUCE_REQUEST_SCHEMA_V3),
            (Produce, V4 | V5) => todo!("not exist"),

            // Fetch API
            (Fetch, V0 | V1 | V2 | V3 | V4) => todo!("too old, not support"),
            (Fetch, V5) => Arc::clone(&FETCH_REQUEST_V5_SCHEMA),

            // Metadata API
            (Metadata, V0) => Arc::clone(&METADATA_REQUEST_V0),
            (Metadata, V1 | V2 | V3) => Arc::clone(&METADATA_REQUEST_V1),
            (Metadata, V4) => Arc::clone(&METADATA_REQUEST_V4),
            (Metadata, V5) => todo!("not exist"),

            // ApiVersion API
            (ApiVersionKey, V0 | V1) => Arc::clone(&API_VERSIONS_REQUEST_V0),
            (ApiVersionKey, V2 | V3 | V4 | V5) => todo!("not exist"),

            // FindCoordinator API
            (FindCoordinator, V0) => todo!("too old, not support"),
            (FindCoordinator, V1) => Arc::clone(&FIND_COORDINATOR_REQUEST_V1_SCHEMA),
            (FindCoordinator, V2 | V3 | V4 | V5) => todo!("not exist"),

            // JoinGroup API
            (JoinGroup, V0) => todo!("too old, not support"),
            (JoinGroup, V1 | V2) => Arc::clone(&JOIN_GROUP_REQUEST_V2_SCHEMA),
            (JoinGroup, V3 | V4 | V5) => todo!("not exist"),

            // SyncGroup API
            (SyncGroup, V0 | V1) => Arc::clone(&SYNC_GROUP_REQUEST_V1_SCHEMA),
            (SyncGroup, V2 | V3 | V4 | V5) => todo!("not exist"),

            // LeaveGroup API
            (LeaveGroup, V0 | V1) => Arc::clone(&LEAVE_GROUP_RESPONSE_V1_SCHEMA),
            (LeaveGroup, V2 | V3 | V4 | V5) => todo!("not exist"),

            // Heartbeat API
            (Heartbeat, V0) => todo!("too old, not support"),
            (Heartbeat, V1) => Arc::clone(&HEARTBEAT_REQUEST_V1_SCHEMA),
            (Heartbeat, V2 | V3 | V4 | V5) => todo!("not exist"),

            // OffsetCommit API
            (OffsetCommit, V0 | V1) => todo!("too old, not support"),
            (OffsetCommit, V2 | V3) => Arc::clone(&OFFSET_COMMIT_REQUEST_V3_SCHEMA),
            (OffsetCommit, V4 | V5) => todo!("not exist"),

            // OffsetFetch API
            (OffsetFetch, V0 | V1) => todo!("too old, not support"),
            (OffsetFetch, V2 | V3) => Arc::clone(&OFFSET_FETCH_REQUEST_V3_SCHEMA),
            (OffsetFetch, V4 | V5) => todo!("not exist"),
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
            (ApiVersionKey, _) => todo!(),

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
