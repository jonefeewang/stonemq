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

mod api_version;
mod fetch;
mod find_coordinator;
mod heartbeat;
mod join_group;
mod leave_group;
mod metadata_reps;
mod metadata_req;
mod offset_commit;
mod offset_fetch;
mod produce_reps;
mod produce_req;
mod request_header;
mod sync_group;

// api version schema
pub use api_version::{
    API_VERSIONS_REQUEST_V0, API_VERSIONS_RESPONSE_V0, API_VERSIONS_RESPONSE_V1,
};
pub use fetch::FETCH_REQUEST_V5_SCHEMA;
pub use fetch::FETCH_RESPONSE_V5_SCHEMA;
pub use find_coordinator::FIND_COORDINATOR_REQUEST_V1_SCHEMA;
pub use find_coordinator::FIND_COORDINATOR_RESPONSE_V1_SCHEMA;
pub use heartbeat::HEARTBEAT_REQUEST_V1_SCHEMA;
pub use heartbeat::HEARTBEAT_RESPONSE_V1_SCHEMA;
pub use join_group::JOIN_GROUP_REQUEST_V2_SCHEMA;
pub use join_group::JOIN_GROUP_RESPONSE_V2_SCHEMA;
pub use leave_group::LEAVE_GROUP_REQUEST_V1_SCHEMA;
pub use leave_group::LEAVE_GROUP_RESPONSE_V1_SCHEMA;
pub use metadata_reps::METADATA_RESPONSE_V0;
pub use metadata_reps::METADATA_RESPONSE_V1;
pub use metadata_reps::METADATA_RESPONSE_V2;
pub use metadata_reps::METADATA_RESPONSE_V3;
pub use metadata_req::METADATA_REQUEST_V0;
pub use metadata_req::METADATA_REQUEST_V1;
pub use metadata_req::METADATA_REQUEST_V4;
pub use offset_commit::OFFSET_COMMIT_REQUEST_V3_SCHEMA;
pub use offset_commit::OFFSET_COMMIT_RESPONSE_V3_SCHEMA;
pub use offset_fetch::OFFSET_FETCH_REQUEST_V3_SCHEMA;
pub use offset_fetch::OFFSET_FETCH_RESPONSE_V3_SCHEMA;
pub use produce_reps::PRODUCE_RESPONSE_V0;
pub use produce_reps::PRODUCE_RESPONSE_V1;
pub use produce_reps::PRODUCE_RESPONSE_V2;
pub use produce_req::PRODUCE_REQUEST_SCHEMA_V0;
pub use produce_req::PRODUCE_REQUEST_SCHEMA_V3;
pub use sync_group::SYNC_GROUP_REQUEST_V1_SCHEMA;
pub use sync_group::SYNC_GROUP_RESPONSE_V1_SCHEMA;
