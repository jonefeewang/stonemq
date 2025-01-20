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

mod api;
mod base;
mod schema_base;
mod types;
// protocol codec
pub use schema_base::ProtocolCodec;
// api key and version
pub use types::{Acks, ApiKey, ApiVersion};

use std::{collections::HashMap, sync::Arc, sync::LazyLock};
use types::ApiVersion::{V0, V1, V2, V3, V4, V5};

// supported api versions summary
pub static SUPPORTED_API_VERSIONS: LazyLock<Arc<HashMap<i16, Vec<ApiVersion>>>> =
    LazyLock::new(|| {
        let mut map = HashMap::new();
        map.insert(ApiKey::ApiVersionKey as i16, vec![V0, V1]);
        map.insert(ApiKey::Metadata as i16, vec![V0, V1, V2, V3, V4]);
        map.insert(ApiKey::Produce as i16, vec![V0, V1, V2, V3]);
        map.insert(ApiKey::Fetch as i16, vec![V5]);
        map.insert(ApiKey::JoinGroup as i16, vec![V1, V2]);
        map.insert(ApiKey::SyncGroup as i16, vec![V0, V1]);
        map.insert(ApiKey::Heartbeat as i16, vec![V1]);
        map.insert(ApiKey::LeaveGroup as i16, vec![V0, V1]);
        map.insert(ApiKey::OffsetFetch as i16, vec![V2, V3]);
        map.insert(ApiKey::OffsetCommit as i16, vec![V2, V3]);
        map.insert(ApiKey::FindCoordinator as i16, vec![V1]);
        Arc::new(map)
    });
