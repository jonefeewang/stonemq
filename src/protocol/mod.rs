mod api;
mod base;
mod schema_base;
mod types;
// protocol codec
pub use schema_base::ProtocolCodec;
// api key and version
pub use types::{Acks, ApiKey, ApiVersion};

use once_cell::sync::Lazy;
use std::{collections::HashMap, sync::Arc};
use types::ApiVersion::{V0, V1, V2, V3, V4, V5};

// supported api versions summary
pub static SUPPORTED_API_VERSIONS: Lazy<Arc<HashMap<i16, Vec<ApiVersion>>>> = Lazy::new(|| {
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
