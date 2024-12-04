use crate::group_consume::GroupCoordinator;
use crate::replica::ReplicaManager;
use std::sync::Arc;

use super::RequestHeader;

pub struct RequestContext {
    pub client_ip: String,
    pub request_header: RequestHeader,
    pub replica_manager: Arc<ReplicaManager>,
    pub group_coordinator: Arc<GroupCoordinator>,
}
