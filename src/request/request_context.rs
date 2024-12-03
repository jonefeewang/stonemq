use std::sync::Arc;

use crate::{message::GroupCoordinator, ReplicaManager};

use super::request_header::RequestHeader;

#[derive(Debug)]
pub struct RequestContext {
    pub replica_manager: Arc<ReplicaManager>,
    pub group_coordinator: Arc<GroupCoordinator>,
    pub request_header: RequestHeader,
}
impl RequestContext {
    pub fn new(
        request_header: RequestHeader,
        replica_manager: Arc<ReplicaManager>,
        group_coordinator: Arc<GroupCoordinator>,
    ) -> Self {
        RequestContext {
            request_header,
            replica_manager,
            group_coordinator,
        }
    }
}
