use crate::service::Node;

use crate::request::RequestContext;

use super::ApiHandler;

pub struct FindCoordinatorRequestHandler;
impl ApiHandler for FindCoordinatorRequestHandler {
    type Request = FindCoordinatorRequest;
    type Response = FindCoordinatorResponse;

    async fn handle_request(
        &self,
        request: FindCoordinatorRequest,
        context: &RequestContext,
    ) -> FindCoordinatorResponse {
        context.group_coordinator.find_coordinator(request).await
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FindCoordinatorRequest {
    pub coordinator_key: String,
    pub coordinator_type: i8,
}

#[derive(Debug)]
pub struct FindCoordinatorResponse {
    pub throttle_time_ms: i32,
    pub error_message: Option<String>,
    pub error: i16,
    pub node: Node,
}

impl From<Node> for FindCoordinatorResponse {
    fn from(node: Node) -> Self {
        FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_message: None,
            error: 0,
            node,
        }
    }
}
