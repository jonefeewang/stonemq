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
pub struct FindCoordinatorRequest {
    pub coordinator_key: String,
    pub coordinator_type: i8,
}

impl FindCoordinatorRequest {
    pub const GROUP_ID_KEY_NAME: &'static str = "group_id";
    pub const COORDINATOR_KEY_KEY_NAME: &'static str = "coordinator_key";
    pub const COORDINATOR_TYPE_KEY_NAME: &'static str = "coordinator_type";
}
#[derive(Debug)]
pub struct FindCoordinatorResponse {
    pub throttle_time_ms: i32,
    pub error_message: Option<String>,
    pub error: i16,
    pub node: Node,
}
impl FindCoordinatorResponse {
    pub const ERROR_CODE_KEY_NAME: &'static str = "error_code";
    pub const ERROR_MESSAGE_KEY_NAME: &'static str = "error_message";
    pub const COORDINATOR_KEY_NAME: &'static str = "coordinator";

    // 可能的错误代码：
    //
    // COORDINATOR_NOT_AVAILABLE (15)
    // NOT_COORDINATOR (16)
    // GROUP_AUTHORIZATION_FAILED (30)

    // 协调器级别的字段名
    pub const NODE_ID_KEY_NAME: &'static str = "node_id";
    pub const HOST_KEY_NAME: &'static str = "host";
    pub const PORT_KEY_NAME: &'static str = "port";
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
