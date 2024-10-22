use crate::{
    request::consumer_group::{FindCoordinatorRequest, FindCoordinatorResponse},
    service::Node,
    AppResult,
};

pub struct Coordinator {
    pub node: Node,
}

impl Coordinator {
    pub fn new(node: Node) -> Self {
        Self { node }
    }

    pub fn find_coordinator(&self, group_id: &str) -> AppResult<FindCoordinatorResponse> {
        // 因为stonemq目前支持单机，所以coordinator就是自身
        let response: FindCoordinatorResponse = self.node.clone().into();
        Ok(response)
    }
}
