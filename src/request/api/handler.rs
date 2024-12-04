use crate::{protocol::ProtocolCodec, request::RequestContext};

pub trait ApiHandler {
    type Request: ProtocolCodec<Self::Request> + Send + 'static;
    type Response: ProtocolCodec<Self::Response> + Send + 'static;

    // 处理请求并返回响应
    fn handle_request(
        &self,
        request: Self::Request,
        context: &RequestContext,
    ) -> impl std::future::Future<Output = Self::Response> + Send;
}
