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
