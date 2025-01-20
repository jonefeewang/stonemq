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

mod delayed_fetch;
mod partation_appender;
mod replica_manager;

pub use partation_appender::PartitionAppender;
pub use replica_manager::ReplicaManager;

use crate::log::JournalLog;
use crate::log::PositionInfo;
use crate::log::QueueLog;
use crate::message::LogFetchInfo;
use crate::message::TopicPartition;
use crate::request::FetchRequest;
use std::collections::BTreeMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::oneshot;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct JournalReplica {
    pub log: Arc<JournalLog>,
}

impl JournalReplica {
    pub fn new(log: Arc<JournalLog>) -> Self {
        Self { log }
    }
}
#[derive(Debug)]
pub struct QueueReplica {
    pub log: Arc<QueueLog>,
}
impl QueueReplica {
    pub fn new(log: Arc<QueueLog>) -> Self {
        Self { log }
    }
}

type FetchResultSender = oneshot::Sender<BTreeMap<TopicPartition, LogFetchInfo>>;
#[derive(Debug)]
pub struct DelayedFetch {
    pub replica_manager: Arc<ReplicaManager>,
    pub request: FetchRequest,
    pub read_position_infos: BTreeMap<TopicPartition, PositionInfo>,
    pub tx: Arc<Mutex<Option<FetchResultSender>>>,
    is_completed: AtomicBool,
    pub correlation_id: i32,
}
