use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::oneshot;

use crate::{
    message::{MemoryRecords, TopicPartition},
    ReplicaManager,
};

use super::{delayed_operation::DelayedOperation, fetch::FetchRequest};

#[derive(Debug)]
pub struct DelayedFetch {
    pub replica_manager: Arc<ReplicaManager>,
    pub request: FetchRequest,
    pub tx: oneshot::Sender<BTreeMap<TopicPartition, (MemoryRecords, i64, i64)>>,
}
impl DelayedOperation for DelayedFetch {
    fn delay_ms(&self) -> u64 {
        self.request.max_wait as u64
    }

    
    async fn try_complete(&self) -> bool {
        // 1.读取的partition里有segment roll，读取的offset已经不在active的segment里了
        // 2.读取的offset还在active的segment里，但计算可读消息的总量已经够了
        todo!()
    }

    async fn on_complete(&self) {
        todo!()
    }

    async fn on_expiration(&self) {
        todo!()
    }

    fn is_completed(&self) -> &crossbeam_utils::atomic::AtomicCell<bool> {
        todo!()
    }

    fn cancel(&self) {
        todo!()
    }

    fn lock(&self) -> Option<&tokio::sync::RwLock<()>> {
        todo!()
    }
}
