use std::hash::{Hash, Hasher};
use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::oneshot;
use tracing::error;

use crate::log::PositionInfo;
use crate::{message::TopicPartition, request::fetch::FetchRequest, ReplicaManager};

use super::{delayed_operation::DelayedOperation, LogFetchInfo};

#[derive(Debug)]
pub struct DelayedFetch {
    pub replica_manager: Arc<ReplicaManager>,
    pub request: FetchRequest,
    pub position_infos: BTreeMap<TopicPartition, PositionInfo>,
    pub tx: Option<oneshot::Sender<BTreeMap<TopicPartition, LogFetchInfo>>>,
}
impl DelayedOperation for DelayedFetch {
    fn delay_ms(&self) -> u64 {
        self.request.max_wait as u64
    }

    async fn try_complete(&mut self) -> bool {
        // 1.读取的partition里有segment roll，读取的offset已经不在active的segment里了
        // 2.读取的offset还在active的segment里，但计算可读消息的总量已经够了
        let mut accumulated_size = 0;
        for (tp, partition_data_req) in self.request.fetch_data.iter() {
            let log_fetch_info = self.position_infos.get(tp).unwrap();
            if let Ok(partition_current_position) = self.replica_manager.get_leo_info(tp).await {
                if partition_current_position.base_offset < log_fetch_info.base_offset {
                    return self.force_complete(true).await;
                } else if partition_current_position.offset <= log_fetch_info.offset {
                    accumulated_size +=
                        log_fetch_info.position - partition_current_position.position;
                }
            } else {
                error!("get leo info failed, tp: {:?}", tp);
                return false;
            }
        }
        if accumulated_size >= self.request.max_bytes as i64 {
            return self.force_complete(true).await;
        }
        false
    }

    async fn on_complete(&mut self) {
        let result = self.replica_manager.do_fetch(&self.request).await.unwrap();

        self.tx.take().unwrap().send(result);
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

impl Hash for DelayedFetch {
    fn hash<H: Hasher>(&self, state: &mut H) {
        format!("{}-{}", self.request.client_ip, self.request.correlation_id).hash(state);
    }
}

impl PartialEq for DelayedFetch {
    fn eq(&self, other: &Self) -> bool {
        self.request.client_ip == other.request.client_ip
            && self.request.correlation_id == other.request.correlation_id
    }
}
impl Eq for DelayedFetch {}
