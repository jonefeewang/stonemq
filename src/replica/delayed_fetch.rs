use std::future::Future;
use std::pin::Pin;

use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::Mutex;
use tracing::{debug, error};

use crate::log::PositionInfo;
use crate::utils::DelayedAsyncOperation;
use crate::{message::TopicPartition, request::FetchRequest};

use super::FetchResultSender;
use super::ReplicaManager;

use super::DelayedFetch;
impl DelayedFetch {
    pub fn new(
        request: FetchRequest,
        replica_manager: Arc<ReplicaManager>,
        read_position_infos: BTreeMap<TopicPartition, PositionInfo>,
        tx: FetchResultSender,
        correlation_id: i32,
    ) -> Self {
        Self {
            replica_manager,
            request,
            read_position_infos,
            tx: Arc::new(Mutex::new(Some(tx))),
            is_completed: AtomicBool::new(false),
            correlation_id,
        }
    }
}

impl DelayedAsyncOperation for DelayedFetch {
    fn delay_ms(&self) -> u64 {
        self.request.max_wait_ms as u64
    }

    async fn try_complete(&self) -> bool {
        if self.is_completed.load(Ordering::Acquire) {
            return true;
        }
        // 1. the partition has segment roll, the read offset is not in the active segment
        // 2. the read offset is in the active segment, but the total number of readable messages is enough
        let mut accumulated_size = 0;
        for (tp, _) in self.request.fetch_data.iter() {
            let log_fetch_info = self.read_position_infos.get(tp).unwrap();
            if let Ok(partition_current_position) = self.replica_manager.get_leo_info(tp) {
                if partition_current_position.base_offset < log_fetch_info.base_offset {
                    return true;
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
            return true;
        }
        false
    }

    fn on_complete(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let result = self.replica_manager.do_fetch(&self.request).await.unwrap();
            if let Some(tx) = self.tx.lock().await.take() {
                tx.send(result).unwrap();
            }
        })
    }

    async fn on_expiration(&self) {
        debug!(
            "delayed fetch expired, correlation_id: {}",
            self.correlation_id
        );
    }
}
