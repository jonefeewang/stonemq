use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::{collections::BTreeMap, sync::Arc};

use crossbeam::atomic::AtomicCell;
use tokio::sync::oneshot;
use tracing::{debug, error};

use crate::log::PositionInfo;
use crate::utils::DelayedAsyncOperation;
use crate::{message::TopicPartition, request::fetch::FetchRequest};

use super::replica::ReplicaManager;
use super::LogFetchInfo;

type FetchResultSender = oneshot::Sender<BTreeMap<TopicPartition, LogFetchInfo>>;

#[derive(Debug)]
pub struct DelayedFetch {
    pub replica_manager: Arc<ReplicaManager>,
    pub request: FetchRequest,
    pub read_position_infos: BTreeMap<TopicPartition, PositionInfo>,
    pub tx: Arc<Mutex<Option<FetchResultSender>>>,
    is_completed: AtomicCell<bool>,
}
impl DelayedFetch {
    pub fn new(
        request: FetchRequest,
        replica_manager: Arc<ReplicaManager>,
        read_position_infos: BTreeMap<TopicPartition, PositionInfo>,
        tx: FetchResultSender,
    ) -> Self {
        Self {
            replica_manager,
            request,
            read_position_infos,
            tx: Arc::new(Mutex::new(Some(tx))),
            is_completed: AtomicCell::new(false),
        }
    }
}

impl DelayedAsyncOperation for DelayedFetch {
    fn delay_ms(&self) -> u64 {
        self.request.max_wait_ms as u64
    }

    async fn try_complete(&self) -> bool {
        if self.is_completed.load() {
            return true;
        }
        // 1.读取的partition里有segment roll，读取的offset已经不在active的segment里了
        // 2.读取的offset还在active的segment里，但计算可读消息的总量已经够了
        let mut accumulated_size = 0;
        for (tp, _) in self.request.fetch_data.iter() {
            let log_fetch_info = self.read_position_infos.get(tp).unwrap();
            if let Ok(partition_current_position) = self.replica_manager.get_leo_info(tp).await {
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
            if let Some(tx) = self.tx.lock().unwrap().take() {
                tx.send(result).unwrap();
            }
        })
    }

    async fn on_expiration(&self) {
        debug!("delayed fetch expired");
    }
}
