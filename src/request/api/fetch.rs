use std::collections::BTreeMap;

use crate::{
    message::{LogFetchInfo, MemoryRecords, TopicPartition},
    AppError, AppResult,
};

use super::ApiHandler;
use crate::request::RequestContext;

pub struct FetchRequestHandler;

impl ApiHandler for FetchRequestHandler {
    type Request = FetchRequest;
    type Response = FetchResponse;

    async fn handle_request(
        &self,
        request: FetchRequest,
        context: &RequestContext,
    ) -> FetchResponse {
        context.replica_manager.clone().fetch_message(request).await
    }
}

#[derive(Debug)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

impl TryFrom<i8> for IsolationLevel {
    type Error = AppError;
    fn try_from(value: i8) -> AppResult<Self> {
        match value {
            0 => Ok(IsolationLevel::ReadUncommitted),
            1 => Ok(IsolationLevel::ReadCommitted),
            _ => Err(AppError::MalformedProtocol(
                "invalid isolation level".to_string(),
            )),
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct FetchRequest {
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: IsolationLevel,
    pub fetch_data: BTreeMap<TopicPartition, PartitionDataReq>,
}

#[derive(Debug)]
pub struct PartitionDataReq {
    pub fetch_offset: i64,
    pub log_start_offset: i64,
    pub max_bytes: i32,
}

impl PartitionDataReq {
    pub fn new(fetch_offset: i64, log_start_offset: i64, max_bytes: i32) -> Self {
        PartitionDataReq {
            fetch_offset,
            log_start_offset,
            max_bytes,
        }
    }
}

impl std::fmt::Display for PartitionDataReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(offset={}, logStartOffset={}, maxBytes={})",
            self.fetch_offset, self.log_start_offset, self.max_bytes
        )
    }
}

#[derive(Debug)]
pub struct FetchResponse {
    /**
     * Possible error codes:
     *
     *  OFFSET_OUT_OF_RANGE (1)
     *  UNKNOWN_TOPIC_OR_PARTITION (3)
     *  NOT_LEADER_FOR_PARTITION (6)
     *  REPLICA_NOT_AVAILABLE (9)
     *  UNKNOWN (-1)
     */
    pub responses: BTreeMap<TopicPartition, PartitionDataRep>,
    pub throttle_time: i32,
}

#[derive(Debug)]
pub struct PartitionDataRep {
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
    #[allow(dead_code)]
    pub aborted_transactions: Option<Vec<AbortedTransaction>>,
    pub records: MemoryRecords,
}

#[derive(Debug)]
pub struct AbortedTransaction {
    #[allow(dead_code)]
    pub producer_id: i64,
    #[allow(dead_code)]
    pub first_offset: i64,
}

impl From<BTreeMap<TopicPartition, LogFetchInfo>> for FetchResponse {
    fn from(value: BTreeMap<TopicPartition, LogFetchInfo>) -> Self {
        let mut responses = BTreeMap::new();
        for (topic_partition, log_fetch_info) in value {
            responses.insert(
                topic_partition,
                PartitionDataRep {
                    error_code: 0,
                    high_watermark: log_fetch_info.log_end_offset,
                    last_stable_offset: log_fetch_info.log_end_offset,
                    log_start_offset: log_fetch_info.log_start_offset,
                    aborted_transactions: None,
                    records: log_fetch_info.records,
                },
            );
        }
        FetchResponse {
            responses,
            throttle_time: 0,
        }
    }
}
