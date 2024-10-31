use std::fmt;
use thiserror::Error;

/// Kafka错误类型
#[derive(Error, Debug, Clone, PartialEq)]
pub enum KafkaError {
    #[error("The server experienced an unexpected error: {0}")]
    Unknown(String),
    
    #[error("The requested offset is not within range: {0}")]
    OffsetOutOfRange(String),
    
    #[error("Corrupt message: {0}")]
    CorruptMessage(String),
    
    #[error("Unknown topic or partition: {0}")]
    UnknownTopicOrPartition(String),
    
    #[error("Invalid fetch size: {0}")]
    InvalidFetchSize(String),
    
    #[error("Leader not available: {0}")]
    LeaderNotAvailable(String),
    
    #[error("Not leader for partition: {0}")]
    NotLeaderForPartition(String),
    
    #[error("Request timed out: {0}")]
    RequestTimedOut(String),
    
    #[error("Broker not available: {0}")]
    BrokerNotAvailable(String),
    
    #[error("Replica not available: {0}")]
    ReplicaNotAvailable(String),
    
    #[error("Message too large: {0}")]
    MessageTooLarge(String),
    
    #[error("Stale controller epoch: {0}")]
    StaleControllerEpoch(String),
    
    #[error("Offset metadata too large: {0}")]
    OffsetMetadataTooLarge(String),
    
    #[error("Network exception: {0}")]
    NetworkException(String),
    
    #[error("Group coordinator load in progress: {0}")]
    CoordinatorLoadInProgress(String),
    
    #[error("Group coordinator not available: {0}")]
    CoordinatorNotAvailable(String),
    
    #[error("Not coordinator: {0}")]
    NotCoordinator(String),
    
    #[error("Invalid topic: {0}")]
    InvalidTopic(String),
    
    #[error("Record batch too large: {0}")]
    RecordBatchTooLarge(String),
    
    #[error("Not enough replicas: {0}")]
    NotEnoughReplicas(String),

    #[error("Not enough replicas after append: {0}")]
    NotEnoughReplicasAfterAppend(String),
    
    #[error("Invalid required acks: {0}")]
    InvalidRequiredAcks(String),
    
    #[error("Illegal generation: {0}")]
    IllegalGeneration(String),
    
    #[error("Inconsistent group protocol: {0}")]
    InconsistentGroupProtocol(String),
    
    #[error("Invalid group id: {0}")]
    InvalidGroupId(String),
    
    #[error("Unknown member id: {0}")]
    UnknownMemberId(String),
    
    #[error("Invalid session timeout: {0}")]
    InvalidSessionTimeout(String),
    
    #[error("Rebalance in progress: {0}")]
    RebalanceInProgress(String),
    
    #[error("Invalid commit offset size: {0}")]
    InvalidCommitOffsetSize(String),
    
    #[error("Topic authorization failed: {0}")]
    TopicAuthorizationFailed(String),
    
    #[error("Group authorization failed: {0}")]
    GroupAuthorizationFailed(String),
    
    #[error("Cluster authorization failed: {0}")]
    ClusterAuthorizationFailed(String),
    
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String),
    
    #[error("Unsupported SASL mechanism: {0}")]
    UnsupportedSaslMechanism(String),
    
    #[error("Illegal SASL state: {0}")]
    IllegalSaslState(String),
    
    #[error("Unsupported version: {0}")]
    UnsupportedVersion(String),
    
    #[error("Topic already exists: {0}")]
    TopicAlreadyExists(String),
    
    #[error("Invalid partitions: {0}")]
    InvalidPartitions(String),
    
    #[error("Invalid replication factor: {0}")]
    InvalidReplicationFactor(String),
    
    #[error("Invalid replica assignment: {0}")]
    InvalidReplicaAssignment(String),
    
    #[error("Invalid config: {0}")]
    InvalidConfig(String),
    
    #[error("Not controller: {0}")]
    NotController(String),
    
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
    
    #[error("Unsupported for message format: {0}")]
    UnsupportedForMessageFormat(String),
    
    #[error("Policy violation: {0}")]
    PolicyViolation(String),
    
    #[error("Out of order sequence number: {0}")]
    OutOfOrderSequenceNumber(String),
    
    #[error("Duplicate sequence number: {0}")]
    DuplicateSequenceNumber(String),
    
    #[error("Invalid producer epoch: {0}")]
    InvalidProducerEpoch(String),
    
    #[error("Invalid txn state: {0}")]
    InvalidTxnState(String),
    
    #[error("Invalid producer id mapping: {0}")]
    InvalidProducerIdMapping(String),
    
    #[error("Invalid transaction timeout: {0}")]
    InvalidTransactionTimeout(String),
    
    #[error("Concurrent transactions: {0}")]
    ConcurrentTransactions(String),
    
    #[error("Transaction coordinator fenced: {0}")]
    TransactionCoordinatorFenced(String),
    
    #[error("Transactional id authorization failed: {0}")]
    TransactionalIdAuthorizationFailed(String),
    
    #[error("Security disabled: {0}")]
    SecurityDisabled(String),
    
    #[error("Operation not attempted: {0}")]
    OperationNotAttempted(String),
}

/// Kafka错误码
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i16)]
pub enum ErrorCode {

    /// generic request error code
    UnsupportedVersion = 35,
    InvalidRequest = 42,
    RequestTimedOut = 7,
    NetworkException = 13,
    OperationNotAttempted = 55,    
    Unknown = -1,
    None = 0,

    /// topic produce error code
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidTopic = 17,
    RecordBatchTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    NotController = 41,    
    InvalidProducerEpoch = 47,
    MessageTooLarge = 10,
    InvalidTimestamp = 32,
    UnsupportedForMessageFormat = 43,
    OutOfOrderSequenceNumber = 45,
    DuplicateSequenceNumber = 46,


    //// consumer group error code
    OffsetOutOfRange = 1,
    InvalidFetchSize = 4,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    OffsetMetadataTooLarge = 12,


    /// topic error code
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidReplicaAssignment = 39,
    InvalidConfig = 40,


    /// cluster error code
    LeaderNotAvailable = 5,
    NotLeaderForPartition = 6,    
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    StaleControllerEpoch = 11,


    /// authorization error code
    PolicyViolation = 44,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,


    /// sasl error code
    UnsupportedSaslMechanism = 33,
    IllegalSaslState = 34,
    SecurityDisabled = 54,
   

    /// transaction error code
    InvalidTxnState = 48,
    InvalidProducerIdMapping = 49,
    InvalidTransactionTimeout = 50,
    ConcurrentTransactions = 51,
    TransactionCoordinatorFenced = 52,
    TransactionalIdAuthorizationFailed = 53,

}

impl ErrorCode {
    /// 获取错误描述信息
    pub fn message(&self) -> &'static str {
        match self {
            ErrorCode::Unknown => "The server experienced an unexpected error when processing the request",
            ErrorCode::None => "",
            ErrorCode::OffsetOutOfRange => "The requested offset is not within the range of offsets maintained by the server",
            ErrorCode::CorruptMessage => "This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt",
            ErrorCode::UnknownTopicOrPartition => "This server does not host this topic-partition",
            ErrorCode::InvalidFetchSize => "The requested fetch size is invalid",
            ErrorCode::LeaderNotAvailable => "There is no leader for this topic-partition as we are in the middle of a leadership election",
            ErrorCode::NotLeaderForPartition => "This server is not the leader for that topic-partition",
            ErrorCode::RequestTimedOut => "The request timed out",
            ErrorCode::BrokerNotAvailable => "The broker is not available",
            ErrorCode::ReplicaNotAvailable => "The replica is not available for the requested topic-partition",
            ErrorCode::MessageTooLarge => "The request included a message larger than the max message size the server will accept",
            ErrorCode::StaleControllerEpoch => "The controller moved to another broker",
            ErrorCode::OffsetMetadataTooLarge => "The metadata field of the offset request was too large",
            ErrorCode::NetworkException => "The server disconnected before a response was received",
            ErrorCode::CoordinatorLoadInProgress => "The coordinator is loading and hence can't process requests",
            ErrorCode::CoordinatorNotAvailable => "The coordinator is not available",
            ErrorCode::NotCoordinator => "This is not the correct coordinator",
            ErrorCode::InvalidTopic => "The request attempted to perform an operation on an invalid topic",
            ErrorCode::RecordBatchTooLarge => "The request included message batch larger than the configured segment size on the server",
            ErrorCode::NotEnoughReplicas => "Messages are rejected since there are fewer in-sync replicas than required",
            ErrorCode::NotEnoughReplicasAfterAppend => "Messages are written to the log, but to fewer in-sync replicas than required",
            ErrorCode::InvalidRequiredAcks => "Produce request specified an invalid value for required acks",
            ErrorCode::IllegalGeneration => "Specified group generation id is not valid",
            ErrorCode::InconsistentGroupProtocol => "The group member's supported protocols are incompatible with those of existing members",
            ErrorCode::InvalidGroupId => "The configured groupId is invalid",
            ErrorCode::UnknownMemberId => "The coordinator is not aware of this member",
            ErrorCode::InvalidSessionTimeout => "The session timeout is not within the range allowed by the broker",
            ErrorCode::RebalanceInProgress => "The group is rebalancing, so a rejoin is needed",
            ErrorCode::InvalidCommitOffsetSize => "The committing offset data size is not valid",
            ErrorCode::TopicAuthorizationFailed => "Topic authorization failed",
            ErrorCode::GroupAuthorizationFailed => "Group authorization failed",
            ErrorCode::ClusterAuthorizationFailed => "Cluster authorization failed",
            ErrorCode::InvalidTimestamp => "The timestamp of the message is out of acceptable range",
            ErrorCode::UnsupportedSaslMechanism => "The broker does not support the requested SASL mechanism",
            ErrorCode::IllegalSaslState => "Request is not valid given the current SASL state",
            ErrorCode::UnsupportedVersion => "The version of API is not supported",
            ErrorCode::TopicAlreadyExists => "Topic with this name already exists",
            ErrorCode::InvalidPartitions => "Number of partitions is invalid",
            ErrorCode::InvalidReplicationFactor => "Replication-factor is invalid",
            ErrorCode::InvalidReplicaAssignment => "Replica assignment is invalid",
            ErrorCode::InvalidConfig => "Configuration is invalid",
            ErrorCode::NotController => "This is not the correct controller for this cluster",
            ErrorCode::InvalidRequest => "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker",
            ErrorCode::UnsupportedForMessageFormat => "The message format version on the broker does not support the request",
            ErrorCode::PolicyViolation => "Request parameters do not satisfy the configured policy",
            ErrorCode::OutOfOrderSequenceNumber => "The broker received an out of order sequence number",
            ErrorCode::DuplicateSequenceNumber => "The broker received a duplicate sequence number",
            ErrorCode::InvalidProducerEpoch => "Producer attempted an operation with an old epoch",
            ErrorCode::InvalidTxnState => "The producer attempted a transactional operation in an invalid state",
            ErrorCode::InvalidProducerIdMapping => "The producer attempted to use a producer id which is not currently assigned to its transactional id",
            ErrorCode::InvalidTransactionTimeout => "The transaction timeout is larger than the maximum value allowed by the broker",
            ErrorCode::ConcurrentTransactions => "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing",
            ErrorCode::TransactionCoordinatorFenced => "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer",
            ErrorCode::TransactionalIdAuthorizationFailed => "Transactional Id authorization failed",
            ErrorCode::SecurityDisabled => "Security features are disabled",
            ErrorCode::OperationNotAttempted => "The broker did not attempt to execute this operation",
            

        }
    }

    /// 从错误码数字转换为ErrorCode枚举
    pub fn from_code(code: i16) -> Self {
        match code {
            -1 => ErrorCode::Unknown,
            0 => ErrorCode::None,
            1 => ErrorCode::OffsetOutOfRange,
            2 => ErrorCode::CorruptMessage,
            3 => ErrorCode::UnknownTopicOrPartition,
            4 => ErrorCode::InvalidFetchSize,
            5 => ErrorCode::LeaderNotAvailable,
            6 => ErrorCode::NotLeaderForPartition,
            7 => ErrorCode::RequestTimedOut,
            8 => ErrorCode::BrokerNotAvailable,
            9 => ErrorCode::ReplicaNotAvailable,
            10 => ErrorCode::MessageTooLarge,
            11 => ErrorCode::StaleControllerEpoch,
            12 => ErrorCode::OffsetMetadataTooLarge,
            13 => ErrorCode::NetworkException,
            14 => ErrorCode::CoordinatorLoadInProgress,
            15 => ErrorCode::CoordinatorNotAvailable,
            16 => ErrorCode::NotCoordinator,
            17 => ErrorCode::InvalidTopic,
            18 => ErrorCode::RecordBatchTooLarge,
            19 => ErrorCode::NotEnoughReplicas,
            20 => ErrorCode::NotEnoughReplicasAfterAppend,
            21 => ErrorCode::InvalidRequiredAcks,
            22 => ErrorCode::IllegalGeneration,
            23 => ErrorCode::InconsistentGroupProtocol,
            24 => ErrorCode::InvalidGroupId,
            25 => ErrorCode::UnknownMemberId,
            26 => ErrorCode::InvalidSessionTimeout,
            27 => ErrorCode::RebalanceInProgress,
            28 => ErrorCode::InvalidCommitOffsetSize,
            29 => ErrorCode::TopicAuthorizationFailed,
            30 => ErrorCode::GroupAuthorizationFailed,
            31 => ErrorCode::ClusterAuthorizationFailed,
            32 => ErrorCode::InvalidTimestamp,
            33 => ErrorCode::UnsupportedSaslMechanism,
            34 => ErrorCode::IllegalSaslState,
            35 => ErrorCode::UnsupportedVersion,
            36 => ErrorCode::TopicAlreadyExists,
            37 => ErrorCode::InvalidPartitions,
            38 => ErrorCode::InvalidReplicationFactor,
            39 => ErrorCode::InvalidReplicaAssignment,
            40 => ErrorCode::InvalidConfig,
            41 => ErrorCode::NotController,
            42 => ErrorCode::InvalidRequest,
            43 => ErrorCode::UnsupportedForMessageFormat,
            44 => ErrorCode::PolicyViolation,
            45 => ErrorCode::OutOfOrderSequenceNumber,
            46 => ErrorCode::DuplicateSequenceNumber,
            47 => ErrorCode::InvalidProducerEpoch,
            48 => ErrorCode::InvalidTxnState,
            49 => ErrorCode::InvalidProducerIdMapping,
            50 => ErrorCode::InvalidTransactionTimeout,
            51 => ErrorCode::ConcurrentTransactions,
            52 => ErrorCode::TransactionCoordinatorFenced,
            53 => ErrorCode::TransactionalIdAuthorizationFailed,
            54 => ErrorCode::SecurityDisabled,
            55 => ErrorCode::OperationNotAttempted,
            _ => ErrorCode::Unknown,
         
        }
    }

    /// 将ErrorCode转换为KafkaError
    pub fn into_error(self) -> Option<KafkaError> {
        match self {
            ErrorCode::None => None,
            ErrorCode::Unknown => Some(KafkaError::Unknown(self.message().to_string())),
            ErrorCode::OffsetOutOfRange => Some(KafkaError::OffsetOutOfRange(self.message().to_string())),
            ErrorCode::CorruptMessage => Some(KafkaError::CorruptMessage(self.message().to_string())),
            ErrorCode::UnknownTopicOrPartition => Some(KafkaError::UnknownTopicOrPartition(self.message().to_string())),
            ErrorCode::InvalidFetchSize => Some(KafkaError::InvalidFetchSize(self.message().to_string())),
            ErrorCode::LeaderNotAvailable => Some(KafkaError::LeaderNotAvailable(self.message().to_string())),
            ErrorCode::NotLeaderForPartition => Some(KafkaError::NotLeaderForPartition(self.message().to_string())),
            ErrorCode::RequestTimedOut => Some(KafkaError::RequestTimedOut(self.message().to_string())),
            ErrorCode::BrokerNotAvailable => Some(KafkaError::BrokerNotAvailable(self.message().to_string())),
            ErrorCode::ReplicaNotAvailable => Some(KafkaError::ReplicaNotAvailable(self.message().to_string())),
            ErrorCode::MessageTooLarge => Some(KafkaError::MessageTooLarge(self.message().to_string())),
            ErrorCode::StaleControllerEpoch => Some(KafkaError::StaleControllerEpoch(self.message().to_string())),
            ErrorCode::OffsetMetadataTooLarge => Some(KafkaError::OffsetMetadataTooLarge(self.message().to_string())),
            ErrorCode::NetworkException => Some(KafkaError::NetworkException(self.message().to_string())),
            ErrorCode::CoordinatorLoadInProgress => Some(KafkaError::CoordinatorLoadInProgress(self.message().to_string())),
            ErrorCode::CoordinatorNotAvailable => Some(KafkaError::CoordinatorNotAvailable(self.message().to_string())),
            ErrorCode::NotCoordinator => Some(KafkaError::NotCoordinator(self.message().to_string())),
            ErrorCode::InvalidTopic => Some(KafkaError::InvalidTopic(self.message().to_string())),
            ErrorCode::RecordBatchTooLarge => Some(KafkaError::RecordBatchTooLarge(self.message().to_string())),
            ErrorCode::NotEnoughReplicas => Some(KafkaError::NotEnoughReplicas(self.message().to_string())),
            ErrorCode::NotEnoughReplicasAfterAppend => Some(KafkaError::NotEnoughReplicasAfterAppend(self.message().to_string())),
            ErrorCode::InvalidRequiredAcks => Some(KafkaError::InvalidRequiredAcks(self.message().to_string())),
            ErrorCode::IllegalGeneration => Some(KafkaError::IllegalGeneration(self.message().to_string())),
            ErrorCode::InconsistentGroupProtocol => Some(KafkaError::InconsistentGroupProtocol(self.message().to_string())),
            ErrorCode::InvalidGroupId => Some(KafkaError::InvalidGroupId(self.message().to_string())),
            ErrorCode::UnknownMemberId => Some(KafkaError::UnknownMemberId(self.message().to_string())),
            ErrorCode::InvalidSessionTimeout => Some(KafkaError::InvalidSessionTimeout(self.message().to_string())),
            ErrorCode::RebalanceInProgress => Some(KafkaError::RebalanceInProgress(self.message().to_string())),
            ErrorCode::InvalidCommitOffsetSize => Some(KafkaError::InvalidCommitOffsetSize(self.message().to_string())),
            ErrorCode::TopicAuthorizationFailed => Some(KafkaError::TopicAuthorizationFailed(self.message().to_string())),
            ErrorCode::GroupAuthorizationFailed => Some(KafkaError::GroupAuthorizationFailed(self.message().to_string())),
            ErrorCode::ClusterAuthorizationFailed => Some(KafkaError::ClusterAuthorizationFailed(self.message().to_string())),
            ErrorCode::InvalidTimestamp => Some(KafkaError::InvalidTimestamp(self.message().to_string())),
            ErrorCode::UnsupportedSaslMechanism => Some(KafkaError::UnsupportedSaslMechanism(self.message().to_string())),
            ErrorCode::IllegalSaslState => Some(KafkaError::IllegalSaslState(self.message().to_string())),
            ErrorCode::UnsupportedVersion => Some(KafkaError::UnsupportedVersion(self.message().to_string())),
            ErrorCode::TopicAlreadyExists => Some(KafkaError::TopicAlreadyExists(self.message().to_string())),
            ErrorCode::InvalidPartitions => Some(KafkaError::InvalidPartitions(self.message().to_string())),
            ErrorCode::InvalidReplicationFactor => Some(KafkaError::InvalidReplicationFactor(self.message().to_string())),
            ErrorCode::InvalidReplicaAssignment => Some(KafkaError::InvalidReplicaAssignment(self.message().to_string())),
            ErrorCode::InvalidConfig => Some(KafkaError::InvalidConfig(self.message().to_string())),
            ErrorCode::NotController => Some(KafkaError::NotController(self.message().to_string())),
            ErrorCode::InvalidRequest => Some(KafkaError::InvalidRequest(self.message().to_string())),
            ErrorCode::UnsupportedForMessageFormat => Some(KafkaError::UnsupportedForMessageFormat(self.message().to_string())),
            ErrorCode::PolicyViolation => Some(KafkaError::PolicyViolation(self.message().to_string())),
            ErrorCode::OutOfOrderSequenceNumber => Some(KafkaError::OutOfOrderSequenceNumber(self.message().to_string())),
            ErrorCode::DuplicateSequenceNumber => Some(KafkaError::DuplicateSequenceNumber(self.message().to_string())),
            ErrorCode::InvalidProducerEpoch => Some(KafkaError::InvalidProducerEpoch(self.message().to_string())),
            ErrorCode::InvalidTxnState => Some(KafkaError::InvalidTxnState(self.message().to_string())),
            ErrorCode::InvalidProducerIdMapping => Some(KafkaError::InvalidProducerIdMapping(self.message().to_string())),
            ErrorCode::InvalidTransactionTimeout => Some(KafkaError::InvalidTransactionTimeout(self.message().to_string())),
            ErrorCode::ConcurrentTransactions => Some(KafkaError::ConcurrentTransactions(self.message().to_string())),
            ErrorCode::TransactionCoordinatorFenced => Some(KafkaError::TransactionCoordinatorFenced(self.message().to_string())),
            ErrorCode::TransactionalIdAuthorizationFailed => Some(KafkaError::TransactionalIdAuthorizationFailed(self.message().to_string())),
            ErrorCode::SecurityDisabled => Some(KafkaError::SecurityDisabled(self.message().to_string())),
            ErrorCode::OperationNotAttempted => Some(KafkaError::OperationNotAttempted(self.message().to_string())),
       
        }
    }
}

/// 实现从KafkaError到ErrorCode的转换
impl From<&KafkaError> for ErrorCode {
    fn from(error: &KafkaError) -> Self {
        match error {
            KafkaError::Unknown(_) => ErrorCode::Unknown,
            KafkaError::OffsetOutOfRange(_) => ErrorCode::OffsetOutOfRange,
            KafkaError::CorruptMessage(_) => ErrorCode::CorruptMessage,
            KafkaError::UnknownTopicOrPartition(_) => ErrorCode::UnknownTopicOrPartition,
            KafkaError::InvalidFetchSize(_) => ErrorCode::InvalidFetchSize,
            KafkaError::LeaderNotAvailable(_) => ErrorCode::LeaderNotAvailable,
            KafkaError::NotLeaderForPartition(_) => ErrorCode::NotLeaderForPartition,
            KafkaError::RequestTimedOut(_) => ErrorCode::RequestTimedOut,
            KafkaError::BrokerNotAvailable(_) => ErrorCode::BrokerNotAvailable,
            KafkaError::ReplicaNotAvailable(_) => ErrorCode::ReplicaNotAvailable,
            KafkaError::MessageTooLarge(_) => ErrorCode::MessageTooLarge,
            KafkaError::StaleControllerEpoch(_) => ErrorCode::StaleControllerEpoch,
            KafkaError::OffsetMetadataTooLarge(_) => ErrorCode::OffsetMetadataTooLarge,
            KafkaError::NetworkException(_) => ErrorCode::NetworkException,
            KafkaError::CoordinatorLoadInProgress(_) => ErrorCode::CoordinatorLoadInProgress,
            KafkaError::CoordinatorNotAvailable(_) => ErrorCode::CoordinatorNotAvailable,
            KafkaError::NotCoordinator(_) => ErrorCode::NotCoordinator,
            KafkaError::InvalidTopic(_) => ErrorCode::InvalidTopic,
            KafkaError::RecordBatchTooLarge(_) => ErrorCode::RecordBatchTooLarge,
            KafkaError::NotEnoughReplicas(_) => ErrorCode::NotEnoughReplicas,
            KafkaError::NotEnoughReplicasAfterAppend(_) => ErrorCode::NotEnoughReplicasAfterAppend,
            KafkaError::InvalidRequiredAcks(_) => ErrorCode::InvalidRequiredAcks,
            KafkaError::IllegalGeneration(_) => ErrorCode::IllegalGeneration,
            KafkaError::InconsistentGroupProtocol(_) => ErrorCode::InconsistentGroupProtocol,
            KafkaError::InvalidGroupId(_) => ErrorCode::InvalidGroupId,
            KafkaError::UnknownMemberId(_) => ErrorCode::UnknownMemberId,
            KafkaError::InvalidSessionTimeout(_) => ErrorCode::InvalidSessionTimeout,
            KafkaError::RebalanceInProgress(_) => ErrorCode::RebalanceInProgress,
            KafkaError::InvalidCommitOffsetSize(_) => ErrorCode::InvalidCommitOffsetSize,
            KafkaError::TopicAuthorizationFailed(_) => ErrorCode::TopicAuthorizationFailed,
            KafkaError::GroupAuthorizationFailed(_) => ErrorCode::GroupAuthorizationFailed,
            KafkaError::ClusterAuthorizationFailed(_) => ErrorCode::ClusterAuthorizationFailed,
            KafkaError::InvalidTimestamp(_) => ErrorCode::InvalidTimestamp,
            KafkaError::UnsupportedSaslMechanism(_) => ErrorCode::UnsupportedSaslMechanism,
            KafkaError::IllegalSaslState(_) => ErrorCode::IllegalSaslState,
            KafkaError::UnsupportedVersion(_) => ErrorCode::UnsupportedVersion,
            KafkaError::TopicAlreadyExists(_) => ErrorCode::TopicAlreadyExists,
            KafkaError::InvalidPartitions(_) => ErrorCode::InvalidPartitions,
            KafkaError::InvalidReplicationFactor(_) => ErrorCode::InvalidReplicationFactor,
            KafkaError::InvalidReplicaAssignment(_) => ErrorCode::InvalidReplicaAssignment,
            KafkaError::InvalidConfig(_) => ErrorCode::InvalidConfig,
            KafkaError::NotController(_) => ErrorCode::NotController,
            KafkaError::InvalidRequest(_) => ErrorCode::InvalidRequest,
            KafkaError::UnsupportedForMessageFormat(_) => ErrorCode::UnsupportedForMessageFormat,
            KafkaError::PolicyViolation(_) => ErrorCode::PolicyViolation,
            KafkaError::OutOfOrderSequenceNumber(_) => ErrorCode::OutOfOrderSequenceNumber,
            KafkaError::DuplicateSequenceNumber(_) => ErrorCode::DuplicateSequenceNumber,
            KafkaError::InvalidProducerEpoch(_) => ErrorCode::InvalidProducerEpoch,
            KafkaError::InvalidTxnState(_) => ErrorCode::InvalidTxnState,
            KafkaError::InvalidProducerIdMapping(_) => ErrorCode::InvalidProducerIdMapping,
            KafkaError::InvalidTransactionTimeout(_) => ErrorCode::InvalidTransactionTimeout,
            KafkaError::ConcurrentTransactions(_) => ErrorCode::ConcurrentTransactions,
            KafkaError::TransactionCoordinatorFenced(_) => ErrorCode::TransactionCoordinatorFenced,
            KafkaError::TransactionalIdAuthorizationFailed(_) => ErrorCode::TransactionalIdAuthorizationFailed,
            KafkaError::SecurityDisabled(_) => ErrorCode::SecurityDisabled,
            KafkaError::OperationNotAttempted(_) => ErrorCode::OperationNotAttempted,
      
        }
    }
}

/// 定义Result类型别名，方便使用
pub type KafkaResult<T> = Result<T, KafkaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_conversion() {
        let code = ErrorCode::OffsetOutOfRange;
        let error = code.into_error().unwrap();
        assert_eq!(ErrorCode::from(&error), code);
    }

    #[test]
    fn test_error_messages() {
        let code = ErrorCode::OffsetOutOfRange;
        let error = code.into_error().unwrap();
        assert!(error.to_string().contains("offset is not within range"));
    }

    #[test]
    fn test_from_code() {
        assert_eq!(ErrorCode::from_code(1), ErrorCode::OffsetOutOfRange);
        assert_eq!(ErrorCode::from_code(-1), ErrorCode::Unknown);
        assert_eq!(ErrorCode::from_code(999), ErrorCode::Unknown);
    }
}