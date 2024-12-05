use crate::request::KafkaError;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum AppError {
    /// general errors
    #[error("illegal state: {0}")]
    IllegalStateError(String),

    #[error("malformed protocol : {0}")]
    MalformedProtocol(String),

    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error("I/O error: {0}")]
    DetailedIoError(String),

    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("channel send error: {0}")]
    ChannelSendError(String),

    #[error("channel recv error: {0}")]
    ChannelRecvError(String),

    #[error("invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Accept error = {0}")]
    Accept(String),

    #[error("config file error: {0}")]
    ConfigFileError(#[from] config::ConfigError),

    /// marker error    
    Incomplete,

    /// kafka protocol errors
    #[error("unknown error: {0}")]
    Unknown(String),

    #[error("corrupt message: {0}")]
    CorruptMessage(String),

    #[error("message too large: {0}")]
    MessageTooLarge(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("invalid topic: {0}")]
    InvalidTopic(String),
}

impl From<AppError> for KafkaError {
    fn from(value: AppError) -> Self {
        match value {
            AppError::Unknown(s) => KafkaError::Unknown(s),
            AppError::CorruptMessage(s) => KafkaError::CorruptMessage(s),
            AppError::MessageTooLarge(s) => KafkaError::MessageTooLarge(s),
            AppError::InvalidRequest(s) => KafkaError::InvalidRequest(s),
            AppError::InvalidTopic(s) => KafkaError::InvalidTopic(s),
            _ => KafkaError::Unknown(value.to_string()),
        }
    }
}
