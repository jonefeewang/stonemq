extern crate config as rs_config;

use std::borrow::Cow;
use std::io;
use std::path::Path;
use std::process::exit;
use std::string::FromUtf8Error;
use std::sync::Arc;

use getset::{CopyGetters, Getters};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::error::SendError;
use tokio::time::error::Elapsed;

use crate::AppError::InvalidValue;

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum AppError {
    #[error("malformed protocol encoding : {0}")]
    MalformedProtocolEncoding(&'static str),
    #[error("error in reading network stream : {0}")]
    NetworkReadError(Cow<'static, str>),
    #[error("error in writing network stream : {0}")]
    NetworkWriteError(Cow<'static, str>),
    #[error("{0}")]
    ProtocolError(Cow<'static, str>),
    #[error("error in convention : {0}")]
    ConventionError(#[from] FromUtf8Error),
    #[error("invalid provided {0} value = {1}")]
    InvalidValue(&'static str, String),
    Incomplete,
    #[error("I/O {0}")]
    Io(#[from] io::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("socket channel flag send error")]
    Recv(#[from] SendError<()>),
    #[error("Accept error = {0}")]
    Accept(String),
    TracingError(#[from] tracing::dispatcher::SetGlobalDefaultError),
}

pub trait IO: AsyncRead + AsyncWrite + Send + Sync + Unpin {}

impl<T: AsyncRead + AsyncWrite + Send + Sync + Unpin> IO for T {}

/*
 Broker动态配置，可以通过admin接口或控制台配置
 初始化值会从静态配置中读取，保障了每次broker重启都会以静态配置为准
 注意：因为动态配置需要加锁，访问时需要获取一个快照，因此配置分类最好细化，以减少快照时clone对象的大小
*/
#[derive(Getters, CopyGetters, Clone, Debug)]
#[get_copy = "pub"]
pub struct DynamicConfig {
    max_connection: usize,
    max_package_size: usize,
}

impl DynamicConfig {
    pub fn new(broker_config: Arc<BrokerConfig>) -> Self {
        Self {
            max_connection: broker_config.network.max_connection,
            max_package_size: broker_config.network.max_package_size,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Getters)]
#[get = "pub"]
pub struct GeneralConfig {
    id: String,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Getters, CopyGetters)]
pub struct NetworkConfig {
    #[getset(get = "pub")]
    ip: String,
    #[getset(get_copy = "pub")]
    port: u16,
    #[getset(get_copy = "pub")]
    max_connection: usize,
    #[getset(get_copy = "pub")]
    max_package_size: usize,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Getters)]
#[get = "pub"]
pub struct BrokerConfig {
    general: GeneralConfig,
    network: NetworkConfig,
}

impl BrokerConfig {
    pub fn set_up_config<P: AsRef<Path>>(path: P) -> AppResult<BrokerConfig> {
        let path_str = path
            .as_ref()
            .to_str()
            .ok_or(InvalidValue("config file path", String::new()))?;
        let config = rs_config::Config::builder()
            .add_source(rs_config::File::with_name(path_str))
            .build()
            // .expect("error in reading config files:");
            .unwrap_or_else(|err| {
                eprintln!("error in reading config files: {:?}", err);
                // io::stderr().flush().unwrap();
                exit(1);
            });

        // println!("raw config {:?}",config);

        let server_config: BrokerConfig = config.try_deserialize().unwrap_or_else(|err| {
            eprintln!("error in deserializing config: {:?}", err);
            exit(1);
        });

        Self::validate_config(&server_config);

        Ok(server_config)
    }

    fn validate_config(config: &BrokerConfig) {}
}
