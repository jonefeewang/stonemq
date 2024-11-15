extern crate config as _;

use std::io;
use std::path::Path;
use std::process::exit;
use std::string::FromUtf8Error;
use std::{borrow::Cow, time::Duration};

use crate::log::FileOp;
use crate::AppError::InvalidValue;

use getset::{CopyGetters, Getters};
use once_cell::sync::OnceCell;

use opentelemetry::{global, trace::TracerProvider, Key, KeyValue};
use opentelemetry_sdk::trace::SpanLimits;
use opentelemetry_sdk::{
    metrics::{
        reader::{DefaultAggregationSelector, DefaultTemporalitySelector},
        Aggregation, Instrument, MeterProviderBuilder, PeriodicReader, SdkMeterProvider, Stream,
    },
    runtime,
    trace::{BatchConfig, BatchConfigBuilder, RandomIdGenerator, Sampler, Tracer},
    Resource,
};
use opentelemetry_semantic_conventions::{
    resource::{SERVICE_NAME, SERVICE_VERSION},
    SCHEMA_URL,
};
use tracing_core::Level;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::fmt::time::ChronoLocal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use serde::{Deserialize, Serialize};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc::Sender,
};

use tokio::sync::broadcast::error::SendError as BroadcastSendError;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot::error::RecvError;
use tokio::time::error::Elapsed;

use dotenv::dotenv;

pub type AppResult<T> = Result<T, AppError>;
pub fn global_config() -> &'static BrokerConfig {
    GLOBAL_CONFIG.get().unwrap()
}

fn resource() -> Resource {
    Resource::from_schema_url(
        [
            KeyValue::new(SERVICE_NAME, env!("CARGO_PKG_NAME")),
            KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
        ],
        SCHEMA_URL,
    )
}

// Construct MeterProvider for MetricsLayer
fn init_meter_provider() -> SdkMeterProvider {
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .build_metrics_exporter(
            Box::new(DefaultAggregationSelector::new()),
            Box::new(DefaultTemporalitySelector::new()),
        )
        .unwrap();

    let reader = PeriodicReader::builder(exporter, runtime::Tokio)
        .with_interval(std::time::Duration::from_secs(30))
        .build();

    // For debugging in development
    let stdout_reader = PeriodicReader::builder(
        opentelemetry_stdout::MetricsExporter::default(),
        runtime::Tokio,
    )
    .build();

    // Rename foo metrics to foo_named and drop key_2 attribute
    let view_foo = |instrument: &Instrument| -> Option<Stream> {
        if instrument.name == "foo" {
            Some(
                Stream::new()
                    .name("foo_named")
                    .allowed_attribute_keys([Key::from("key_1")]),
            )
        } else {
            None
        }
    };

    // Set Custom histogram boundaries for baz metrics
    let view_baz = |instrument: &Instrument| -> Option<Stream> {
        if instrument.name == "baz" {
            Some(
                Stream::new()
                    .name("baz")
                    .aggregation(Aggregation::ExplicitBucketHistogram {
                        boundaries: vec![0.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0],
                        record_min_max: true,
                    }),
            )
        } else {
            None
        }
    };

    let meter_provider = MeterProviderBuilder::default()
        .with_resource(resource())
        .with_reader(reader)
        .with_reader(stdout_reader)
        .with_view(view_foo)
        .with_view(view_baz)
        .build();

    global::set_meter_provider(meter_provider.clone());

    meter_provider
}

// Construct Tracer for OpenTelemetryLayer
fn init_tracer() -> Tracer {
    let mut span_limits = SpanLimits::default();
    span_limits.max_events_per_span = 100000;
    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_batch_config(
            BatchConfigBuilder::default()
                .with_scheduled_delay(Duration::from_secs(5))
                .build(),
        )
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default()
                // Customize sampling strategy
                .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                    1.0,
                ))))
                // If export trace to AWS X-Ray, you can use XrayIdGenerator
                .with_id_generator(RandomIdGenerator::default())
                .with_span_limits(span_limits)
                .with_resource(resource()),
        )
        .with_exporter(opentelemetry_otlp::new_exporter().tonic())
        .install_batch(runtime::Tokio)
        .unwrap()
}

pub struct OtelGuard {
    meter_provider: SdkMeterProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(err) = self.meter_provider.shutdown() {
            eprintln!("{err:?}");
        }

        opentelemetry::global::shutdown_tracer_provider();
        tracing::info!("shutdown otel tracer provider");
    }
}

pub fn setup_local_tracing() -> AppResult<()> {
    // 加载 .env 文件
    dotenv().ok();
    let timer = ChronoLocal::new("%Y-%m-%d %H:%M:%S%.6f".to_string());
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_timer(timer)
        .with_target(true) // 是否显示日志目标
        .with_thread_names(true) // 是否显示线程名称
        .with_thread_ids(true) // 是否显示线程ID
        .with_line_number(true);
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    Ok(())
}

pub async fn setup_tracing() -> OtelGuard {
    // stdout fmt layer
    let timer = ChronoLocal::new("%Y-%m-%d %H:%M:%S%.6f".to_string());
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_timer(timer)
        .with_target(true) // 是否显示日志目标
        .with_thread_names(true) // 是否显示线程名称
        .with_thread_ids(true) // 是否显示线程ID
        .with_file(true)
        .with_line_number(true)
        .with_ansi(true);

    // otel meter provider
    let meter_provider = init_meter_provider();
    // otel tracer
    let tracer = init_tracer();

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(MetricsLayer::new(meter_provider.clone()))
        .with(OpenTelemetryLayer::new(tracer))
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // otel guard
    OtelGuard { meter_provider }
    // Ok(())
}
pub static GLOBAL_CONFIG: OnceCell<BrokerConfig> = OnceCell::new();
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
    #[error("{0}")]
    RequestError(Cow<'static, str>),
    #[error("error in convention : {0}")]
    ConventionError(#[from] FromUtf8Error),
    #[error("IllegalState : {0}")]
    IllegalStateError(Cow<'static, str>),
    ParseError(#[from] std::num::ParseIntError),
    #[error("invalid provided {0} value = {1}")]
    InvalidValue(&'static str, String),
    #[error("{0}")]
    CommonError(String),
    #[error("{0}")]
    FileContentUnavailableError(String),
    FormatError(#[from] serde_json::Error),
    Incomplete,
    #[error("I/O {0}")]
    Io(#[from] io::Error),
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
    #[error("socket channel flag send error")]
    SendLogMsg(#[from] SendError<FileOp>),
    SendToken(#[from] SendError<()>),
    // #[error("tracing error")]
    // TracingError(#[from] tracing::dispatcher::SetGlobalDefaultError),
    BroadcastSendToken(#[from] BroadcastSendError<()>),
    #[error("receive error")]
    Recv(#[from] RecvError),
    #[error("Accept error = {0}")]
    Accept(String),

    #[error("无法修改只读索引文件: {0}")]
    ReadOnlyIndexModification(Cow<'static, str>),

    #[error("索引文件已满")]
    IndexFileFull,

    #[error("InsufficientData")]
    InsufficientData,
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
    pub fn new() -> Self {
        Self {
            max_connection: global_config().network.max_connection,
            max_package_size: global_config().network.max_package_size,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct GeneralConfig {
    pub id: i32,
    pub max_msg_size: i32,
    pub local_db_path: String,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct NetworkConfig {
    pub ip: String,
    pub port: u16,
    pub max_connection: usize,
    pub max_package_size: usize,
}
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct GroupConfig {
    pub group_min_session_timeout: i32,
    pub group_max_session_timeout: i32,
    pub group_initial_rebalance_delay: i32,
}
/// Represents the configuration for a log.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LogConfig {
    /// The base directory for the journal.
    pub journal_base_dir: String,
    /// The size of each journal segment.
    pub journal_segment_size: u64,
    /// The size of the journal index file.
    pub journal_index_file_size: usize,
    /// The interval at which journal index entries are written.
    pub journal_index_interval_bytes: usize,

    /// The base directory for the queue.
    pub queue_base_dir: String,
    /// The size of each queue segment.
    pub queue_segment_size: u64,
    /// The size of the queue index file.
    pub queue_index_file_size: usize,
    /// The interval at which queue index entries are written.
    pub queue_index_interval_bytes: usize,

    /// The path to the key-value store.
    pub kv_store_path: String,
    /// The interval at which recovery checkpoints are written.
    pub recovery_checkpoint_interval: u64,

    pub splitter_read_buffer_size: u32,

    pub splitter_wait_interval: u32,
}
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct BrokerConfig {
    pub general: GeneralConfig,
    pub network: NetworkConfig,
    pub log: LogConfig,
    pub group: GroupConfig,
}

impl BrokerConfig {
    pub fn set_up_config<P: AsRef<Path>>(path: P) -> AppResult<BrokerConfig> {
        let path_str = path
            .as_ref()
            .to_str()
            .ok_or(InvalidValue("config file path", String::new()))?;
        let config = config::Config::builder()
            .add_source(config::File::with_name(path_str))
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
