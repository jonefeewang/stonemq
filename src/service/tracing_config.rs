use std::time::Duration;

use opentelemetry::{global, Key, KeyValue};
use opentelemetry_sdk::trace::SpanLimits;
use opentelemetry_sdk::{
    metrics::{
        reader::{DefaultAggregationSelector, DefaultTemporalitySelector},
        Aggregation, Instrument, MeterProviderBuilder, PeriodicReader, SdkMeterProvider, Stream,
    },
    runtime,
    trace::{BatchConfigBuilder, RandomIdGenerator, Sampler, Tracer},
    Resource,
};
use opentelemetry_semantic_conventions::{
    resource::{SERVICE_NAME, SERVICE_VERSION},
    SCHEMA_URL,
};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_opentelemetry::{MetricsLayer, OpenTelemetryLayer};
use tracing_subscriber::fmt::time::ChronoLocal;

use dotenv::dotenv;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use super::AppError;

pub type AppResult<T> = Result<T, AppError>;

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
    let span_limits = SpanLimits::default();
    // span_limits.max_events_per_span = 100000;
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
    _worker_guard: WorkerGuard,
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
    let file_appender = tracing_appender::rolling::hourly("logs", "stonemq.log");
    // 创建同时写入到控制台和文件的写入器

    // 创建一个非阻塞的写入器
    let (non_blocking, worker_guard) = tracing_appender::non_blocking(file_appender);

    // 创建同时写入到控制台和文件的写入器
    let writer = non_blocking.and(std::io::stdout);

    // stdout fmt layer
    let timer = ChronoLocal::new("%Y-%m-%d %H:%M:%S%.6f".to_string());
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_timer(timer)
        .with_target(true) // 是否显示日志目标
        .with_thread_names(true) // 是否显示线程名称
        .with_thread_ids(true) // 是否显示线程ID
        .with_file(true)
        .with_line_number(true)
        .with_ansi(true)
        .with_writer(writer);

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

    // console_subscriber::ConsoleLayer::builder()
    //     .retention(Duration::from_secs(30))
    //     .init();

    // otel guard
    OtelGuard {
        meter_provider,
        _worker_guard: worker_guard,
    }
    // Ok(())
}
