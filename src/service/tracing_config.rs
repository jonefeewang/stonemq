// Copyright 2025 jonefeewang@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

pub enum LogMode {
    Prod,
    Perf,
    Dev,
}

pub struct OtelGuard {
    meter_provider: Option<SdkMeterProvider>,
    _worker_guard: Option<WorkerGuard>,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Some(meter_provider) = self.meter_provider.as_mut() {
            if let Err(err) = meter_provider.shutdown() {
                eprintln!("{err:?}");
            }
        }

        opentelemetry::global::shutdown_tracer_provider();
        tracing::info!("shutdown otel tracer provider");
    }
}

/// Sets up local tracing configuration
///
/// Initializes a basic tracing configuration suitable for local development.
/// The configuration includes:
/// - Timestamp format: YYYY-MM-DD HH:MM:SS.microseconds
/// - Log target display
/// - Thread name display
/// - Thread ID display
/// - Line number display
///
/// # Returns
///
/// Returns `AppResult<()>` indicating whether initialization was successful
pub fn setup_local_tracing() -> AppResult<()> {
    dotenv().ok();
    let timer = ChronoLocal::new("%Y-%m-%d %H:%M:%S%.6f".to_string());
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_timer(timer)
        .with_target(true) // show log target
        .with_thread_names(true) // show thread name
        .with_thread_ids(true) // show thread id
        .with_line_number(true);
    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    Ok(())
}

/// Sets up the complete tracing system
///
/// Initializes the tracing system based on different logging modes, supporting file output
/// and OpenTelemetry integration.
///
/// # Arguments
///
/// * `tracing_off` - Boolean flag to control whether OpenTelemetry tracing is disabled
/// * `log_mode` - Logging mode enum with three options:
///   - `LogMode::Prod`: Production mode, outputs to file only
///   - `LogMode::Perf`: Performance mode, outputs to both console and file
///   - `LogMode::Dev`: Development mode, outputs to both console and file
///
/// # Returns
///
/// Returns an `OtelGuard` struct that manages the lifecycle of OpenTelemetry resources
pub async fn setup_tracing(tracing_off: bool, log_mode: LogMode) -> OtelGuard {
    // file appender
    let file_appender = tracing_appender::rolling::hourly("logs", "stonemq.log");
    let (non_blocking, worker_guard) = tracing_appender::non_blocking(file_appender);

    let timer = ChronoLocal::new("%Y-%m-%d %H:%M:%S%.6f".to_string());

    // config by log_mode
    match log_mode {
        LogMode::Prod => {
            // PROD mode: only use file appender
            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_timer(timer)
                .with_target(true)
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_ansi(true)
                .with_writer(non_blocking);

            if !tracing_off {
                // enable OpenTelemetry
                let meter_provider = init_meter_provider();
                let tracer = init_tracer();

                tracing_subscriber::registry()
                    .with(fmt_layer)
                    .with(MetricsLayer::new(meter_provider.clone()))
                    .with(OpenTelemetryLayer::new(tracer))
                    .with(tracing_subscriber::EnvFilter::from_default_env())
                    .init();

                OtelGuard {
                    meter_provider: Some(meter_provider),
                    _worker_guard: Some(worker_guard),
                }
            } else {
                // disable tracing, only use fmt_layer
                tracing_subscriber::registry()
                    .with(fmt_layer)
                    .with(tracing_subscriber::EnvFilter::from_default_env())
                    .init();

                OtelGuard {
                    meter_provider: None,
                    _worker_guard: Some(worker_guard),
                }
            }
        }

        LogMode::Perf => {
            // PERF mode: output to console and file
            let writer = non_blocking.and(std::io::stdout);
            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_timer(timer.clone())
                .with_target(true)
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_ansi(true)
                .with_writer(writer);

            if !tracing_off {
                let meter_provider = init_meter_provider();
                let tracer = init_tracer();

                tracing_subscriber::registry()
                    .with(fmt_layer)
                    .with(MetricsLayer::new(meter_provider.clone()))
                    .with(OpenTelemetryLayer::new(tracer))
                    .with(console_subscriber::spawn())
                    .with(tracing_subscriber::EnvFilter::from_default_env())
                    .init();

                OtelGuard {
                    meter_provider: Some(meter_provider),
                    _worker_guard: Some(worker_guard),
                }
            } else {
                tracing_subscriber::registry()
                    .with(fmt_layer)
                    .with(console_subscriber::spawn())
                    .with(tracing_subscriber::EnvFilter::from_default_env())
                    .init();

                OtelGuard {
                    meter_provider: None,
                    _worker_guard: Some(worker_guard),
                }
            }
        }

        LogMode::Dev => {
            // DEV mode: output to console and file
            let writer = non_blocking.and(std::io::stdout);
            let fmt_layer = tracing_subscriber::fmt::layer()
                .with_timer(timer)
                .with_target(true)
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
                .with_ansi(true)
                .with_writer(writer);

            if !tracing_off {
                let meter_provider: SdkMeterProvider = init_meter_provider();
                let tracer: Tracer = init_tracer();

                tracing_subscriber::registry()
                    .with(fmt_layer)
                    .with(MetricsLayer::new(meter_provider.clone()))
                    .with(OpenTelemetryLayer::new(tracer))
                    .with(tracing_subscriber::EnvFilter::from_default_env())
                    .init();

                OtelGuard {
                    meter_provider: Some(meter_provider),
                    _worker_guard: Some(worker_guard),
                }
            } else {
                tracing_subscriber::registry()
                    .with(fmt_layer)
                    .with(tracing_subscriber::EnvFilter::from_default_env())
                    .init();

                OtelGuard {
                    meter_provider: None,
                    _worker_guard: Some(worker_guard),
                }
            }
        }
    }
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
