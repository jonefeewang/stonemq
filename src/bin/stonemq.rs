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

use clap::Parser;
use dotenv::dotenv;
use std::path::PathBuf;
use stonemq::{setup_tracing, LogMode, GLOBAL_CONFIG};
use stonemq::{AppResult, Broker, BrokerConfig};
use tokio::runtime;
use tracing::error;

#[derive(Parser)]
#[command(version)]
pub struct CommandLine {
    /// path to config file
    #[arg(short, long)]
    pub conf: Option<String>,
    /// log level (v: info, vv: debug, vvv: trace)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    pub verbose: u8,
}

#[derive(Parser)]
pub enum Command {
    PrintConfig,
}

fn main() {
    if let Err(e) = run() {
        error!("Stonemq started failed: {}", e);
        eprintln!("Stonemq started failed: {}", e);
        std::process::exit(1);
    }
}

fn run() -> AppResult<()> {
    let commandline: CommandLine = CommandLine::parse();
    dotenv().ok();
    // command line override env RUST_LOG
    // let log_level = match commandline.verbose {
    //     0 => "info",
    //     1 => "debug",
    //     2 => "trace",
    //     _ => "trace",
    // };
    // std::env::set_var("RUST_LOG", log_level);

    let worker_num = num_cpus::get() * 2;
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(worker_num)
        .enable_all()
        .build()?;
    let _otel_guard = rt.block_on(setup_tracing(true, LogMode::Prod));

    // setup config
    let config_path = commandline.conf.as_ref().map_or_else(
        || {
            let mut path = PathBuf::from("./");
            path.push("conf.toml");
            path
        },
        PathBuf::from,
    );

    let broker_config = BrokerConfig::set_up_config(config_path)?;
    GLOBAL_CONFIG
        .set(broker_config)
        .expect("set broker config failed");

    rt.block_on(Broker::start())?;

    Ok(())
}
