use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use getset::{CopyGetters, Getters};
use tokio::runtime;

use server::{AppError, Broker, BrokerConfig};

#[derive(Parser, Getters, CopyGetters)]
#[command(version)]
pub struct CommandLine {
    /// path to config file
    #[arg(short, long)]
    #[get = "pub"]
    conf: Option<String>,
    #[command(subcommand)]
    #[get = "pub"]
    command: Option<Command>,
    /// log level (v: info, vv: debug, vvv: trace)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    #[get_copy = "pub"]
    verbose: u8,
}

#[derive(Parser)]
pub enum Command {
    PrintConfig,
}

fn main() -> Result<(), AppError> {
    let commandline: CommandLine = CommandLine::parse();
    let config_path = commandline.conf().as_ref().map_or_else(
        || {
            let mut path = PathBuf::from("server");
            path.push("conf.toml");
            path
        },
        |config| PathBuf::from(config),
    );
    let broker_config = BrokerConfig::set_up_config(config_path)?;
    let config = Arc::new(broker_config);
    let broker = Broker::new(Arc::clone(&config));
    runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(broker.start());
    Ok(())
}
