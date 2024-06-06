use std::path::PathBuf;

use clap::Parser;
use getset::{CopyGetters, Getters};
use tokio::runtime;

use server::{AppResult, Broker, BrokerConfig, LogManager, ReplicaManager};
use server::BROKER_CONFIG;

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

fn main() -> AppResult<()> {
    //setup tracing
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    //setup config
    let commandline: CommandLine = CommandLine::parse();
    let config_path = commandline.conf().as_ref().map_or_else(
        || {
            let mut path = PathBuf::from("server");
            path.push("conf.toml");
            path
        },
        PathBuf::from,
    );
    // note: 这里无法使用once cell，因为config_path需要从命令行引入
    let broker_config = BrokerConfig::set_up_config(config_path)?;
    BROKER_CONFIG
        .set(broker_config)
        .expect("set broker config failed");

    // startup tokio runtime
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // startup log manager
    let mut log_manager = LogManager::new();
    log_manager.startup(&rt)?;

    // startup replica manager
    let mut replica_manager = ReplicaManager::new(log_manager);
    replica_manager.startup()?;

    // startup broker
    rt.block_on(async {
        // Start broker
        let mut broker = Broker::new(replica_manager);
        broker.start().await
    })?;

    Ok(())
}
