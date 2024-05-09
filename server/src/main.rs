use std::path::PathBuf;

use clap::Parser;
use getset::{CopyGetters, Getters};
use tokio::{runtime, task};

use server::{AppError, Broker, BrokerConfig, LogManager, ReplicaManager};
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

fn main() -> Result<(), AppError> {
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

    // start log manager
    let log_manager = LogManager::default();
    rt.block_on(async {
        let _ = task::spawn(async {
            log_manager.startup().await.unwrap();
        });
    });

    // start replica manager
    let replica_manager = ReplicaManager::new(log_manager);
    rt.block_on(async {
        let _ = task::spawn(async {
            replica_manager.startup().await.unwrap();
        });
    });

    //start broker
    let broker = Broker::new(replica_manager);
    rt.block_on(broker.start());
    Ok(())
}
