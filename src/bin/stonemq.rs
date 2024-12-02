use clap::Parser;
use dotenv::dotenv;
use std::path::PathBuf;
use stonemq::service::setup_tracing;
use stonemq::{AppResult, Broker, BrokerConfig, GLOBAL_CONFIG};
use tokio::runtime;

#[derive(Parser)]
#[command(version)]
pub struct CommandLine {
    /// path to config file
    #[arg(short, long)]
    pub conf: Option<String>,
    #[command(subcommand)]
    pub command: Option<Command>,
    /// log level (v: info, vv: debug, vvv: trace)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    pub verbose: u8,
}

#[derive(Parser)]
pub enum Command {
    PrintConfig,
}

fn main() -> AppResult<()> {
    // 加载 .env 文件
    dotenv().ok();

    // startup tokio runtime
    let rt = runtime::Builder::new_multi_thread().enable_all().build()?;

    let _otel_guard = rt.block_on(setup_tracing());

    //setup config
    let commandline: CommandLine = CommandLine::parse();
    let config_path = commandline.conf.as_ref().map_or_else(
        || {
            let mut path = PathBuf::from("./");
            path.push("conf.toml");
            path
        },
        PathBuf::from,
    );
    // note: 这里无法使用once cell，因为config_path需要从命令行引入
    let broker_config = BrokerConfig::set_up_config(config_path)?;
    GLOBAL_CONFIG
        .set(broker_config)
        .expect("set broker config failed");

    let mut broker = Broker::default();
    broker.start(&rt)?;

    Ok(())
}
