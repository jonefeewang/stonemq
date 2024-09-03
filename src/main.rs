use clap::Parser;


use std::path::PathBuf;
use tracing_subscriber::fmt::time::ChronoLocal;
use stonemq::{AppResult, Broker, BrokerConfig, GLOBAL_CONFIG};

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
    //setup tracing
    let timer = ChronoLocal::new("%Y-%m-%d %H:%M:%S%.6f".to_string());
    let subscriber = tracing_subscriber::fmt()
        .with_timer(timer)
        .with_max_level(tracing::Level::TRACE) // 设置最大日志级别
        .with_target(true) // 是否显示日志目标
        .with_thread_names(true) // 是否显示线程名称
        .with_thread_ids(true) // 是否显示线程ID
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    //setup config
    let commandline: CommandLine = CommandLine::parse();
    let config_path = commandline.conf.as_ref().map_or_else(
        || {
            let mut path = PathBuf::from("server");
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

    let mut broker = Broker::new();
    broker.start()?;

    Ok(())
}
