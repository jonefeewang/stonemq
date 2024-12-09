use clap::Parser;
use dotenv::dotenv;
use std::path::PathBuf;
use stonemq::{setup_tracing, GLOBAL_CONFIG};
use stonemq::{AppResult, Broker, BrokerConfig};
use tokio::runtime;
use tracing::error;

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

fn main() {
    print_art();
    if let Err(e) = run() {
        error!("Stonemq started failed: {}", e);
        eprintln!("Stonemq started failed: {}", e);
        std::process::exit(1);
    }
}

fn run() -> AppResult<()> {
    dotenv().ok();

    let rt = runtime::Builder::new_multi_thread().enable_all().build()?;

    let _otel_guard = rt.block_on(setup_tracing());

    // setup config
    let commandline: CommandLine = CommandLine::parse();
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

    Broker::start(&rt)?;

    Ok(())
}

fn print_art() {
    let stone_mq_art = r#"
    ================================================
                   Welcome to StoneMQ 🚀
                The Rock of Message Queues!

                  __________
               .-'          `-.
             .'   .-"""""""-.  '.
            /    /  .---.   |    \
           |    |  (o   o)  |     |   ____
           |    |    (_)    ;     |  [____]
           |     \         /     /
            \     '.     .'     /
             '-.    `"""'    .-'
                `-._______.-'

    StoneMQ Features:
    -----------------
    * Reliable, High-Performance Queue 🧱
    * Built for Distributed Systems 🌐
    * Written in Rust 🦀, Lightning Fast
    ================================================
"#;
    println!("{}", stone_mq_art);
}
