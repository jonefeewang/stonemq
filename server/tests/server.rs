use server::{AppResult, Broker, BrokerConfig};
use std::sync::Arc;

async fn start_server() -> AppResult<()> {
    let broker_config = BrokerConfig::set_up_config("conf.toml")?;
    let config = Arc::new(broker_config);
    let broker = Broker::new(Arc::clone(&config));
    broker.start().await;
    Ok(())
}
