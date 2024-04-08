use server::AppResult;
use server::BrokerConfig;

#[tokio::test]
async fn test_get_static_config() -> AppResult<()> {
    let broker_config = BrokerConfig::set_up_config("conf.toml")?;
    assert_eq!(broker_config.network().max_package_size(), 1_048_576);
    assert_eq!(broker_config.general().id(), "123");
    Ok(())
}
