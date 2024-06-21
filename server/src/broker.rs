use crate::BROKER_CONFIG;

#[derive(Clone, Debug)]
pub struct Node {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}
impl Node {
    pub fn new_localhost() -> Self {
        Node {
            node_id: 0,
            host: BROKER_CONFIG.get().unwrap().network.ip.clone(),
            port: BROKER_CONFIG.get().unwrap().network.port as i32,
            rack: None,
        }
    }
}
