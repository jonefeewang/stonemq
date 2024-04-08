use crate::KafkaFrame;
pub enum API{
    ProduceRequest,
    ProduceResponse
}

impl API{
    pub fn from_frame(frame:KafkaFrame){

    }
}