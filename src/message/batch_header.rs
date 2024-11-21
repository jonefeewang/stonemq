use chrono::{Local, TimeZone};
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct BatchHeader {
    pub first_offset: i64,
    pub length: i32,
    pub partition_leader_epoch: i32,
    pub magic: i8,
    pub crc: u32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub first_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub first_sequence: i32,
    pub records_count: i32,
}

impl Display for BatchHeader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let chrono_first_timestamp = Local.timestamp_millis_opt(self.first_timestamp).unwrap();
        let chrono_max_timestamp = Local.timestamp_millis_opt(self.max_timestamp).unwrap();
        f.debug_struct("BatchHeader")
            .field("first_offset", &self.first_offset)
            .field("length", &self.length)
            .field("partition_leader_epoch", &self.partition_leader_epoch)
            .field("magic", &self.magic)
            .field("crc", &self.crc)
            .field("attributes", &self.attributes)
            .field("last_offset_delta", &self.last_offset_delta)
            .field("first_timestamp", &chrono_first_timestamp)
            .field("max_timestamp", &chrono_max_timestamp)
            .field("producer_id", &self.producer_id)
            .field("producer_epoch", &self.producer_epoch)
            .field("first_sequence", &self.first_sequence)
            .field("records_count", &self.records_count)
            .finish()
    }
}
