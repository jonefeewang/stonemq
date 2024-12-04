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

#[cfg(test)]
#[test]
fn test_batch_header_display() {
    let header = BatchHeader {
        first_offset: 0,
        length: 100,
        partition_leader_epoch: -1,
        magic: 2,
        crc: 123456,
        attributes: 0,
        last_offset_delta: 0,
        first_timestamp: 1000,
        max_timestamp: 2000,
        producer_id: -1,
        producer_epoch: -1,
        first_sequence: -1,
        records_count: 1,
    };

    let display_str = format!("{}", header);
    assert!(display_str.contains("first_offset: 0"));
    assert!(display_str.contains("length: 100"));
}
