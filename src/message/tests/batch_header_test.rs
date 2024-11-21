#[test]
fn test_batch_header_display() {
    use crate::message::BatchHeader;

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
