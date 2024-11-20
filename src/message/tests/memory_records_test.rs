mod tests {
    use crate::message::{MemoryRecords, RecordBatchBuilder};

    #[test]
    fn test_memory_records_iteration() {
        let mut builder = RecordBatchBuilder::default();
        builder.append_record(
            Some(0),
            Some(1000),
            "key1".as_bytes(),
            "value1".as_bytes(),
            None,
        );

        let batch = builder.build();
        let mut records = MemoryRecords::new(batch.buffer);

        let first_batch = records.next();
        assert!(first_batch.is_some());
        assert!(records.next().is_none());
    }
}
