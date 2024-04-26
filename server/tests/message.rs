use bytes::Buf;
use server::MemoryRecordBuilder;

#[tokio::test]
pub async fn test_record_batch() {
    let mut builder = MemoryRecordBuilder::new();

    // 直接使用append_record_with_offset来添加记录
    builder.append_record_with_offset(1234567, 1i64, "a", "v");
    builder.append_record_with_offset(1234568, 2i64, "b", "v");

    // 构建record_batch
    let record_batch = builder.build();

    // 打印buffer长度
    println!("buffer length: {}", record_batch.buffer().len());
    let batch_header = record_batch.batch_header();
    let records = record_batch.records();
    println!("batchHeader:{:#?}", batch_header);
    println!("records:{:#?}", records);
}
