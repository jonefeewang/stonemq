// Copyright 2025 jonefeewang@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/// This module contains test cases for the RecordBatch functionality.
/// The tests verify various aspects of record batch creation, manipulation,
/// and validation, ensuring the correct behavior of the RecordBatch implementation.
///
/// Test coverage includes:
/// - Basic record batch building and validation
/// - Record batch header verification
/// - Record headers support
/// - Multiple records handling
/// - Record batch splitting and merging
/// - Memory management and buffer reuse
///
#[cfg(test)]
use crate::message::{
    constants::{MAGIC, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE},
    record::RecordHeader,
    record_batch::{RecordBatch, RecordBatchBuilder},
    MemoryRecords,
};

/// Tests the basic functionality of RecordBatchBuilder.
/// Verifies that a simple record can be added and the resulting batch
/// has the correct header values.
#[test]
fn test_record_batch_builder() {
    let mut builder = RecordBatchBuilder::default();
    builder.append_record(
        Some(0),
        Some(1000),
        "test_key".as_bytes(),
        "test_value".as_bytes(),
        None,
    );

    let batch = builder.build();
    let header = batch.header();

    assert_eq!(header.first_offset, 0);
    assert_eq!(header.magic, 2);
    assert_eq!(header.last_offset_delta, 0);
}

/// Tests the creation and validation of a RecordBatch.
/// Verifies that all header fields are set correctly when creating
/// a new batch with a single record.
#[test]
fn test_record_batch() {
    // 创建一个简单的RecordBatch
    let mut builder = RecordBatchBuilder::default();

    // 添加一条记录
    builder.append_record(
        Some(0),
        Some(1000),
        "test_key".as_bytes(),
        "test_value".as_bytes(),
        None,
    );

    // 构建RecordBatch
    let batch = builder.build();

    // 验证batch header
    let header = batch.header();
    assert_eq!(header.first_offset, 0);
    assert_eq!(header.magic, MAGIC);
    assert_eq!(header.last_offset_delta, 0);
    assert_eq!(header.first_timestamp, 1000);
    assert_eq!(header.max_timestamp, 1000);
    assert_eq!(header.producer_id, NO_PRODUCER_ID);
    assert_eq!(header.producer_epoch, NO_PRODUCER_EPOCH);
    assert_eq!(header.first_sequence, NO_SEQUENCE);
}

/// Tests RecordBatch creation with custom headers.
/// Verifies that records can be created with custom headers
/// and that the resulting batch maintains the correct structure.
#[test]
fn test_record_batch_with_headers() {
    let mut builder = RecordBatchBuilder::default();

    // 创建headers
    let headers = vec![
        RecordHeader::new("header1".to_string(), "value1".as_bytes()),
        RecordHeader::new("header2".to_string(), "value2".as_bytes()),
    ];

    // 添加带headers的记录
    builder.append_record(
        Some(0),
        Some(1000),
        "key".as_bytes(),
        "value".as_bytes(),
        Some(headers),
    );

    let batch = builder.build();
    let header = batch.header();

    assert_eq!(header.first_offset, 0);
    assert_eq!(header.magic, MAGIC);
}

/// Tests handling of multiple records in a batch.
/// Verifies that multiple records can be added to a batch
/// and that the batch metadata (timestamps, offsets) is updated correctly.
#[test]
fn test_multiple_records() {
    let mut builder = RecordBatchBuilder::default();

    // 添加多条记录
    for i in 0..3 {
        builder.append_record(
            Some(i),
            Some(1000 + i),
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
            None,
        );
    }

    let batch = builder.build();
    let header = batch.header();

    assert_eq!(header.first_offset, 0);
    assert_eq!(header.last_offset_delta, 2);
    assert_eq!(header.first_timestamp, 1000);
    assert_eq!(header.max_timestamp, 1002);
}

/// Tests the splitting functionality of RecordBatch.
/// Verifies that a batch can be split into MemoryRecords
/// and that the resulting split maintains data integrity.
#[test]
fn test_split_record_batch() {
    let mut builder = RecordBatchBuilder::default();

    // 添加多条记录到builder
    for i in 0..3 {
        builder.append_record(
            Some(i),
            Some(1000 + i),
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
            None,
        );
    }

    // 创建MemoryRecords
    let batch = builder.build();
    let mut records = MemoryRecords::new(batch.buffer);

    // 从MemoryRecords中拆分出一个RecordBatch
    let split_batch = records.next().unwrap();
    let header = split_batch.header();

    // 验证拆分后的RecordBatch
    assert_eq!(header.first_offset, 0);
    assert_eq!(header.last_offset_delta, 2);
    assert_eq!(header.first_timestamp, 1000);
    assert_eq!(header.max_timestamp, 1002);
    assert_eq!(header.magic, MAGIC);

    // 验证拆分后的MemoryRecords已经为空
    assert!(records.next().is_none());
}

/// Generates a random RecordBatch for testing purposes.
/// Creates a batch with 1-10 random records, each with unique
/// keys, values, and timestamps.
///
/// # Returns
/// A RecordBatch containing random test data
#[cfg(test)]
fn generate_random_record_batch() -> RecordBatch {
    use std::time::{SystemTime, UNIX_EPOCH};

    use rand::Rng;

    let mut builder = RecordBatchBuilder::default();
    let mut rng = rand::thread_rng();

    // 生成1-10条随机记录
    let record_count = rng.gen_range(1..=10);

    for i in 0..record_count {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let timestamp = now + i * 1000; // 增加一秒
        let key = format!("key-{}", rng.gen::<u32>());
        let value = format!("value-{}", rng.gen::<u32>());

        builder.append_record(
            Some(i),
            Some(timestamp),
            key.as_bytes(),
            value.as_bytes(),
            None,
        );
    }

    builder.build()
}

/// Tests that BytesMut's unsplit operation reuses the original memory.
/// Verifies that when batches are split and then merged back together,
/// the system efficiently reuses memory buffers instead of creating new ones.
#[test]
fn test_bytes_mut_unsplit_reuse_original_memory() {
    // 创建原始记录
    let batch1 = generate_random_record_batch();
    let batch2 = generate_random_record_batch();
    let batch3 = generate_random_record_batch();

    // 创建并合并原始 MemoryRecords
    let mut original_memory_records = MemoryRecords::new(batch1.buffer);

    original_memory_records
        .buffer
        .as_mut()
        .unwrap()
        .unsplit(batch2.buffer);
    original_memory_records
        .buffer
        .as_mut()
        .unwrap()
        .unsplit(batch3.buffer);

    let original_ptr = original_memory_records.buffer.as_ref().unwrap().as_ptr() as usize;
    println!("Original buffer address: 0x{:x}", original_ptr);

    // 收集拆分的批次
    let mut batches = vec![];
    for batch in original_memory_records {
        let batch_ptr = batch.buffer.as_ptr() as usize;
        println!("Split batch buffer address: 0x{:x}", batch_ptr);
        batches.push(batch);
    }

    // 修改这部分：使用第一个 batch 的 buffer 初始化 merged_records
    let first_batch = batches.remove(0);
    let mut merged_records = MemoryRecords::new(first_batch.buffer);

    // 合并剩余的 batches
    for batch in batches {
        batch.unsplit(&mut merged_records);
    }

    let merged_ptr = merged_records.buffer.as_ref().unwrap().as_ptr() as usize;
    println!("Merged buffer address: 0x{:x}", merged_ptr);

    // 验证地址相同
    assert_eq!(
        original_ptr, merged_ptr,
        "Buffer addresses should be the same"
    );
}
