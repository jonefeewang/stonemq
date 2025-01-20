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

//! Message Format Constants
//!
//! This module defines the constants used in the message format implementation.
//! It includes offsets and lengths for various record fields, batch metadata,
//! and special values used throughout the messaging system.
//!
//! # Record Format
//!
//! The message record format consists of:
//! - 8-byte offset
//! - 4-byte size
//! - Variable-length message content
//!
//! # Batch Format
//!
//! The record batch format includes:
//! - Base offset (8 bytes)
//! - Length (4 bytes)
//! - Partition leader epoch (4 bytes)
//! - Magic byte (1 byte)
//! - CRC (4 bytes)
//! - Attributes (2 bytes)
//! - Last offset delta (4 bytes)
//! - First timestamp (8 bytes)
//! - Max timestamp (8 bytes)
//! - Producer ID (8 bytes)
//! - Producer epoch (2 bytes)
//! - Base sequence (4 bytes)
//! - Record count (4 bytes)
//! - Records (variable length)

// Record field offsets and lengths
// 记录字段的偏移量和长度
pub const OFFSET_OFFSET: usize = 0;
pub const OFFSET_LENGTH: usize = 8;
pub const SIZE_OFFSET: usize = OFFSET_OFFSET + OFFSET_LENGTH;
pub const SIZE_LENGTH: usize = 4;
pub const LOG_OVERHEAD: usize = SIZE_OFFSET + SIZE_LENGTH;

// Record batch field offsets and lengths
pub const BASE_OFFSET_OFFSET: i32 = 0;
pub const BASE_OFFSET_LENGTH: i32 = 8;
pub const LENGTH_OFFSET: i32 = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
pub const LENGTH_LENGTH: i32 = 4;
pub const PARTITION_LEADER_EPOCH_OFFSET: i32 = LENGTH_OFFSET + LENGTH_LENGTH;
pub const PARTITION_LEADER_EPOCH_LENGTH: i32 = 4;
pub const RB_MAGIC_OFFSET: i32 = PARTITION_LEADER_EPOCH_OFFSET + PARTITION_LEADER_EPOCH_LENGTH;
pub const RB_MAGIC_LENGTH: i32 = 1;
pub const CRC_OFFSET: i32 = RB_MAGIC_OFFSET + RB_MAGIC_LENGTH;
pub const CRC_LENGTH: i32 = 4;
pub const ATTRIBUTES_OFFSET: i32 = CRC_OFFSET + CRC_LENGTH;
pub const ATTRIBUTE_LENGTH: i32 = 2;
pub const LAST_OFFSET_DELTA_OFFSET: i32 = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
pub const LAST_OFFSET_DELTA_LENGTH: i32 = 4;
pub const FIRST_TIMESTAMP_OFFSET: i32 = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
pub const FIRST_TIMESTAMP_LENGTH: i32 = 8;
pub const MAX_TIMESTAMP_OFFSET: i32 = FIRST_TIMESTAMP_OFFSET + FIRST_TIMESTAMP_LENGTH;
pub const MAX_TIMESTAMP_LENGTH: i32 = 8;
pub const PRODUCER_ID_OFFSET: i32 = MAX_TIMESTAMP_OFFSET + MAX_TIMESTAMP_LENGTH;
pub const PRODUCER_ID_LENGTH: i32 = 8;
pub const PRODUCER_EPOCH_OFFSET: i32 = PRODUCER_ID_OFFSET + PRODUCER_ID_LENGTH;
pub const PRODUCER_EPOCH_LENGTH: i32 = 2;
pub const BASE_SEQUENCE_OFFSET: i32 = PRODUCER_EPOCH_OFFSET + PRODUCER_EPOCH_LENGTH;
pub const BASE_SEQUENCE_LENGTH: i32 = 4;
pub const RECORDS_COUNT_OFFSET: i32 = BASE_SEQUENCE_OFFSET + BASE_SEQUENCE_LENGTH;
pub const RECORDS_COUNT_LENGTH: i32 = 4;
pub const RECORDS_OFFSET: i32 = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
pub const RECORD_BATCH_OVERHEAD: i32 = RECORDS_OFFSET;

// Special values and defaults

/// Magic value for the current message format version
pub const MAGIC: i8 = 2;
/// Special value indicating no producer ID
pub const NO_PRODUCER_ID: i64 = -1;
/// Special value indicating no producer epoch
pub const NO_PRODUCER_EPOCH: i16 = -1;
/// Special value indicating no sequence number
pub const NO_SEQUENCE: i32 = -1;
/// Special value indicating no partition leader epoch
pub const NO_PARTITION_LEADER_EPOCH: i32 = -1;
/// Default attributes value
pub const ATTRIBUTES: i16 = 0;
