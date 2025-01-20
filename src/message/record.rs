//! Message Record Implementation
//!
//! This module implements the core message record structure and its associated
//! header information. It provides the fundamental building blocks for storing
//! and managing individual messages within the messaging system.
//!
//! # Record Format
//!
//! Each record contains:
//! - Metadata (length, attributes, timestamps)
//! - Key-value content
//! - Optional headers
//!
//! # Features
//!
//! - Variable-length key-value storage
//! - Flexible header support
//! - Efficient size calculation
//! - Delta encoding for timestamps and offsets

use integer_encoding::VarInt;
use std::fmt::Debug;

/// Individual message record containing both metadata and content.
///
/// Represents a single message in the system with its associated metadata,
/// key-value content, and optional headers.
///
/// # Fields
///
/// * `length` - Total length of the record in bytes
/// * `attributes` - Record attributes flags
/// * `timestamp_delta` - Time difference from batch timestamp
/// * `offset_delta` - Offset difference from base offset
/// * `key_len` - Length of the key in bytes
/// * `key` - Optional key bytes
/// * `value_len` - Length of the value in bytes
/// * `value` - Optional value bytes
/// * `headers_count` - Number of headers
/// * `headers` - Optional record headers
#[derive(Debug)]
pub struct Record {
    pub length: i32,
    pub attributes: i8,
    pub timestamp_delta: i64,
    pub offset_delta: i32,
    pub key_len: i32,
    pub key: Option<Vec<u8>>,
    pub value_len: i32,
    pub value: Option<Vec<u8>>,
    pub headers_count: i32,
    pub headers: Option<Vec<RecordHeader>>,
}

/// Header information for a record.
///
/// Contains additional metadata or context for a record in the form
/// of key-value pairs.
///
/// # Fields
///
/// * `header_key` - Name of the header
/// * `header_value` - Optional value associated with the header
#[derive(Debug)]
pub struct RecordHeader {
    pub header_key: String,
    pub header_value: Option<Vec<u8>>,
}

impl RecordHeader {
    /// Creates a new record header.
    ///
    /// # Arguments
    ///
    /// * `key` - Header key name
    /// * `value` - Header value bytes
    ///
    /// # Returns
    ///
    /// A new RecordHeader instance with the specified key and value
    pub fn new<T: AsRef<[u8]>>(key: String, value: T) -> RecordHeader {
        RecordHeader {
            header_key: key,
            header_value: {
                if value.as_ref().is_empty() {
                    None
                } else {
                    Some(value.as_ref().into())
                }
            },
        }
    }

    /// Calculates the total size of the header in bytes.
    ///
    /// Includes:
    /// - Size of the key
    /// - Size of the value (if present)
    /// - Size of the length fields
    ///
    /// # Returns
    ///
    /// Total size in bytes as i32
    pub fn size(&self) -> i32 {
        let mut size = self.header_key.len().required_space() + self.header_key.len();
        if let Some(ref header_value) = self.header_value {
            size = size + header_value.len().required_space() + header_value.len()
        }
        size as i32
    }
}
