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

//! Read operations for journal logs.
//!
//! This module provides functionality for reading from journal logs,
//! including position lookup and metadata access.

use std::sync::atomic::Ordering;

use crate::{
    log::{segment_index::SegmentIndexCommon, PositionInfo},
    message::TopicPartition,
    AppResult,
};

use super::JournalLog;

impl JournalLog {
    /// Looks up the position information for a given offset.
    ///
    /// This method searches through the segments to find the appropriate position
    /// for the given offset. It first finds the correct segment by checking the
    /// base offsets, then retrieves the position within that segment.
    ///
    /// # Arguments
    ///
    /// * `offset` - The absolute offset to look up
    ///
    /// # Returns
    ///
    /// Returns the position information if found, wrapped in `AppResult`
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use your_crate::{JournalLog, AppResult};
    /// # fn example(journal: JournalLog) -> AppResult<()> {
    /// let position = journal.get_relative_position_info(1234);
    /// println!("Found position: offset={}, position={}", position.offset, position.position);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_relative_position_info(&self, offset: i64) -> AppResult<PositionInfo> {
        // Find the segment containing this offset by looking for the largest base offset
        // that is less than or equal to the target offset
        let segment_offset = self
            .segments_order
            .read()
            .iter()
            .rev()
            .find(|&&seg_offset| seg_offset <= offset)
            .copied() // Convert reference to value
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("no segment found for offset {}", offset),
                )
            })?;

        // Check if the offset belongs to the active segment
        if segment_offset == self.active_segment_base_offset.load(Ordering::Acquire) {
            self.active_segment_index
                .read()
                .get_relative_position(offset)
        } else {
            // Look up in the inactive segments
            self.segment_index
                .get(&segment_offset)
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("segment not found for offset {}", offset),
                    )
                })?
                .get_relative_position(offset)
        }
    }

    /// Returns the base offset of the current active segment.
    ///
    /// This method provides access to the base offset of the active segment,
    /// which is useful for determining the range of offsets currently being written to.
    ///
    /// # Returns
    ///
    /// The base offset of the active segment as an `i64`
    #[inline]
    pub fn current_active_seg_offset(&self) -> i64 {
        self.active_segment_base_offset.load(Ordering::Acquire)
    }

    /// Calculates the overhead size for a journal log record.
    ///
    /// The overhead includes:
    /// - 8 bytes for offset
    /// - Variable size for topic partition string
    /// - 8 bytes for first batch queue base offset
    /// - 8 bytes for last batch queue base offset
    /// - 4 bytes for records count
    ///
    /// # Arguments
    ///
    /// * `topic_partition` - The topic partition for which to calculate overhead
    ///
    /// # Returns
    ///
    /// The total overhead size in bytes as a `u32`
    #[inline]
    pub fn calculate_journal_log_overhead(topic_partition: &TopicPartition) -> u32 {
        // offset + topic partition string size + topic partition string +
        // first_batch_queue_base_offset + last_batch_queue_base_offset + records_count
        8 + topic_partition.protocol_size() + 8 + 8 + 4
    }
}
