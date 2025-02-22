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

use std::{
    fs::File,
    io::{self, ErrorKind, Read, Seek, SeekFrom},
};

use super::{LogType, PositionInfo};

/// Seeks to find the position of target offset in the log file
///
/// # Arguments
///
/// * `file` - The log file handle
/// * `target_offset` - The target offset to seek to
/// * `ref_pos` - Reference position info containing base offset, current offset and file position
/// * `log_type` - Type of log, can be Journal or Queue
///
/// # Returns
///
/// Returns an IO result containing a tuple of file handle and new position info:
/// * `Ok((File, PositionInfo))` - Successfully found target position
/// * `Err(io::Error)` - Error occurred during seek operation
///
/// # Errors
///
/// Returns error when target offset does not exist or file operations fail
pub async fn seek_file(
    mut file: File,
    target_offset: i64,
    ref_pos: PositionInfo,
    log_type: LogType,
) -> io::Result<(File, PositionInfo)> {
    tokio::task::spawn_blocking(move || {
        let PositionInfo {
            offset, position, ..
        } = ref_pos;

        if offset == target_offset {
            file.seek(SeekFrom::Start(position as u64))?;
            return Ok((file, ref_pos));
        }

        file.seek(SeekFrom::Start(position as u64))?;

        match log_type {
            LogType::Journal => seek_to_target_offset_journal(&mut file, target_offset),
            LogType::Queue => seek_to_target_offset_queue(&mut file, target_offset),
        }
        .map(|new_position| {
            let mut new_pos = ref_pos;
            new_pos.position = new_position;
            new_pos.offset = target_offset;
            (file, new_pos)
        })
    })
    .await?
}

fn seek_to_target_offset_journal(file: &mut File, target_offset: i64) -> io::Result<i64> {
    let mut buffer = [0u8; 12];

    loop {
        file.read_exact(&mut buffer)?;
        let batch_size = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
        let current_offset = i64::from_be_bytes([
            buffer[4], buffer[5], buffer[6], buffer[7], buffer[8], buffer[9], buffer[10],
            buffer[11],
        ]);

        match current_offset.cmp(&target_offset) {
            std::cmp::Ordering::Equal => {
                // If the current offset equals the target offset, return the current position, rewinding 12 bytes for both size and offset.
                let current_position = file.seek(SeekFrom::Current(-12))?;
                return Ok(current_position as i64);
            }
            std::cmp::Ordering::Greater => {
                let err_msg = format!(
                    "target offset not found {}/{}",
                    target_offset, current_offset
                );
                return Err(io::Error::new(ErrorKind::NotFound, err_msg));
            }
            std::cmp::Ordering::Less => {
                // If it is less than the target offset, skip the current batch. Since the current position has already surpassed the offset, subtract 8 here.
                file.seek(SeekFrom::Current(batch_size as i64 - 8))?;
            }
        }
    }
}

fn seek_to_target_offset_queue(file: &mut File, target_offset: i64) -> io::Result<i64> {
    let mut buffer = [0u8; 12];

    loop {
        file.read_exact(&mut buffer)?;

        let current_offset = i64::from_be_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]);
        let length = i32::from_be_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]);

        match current_offset.cmp(&target_offset) {
            std::cmp::Ordering::Equal => {
                let current_position = file.seek(SeekFrom::Current(-12))?;
                return Ok(current_position as i64);
            }
            std::cmp::Ordering::Greater => return Ok(-1),
            std::cmp::Ordering::Less => {
                file.seek(SeekFrom::Current(length as i64))?;
            }
        }
    }
}
