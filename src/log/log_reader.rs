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
pub async fn seek(
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
                let current_position = file.seek(SeekFrom::Current(-12))?;
                return Ok(current_position as i64);
            }
            // 当前offset大于目标offset，则返回-1，表示找不到,splitter 在读取下一个offset时，这个offset还未生产出来，或者还未落盘
            std::cmp::Ordering::Greater => {
                return Err(io::Error::new(ErrorKind::NotFound, "target offset not found"))
            }
            std::cmp::Ordering::Less => {
                file.seek(SeekFrom::Current(batch_size as i64 - 12))?;
            }
        }
    }
}

fn seek_to_target_offset_queue(file: &mut File, target_offset: i64) -> io::Result<i64> {
    let mut buffer = [0u8; 12];

    loop {
        file.read_exact(&mut buffer)?;
        let length = i32::from_be_bytes([buffer[8], buffer[9], buffer[10], buffer[11]]);
        let current_offset = i64::from_be_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]);

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
