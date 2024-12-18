use crate::message::TopicPartition;
use crate::AppError::{self};
use crate::AppResult;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, BufWriter, Write};
use tracing::{debug, trace, warn};

use super::LogType;

#[derive(Debug)]
pub struct CheckPointFile {
    file_name: String,
    version: i8,
}

impl CheckPointFile {
    pub const CK_FILE_VERSION_1: i8 = 1;

    pub fn new(file_name: impl AsRef<str>) -> Self {
        Self {
            file_name: file_name.as_ref().to_string(),
            version: Self::CK_FILE_VERSION_1,
        }
    }

    pub async fn write_checkpoints(
        &self,
        points: HashMap<TopicPartition, i64>,
    ) -> std::io::Result<()> {
        debug!(
            "write checkpoints to {}, with values: {:?}",
            self.file_name, points
        );

        let file_name = self.file_name.clone();
        let version = self.version;

        tokio::task::spawn_blocking(move || {
            let write_file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&file_name)?;
            let mut buf_writer = BufWriter::new(write_file);
            buf_writer.write_all(format!("{}\n", version).as_bytes())?;
            for (topic_partition, offset) in points {
                buf_writer
                    .write_all(format!("{} {}\n", topic_partition.id(), offset).as_bytes())?;
            }
            buf_writer.flush()?;
            buf_writer.get_ref().sync_all()
        })
        .await??;
        Ok(())
    }

    pub fn read_checkpoints(&self, log_type: LogType) -> AppResult<HashMap<TopicPartition, i64>> {
        let error = |line| AppError::InvalidValue(format!("checkpoint {}", line));
        trace!("read checkpoints from {}", self.file_name);
        let open_file = OpenOptions::new().read(true).open(&self.file_name);
        if open_file.is_err() {
            warn!("The checkpoint file cannot be found; if this is your first time running, please disregard this issue.");
            return Ok(HashMap::new());
        }

        let mut reader = BufReader::new(open_file.unwrap());
        let mut line_buffer = String::new();
        reader.read_line(&mut line_buffer).map_err(|e| {
            AppError::DetailedIoError(format!("read line error: {} while read checkpoints", e))
        })?;
        let version = line_buffer.trim().parse::<i8>().map_err(|err| {
            AppError::InvalidValue(format!(
                "provide version: {}, expect version: {}",
                err, self.version
            ))
        })?;
        if version != self.version {
            return Err(AppError::InvalidValue(format!(
                "version: {}, expect: {}",
                version, self.version
            )));
        }
        let mut points = HashMap::new();
        let mut line = String::new();
        while reader.read_line(&mut line).map_err(|e| {
            AppError::DetailedIoError(format!("read line error: {} while read checkpoints", e))
        })? > 0
        {
            let mut parts = line.split_whitespace();
            if parts.clone().count() != 2 {
                return Err(error(line));
            }
            let tp_str = parts.next().ok_or(error(String::from("topic partition")))?;
            let tp = TopicPartition::from_str(tp_str, log_type)?;
            let offset = parts
                .next()
                .ok_or(error(String::from("offset")))?
                .parse::<i64>()
                .map_err(|err| AppError::InvalidValue(format!("offset: {}", err)))?;
            points.insert(tp, offset);
            line.clear();
        }
        Ok(points)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use tokio::fs;

    #[tokio::test]
    async fn test_write_and_read_checkpoints() -> AppResult<()> {
        let temp_file = NamedTempFile::new().unwrap();
        let file_name = temp_file.path().to_str().unwrap().to_string();
        let checkpoint_file = CheckPointFile::new(&file_name);

        let mut points = HashMap::new();
        points.insert(TopicPartition::new("topic1", 0, LogType::Journal), 100);
        points.insert(TopicPartition::new("topic2", 1, LogType::Journal), 200);

        checkpoint_file
            .write_checkpoints(points.clone())
            .await
            .unwrap();

        let read_points = checkpoint_file.read_checkpoints(LogType::Journal).unwrap();

        assert_eq!(points, read_points);
        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_version() -> AppResult<()> {
        let temp_file = NamedTempFile::new().unwrap();
        let file_name = temp_file.path().to_str().unwrap().to_string();

        // Write an invalid version to the file
        fs::write(&file_name, "2\n").await.unwrap();

        let checkpoint_file = CheckPointFile::new(file_name);
        let result = checkpoint_file.read_checkpoints(LogType::Journal);

        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_format() -> AppResult<()> {
        let temp_file = NamedTempFile::new().unwrap();
        let file_name = temp_file.path().to_str().unwrap().to_string();

        // Write an invalid format to the file
        fs::write(&file_name, "1\ntopic1-0 invalid\n")
            .await
            .unwrap();

        let checkpoint_file = CheckPointFile::new(file_name);
        let result = checkpoint_file.read_checkpoints(LogType::Journal);

        assert!(result.is_err());
        Ok(())
    }
}
