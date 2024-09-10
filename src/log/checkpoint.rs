use crate::message::TopicPartition;
use crate::AppError::InvalidValue;
use crate::AppResult;
use std::borrow::Cow;
use std::collections::HashMap;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tracing::{error, trace, warn};

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

    pub async fn write_checkpoints(&self, points: HashMap<TopicPartition, u64>) -> AppResult<()> {
        let write_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.file_name)
            .await?;
        let mut buf_writer = BufWriter::new(write_file);
        buf_writer.write_all(format!("{}\n", self.version).as_bytes()).await?;
        for (topic_partition, offset) in points {
            buf_writer
                .write_all(format!("{} {}\n", topic_partition.id(), offset).as_bytes())
                .await?;
        }
        buf_writer.flush().await?;
        buf_writer.get_ref().sync_all().await?;
        Ok(())
    }

    pub async fn read_checkpoints(&self) -> AppResult<HashMap<TopicPartition, u64>> {
        let error = |line| InvalidValue("checkpoint {}", line);
        trace!("read checkpoints from {}", self.file_name);
        let open_file = OpenOptions::new()
            .read(true)
            .open(&self.file_name)
            .await;
        if open_file.is_err() {
            warn!("The checkpoint file cannot be found; if this is your first time running, please disregard this issue.");
            return Ok(HashMap::new());
        }

        let mut reader = BufReader::new(open_file?);
        let mut line_buffer = String::new();
        reader.read_line(&mut line_buffer).await?;
        let version = line_buffer.trim().parse::<i8>()?;
        if version != self.version {
            return Err(InvalidValue("version", self.version.to_string()));
        }
        let mut points = HashMap::new();
        let mut line = String::new();
        while reader.read_line(&mut line).await? > 0 {
            let mut parts = line.split_whitespace();
            if parts.clone().count() != 2 {
                return Err(error(line));
            }
            let tp_str = parts.next().ok_or(error(String::from("topic partition")))?;
            let topic_partition = TopicPartition::from_string(Cow::Borrowed(tp_str))?;
            let offset = parts.next().ok_or(error(String::from("offset")))?.parse()?;
            points.insert(topic_partition, offset);
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
        let temp_file = NamedTempFile::new()?;
        let file_name = temp_file.path().to_str().unwrap().to_string();
        let checkpoint_file = CheckPointFile::new(&file_name);

        let mut points = HashMap::new();
        points.insert(TopicPartition::new("topic1".into(), 0), 100);
        points.insert(TopicPartition::new("topic2".into(), 1), 200);

        checkpoint_file.write_checkpoints(points.clone()).await?;

        let read_points = checkpoint_file.read_checkpoints().await?;

        assert_eq!(points, read_points);
        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_version() -> AppResult<()> {
        let temp_file = NamedTempFile::new()?;
        let file_name = temp_file.path().to_str().unwrap().to_string();

        // Write an invalid version to the file
        fs::write(&file_name, "2\n").await?;

        let checkpoint_file = CheckPointFile::new(file_name);
        let result = checkpoint_file.read_checkpoints().await;

        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_format() -> AppResult<()> {
        let temp_file = NamedTempFile::new()?;
        let file_name = temp_file.path().to_str().unwrap().to_string();

        // Write an invalid format to the file
        fs::write(&file_name, "1\ntopic1-0 invalid\n").await?;

        let checkpoint_file = CheckPointFile::new(file_name);
        let result = checkpoint_file.read_checkpoints().await;

        assert!(result.is_err());
        Ok(())
    }
}
