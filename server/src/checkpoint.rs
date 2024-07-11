use std::borrow::Cow;
use std::collections::HashMap;

use tokio::fs::OpenOptions;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

use crate::AppError::InvalidValue;
use crate::AppResult;
use crate::topic_partition::TopicPartition;

pub trait CheckPointFile {
    async fn checkpoints(
        &self,
        file_name: &str,
        points: HashMap<TopicPartition, u64>,
        version: i8,
    ) -> AppResult<()> {
        let write_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&file_name)
            .await?;
        let mut buf_writer = BufWriter::new(write_file);
        buf_writer.write_i8(version).await?;
        buf_writer.write_all("\n".as_bytes()).await?;
        for (topic_partition, offset) in points {
            buf_writer
                .write_all(topic_partition.id().as_bytes())
                .await?;
            buf_writer.write_all(" ".as_bytes()).await?;
            buf_writer.write_u64(offset).await?;
            buf_writer.write_all("\n".as_bytes()).await?;
        }
        buf_writer.flush().await?;
        buf_writer.get_ref().sync_all().await?;
        Ok(())
    }
    async fn read_checkpoints(
        &self,
        file_name: &str,
        version: i8,
    ) -> AppResult<HashMap<TopicPartition, u64>> {
        let error = |line| InvalidValue("checkpoint {}", line);
        let read_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .open(&file_name)
            .await?;
        let mut reader = BufReader::new(read_file);
        let mut read_version = String::new();
        reader.read_line(&mut read_version).await?;
        let read_version = read_version.trim().parse::<i8>()?;
        if read_version != version {
            return Err(InvalidValue("version", version.to_string()));
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
        }
        Ok(points)
    }
}
