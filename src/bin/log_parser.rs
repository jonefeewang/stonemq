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

use bytes::{Buf, BytesMut};
use chrono::Local;
use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::path::PathBuf;
use stonemq::MemoryRecords;
use stonemq::{AppError, AppResult};
use stonemq::{CheckPointFile, LogType};

#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Journal {
        #[arg(short, long)]
        file: PathBuf,
    },
    Queue {
        #[arg(short, long)]
        file: PathBuf,
    },

    Index {
        #[arg(short, long)]
        file: PathBuf,
    },
    Checkpoint {
        #[arg(short, long)]
        file: PathBuf,
    },
}

#[tokio::main]
async fn main() -> AppResult<()> {
    // let _guard = setup_tracing();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Journal { file } => parse_journal_log(file),
        Commands::Queue { file } => parse_queue_log(file),
        Commands::Index { file } => parse_index(file),
        Commands::Checkpoint { file } => parse_checkpoint(file).await,
    }
}

fn parse_journal_log(file_path: &PathBuf) -> AppResult<()> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(&file);
    let mut buffer = BytesMut::with_capacity(1024);

    // 输出解析结果
    println!("┌──────────────────────────────────────────────────────────────────────────────┐");
    println!("│                                  log parser                                    │");
    println!("├──────────────────────────────────────────────────────────────────────────────┤");
    println!(
        "│ print time: {:<70} │",
        Local::now().format("%Y-%m-%d %H:%M:%S")
    );
    println!("│ file path: {:<70} │", file_path.to_str().unwrap_or(""));
    println!("└──────────────────────────────────────────────────────────────────────────────┘");

    let mut batch_count = 0;

    loop {
        // read batch size
        buffer.resize(4, 0);
        let position = reader.stream_position()?;

        match reader.read_exact(&mut buffer) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("read to end of file");
                break;
            }
            Err(e) => {
                println!("read file failed: {}", e);
                return Err(e.into());
            }
        }
        let batch_size = buffer.get_u32();

        // read batch content
        buffer.resize(batch_size as usize, 0);
        reader.read_exact(&mut buffer)?;

        // parse batch content
        let journal_offset: i64 = buffer.get_i64();

        let str_len = buffer.get_u32();
        let queue_topic_name = String::from_utf8(buffer[..str_len as usize].to_vec())
            .map_err(|e| AppError::InvalidValue(e.to_string()))?;
        buffer.advance(str_len as usize);

        // first batch queue base offset
        let first_batch_queue_base_offset: i64 = buffer.get_i64();

        // last batch queue base offset
        let last_batch_queue_base_offset: i64 = buffer.get_i64();

        // records count
        let records_count: u32 = buffer.get_u32();

        println!("--------------------------------------------------");
        println!("journal batch(start from 0): {}", batch_count);
        println!("batch start position: {}", position);
        println!("batch size(not include start size 4 bytes): {}", batch_size);
        println!("--------------------------------------------------");

        println!("Journal Offset: {}", journal_offset);
        println!("Queue Topic Name: {}", queue_topic_name);
        println!(
            "Queue first batch baseoffset: {}",
            first_batch_queue_base_offset
        );
        println!(
            "Queue last batch baseoffset: {}",
            last_batch_queue_base_offset
        );
        println!("Records count: {}", records_count);
        // parse remaining buffer content
        let memory_records = MemoryRecords::new(buffer.clone());

        let mut batchs = vec![];
        for batch in memory_records {
            batchs.push(batch);
        }

        let first_batch = batchs.first().unwrap();
        // let last_batch = batchs.last().unwrap();

        // parse batch header
        // let batch_header = first_batch.header();
        // println!("Batch Header:");
        // println!(
        //     "Queue first batch baseoffset: {}",
        //     batch_header.first_offset
        // );
        // println!(
        //     "Queue first batch offset delta: {}",
        //     batch_header.last_offset_delta
        // );
        // println!(
        //     "  First Timestamp: {}",
        //     format_timestamp(batch_header.first_timestamp)
        // );
        // println!(
        //     "  Max Timestamp: {}",
        //     format_timestamp(batch_header.max_timestamp)
        // );

        // parse records
        let records = first_batch.records();
        println!("Records:");
        for (_i, record) in records.iter().enumerate() {
            if let Some(value) = &record.value {
                print!("Value: {}", String::from_utf8_lossy(value));
                if (_i + 1) % 10 == 0 {
                    println!();
                } else {
                    print!("\t");
                }
            }
        }
        println!();
        batch_count += 1;

        // print parse result
    }

    Ok(())
}

fn parse_queue_log(file: &PathBuf) -> AppResult<()> {
    let file = File::open(file)?;
    let mut reader = BufReader::new(file);
    let mut buffer = BytesMut::with_capacity(1024);
    let mut offset_and_length = [0; 12];

    loop {
        // read batch size
        let position = reader.stream_position()?;

        match reader.read_exact(&mut offset_and_length) {
            Ok(_) => {
                let _ = i64::from_be_bytes(offset_and_length[0..8].try_into().unwrap());
                let length = i32::from_be_bytes(offset_and_length[8..12].try_into().unwrap());
                let _ = reader.seek_relative(-12);
                buffer.resize(12 + length as usize, 0);
                reader.read_exact(&mut buffer)?;

                let memory_records = MemoryRecords::new(buffer.clone());
                let mut batchs = vec![];
                for batch in memory_records {
                    batchs.push(batch);
                }

                let batch_header = batchs.first().unwrap().header();
                println!("batch start position: {}", position);
                println!("first batch header: {}", batch_header);

                let records = batchs.first().unwrap().records();
                for record in records {
                    let value = record.value.as_ref().map(|v| String::from_utf8_lossy(v));
                    println!("record: {:?}", value);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("read to end of file");
                break;
            }
            Err(e) => {
                println!("read file failed: {}", e);
                return Err(e.into());
            }
        }
    }
    Ok(())
}

fn parse_index(file: &PathBuf) -> AppResult<()> {
    println!("parse index file: {:?}", file);
    // implement index file parse logic

    let mut file = File::open(file)?;
    let mut buffer = BytesMut::zeroed(4);

    while file.read_exact(&mut buffer).is_ok() {
        let relative_offset = buffer.get_u32();
        buffer.resize(4, 0);
        file.read_exact(&mut buffer)?;
        let position = buffer.get_u32();
        println!(
            "relative offset: {}, position: {}\n",
            relative_offset, position
        );
        buffer.resize(4, 0);
    }
    Ok(())
}

async fn parse_checkpoint(file: &PathBuf) -> AppResult<()> {
    println!("parse checkpoint file: {:?}", file);

    let checkpoint = CheckPointFile::new(file.to_str().unwrap());
    let points = checkpoint.read_checkpoints(LogType::Journal)?;
    println!("checkpoint file parse result: {:?}", points);

    Ok(())
}
