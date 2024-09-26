use bytes::{Buf, BytesMut};
use chrono::TimeZone;
use clap::{Parser, Subcommand};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use stonemq::message::MemoryRecords;
use stonemq::service::setup_tracing;
use stonemq::AppResult;

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
    setup_tracing().await?;

    let cli = Cli::parse();

    match &cli.command {
        Commands::Journal { file } => parse_journal_log(file),
        Commands::Queue { file } => parse_queue_log(file),
        Commands::Index { file } => parse_index(file),
        Commands::Checkpoint { file } => parse_checkpoint(file),
    }
}

fn parse_journal_log(file: &PathBuf) -> AppResult<()> {
    let file = File::open(file)?;
    let mut reader = BufReader::new(file);
    let mut buffer = BytesMut::with_capacity(1024);

    loop {
        // 读取batch大小
        buffer.resize(4, 0);
        match reader.read_exact(&mut buffer) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("读取到文件末尾");
                break;
            }
            Err(e) => {
                println!("读取文件失败: {}", e);
                return Err(e.into());
            }
        }
        let batch_size = buffer.get_u32();

        // 读取batch内容
        buffer.resize(batch_size as usize, 0);
        reader.read_exact(&mut buffer)?;

        // 解析batch内容
        let journal_offset: i64 = buffer.get_i64();

        let str_len = buffer.get_u32();
        let queue_topic_name = String::from_utf8(buffer[..str_len as usize].to_vec())?;
        buffer.advance(str_len as usize);

        // 使用MemoryRecords解析剩余的buffer内容
        let memory_records = MemoryRecords {
            buffer: Some(buffer.clone()),
        };

        // println!("Journal Batch size: {}", batch_size);
        println!("Journal Offset: {}", journal_offset);
        println!("Queue Topic Name: {}", queue_topic_name);

        // 解析batch header
        if let Some(batch_header) = memory_records.batch_header() {
            // println!("Batch Header:");
            println!("Queue baseoffset: {}", batch_header.first_offset);
            println!(
                "Queue last offset delta: {}",
                batch_header.last_offset_delta
            );
            // println!(
            //     "  First Timestamp: {}",
            //     format_timestamp(batch_header.first_timestamp)
            // );
            // println!(
            //     "  Max Timestamp: {}",
            //     format_timestamp(batch_header.max_timestamp)
            // );
        }

        // 解析records
        if let Some(records) = memory_records.records() {
            print!("Records:");
            for (_i, record) in records.iter().enumerate() {
                // println!("  Record {}:", i + 1);
                // println!("    Offset Delta: {}", record.offset_delta);
                // println!("    Timestamp Delta: {}", record.timestamp_delta);
                // if let Some(key) = &record.key {
                //     println!("    Key: {}", String::from_utf8_lossy(key));
                // }
                if let Some(value) = &record.value {
                    print!("    Value: {}", String::from_utf8_lossy(value));
                }
                // if let Some(headers) = &record.headers {
                //     println!("    Headers:");
                //     for header in headers {
                //         println!("      {}: {:?}", header.header_key, header.header_value);
                //     }
                // }
            }
        }

        // 输出解析结果

        println!("\n---");
    }

    Ok(())
}

fn parse_queue_log(file: &PathBuf) -> AppResult<()> {
    let file = File::open(file)?;
    let mut reader = BufReader::new(file);
    let mut buffer = BytesMut::with_capacity(1024);
    let mut offset_and_length = [0; 12];

    loop {
        // 读取batch大小

        match reader.read_exact(&mut offset_and_length) {
            Ok(_) => {
                let _ = i64::from_be_bytes(offset_and_length[0..8].try_into().unwrap());
                let length = i32::from_be_bytes(offset_and_length[8..12].try_into().unwrap());
                let _ = reader.seek_relative(-12);
                buffer.resize(12 + length as usize, 0);
                reader.read_exact(&mut buffer)?;

                let memory_records = MemoryRecords {
                    buffer: Some(buffer.clone()),
                };

                let batch_header = memory_records.batch_header().unwrap();
                println!("batch_header: {}", batch_header);
                let records = memory_records.records();
                println!("records: {:?}", records);
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("读取到文件末尾");
                break;
            }
            Err(e) => {
                println!("读取文件失败: {}", e);
                return Err(e.into());
            }
        }
    }
    Ok(())
}
// 读取batch大小

fn parse_index(file: &PathBuf) -> AppResult<()> {
    println!("解析索引文件: {:?}", file);
    // 实现索引文件解析逻辑

    let mut file = File::open(file)?;
    let mut buffer = BytesMut::zeroed(4);

    while let Ok(_) = file.read_exact(&mut buffer) {
        let relative_offset = buffer.get_u32();
        buffer.resize(4, 0);
        file.read_exact(&mut buffer)?;
        let position = buffer.get_u32();
        println!(
            "Relative Offset: {}, Position: {}\n",
            relative_offset, position
        );
        buffer.resize(4, 0);
    }
    Ok(())
}

fn parse_checkpoint(file: &PathBuf) -> AppResult<()> {
    println!("解析检查点文件: {:?}", file);
    // 实现检查点文件解析逻辑
    Ok(())
}
fn format_timestamp(timestamp: i64) -> String {
    // 将时间戳转换为 Local 日期时间
    let datetime = chrono::Local.timestamp_millis_opt(timestamp).unwrap();

    // 格式化日期时间为可读格式
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}
