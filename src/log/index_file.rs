use crate::AppError::IllegalStateError;
use crate::AppResult;
use crossbeam_utils::atomic::AtomicCell;
use memmap2::{Mmap, MmapMut, MmapOptions};
use std::borrow::Cow;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::u32;
use tokio::fs::File;
use tokio::io::{self, Result};
use tokio::sync::RwLock;
use tracing::{debug, trace};

const INDEX_ENTRY_SIZE: usize = 8;

/// 相比Kafka中的IndexFile在写入时使用了锁，但是在读取时未使用锁，这样提升了并发，但是却允许脏读的存在
/// Rust从语言机制上不允许对需要锁保护的对象进行脏读，如果需要脏读的话，需要使用unsafe代码，这里分析一下不别要
/// 对于非active得segment的index file，没有写入操作，所以都是读取，不会被写锁block
/// 对于active得segment的index file，有写入操作，所以会被写锁block,这样影响了并发，但是保障读取到的数据不是旧的，
/// 不会出现写入的数据无法及时读取到
#[derive(Debug)]
pub enum IndexFileMode {
    ReadOnly(Mmap),
    ReadWrite(MmapMut),
}
#[derive(Debug)]
pub struct IndexFile {
    mode: RwLock<IndexFileMode>,
    entries: AtomicCell<usize>,
    file: File,
}
impl IndexFile {
    pub async fn new<P: AsRef<Path>>(path: P, max_index_file_size: usize, read_only: bool) -> AppResult<Self> {
        let path = path.as_ref();
        let file_exists = path.exists();

        let file = if read_only {
            File::options().read(true).open(path).await?
        } else {
            File::options().read(true).write(true).create(true).open(path).await?
        };

        let original_len = file.metadata().await?.len() as usize;
        let original_entries = original_len / INDEX_ENTRY_SIZE;

        if !read_only {
            file.set_len(max_index_file_size as u64).await?;
        }

        let mode = if read_only {
            IndexFileMode::ReadOnly(unsafe { MmapOptions::new().map(&file)? })
        } else {
            IndexFileMode::ReadWrite(unsafe { MmapOptions::new().map_mut(&file)? })
        };

        let entries = if file_exists {
            original_entries
        } else {
            0
        };

        trace!("opening index file: {},newly created: {}, entries: {}, readonly: {}", path.display(),!file_exists ,entries,read_only);

        Ok(Self {
            mode: RwLock::new(mode),
            entries: AtomicCell::new(entries),
            file,
        })
    }

    pub async fn add_entry(&self, relative_offset: u32, position: u32) -> AppResult<()> {
        let mut mode = self.mode.write().await;
        match &mut *mode {
            IndexFileMode::ReadOnly(_) => Err(IllegalStateError(Cow::Borrowed("attempt to add entry to read-only index file"))),
            IndexFileMode::ReadWrite(mmap) => {
                let entries = self.entries.load();
                if (entries + 1) * INDEX_ENTRY_SIZE > mmap.len() {
                    return Err(IllegalStateError(Cow::Borrowed(" index file is full")));
                }

                let offset = entries * INDEX_ENTRY_SIZE;
                mmap[offset..offset + 4].copy_from_slice(&relative_offset.to_be_bytes());
                mmap[offset + 4..offset + 8].copy_from_slice(&position.to_be_bytes());
                self.entries.fetch_add(1);
                Ok(())
            }
        }
    }
    pub async fn resize(&self, new_size: usize) -> AppResult<()> {
        let mut mode = self.mode.write().await;
        match &mut *mode {
            IndexFileMode::ReadOnly(_) => Err(IllegalStateError(Cow::Borrowed("attempt to resize read-only index file"))),
            IndexFileMode::ReadWrite(mmap) => {
                // Flush the existing mmap to ensure all data is written to disk
                mmap.flush()?;

                // Set the new file size
                self.file.set_len(new_size as u64).await?;

                // Create a new memory mapping with the updated file size
                let new_mmap = unsafe { MmapOptions::new().map_mut(&self.file)? };
                *mode = IndexFileMode::ReadWrite(new_mmap);

                let max_entries = new_size / INDEX_ENTRY_SIZE;
                self.entries.fetch_min(max_entries);
                Ok(())
            }
        }
    }
    pub async fn lookup(&self, target_offset: u32) -> Option<(u32, u32)> {
        let entries = self.entries.load();
        if entries == 0 {
            return None;
        }

        // 获取读锁
        let mode = self.mode.read().await;
        let mmap = match &*mode {
            IndexFileMode::ReadOnly(mmap) => mmap,
            IndexFileMode::ReadWrite(mmap) => mmap.as_ref(),
        };

        let mut left = 0;
        let mut right = entries - 1;

        while left <= right {
            let mid = (left + right) / 2;
            let offset = mid * INDEX_ENTRY_SIZE;
            let entry_offset = u32::from_be_bytes([
                mmap[offset], mmap[offset + 1], mmap[offset + 2], mmap[offset + 3]
            ]);

            if entry_offset > target_offset {
                if mid == 0 {
                    return None;
                }
                right = mid - 1;
            } else {
                if mid == entries - 1 || u32::from_be_bytes([
                    mmap[offset + INDEX_ENTRY_SIZE],
                    mmap[offset + INDEX_ENTRY_SIZE + 1],
                    mmap[offset + INDEX_ENTRY_SIZE + 2],
                    mmap[offset + INDEX_ENTRY_SIZE + 3],
                ]) > target_offset {
                    let position = u32::from_be_bytes([
                        mmap[offset + 4], mmap[offset + 5], mmap[offset + 6], mmap[offset + 7]
                    ]);
                    return Some((entry_offset, position));
                }
                left = mid + 1;
            }
        }

        None
    }


    pub async fn flush(&self) -> AppResult<()> {
        let mmap = &*self.mode.write().await;
        match mmap {
            IndexFileMode::ReadOnly(_) => Err(IllegalStateError("attempt to flush read-only index file".into())),
            IndexFileMode::ReadWrite(mmap) => {
                mmap.flush()?;
                Ok(())
            }
        }
    }

    pub async fn is_full(&self) -> AppResult<bool> {
        let entries = self.entries.load();
        let file_size = self.file.metadata().await?.len() as usize;
        let is_full = (entries + 1) * INDEX_ENTRY_SIZE > file_size;
        Ok(is_full)
    }
    pub fn entry_count(&self) -> usize {
        self.entries.load()
    }
    pub async fn trim_to_valid_size(&self) -> AppResult<()> {
        // 注意先获得一个读锁，再获取一个写锁，防止死锁
        let new_size = self.entries.load() * INDEX_ENTRY_SIZE;
        {
            let mmap = &*self.mode.read().await;
            if let IndexFileMode::ReadOnly(mmap) = mmap {
                return Err(IllegalStateError(Cow::Borrowed("attempt to trim read-only index file")));
            }
            // 释放读锁
        }
        // 获取写锁
        self.resize(new_size).await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::setup_tracing;
    use rstest::{fixture, rstest};
    use tempfile::tempdir;

    #[fixture]
    #[once]
    fn setup() {
        setup_tracing().expect("failed to setup tracing");
    }

    #[rstest]
    #[tokio::test]
    async fn test_new_index_file(setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();
        assert!(matches!(*index_file.mode.read().await, IndexFileMode::ReadWrite(_)));
        assert_eq!(index_file.entry_count(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_add_entry() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();

        assert!(index_file.add_entry(100, 200).await.is_ok());
        assert_eq!(index_file.entry_count(), 1);
        assert_eq!(index_file.lookup(100).await, Some(200));
    }

    #[rstest]
    #[tokio::test]
    async fn test_add_entry_full() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, INDEX_ENTRY_SIZE, false).await.unwrap();

        assert!(!index_file.is_full().await.unwrap());
        assert!(index_file.add_entry(100, 200).await.is_ok());
        assert!(index_file.is_full().await.unwrap());
        assert!(index_file.add_entry(300, 400).await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn test_lookup() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();

        index_file.add_entry(100, 200).await.unwrap();
        index_file.add_entry(300, 400).await.unwrap();

        assert_eq!(index_file.lookup(100).await, Some(200));
        assert_eq!(index_file.lookup(300).await, Some(400));
        assert_eq!(index_file.lookup(200).await, Some(200));
        assert_eq!(index_file.lookup(400).await, Some(400));
        assert_eq!(index_file.lookup(50).await, None);
    }

    #[rstest]
    #[tokio::test]
    async fn test_resize() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, INDEX_ENTRY_SIZE * 2, false).await.unwrap();

        index_file.add_entry(100, 200).await.unwrap();
        index_file.add_entry(300, 400).await.unwrap();
        assert!(index_file.is_full().await.unwrap());

        index_file.resize(INDEX_ENTRY_SIZE * 4).await.unwrap();
        assert!(!index_file.is_full().await.unwrap());

        index_file.add_entry(500, 600).await.unwrap();
        index_file.add_entry(700, 800).await.unwrap();

        assert_eq!(index_file.lookup(100).await, Some(200));
        assert_eq!(index_file.lookup(300).await, Some(400));
        assert_eq!(index_file.lookup(500).await, Some(600));
        assert_eq!(index_file.lookup(700).await, Some(800));

        assert!(index_file.is_full().await.unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn test_read_only_mode(setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");

        // Create and populate the file in read-write mode
        {
            let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();
            index_file.add_entry(100, 200).await.unwrap();
            index_file.trim_to_valid_size().await.unwrap();
        }

        // Open the file in read-only mode
        let read_only_index = IndexFile::new(&file_path, 1024, true).await.unwrap();
        assert!(matches!(*read_only_index.mode.read().await, IndexFileMode::ReadOnly(_)));

        // Verify that we can read but not write
        assert_eq!(read_only_index.lookup(100).await, Some(200));
        assert!(read_only_index.add_entry(300, 400).await.is_err());
        assert!(read_only_index.resize(2048).await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn test_flush() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();

        index_file.add_entry(100, 200).await.unwrap();
        assert!(index_file.flush().await.is_ok());
    }
}
