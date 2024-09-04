use crate::AppError::IllegalStateError;
use crate::AppResult;
use crossbeam_utils::atomic::AtomicCell;
use memmap2::{Mmap, MmapMut, MmapOptions};
use std::borrow::Cow;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::fs::File;
use tokio::io::{self, Result};
use tokio::sync::RwLock;
use tracing::debug;

const INDEX_ENTRY_SIZE: usize = 8;

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
        let newly_created = !path.as_ref().exists();

        let file = if read_only {
            File::options().read(true).open(&path).await?
        } else {
            File::options().read(true).write(true).create(true).open(&path).await?
        };

        if !read_only {
            file.set_len(max_index_file_size as u64).await?;
        }

        let mode = if read_only {
            IndexFileMode::ReadOnly(unsafe { MmapOptions::new().map(&file)? })
        } else {
            IndexFileMode::ReadWrite(unsafe { MmapOptions::new().map_mut(&file)? })
        };

        let entries =
            if newly_created { 0 } else {
                match &mode {
                    IndexFileMode::ReadOnly(mmap) => mmap.len() / INDEX_ENTRY_SIZE,
                    IndexFileMode::ReadWrite(mmap) => { mmap.as_ref().len() / INDEX_ENTRY_SIZE }
                }
            };

        debug!("create Index file path: {} (read_only: {}) entries:{}", &path.as_ref().to_string_lossy(), read_only,entries);

        Ok(IndexFile {
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

    pub async fn resize(&self, new_size: u64) -> AppResult<()> {
        let mut mode = self.mode.write().await;
        match &mut *mode {
            IndexFileMode::ReadOnly(_) => Err(IllegalStateError(Cow::Borrowed("attempt to resize read-only index file"))),
            IndexFileMode::ReadWrite(_) => {
                self.file.set_len(new_size).await?;
                let new_mmap = unsafe { MmapOptions::new().map_mut(&self.file)? };
                *mode = IndexFileMode::ReadWrite(new_mmap);
                let max_entries = (new_size as usize) / INDEX_ENTRY_SIZE;
                self.entries.fetch_min(max_entries);
                Ok(())
            }
        }
    }

    pub async fn lookup(&self, target_offset: u32) -> Option<u32> {
        let entries = self.entries.load();
        if entries == 0 {
            return None;
        }

        let mut left = 0;
        let mut right = entries - 1;

        let mmap = &*self.mode.read().await;
        let mmap = match mmap {
            IndexFileMode::ReadOnly(mmap) => mmap,
            IndexFileMode::ReadWrite(mmap) => mmap.as_ref(),
        };
        while left <= right {
            let mid = (left + right) / 2;
            let offset = mid * INDEX_ENTRY_SIZE;
            let entry_offset = u32::from_be_bytes([
                mmap[offset],
                mmap[offset + 1],
                mmap[offset + 2],
                mmap[offset + 3],
            ]);

            if entry_offset == target_offset {
                return Some(u32::from_be_bytes([
                    mmap[offset + 4],
                    mmap[offset + 5],
                    mmap[offset + 6],
                    mmap[offset + 7],
                ]));
            } else if entry_offset < target_offset {
                left = mid + 1;
            } else {
                if mid == 0 {
                    return None;
                }
                right = mid - 1;
            }
        }

        if left == 0 {
            return None;
        }

        let offset = (left - 1) * INDEX_ENTRY_SIZE;
        Some(u32::from_be_bytes([
            mmap[offset + 4],
            mmap[offset + 5],
            mmap[offset + 6],
            mmap[offset + 7],
        ]))
    }


    pub async fn flush(&self) -> AppResult<()> {
        let mmap = &*self.mode.write().await;
        match mmap {
            IndexFileMode::ReadOnly(_) => Err(IllegalStateError(Cow::Borrowed("attempt to flush read-only index file"))),
            IndexFileMode::ReadWrite(mmap) => {
                mmap.flush()?;
                Ok(())
            }
        }
    }

    pub async fn is_full(&self) -> bool {
        let entries = self.entries.load();
        let mmap = self.mode.read().await;
        let len = match &*mmap {
            IndexFileMode::ReadOnly(m) => m.len(),
            IndexFileMode::ReadWrite(m) => m.len(),
        };
        (entries + 1) * INDEX_ENTRY_SIZE >= len
    }
    pub fn entry_count(&self) -> usize {
        self.entries.load()
    }
    pub async fn trim_to_valid_size(&self) -> AppResult<()> {
        // 注意先获得一个读锁，再获取一个写锁，防止死锁
        let new_size = self.entries.load() as u64 * INDEX_ENTRY_SIZE as u64;
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
    }

    #[rstest]
    #[tokio::test]
    async fn test_add_entry() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();

        assert!(index_file.add_entry(100, 200).await.is_ok());
        assert_eq!(index_file.entry_count(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_add_entry_full() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, INDEX_ENTRY_SIZE, false).await.unwrap();

        assert!(index_file.add_entry(100, 200).await.is_ok());
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
    }

    #[rstest]
    #[tokio::test]
    async fn test_resize() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, INDEX_ENTRY_SIZE * 2, false).await.unwrap();

        index_file.add_entry(100, 200).await.unwrap();
        index_file.add_entry(300, 400).await.unwrap();
        assert!(index_file.is_full().await);

        index_file.resize((INDEX_ENTRY_SIZE * 4) as u64).await.unwrap();
        assert!(!index_file.is_full().await);

        index_file.add_entry(500, 600).await.unwrap();
        index_file.add_entry(700, 800).await.unwrap();

        assert_eq!(index_file.lookup(100).await, Some(200));
        assert_eq!(index_file.lookup(300).await, Some(400));
        assert_eq!(index_file.lookup(500).await, Some(600));
        assert_eq!(index_file.lookup(700).await, Some(800));

        assert!(index_file.is_full().await);
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
            debug!("index file entries: {}", index_file.entry_count());
        }


        // Open the file in read-only mode
        let read_only_index = IndexFile::new(&file_path, 1024, true).await.unwrap();
        assert!(matches!(*read_only_index.mode.read().await, IndexFileMode::ReadOnly(_)));

        debug!("index file entries: {}", read_only_index.entry_count());

        // Verify that we can read but not write
        assert_eq!(read_only_index.lookup(100).await, Some(200));
        assert!(read_only_index.add_entry(300, 400).await.is_err());
        assert!(read_only_index.resize(2048).await.is_err());
    }
}
