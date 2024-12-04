use crossbeam::atomic::AtomicCell;
use memmap2::{Mmap, MmapMut, MmapOptions};
use std::path::Path;
use tokio::fs::File;
use tokio::sync::RwLock;
use tracing::{debug, trace};

use crate::{AppError, AppResult};

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
    /// Creates a new IndexFile with the specified path, size, and mode.
    ///
    /// # Arguments
    /// * `path` - The path to the index file
    /// * `max_index_file_size` - The maximum size of the index file
    /// * `read_only` - Whether the file should be opened in read-only mode
    ///
    /// # Returns
    /// A Result containing the new IndexFile or an error
    pub async fn new<P: AsRef<Path>>(
        path: P,
        index_file_max_size: usize,
        read_only: bool,
    ) -> AppResult<Self> {
        let path = path.as_ref();
        let file_exists = path.exists();

        let file = if read_only {
            File::options().read(true).open(path).await?
        } else {
            File::options()
                .write(true)
                .create(true)
                .append(true)
                .open(path)
                .await?
        };

        let original_len = file.metadata().await?.len() as usize;
        let original_entries = original_len / INDEX_ENTRY_SIZE;

        if !read_only {
            file.set_len(index_file_max_size as u64).await?;
        }

        let mode = if read_only {
            IndexFileMode::ReadOnly(unsafe {
                let mmap_options = MmapOptions::new();
                mmap_options.map(&file)?
            })
        } else {
            IndexFileMode::ReadWrite(unsafe { MmapOptions::new().map_mut(&file)? })
        };

        let entries = if file_exists { original_entries } else { 0 };

        trace!(
            "opening index file: {},newly created: {}, entries: {}, readonly: {}",
            path.display(),
            !file_exists,
            entries,
            read_only
        );

        Ok(Self {
            mode: RwLock::new(mode),
            entries: AtomicCell::new(entries),
            file,
        })
    }

    /// Adds a new entry to the index file.
    ///
    /// # Arguments
    /// * `relative_offset` - The relative offset of the entry
    /// * `position` - The position of the entry
    ///
    /// # Returns
    /// A Result indicating success or failure
    pub async fn add_entry(&self, relative_offset: u32, position: u32) -> AppResult<()> {
        let mut mode = self.mode.write().await;
        match &mut *mode {
            IndexFileMode::ReadOnly(_) => Err(AppError::InvalidOperation(
                "try to add entry to read-only index file".into(),
            )),
            IndexFileMode::ReadWrite(mmap) => {
                let entries = self.entries.load();
                if (entries + 1) * INDEX_ENTRY_SIZE > mmap.len() {
                    return Err(AppError::InvalidOperation("index file is full".into()));
                }

                let offset = entries * INDEX_ENTRY_SIZE;
                mmap[offset..offset + 4].copy_from_slice(&relative_offset.to_be_bytes());
                mmap[offset + 4..offset + 8].copy_from_slice(&position.to_be_bytes());
                self.entries.fetch_add(1);
                Ok(())
            }
        }
    }

    /// Resizes the index file to the specified size.
    ///
    /// # Arguments
    /// * `new_size` - The new size of the index file
    ///
    /// # Returns
    /// A Result indicating success or failure
    pub async fn resize(&self, new_size: usize) -> AppResult<()> {
        let mut mode = self.mode.write().await;
        match &mut *mode {
            IndexFileMode::ReadOnly(_) => Err(AppError::InvalidOperation(
                "try to resize read-only index file".into(),
            )),
            IndexFileMode::ReadWrite(mmap) => {
                debug!("resizing index file:{:?} to size: {}", self.file, new_size);
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
    /// 读取索引文件中的第一个条目
    ///
    /// # 参数
    /// * `mmap` - 内存映射的引用
    ///
    /// # 返回值
    /// 返回第一个条目的偏移量，如果文件为空则返回0
    async fn read_first_entry(&self, mmap: &[u8]) -> u32 {
        if self.entries.load() == 0 {
            return 0;
        }

        u32::from_be_bytes([mmap[0], mmap[1], mmap[2], mmap[3]])
    }

    /// Looks up an entry in the index file based on the target offset.
    ///
    /// # Arguments
    /// * `target_offset` - The target offset to search for
    ///
    /// # Returns
    /// An Option containing the entry offset and position if found, or None if not found
    pub async fn lookup(&self, target_offset: u32) -> Option<(u32, u32)> {
        // 获取读锁
        let mode = self.mode.read().await;
        let mmap = match &*mode {
            IndexFileMode::ReadOnly(mmap) => mmap,
            IndexFileMode::ReadWrite(mmap) => mmap.as_ref(),
        };
        let entries = self.entries.load();
        if entries == 0 || target_offset < self.read_first_entry(mmap).await {
            return Some((0, 0));
        }

        let mut left = 0;
        let mut right = entries - 1;

        while left <= right {
            let mid = (left + right) / 2;
            let offset = mid * INDEX_ENTRY_SIZE;
            let entry_offset = u32::from_be_bytes([
                mmap[offset],
                mmap[offset + 1],
                mmap[offset + 2],
                mmap[offset + 3],
            ]);

            if entry_offset > target_offset {
                if mid == 0 {
                    return None;
                }
                right = mid - 1;
            } else {
                if mid == entries - 1
                    || u32::from_be_bytes([
                        mmap[offset + INDEX_ENTRY_SIZE],
                        mmap[offset + INDEX_ENTRY_SIZE + 1],
                        mmap[offset + INDEX_ENTRY_SIZE + 2],
                        mmap[offset + INDEX_ENTRY_SIZE + 3],
                    ]) > target_offset
                {
                    let position = u32::from_be_bytes([
                        mmap[offset + 4],
                        mmap[offset + 5],
                        mmap[offset + 6],
                        mmap[offset + 7],
                    ]);
                    return Some((entry_offset, position));
                }
                left = mid + 1;
            }
        }

        None
    }

    /// Flushes the index file to disk.
    ///
    /// # Returns
    /// A Result indicating success or failure
    pub async fn flush(&self) -> AppResult<()> {
        let mmap = &*self.mode.write().await;
        match mmap {
            IndexFileMode::ReadOnly(_) => Err(AppError::InvalidOperation(
                "try to flush read-only index file".into(),
            )),
            IndexFileMode::ReadWrite(mmap) => {
                mmap.flush()?;
                Ok(())
            }
        }
    }

    /// Checks if the index file is full.
    ///
    /// # Returns
    /// A Result containing a boolean indicating if the index file is full or an error
    pub async fn is_full(&self) -> AppResult<bool> {
        let entries = self.entries.load();
        let file_size = self.file.metadata().await?.len() as usize;
        let is_full = (entries + 1) * INDEX_ENTRY_SIZE > file_size;
        Ok(is_full)
    }

    /// Returns the number of entries in the index file.
    pub fn entry_count(&self) -> usize {
        self.entries.load()
    }

    /// Closes the index file.
    ///
    /// # Returns
    /// A Result indicating success or failure
    pub async fn close(&self) -> AppResult<()> {
        self.trim_to_valid_size().await?;
        let mut mode = self.mode.write().await;
        match &mut *mode {
            IndexFileMode::ReadOnly(_) => {
                debug!("close read-only index file");
            }
            IndexFileMode::ReadWrite(mmap) => {
                mmap.flush()?;
                debug!("close read-write index file");
            }
        }
        Ok(())
    }

    /// Trims is the index file to the valid size.
    ///
    /// # Returns
    /// A Result indicating success or failure
    pub async fn trim_to_valid_size(&self) -> AppResult<()> {
        // 注意先获得一个读锁，再获取一个写锁，防止死锁
        let new_size = self.entries.load() * INDEX_ENTRY_SIZE;
        {
            let mmap = &*self.mode.read().await;
            if let IndexFileMode::ReadOnly(_) = mmap {
                return Err(AppError::InvalidOperation(
                    "try to trim read-only index file".into(),
                ));
            }
            // 释放读锁
        }
        // 获取写锁
        if new_size > 0 {
            self.resize(new_size).await?;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::service::setup_local_tracing;
    use rstest::{fixture, rstest};
    use tempfile::tempdir;

    #[fixture]
    #[once]
    fn setup() {
        setup_local_tracing().expect("failed to setup tracing");
    }

    #[rstest]
    #[tokio::test]
    async fn test_new_index_file(_setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();
        assert!(matches!(
            *index_file.mode.read().await,
            IndexFileMode::ReadWrite(_)
        ));
        assert_eq!(index_file.entry_count(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_add_entry(_setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();

        assert!(index_file.add_entry(100, 200).await.is_ok());
        assert_eq!(index_file.entry_count(), 1);
        assert_eq!(index_file.lookup(100).await, Some((100, 200)));
    }

    #[rstest]
    #[tokio::test]
    async fn test_add_entry_full(_setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, INDEX_ENTRY_SIZE, false)
            .await
            .unwrap();

        assert!(!index_file.is_full().await.unwrap());
        assert!(index_file.add_entry(100, 200).await.is_ok());
        assert!(index_file.is_full().await.unwrap());
        assert!(index_file.add_entry(300, 400).await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn test_lookup(_setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();

        index_file.add_entry(100, 200).await.unwrap();
        index_file.add_entry(300, 400).await.unwrap();

        assert_eq!(index_file.lookup(0).await, Some((0, 0)));
        assert_eq!(index_file.lookup(100).await, Some((100, 200)));
        assert_eq!(index_file.lookup(300).await, Some((300, 400)));
        assert_eq!(index_file.lookup(200).await, Some((100, 200)));
        assert_eq!(index_file.lookup(400).await, Some((300, 400)));
        assert_eq!(index_file.lookup(50).await, Some((0, 0)));
    }

    #[rstest]
    #[tokio::test]
    async fn test_resize(_setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, INDEX_ENTRY_SIZE * 2, false)
            .await
            .unwrap();

        index_file.add_entry(100, 200).await.unwrap();
        index_file.add_entry(300, 400).await.unwrap();
        assert!(index_file.is_full().await.unwrap());

        index_file.resize(INDEX_ENTRY_SIZE * 4).await.unwrap();
        assert!(!index_file.is_full().await.unwrap());

        index_file.add_entry(500, 600).await.unwrap();
        index_file.add_entry(700, 800).await.unwrap();

        assert_eq!(index_file.lookup(100).await, Some((100, 200)));
        assert_eq!(index_file.lookup(300).await, Some((300, 400)));
        assert_eq!(index_file.lookup(500).await, Some((500, 600)));
        assert_eq!(index_file.lookup(700).await, Some((700, 800)));

        assert!(index_file.is_full().await.unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn test_read_only_mode(_setup: ()) {
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
        assert!(matches!(
            *read_only_index.mode.read().await,
            IndexFileMode::ReadOnly(_)
        ));

        // Verify that we can read but not write
        assert_eq!(read_only_index.lookup(100).await, Some((100, 200)));
        assert!(read_only_index.add_entry(300, 400).await.is_err());
        assert!(read_only_index.resize(2048).await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn test_flush(_setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();

        index_file.add_entry(100, 200).await.unwrap();
        assert!(index_file.flush().await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_trim_to_valid_size(_setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();

        index_file.add_entry(100, 200).await.unwrap();
        index_file.add_entry(300, 400).await.unwrap();

        let original_size = index_file.file.metadata().await.unwrap().len();
        index_file.trim_to_valid_size().await.unwrap();
        let new_size = index_file.file.metadata().await.unwrap().len();

        assert!(new_size < original_size);
        assert_eq!(new_size, INDEX_ENTRY_SIZE as u64 * 2);
    }

    #[rstest]
    #[tokio::test]
    async fn test_multiple_lookups(_setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();

        for i in 0..100 {
            index_file.add_entry(i * 100, i * 200).await.unwrap();
        }

        assert_eq!(index_file.lookup(0).await, Some((0, 0)));
        for i in 1..100 {
            assert_eq!(index_file.lookup(i * 100).await, Some((i * 100, i * 200)));
        }

        // 测试查找位于条目之间的值
        for i in 0..99 {
            assert_eq!(
                index_file.lookup(i * 100 + 50).await,
                Some((i * 100, i * 200))
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_concurrent_access(_setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = Arc::new(IndexFile::new(&file_path, 1024, false).await.unwrap());

        let mut handles = vec![];
        for i in 0..10 {
            let index_file = Arc::clone(&index_file);
            handles.push(tokio::spawn(async move {
                index_file.add_entry(i * 100, i * 200).await.unwrap();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(index_file.entry_count(), 10);
        for i in 0..10 {
            assert_eq!(index_file.lookup(i * 100).await, Some((i * 100, i * 200)));
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_error_handling(_setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, INDEX_ENTRY_SIZE, false)
            .await
            .unwrap();

        index_file.add_entry(100, 200).await.unwrap();
        let result = index_file.add_entry(300, 400).await;
        assert!(matches!(result, Err(AppError::InvalidOperation(_))));

        let read_only_file = IndexFile::new(&file_path, INDEX_ENTRY_SIZE, true)
            .await
            .unwrap();
        let result = read_only_file.add_entry(500, 600).await;
        assert!(matches!(result, Err(AppError::InvalidOperation(_))));

        let result = read_only_file.resize(2048).await;
        assert!(matches!(result, Err(AppError::InvalidOperation(_))));

        let result = read_only_file.flush().await;
        assert!(matches!(result, Err(AppError::InvalidOperation(_))));

        let result = read_only_file.trim_to_valid_size().await;
        assert!(matches!(result, Err(AppError::InvalidOperation(_))));
    }

    #[rstest]
    #[tokio::test]
    async fn test_edge_cases(_setup: ()) {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024, false).await.unwrap();

        // 测试空索引文件
        assert_eq!(index_file.lookup(0).await, Some((0, 0)));
        assert_eq!(index_file.lookup(100).await, Some((0, 0)));

        // 添加一个条目
        index_file.add_entry(100, 200).await.unwrap();

        // 测试小于第一个条目的查找
        assert_eq!(index_file.lookup(0).await, Some((0, 0)));
        assert_eq!(index_file.lookup(50).await, Some((0, 0)));

        // 测试等于第一个条目的查找
        assert_eq!(index_file.lookup(100).await, Some((100, 200)));

        // 测试大于第一个条目的查找
        assert_eq!(index_file.lookup(150).await, Some((100, 200)));
    }
}
