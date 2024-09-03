use memmap2::{MmapMut, MmapOptions};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{self, Result};
use tokio::sync::RwLock;
use tracing::debug;
use crate::AppResult;

const INDEX_ENTRY_SIZE: usize = 8; // 4 bytes for relative offset, 4 bytes for position

#[derive(Debug)]
pub struct IndexFile {
    mmap: RwLock<MmapMut>,
    entries: usize,
    file: File,
}

impl IndexFile {
    pub async fn new<P: AsRef<Path>>(path: P, index_file_size:usize) -> AppResult<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await?;

        file.set_len(index_file_size as u64).await?;

        // 注意：mmap 操作仍然是同步的，因为它涉及系统调用
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        debug!("create Index file path: {}", &path.as_ref().to_string_lossy());

        Ok(IndexFile {
            mmap:RwLock::new(mmap),
            entries: 0,
            file,
        })
    }
  pub async fn resize(&mut self, new_size: u64) -> AppResult<()> {
      let mut lock = self.mmap.write().await;
      self.file.set_len(new_size).await?;
      let new_mmap = unsafe { MmapOptions::new().map_mut(&self.file)? };
      *lock = new_mmap;
      let max_entries = (new_size as usize) / INDEX_ENTRY_SIZE;
      if max_entries < self.entries {
          self.entries = max_entries;
      }
      Ok(())
  }
    pub async fn add_entry(&mut self, relative_offset: u32, position: u32) -> Result<()> {
        let mut lock = self.mmap.write().await;
        if (self.entries + 1) * INDEX_ENTRY_SIZE > lock.len() {
            return Err(io::Error::new(io::ErrorKind::Other, "Index file is full"));
        }

        let offset = self.entries * INDEX_ENTRY_SIZE;
        lock[offset..offset + 4].copy_from_slice(&relative_offset.to_be_bytes());
        lock[offset + 4..offset + 8].copy_from_slice(&position.to_be_bytes());
        self.entries += 1;
        Ok(())
    }


    /// Looks up the position of a log entry in the index file by its relative offset.
    /// 不一定返回完全相等的值，可能返回第一个小于目标的最大值
    ///
    /// # Arguments
    /// * `target_offset` - The relative offset of the log entry to look up.
    ///
    /// # Returns
    /// The position of the log entry in the index file, or `None` if the entry is not found.
    pub async fn lookup(&self, target_offset: u32) -> Option<u32> {

        let mut lock = self.mmap.read().await;
        if self.entries==0{
            return None;
        }
        let mut left = 0;
        let mut right = self.entries - 1;

        while left <= right {
            let mid = (left + right) / 2;
            let offset = mid * INDEX_ENTRY_SIZE;
            let entry_offset = u32::from_be_bytes([
                lock[offset],
                lock[offset + 1],
                lock[offset + 2],
                lock[offset + 3],
            ]);

            if entry_offset == target_offset {
                return Some(u32::from_be_bytes([
                    lock[offset + 4],
                    lock[offset + 5],
                    lock[offset + 6],
                    lock[offset + 7],
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
            lock[offset + 4],
            lock[offset + 5],
            lock[offset + 6],
            lock[offset + 7],
        ]))
    }

    pub async fn flush(&self) -> Result<()> {
        let mut lock = self.mmap.write().await;
        lock.flush()
    }

    pub async fn is_full(&self) -> bool {
        let lock = self.mmap.read().await;
        self.entries * INDEX_ENTRY_SIZE >= lock.len()
    }
    pub fn entry_count(&self) -> usize {
        self.entries
    }


}

mod tests {
    use rstest::{fixture, rstest};
    use super::*;
    use tempfile::tempdir;
    use crate::service::setup_tracing;

    #[fixture]
    #[once]
    fn setup(){
        setup_tracing().expect("failed to setup tracing");
    }


    #[rstest]
    #[tokio::test]
    async fn test_add_entry_success() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let mut index_file = IndexFile::new(&file_path, 1024).await.unwrap();

        assert!(index_file.add_entry(100, 200).await.is_ok());
        assert_eq!(index_file.entries, 1);
    }

    #[rstest]
    #[tokio::test]
    async fn test_add_entry_full() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let mut index_file = IndexFile::new(&file_path, INDEX_ENTRY_SIZE).await.unwrap();

        assert!(index_file.add_entry(100, 200).await.is_ok());
        assert!(index_file.add_entry(300, 400).await.is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn test_lookup_existing_entry() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let mut index_file = IndexFile::new(&file_path, 1024).await.unwrap();

        index_file.add_entry(100, 200).await.unwrap();
        index_file.add_entry(300, 400).await.unwrap();

        assert_eq!(index_file.lookup(100).await, Some(200));
        assert_eq!(index_file.lookup(300).await, Some(400));
    }

    #[rstest]
    #[tokio::test]
    async fn test_lookup_non_existing_entry() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let mut index_file = IndexFile::new(&file_path, 1024).await.unwrap();

        index_file.add_entry(100, 200).await.unwrap();
        index_file.add_entry(300, 400).await.unwrap();

        assert_eq!(index_file.lookup(200).await, Some(200));
        assert_eq!(index_file.lookup(400).await, Some(400));
    }

    #[rstest]
    #[tokio::test]
    async fn test_lookup_empty_index() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024).await.unwrap();
        assert_eq!(index_file.lookup(100).await, None);
    }

    #[rstest]
    #[tokio::test]
    async fn test_flush() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let index_file = IndexFile::new(&file_path, 1024).await.unwrap();
        assert!(index_file.flush().await.is_ok());
    }

    #[rstest]
    #[tokio::test]
    async fn test_is_full() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let mut index_file = IndexFile::new(&file_path, INDEX_ENTRY_SIZE * 2).await.unwrap();

        assert!(!index_file.is_full().await);
        index_file.add_entry(100, 200).await.unwrap();
        assert!(!index_file.is_full().await);
        index_file.add_entry(300, 400).await.unwrap();
        assert!(index_file.is_full().await);
    }
    #[rstest]
    #[tokio::test]
    async fn test_resize() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("index.idx");
        let mut index_file = IndexFile::new(&file_path, INDEX_ENTRY_SIZE * 2).await.unwrap();

        // Add entries to fill the index
        index_file.add_entry(100, 200).await.unwrap();
        index_file.add_entry(300, 400).await.unwrap();
        assert!(index_file.is_full().await);

        // Resize the index
        index_file.resize((INDEX_ENTRY_SIZE * 4) as u64).await.unwrap();

        // Check if the index is no longer full
        assert!(!index_file.is_full().await);

        // Add more entries
        index_file.add_entry(500, 600).await.unwrap();
        index_file.add_entry(700, 800).await.unwrap();

        // Verify all entries are still accessible
        assert_eq!(index_file.lookup(100).await, Some(200));
        assert_eq!(index_file.lookup(300).await, Some(400));
        assert_eq!(index_file.lookup(500).await, Some(600));
        assert_eq!(index_file.lookup(700).await, Some(800));

        // Verify the index is full again
        assert!(index_file.is_full().await);
    }

}

