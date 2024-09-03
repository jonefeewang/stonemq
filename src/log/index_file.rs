use memmap2::{MmapMut, MmapOptions};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{self, Result};

const INDEX_ENTRY_SIZE: usize = 8; // 4 bytes for relative offset, 4 bytes for position
const INDEX_FILE_SIZE: usize = 10 * 1024 * 1024; // 10MB, you can adjust this

pub struct IndexFile {
    mmap: MmapMut,
    entries: usize,
    file: File,
}

impl IndexFile {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .await?;

        file.set_len(INDEX_FILE_SIZE as u64).await?;

        // 注意：mmap 操作仍然是同步的，因为它涉及系统调用
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        Ok(IndexFile {
            mmap,
            entries: 0,
            file,
        })
    }
    pub async fn resize(&mut self, new_size: u64) -> Result<()> {
        self.file.set_len(new_size).await?;
        self.mmap = unsafe { MmapOptions::new().map_mut(&self.file)? };

        let max_entries = (new_size as usize) / INDEX_ENTRY_SIZE;
        if max_entries < self.entries {
            self.entries = max_entries;
        }

        Ok(())
    }

    pub fn add_entry(&mut self, relative_offset: u32, position: u32) -> Result<()> {
        if (self.entries + 1) * INDEX_ENTRY_SIZE > self.mmap.len() {
            return Err(io::Error::new(io::ErrorKind::Other, "Index file is full"));
        }

        let offset = self.entries * INDEX_ENTRY_SIZE;
        self.mmap[offset..offset + 4].copy_from_slice(&relative_offset.to_be_bytes());
        self.mmap[offset + 4..offset + 8].copy_from_slice(&position.to_be_bytes());

        self.entries += 1;
        Ok(())
    }

    pub fn lookup(&self, target_offset: u32) -> Option<u32> {
        let mut left = 0;
        let mut right = self.entries - 1;

        while left <= right {
            let mid = (left + right) / 2;
            let offset = mid * INDEX_ENTRY_SIZE;
            let entry_offset = u32::from_be_bytes([
                self.mmap[offset],
                self.mmap[offset + 1],
                self.mmap[offset + 2],
                self.mmap[offset + 3],
            ]);

            if entry_offset == target_offset {
                return Some(u32::from_be_bytes([
                    self.mmap[offset + 4],
                    self.mmap[offset + 5],
                    self.mmap[offset + 6],
                    self.mmap[offset + 7],
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
            self.mmap[offset + 4],
            self.mmap[offset + 5],
            self.mmap[offset + 6],
            self.mmap[offset + 7],
        ]))
    }

    pub fn flush(&self) -> Result<()> {
        self.mmap.flush()
    }
}
