use crossbeam::atomic::AtomicCell;
use memmap2::{Mmap, MmapMut, MmapOptions};
use std::{fs::File, path::Path};

use crate::{utils::MemoryUsage, AppError, AppResult};

const INDEX_ENTRY_SIZE: usize = 8;

/// 只读的索引文件，用于已经完成的旧索引文件
#[derive(Debug)]
pub struct ReadOnlyIndexFile {
    mmap: Mmap,
    entries: usize,
}

/// 可读写的索引文件，用于活动的索引文件
#[derive(Debug)]
pub struct WritableIndexFile {
    mmap: MmapMut,
    entries: AtomicCell<usize>,
    max_entry_count: usize,
}

impl ReadOnlyIndexFile {
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::options().read(true).open(path.as_ref())?;
        let len = file.metadata()?.len() as usize;
        let entries = len / INDEX_ENTRY_SIZE;

        let mmap = unsafe { MmapOptions::new().map(&file)? };

        Ok(Self { mmap, entries })
    }

    pub fn lookup(&self, target_offset: u32) -> Option<(u32, u32)> {
        binary_search_index(&self.mmap[..], self.entries, target_offset)
    }
}

impl WritableIndexFile {
    pub fn new<P: AsRef<Path>>(file_name: P, max_size: usize) -> std::io::Result<Self> {
        let file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(file_name.as_ref())?;

        file.set_len(max_size as u64)?;

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        Ok(Self {
            mmap,
            entries: AtomicCell::new(0),
            max_entry_count: max_size / INDEX_ENTRY_SIZE,
        })
    }

    pub fn add_entry(&mut self, relative_offset: u32, position: u32) -> AppResult<()> {
        let entries = self.entries.load();

        if entries + 1 > self.max_entry_count {
            return Err(AppError::InvalidOperation("index file is full".into()));
        }

        let offset = entries * INDEX_ENTRY_SIZE;
        self.mmap[offset..offset + 4].copy_from_slice(&relative_offset.to_be_bytes());
        self.mmap[offset + 4..offset + 8].copy_from_slice(&position.to_be_bytes());
        self.entries.fetch_add(1);
        Ok(())
    }

    pub fn lookup(&self, target_offset: u32) -> Option<(u32, u32)> {
        let entries = self.entries.load();
        binary_search_index(&self.mmap[..], entries, target_offset)
    }

    pub fn flush(&self) -> AppResult<()> {
        self.mmap
            .flush()
            .map_err(|e| AppError::DetailedIoError(format!("flush index file error: {}", e)))
    }

    /// 转换为只读索引文件
    pub fn into_readonly(self) -> std::io::Result<ReadOnlyIndexFile> {
        // 获取当前条目数
        let entries = self.entries.load();

        // 刷新数据
        self.mmap.flush()?;

        // 直接转换为只读映射
        let readonly_mmap = self.mmap.make_read_only()?;

        Ok(ReadOnlyIndexFile {
            mmap: readonly_mmap,
            entries,
        })
    }

    pub fn is_full(&self) -> bool {
        self.entries.load() + 1 > self.max_entry_count
    }
}

/// 在字节切片中进行二分查找
fn binary_search_index(slice: &[u8], entries: usize, target_offset: u32) -> Option<(u32, u32)> {
    if entries == 0 {
        return Some((0, 0));
    }

    // 读取第一个条目
    let first_entry = u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]);
    if target_offset < first_entry {
        return Some((0, 0));
    }

    // 二分查找
    let mut left = 0;
    let mut right = entries - 1;

    while left <= right {
        let mid = (left + right) / 2;
        let offset = mid * INDEX_ENTRY_SIZE;
        let entry_offset = u32::from_be_bytes([
            slice[offset],
            slice[offset + 1],
            slice[offset + 2],
            slice[offset + 3],
        ]);

        if entry_offset > target_offset {
            if mid == 0 {
                return None;
            }
            right = mid - 1;
        } else {
            if mid == entries - 1
                || u32::from_be_bytes([
                    slice[offset + INDEX_ENTRY_SIZE],
                    slice[offset + INDEX_ENTRY_SIZE + 1],
                    slice[offset + INDEX_ENTRY_SIZE + 2],
                    slice[offset + INDEX_ENTRY_SIZE + 3],
                ]) > target_offset
            {
                let position = u32::from_be_bytes([
                    slice[offset + 4],
                    slice[offset + 5],
                    slice[offset + 6],
                    slice[offset + 7],
                ]);
                return Some((entry_offset, position));
            }
            left = mid + 1;
        }
    }

    None
}

impl MemoryUsage for ReadOnlyIndexFile {
    fn memory_usage(&self) -> usize {
        // 基础结构体大小
        let struct_size = std::mem::size_of::<Self>();

        // mmap 映射的内存大小
        // 注意：这个值表示映射的文件大小，但实际内存使用可能更少
        // 因为操作系统可能只加载了部分页面到物理内存中
        let mmap_size = self.mmap.len();

        // entries 字段的大小
        let entries_size = std::mem::size_of::<usize>();

        struct_size + mmap_size + entries_size
    }
}

impl MemoryUsage for WritableIndexFile {
    fn memory_usage(&self) -> usize {
        // 基础结构体大小
        let struct_size = std::mem::size_of::<Self>();

        // mmap 映射的内存大小
        // let mmap_size = parse_smaps_for_rss;

        // AtomicCell<usize> 的大小
        let entries_size = std::mem::size_of::<AtomicCell<usize>>();

        // max_entry_count 的大小
        let max_entry_count_size = std::mem::size_of::<usize>();

        struct_size + entries_size + max_entry_count_size
    }
}
