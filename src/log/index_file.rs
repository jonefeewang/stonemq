use memmap2::{Mmap, MmapMut, MmapOptions};
use std::{
    fs::File,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};
use tracing::trace;

use crate::{AppError, AppResult};

const INDEX_ENTRY_SIZE: usize = 8;

/// A read-only index file that stores offset mappings.
/// Used for completed old index files that no longer need to be modified.
#[derive(Debug)]
pub struct ReadOnlyIndexFile {
    mmap: Mmap,
    entries: usize,
}

/// A writable index file that stores offset mappings.
/// Used for active index files that need to be modified.
#[derive(Debug)]
pub struct WritableIndexFile {
    file: File,
    mmap: MmapMut,
    entries: AtomicUsize,
    max_entry_count: usize,
}

impl ReadOnlyIndexFile {
    /// Creates a new ReadOnlyIndexFile from the given path.
    ///
    /// # Arguments
    /// * `path` - The path to the index file
    ///
    /// # Returns
    /// * `std::io::Result<Self>` - The created ReadOnlyIndexFile or an error
    pub fn new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::options().read(true).open(path.as_ref())?;
        let len = file.metadata()?.len() as usize;
        let entries = len / INDEX_ENTRY_SIZE;

        let mmap = unsafe { MmapOptions::new().map(&file)? };
        trace!("open read only index file: {:?}/{}", path.as_ref(), entries);

        Ok(Self { mmap, entries })
    }

    /// Looks up an entry by target offset using binary search.
    ///
    /// # Arguments
    /// * `target_offset` - The offset to look up
    ///
    /// # Returns
    /// * `Option<(u32, u32)>` - Returns (offset, position) if found, None if not found
    pub fn lookup(&self, target_offset: u32) -> Option<(u32, u32)> {
        let search_result = binary_search_index(&self.mmap[..], self.entries, target_offset);
        trace!(
            "read only index file search_result: {:?}/{}",
            search_result,
            target_offset
        );
        search_result
    }
}

impl WritableIndexFile {
    /// Creates a new WritableIndexFile with the specified maximum size.
    ///
    /// # Arguments
    /// * `file_name` - The path where the index file should be created
    /// * `max_size` - The maximum size in bytes for this index file
    ///
    /// # Returns
    /// * `std::io::Result<Self>` - The created WritableIndexFile or an error
    pub fn new<P: AsRef<Path>>(file_name: P, max_size: usize) -> std::io::Result<Self> {
        let file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(file_name.as_ref())?;

        // todo: In the event of an ungraceful shutdown, the file is not truncated, resulting in an incorrect file length.
        let entries = file.metadata()?.len() as usize / INDEX_ENTRY_SIZE;
        trace!(
            "open writable index file: {:?}/{}",
            file_name.as_ref(),
            entries
        );
        file.set_len(max_size as u64)?;

        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        Ok(Self {
            file,
            mmap,
            entries: AtomicUsize::new(entries),
            max_entry_count: max_size / INDEX_ENTRY_SIZE,
        })
    }

    /// Adds a new entry to the index file.
    ///
    /// # Arguments
    /// * `relative_offset` - The relative offset for this entry
    /// * `position` - The position value for this entry
    ///
    /// # Returns
    /// * `AppResult<()>` - Ok if successful, Error if the index is full
    pub fn add_entry(&mut self, relative_offset: u32, position: u32) -> AppResult<()> {
        let entries = self.entries.load(Ordering::Acquire);

        if entries + 1 > self.max_entry_count {
            return Err(AppError::InvalidOperation("index file is full".into()));
        }

        let offset = entries * INDEX_ENTRY_SIZE;
        self.mmap[offset..offset + 4].copy_from_slice(&relative_offset.to_be_bytes());
        self.mmap[offset + 4..offset + 8].copy_from_slice(&position.to_be_bytes());
        self.entries.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Looks up an entry by target offset using binary search.
    ///
    /// # Arguments
    /// * `target_offset` - The offset to look up
    ///
    /// # Returns
    /// * `Option<(u32, u32)>` - Returns (offset, position) if found, None if not found
    pub fn lookup(&self, target_offset: u32) -> Option<(u32, u32)> {
        let entries = self.entries.load(Ordering::Acquire);
        let search_result = binary_search_index(&self.mmap[..], entries, target_offset);
        trace!(
            "writable index file search_result: {:?}/{}",
            search_result,
            target_offset
        );
        search_result
    }

    /// Flushes any pending changes to disk.
    ///
    /// # Returns
    /// * `AppResult<()>` - Ok if successful, Error if flush fails
    pub fn flush(&self) -> AppResult<()> {
        self.mmap
            .flush()
            .map_err(|e| AppError::DetailedIoError(format!("flush index file error: {}", e)))
    }

    /// Closes the index file, flushing changes and truncating to the actual size used.
    ///
    /// # Returns
    /// * `AppResult<()>` - Ok if successful, Error if close operations fail
    pub fn close(&mut self) -> AppResult<()> {
        self.mmap.flush()?;
        self.file
            .set_len((self.entries.load(Ordering::Acquire) * INDEX_ENTRY_SIZE) as u64)?;
        Ok(())
    }

    /// Converts this WritableIndexFile into a ReadOnlyIndexFile.
    ///
    /// # Returns
    /// * `std::io::Result<ReadOnlyIndexFile>` - The converted ReadOnlyIndexFile or an error
    pub fn into_readonly(self) -> std::io::Result<ReadOnlyIndexFile> {
        let entries = self.entries.load(Ordering::Acquire);

        self.mmap.flush()?;
        let readonly_mmap = self.mmap.make_read_only()?;
        self.file.set_len((entries * INDEX_ENTRY_SIZE) as u64)?;

        Ok(ReadOnlyIndexFile {
            mmap: readonly_mmap,
            entries,
        })
    }

    /// Checks if the index file is full.
    ///
    /// # Returns
    /// * `bool` - true if the index is full, false otherwise
    pub fn is_full(&self) -> bool {
        self.entries.load(Ordering::Acquire) + 1 > self.max_entry_count
    }
}

/// Performs a binary search in the index file's byte slice to find a target offset.
///
/// # Arguments
/// * `slice` - The byte slice to search in
/// * `entries` - The number of entries in the slice
/// * `target_offset` - The offset to search for
///
/// # Returns
/// * `Option<(u32, u32)>` - Returns (offset, position) if found, None if not found
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
