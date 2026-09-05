// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Read-only `tantivy::Directory` backed by an FFI callback into C++.
//!
//! Tantivy reads index files through its `Directory` trait. In StarRocks's
//! shared-data mode, index data lives on object storage and is accessed via
//! `RandomAccessFile` (which transparently integrates BlockCache). This
//! module bridges the two: each logical file inside the compound `.idx` is
//! represented by a `(base_offset, length)` pair, and reads are dispatched
//! through `sr_random_access_read` — a C function defined in BE that calls
//! `RandomAccessFile::read_at_fully`.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;
use std::ops::Deref;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ownedbytes::StableDeref;
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    DirectoryLock, FileHandle, Lock, OwnedBytes, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::{Directory, HasLen};

extern "C" {
    /// Defined in BE `random_access_bridge.cpp`. Reads `len` bytes at `offset`
    /// from the `RandomAccessFile*` wrapped by `handle`.
    /// Returns 0 on success, -1 on failure.
    fn sr_random_access_read(
        handle: *mut std::ffi::c_void,
        offset: u64,
        buf: *mut u8,
        len: usize,
    ) -> i32;

    /// Lease a buffer from the C++ process-local Tantivy read-buffer pool.
    fn sr_tantivy_read_buffer_acquire(
        pool: *mut std::ffi::c_void,
        requested_bytes: usize,
        capacity_bytes: *mut usize,
    ) -> *mut u8;

    /// Return a buffer after the last OwnedBytes view has been dropped.
    fn sr_tantivy_read_buffer_release(
        pool: *mut std::ffi::c_void,
        buffer: *mut u8,
        capacity_bytes: usize,
    );
}

/// Stable backing storage leased from the BE-side pool. OwnedBytes wraps this
/// object in an Arc, so Drop cannot run until every clone and slice is gone.
struct LeasedBuffer {
    pool: *mut std::ffi::c_void,
    buffer: NonNull<u8>,
    len: usize,
    capacity: usize,
}

unsafe impl Send for LeasedBuffer {}
unsafe impl Sync for LeasedBuffer {}
unsafe impl StableDeref for LeasedBuffer {}

impl Deref for LeasedBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // SAFETY: the C++ lease owns at least `capacity` bytes, the successful
        // read initialized `len <= capacity`, and the lease is not returned
        // until this backing object is dropped.
        unsafe { std::slice::from_raw_parts(self.buffer.as_ptr(), self.len) }
    }
}

impl Drop for LeasedBuffer {
    fn drop(&mut self) {
        // SAFETY: the pool outlives PullDirectory and all OwnedBytes created
        // from it; this lease is returned exactly once by Drop.
        unsafe { sr_tantivy_read_buffer_release(self.pool, self.buffer.as_ptr(), self.capacity) };
    }
}

/// Metadata for one logical file inside the compound `.idx`.
#[derive(Debug, Clone)]
struct FileEntry {
    offset: u64,
    length: u64,
}

/// Process-local counters shared by all clones and file handles belonging to
/// one PullDirectory. The values are cumulative and monotonic so the C++ side
/// can take deltas around a query without adding callbacks to the hot read path.
#[derive(Debug, Default)]
pub struct PullDirectoryStats {
    materialized_bytes: AtomicU64,
    read_time_ns: AtomicU64,
    read_lock_wait_time_ns: AtomicU64,
}

impl PullDirectoryStats {
    pub fn materialized_bytes(&self) -> u64 {
        self.materialized_bytes.load(Ordering::Relaxed)
    }

    pub fn read_time_ns(&self) -> u64 {
        self.read_time_ns.load(Ordering::Relaxed)
    }

    pub fn read_lock_wait_time_ns(&self) -> u64 {
        self.read_lock_wait_time_ns.load(Ordering::Relaxed)
    }
}

/// Read-only Directory implementation.
///
/// All write/delete/sync methods panic — this directory is only used on the
/// read path after the compound `.idx` has been finalized.
///
/// The `read_lock` serializes all FFI reads through the shared
/// `RandomAccessFile*`. In lake mode the underlying C++
/// `SeekableInputStream::read_at_fully` is seek+read (not pread),
/// so concurrent calls on the same handle corrupt data.
#[derive(Clone)]
pub struct PullDirectory {
    handle: *mut std::ffi::c_void,
    read_buffer_pool: *mut std::ffi::c_void,
    files: Arc<HashMap<PathBuf, FileEntry>>,
    read_lock: Arc<Mutex<()>>,
    stats: Arc<PullDirectoryStats>,
}

impl fmt::Debug for PullDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PullDirectory")
            .field("files", &self.files.len())
            .finish()
    }
}

// SAFETY: `handle` and `read_buffer_pool` point to C++ objects whose lifetimes
// are managed by the caller. The caller guarantees both pointers remain valid
// for the lifetime of the PullDirectory, its clones, and its OwnedBytes leases.
// All reads through the handle are serialized by `read_lock`.
unsafe impl Send for PullDirectory {}
unsafe impl Sync for PullDirectory {}

impl PullDirectory {
    pub fn new(
        handle: *mut std::ffi::c_void,
        read_buffer_pool: *mut std::ffi::c_void,
        file_table: HashMap<PathBuf, (u64, u64)>,
    ) -> Self {
        let files: HashMap<PathBuf, FileEntry> = file_table
            .into_iter()
            .map(|(k, (offset, length))| (k, FileEntry { offset, length }))
            .collect();
        Self {
            handle,
            read_buffer_pool,
            files: Arc::new(files),
            read_lock: Arc::new(Mutex::new(())),
            stats: Arc::new(PullDirectoryStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<PullDirectoryStats> {
        Arc::clone(&self.stats)
    }

    pub fn estimated_bytes(&self) -> u64 {
        let paths = self
            .files
            .keys()
            .map(|path| path.as_os_str().len())
            .sum::<usize>();
        (std::mem::size_of::<Self>()
            + std::mem::size_of::<HashMap<PathBuf, FileEntry>>()
            + self.files.len() * std::mem::size_of::<(PathBuf, FileEntry)>()
            + paths) as u64
    }

    /// Materializes each logical compound-index file exactly once, in physical
    /// offset order. The returned `OwnedBytes` values retain their BE buffer
    /// leases and can therefore back a zero-copy `ResidentDirectory`.
    pub fn materialize_files(&self) -> io::Result<HashMap<PathBuf, OwnedBytes>> {
        self.materialize_selected_files(&self.files.keys().cloned().collect())
    }

    /// Materializes only the requested logical files. This is used by the
    /// hybrid resident directory to keep high-value metadata in memory while
    /// large postings/positions files continue to use the PullDirectory.
    pub fn materialize_selected_files(
        &self,
        selected_paths: &HashSet<PathBuf>,
    ) -> io::Result<HashMap<PathBuf, OwnedBytes>> {
        let mut entries = self.files.iter().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|(_, entry)| entry.offset);

        let mut files = HashMap::with_capacity(selected_paths.len());
        for (path, entry) in entries {
            if !selected_paths.contains(path) {
                continue;
            }
            let handle = self.get_file_handle(path).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("failed to open resident file {path:?}: {error}"),
                )
            })?;
            let length = usize::try_from(entry.length).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("resident file {path:?} is too large: {}", entry.length),
                )
            })?;
            let bytes = handle.read_bytes(0..length)?;
            files.insert(path.clone(), bytes);
        }
        if files.len() != selected_paths.len() {
            let missing = selected_paths
                .iter()
                .filter(|path| !files.contains_key(*path))
                .collect::<Vec<_>>();
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("resident file table contains unknown paths: {missing:?}"),
            ));
        }
        Ok(files)
    }

    fn get_entry(&self, path: &Path) -> std::result::Result<&FileEntry, OpenReadError> {
        self.files
            .get(path)
            .ok_or_else(|| OpenReadError::FileDoesNotExist(path.to_path_buf()))
    }
}

impl Directory for PullDirectory {
    fn get_file_handle(
        &self,
        path: &Path,
    ) -> std::result::Result<Arc<dyn FileHandle>, OpenReadError> {
        let entry = self.get_entry(path)?;
        let fh = PullFileHandle {
            handle: self.handle,
            read_buffer_pool: self.read_buffer_pool,
            base_offset: entry.offset,
            length: entry.length,
            read_lock: Arc::clone(&self.read_lock),
            stats: Arc::clone(&self.stats),
        };
        Ok(Arc::new(fh))
    }

    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        Ok(self.files.contains_key(path))
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        let entry = self.get_entry(path)?;
        let len = entry.length as usize;
        // `sr_random_access_read` delegates to RandomAccessFile::read_at_fully,
        // so every byte is initialized on success. Reserving without setting
        // the length avoids clearing the buffer once here only to overwrite it
        // immediately in the C++ read path.
        let mut buf = Vec::<u8>::with_capacity(len);
        let wait_start = Instant::now();
        let guard = self.read_lock.lock().unwrap();
        self.stats
            .read_lock_wait_time_ns
            .fetch_add(wait_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let read_start = Instant::now();
        let rc = unsafe { sr_random_access_read(self.handle, entry.offset, buf.as_mut_ptr(), len) };
        self.stats
            .read_time_ns
            .fetch_add(read_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        drop(guard);
        if rc != 0 {
            return Err(OpenReadError::IoError {
                io_error: Arc::new(io::Error::new(
                    io::ErrorKind::Other,
                    format!("sr_random_access_read failed for {:?}", path),
                )),
                filepath: path.to_path_buf(),
            });
        }
        // SAFETY: read_at_fully returned success and initialized exactly `len`
        // bytes starting at `buf.as_mut_ptr()`.
        unsafe { buf.set_len(len) };
        self.stats
            .materialized_bytes
            .fetch_add(len as u64, Ordering::Relaxed);
        Ok(buf)
    }

    fn delete(&self, path: &Path) -> std::result::Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(io::Error::new(
                io::ErrorKind::Unsupported,
                "PullDirectory is read-only",
            )),
            filepath: path.to_path_buf(),
        })
    }

    fn open_write(&self, path: &Path) -> std::result::Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::IoError {
            io_error: Arc::new(io::Error::new(
                io::ErrorKind::Unsupported,
                "PullDirectory is read-only",
            )),
            filepath: path.to_path_buf(),
        })
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "PullDirectory is read-only",
        ))
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }

    fn acquire_lock(&self, _lock: &Lock) -> std::result::Result<DirectoryLock, LockError> {
        // Read-only directory — return a no-op lock.
        Ok(DirectoryLock::from(Box::new(())))
    }
}

/// File handle for a single logical file within the compound `.idx`.
struct PullFileHandle {
    handle: *mut std::ffi::c_void,
    read_buffer_pool: *mut std::ffi::c_void,
    base_offset: u64,
    length: u64,
    read_lock: Arc<Mutex<()>>,
    stats: Arc<PullDirectoryStats>,
}

unsafe impl Send for PullFileHandle {}
unsafe impl Sync for PullFileHandle {}

impl fmt::Debug for PullFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PullFileHandle")
            .field("base_offset", &self.base_offset)
            .field("length", &self.length)
            .finish()
    }
}

impl HasLen for PullFileHandle {
    fn len(&self) -> usize {
        self.length as usize
    }
}

/// Validate a `read_bytes` range against the logical file length.
///
/// Returns an `InvalidInput` error when the range is inverted or extends past
/// the file end. Tantivy expects directories to surface bad ranges as IO
/// errors rather than panic via subtraction underflow or read past the logical
/// file end.
pub(crate) fn validate_read_range(range: &Range<usize>, file_len: usize) -> io::Result<()> {
    if range.start > range.end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "PullFileHandle::read_bytes inverted range: start={}, end={}",
                range.start, range.end
            ),
        ));
    }
    if range.end > file_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "PullFileHandle::read_bytes out of bounds: end={}, file_len={}",
                range.end, file_len
            ),
        ));
    }
    Ok(())
}

impl FileHandle for PullFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        validate_read_range(&range, self.length as usize)?;
        let read_len = range.end - range.start;
        if read_len == 0 {
            return Ok(OwnedBytes::empty());
        }
        let abs_offset = self.base_offset + range.start as u64;
        let mut leased_capacity = 0usize;
        let leased_buffer = if self.read_buffer_pool.is_null() {
            None
        } else {
            // SAFETY: the pool pointer is retained by the owning C++ reader
            // resource, and `leased_capacity` is a valid out parameter.
            NonNull::new(unsafe {
                sr_tantivy_read_buffer_acquire(
                    self.read_buffer_pool,
                    read_len,
                    &mut leased_capacity,
                )
            })
        };
        let mut fallback = Vec::<u8>::new();
        let out = if let Some(buffer) = leased_buffer {
            buffer.as_ptr()
        } else {
            // Keep the fallback allocation uninitialized until read_at_fully
            // succeeds. This removes a redundant memset before the FFI call.
            fallback = Vec::<u8>::with_capacity(read_len);
            fallback.as_mut_ptr()
        };
        let wait_start = Instant::now();
        let guard = self.read_lock.lock().unwrap();
        self.stats
            .read_lock_wait_time_ns
            .fetch_add(wait_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        let read_start = Instant::now();
        let rc = unsafe { sr_random_access_read(self.handle, abs_offset, out, read_len) };
        self.stats
            .read_time_ns
            .fetch_add(read_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        drop(guard);
        if rc != 0 {
            if let Some(buffer) = leased_buffer {
                // Return a lease that was never published as OwnedBytes.
                unsafe {
                    sr_tantivy_read_buffer_release(
                        self.read_buffer_pool,
                        buffer.as_ptr(),
                        leased_capacity,
                    )
                };
            }
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "sr_random_access_read failed: offset={}, len={}",
                    abs_offset, read_len
                ),
            ));
        }
        self.stats
            .materialized_bytes
            .fetch_add(read_len as u64, Ordering::Relaxed);
        if let Some(buffer) = leased_buffer {
            debug_assert!(leased_capacity >= read_len);
            Ok(OwnedBytes::new(LeasedBuffer {
                pool: self.read_buffer_pool,
                buffer,
                len: read_len,
                capacity: leased_capacity,
            }))
        } else {
            // SAFETY: sr_random_access_read calls read_at_fully and therefore
            // initialized the entire requested range before returning success.
            unsafe { fallback.set_len(read_len) };
            Ok(OwnedBytes::new(fallback))
        }
    }
}
