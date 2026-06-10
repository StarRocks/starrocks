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

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{DirectoryLock, FileHandle, Lock, OwnedBytes, WatchCallback, WatchHandle, WritePtr};
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
}

/// Metadata for one logical file inside the compound `.idx`.
#[derive(Debug, Clone)]
struct FileEntry {
    offset: u64,
    length: u64,
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
    files: Arc<HashMap<PathBuf, FileEntry>>,
    read_lock: Arc<Mutex<()>>,
}

impl fmt::Debug for PullDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PullDirectory")
            .field("files", &self.files.len())
            .finish()
    }
}

// SAFETY: The `handle` is a C++ `RandomAccessFile*` whose lifetime is managed
// by the C++ caller. The caller guarantees the pointer remains valid for the
// lifetime of the PullDirectory (and its clones). All reads through the handle
// are serialized by `read_lock`.
unsafe impl Send for PullDirectory {}
unsafe impl Sync for PullDirectory {}

impl PullDirectory {
    pub fn new(
        handle: *mut std::ffi::c_void,
        file_table: HashMap<PathBuf, (u64, u64)>,
    ) -> Self {
        let files: HashMap<PathBuf, FileEntry> = file_table
            .into_iter()
            .map(|(k, (offset, length))| (k, FileEntry { offset, length }))
            .collect();
        Self {
            handle,
            files: Arc::new(files),
            read_lock: Arc::new(Mutex::new(())),
        }
    }

    fn get_entry(&self, path: &Path) -> std::result::Result<&FileEntry, OpenReadError> {
        self.files.get(path).ok_or_else(|| {
            OpenReadError::FileDoesNotExist(path.to_path_buf())
        })
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
            base_offset: entry.offset,
            length: entry.length,
            read_lock: Arc::clone(&self.read_lock),
        };
        Ok(Arc::new(fh))
    }

    fn exists(&self, path: &Path) -> std::result::Result<bool, OpenReadError> {
        Ok(self.files.contains_key(path))
    }

    fn atomic_read(&self, path: &Path) -> std::result::Result<Vec<u8>, OpenReadError> {
        let entry = self.get_entry(path)?;
        let len = entry.length as usize;
        let mut buf = vec![0u8; len];
        let rc = unsafe {
            let _guard = self.read_lock.lock().unwrap();
            sr_random_access_read(self.handle, entry.offset, buf.as_mut_ptr(), len)
        };
        if rc != 0 {
            return Err(OpenReadError::IoError {
                io_error: Arc::new(io::Error::new(
                    io::ErrorKind::Other,
                    format!("sr_random_access_read failed for {:?}", path),
                )),
                filepath: path.to_path_buf(),
            });
        }
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
    base_offset: u64,
    length: u64,
    read_lock: Arc<Mutex<()>>,
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
        let mut buf = vec![0u8; read_len];
        let rc = unsafe {
            let _guard = self.read_lock.lock().unwrap();
            sr_random_access_read(self.handle, abs_offset, buf.as_mut_ptr(), read_len)
        };
        if rc != 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "sr_random_access_read failed: offset={}, len={}",
                    abs_offset, read_len
                ),
            ));
        }
        Ok(OwnedBytes::new(buf))
    }
}
