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

//! Read-only Tantivy directory backed by immutable process-resident bytes.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    DirectoryLock, FileHandle, Lock, OwnedBytes, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::{Directory, HasLen};

#[derive(Debug, Default)]
pub struct ResidentDirectoryStats {
    read_count: AtomicU64,
    read_bytes: AtomicU64,
}

impl ResidentDirectoryStats {
    pub fn read_count(&self) -> u64 {
        self.read_count.load(Ordering::Relaxed)
    }

    pub fn read_bytes(&self) -> u64 {
        self.read_bytes.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct ResidentDirectory {
    files: Arc<HashMap<PathBuf, OwnedBytes>>,
    fallback: Option<Arc<dyn Directory>>,
    stats: Arc<ResidentDirectoryStats>,
    resident_bytes: u64,
}

impl fmt::Debug for ResidentDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResidentDirectory")
            .field("files", &self.files.len())
            .field("has_fallback", &self.fallback.is_some())
            .field("resident_bytes", &self.resident_bytes)
            .finish()
    }
}

impl ResidentDirectory {
    pub fn new(files: HashMap<PathBuf, OwnedBytes>) -> Self {
        let resident_bytes = files.values().map(|bytes| bytes.len() as u64).sum();
        Self {
            files: Arc::new(files),
            fallback: None,
            stats: Arc::new(ResidentDirectoryStats::default()),
            resident_bytes,
        }
    }

    pub fn with_fallback<D: Directory>(files: HashMap<PathBuf, OwnedBytes>, fallback: D) -> Self {
        let mut directory = Self::new(files);
        directory.fallback = Some(Arc::new(fallback));
        directory
    }

    pub fn stats(&self) -> Arc<ResidentDirectoryStats> {
        Arc::clone(&self.stats)
    }

    pub fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }

    pub fn estimated_bytes(&self) -> u64 {
        let paths = self
            .files
            .keys()
            .map(|path| path.as_os_str().len())
            .sum::<usize>();
        self.resident_bytes.saturating_add(
            (std::mem::size_of::<Self>()
                + std::mem::size_of::<HashMap<PathBuf, OwnedBytes>>()
                + self.files.len() * std::mem::size_of::<(PathBuf, OwnedBytes)>()
                + paths) as u64,
        )
    }
}

impl Directory for ResidentDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        if let Some(bytes) = self.files.get(path) {
            return Ok(Arc::new(ResidentFileHandle {
                bytes: bytes.clone(),
                stats: Arc::clone(&self.stats),
            }));
        }
        match &self.fallback {
            Some(fallback) => fallback.get_file_handle(path),
            None => Err(OpenReadError::FileDoesNotExist(path.to_path_buf())),
        }
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        if self.files.contains_key(path) {
            return Ok(true);
        }
        self.fallback
            .as_ref()
            .map_or(Ok(false), |fallback| fallback.exists(path))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        if let Some(bytes) = self.files.get(path) {
            return Ok(bytes.as_slice().to_vec());
        }
        match &self.fallback {
            Some(fallback) => fallback.atomic_read(path),
            None => Err(OpenReadError::FileDoesNotExist(path.to_path_buf())),
        }
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        Err(DeleteError::IoError {
            io_error: Arc::new(io::Error::new(
                io::ErrorKind::Unsupported,
                "ResidentDirectory is read-only",
            )),
            filepath: path.to_path_buf(),
        })
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        Err(OpenWriteError::IoError {
            io_error: Arc::new(io::Error::new(
                io::ErrorKind::Unsupported,
                "ResidentDirectory is read-only",
            )),
            filepath: path.to_path_buf(),
        })
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "ResidentDirectory is read-only",
        ))
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }

    fn watch(&self, _watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(WatchHandle::empty())
    }

    fn acquire_lock(&self, _lock: &Lock) -> Result<DirectoryLock, LockError> {
        Ok(DirectoryLock::from(Box::new(())))
    }
}

struct ResidentFileHandle {
    bytes: OwnedBytes,
    stats: Arc<ResidentDirectoryStats>,
}

impl fmt::Debug for ResidentFileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResidentFileHandle")
            .field("length", &self.bytes.len())
            .finish()
    }
}

impl HasLen for ResidentFileHandle {
    fn len(&self) -> usize {
        self.bytes.len()
    }
}

fn validate_read_range(range: &Range<usize>, file_len: usize) -> io::Result<()> {
    if range.start > range.end {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "ResidentFileHandle::read_bytes inverted range: start={}, end={}",
                range.start, range.end
            ),
        ));
    }
    if range.end > file_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "ResidentFileHandle::read_bytes out of bounds: end={}, file_len={}",
                range.end, file_len
            ),
        ));
    }
    Ok(())
}

impl FileHandle for ResidentFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        validate_read_range(&range, self.bytes.len())?;
        let read_len = range.end - range.start;
        self.stats.read_count.fetch_add(1, Ordering::Relaxed);
        self.stats
            .read_bytes
            .fetch_add(read_len as u64, Ordering::Relaxed);
        if read_len == 0 {
            return Ok(OwnedBytes::empty());
        }
        Ok(self.bytes.slice(range))
    }
}
