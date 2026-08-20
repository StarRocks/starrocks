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

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use tantivy::directory::{OwnedBytes, RamDirectory};
use tantivy::{Directory, ReloadPolicy};
use tempfile::TempDir;

use crate::safe::resident_directory::ResidentDirectory;
use crate::safe::{IndexReaderWrapper, IndexWriterWrapper};

fn directory_with_file(path: &str, contents: &[u8]) -> ResidentDirectory {
    let mut files = HashMap::new();
    files.insert(PathBuf::from(path), OwnedBytes::new(contents.to_vec()));
    ResidentDirectory::new(files)
}

#[test]
fn read_bytes_returns_zero_copy_slice() {
    let directory = directory_with_file("segment.term", b"0123456789");
    let handle = directory
        .get_file_handle(Path::new("segment.term"))
        .unwrap();
    let full = handle.read_bytes(0..10).unwrap();
    let slice = handle.read_bytes(3..7).unwrap();

    assert_eq!(slice.as_slice(), b"3456");
    assert_eq!(
        unsafe { full.as_slice().as_ptr().add(3) },
        slice.as_slice().as_ptr()
    );
    assert_eq!(2, directory.stats().read_count());
    assert_eq!(14, directory.stats().read_bytes());
}

#[test]
fn empty_range_and_empty_file_are_supported() {
    let directory = directory_with_file("empty", b"");
    let handle = directory.get_file_handle(Path::new("empty")).unwrap();
    assert!(handle.read_bytes(0..0).unwrap().is_empty());
    assert_eq!(1, directory.stats().read_count());
    assert_eq!(0, directory.stats().read_bytes());
}

#[test]
fn invalid_ranges_are_rejected() {
    let directory = directory_with_file("segment.pos", b"position");
    let handle = directory.get_file_handle(Path::new("segment.pos")).unwrap();

    let inverted = handle.read_bytes(5..3).unwrap_err();
    assert_eq!(ErrorKind::InvalidInput, inverted.kind());
    assert!(inverted.to_string().contains("inverted range"));

    let past_end = handle.read_bytes(0..9).unwrap_err();
    assert_eq!(ErrorKind::InvalidInput, past_end.kind());
    assert!(past_end.to_string().contains("out of bounds"));
}

#[test]
fn atomic_read_and_exists_use_resident_content() {
    let directory = directory_with_file("meta.json", br#"{"segments":[]}"#);
    assert!(directory.exists(Path::new("meta.json")).unwrap());
    assert!(!directory.exists(Path::new("missing")).unwrap());
    assert_eq!(
        br#"{"segments":[]}"#,
        directory
            .atomic_read(Path::new("meta.json"))
            .unwrap()
            .as_slice()
    );
    assert!(directory.atomic_read(Path::new("missing")).is_err());
}

#[test]
fn resident_bytes_and_estimate_include_file_contents() {
    let mut files = HashMap::new();
    files.insert(PathBuf::from("a"), OwnedBytes::new(vec![1u8; 7]));
    files.insert(PathBuf::from("longer-name"), OwnedBytes::new(vec![2u8; 11]));
    let directory = ResidentDirectory::new(files);

    assert_eq!(18, directory.resident_bytes());
    assert!(directory.estimated_bytes() > directory.resident_bytes());
}

#[test]
fn non_resident_files_fall_back_to_pull_layer() {
    let fallback = RamDirectory::create();
    fallback
        .atomic_write(Path::new("postings.idx"), b"pulled postings")
        .unwrap();
    let directory = ResidentDirectory::with_fallback(
        HashMap::from([(
            PathBuf::from("segment.term"),
            OwnedBytes::new(b"resident terms".to_vec()),
        )]),
        fallback,
    );

    assert_eq!(
        b"resident terms",
        directory
            .atomic_read(Path::new("segment.term"))
            .unwrap()
            .as_slice()
    );
    assert_eq!(
        b"pulled postings",
        directory
            .atomic_read(Path::new("postings.idx"))
            .unwrap()
            .as_slice()
    );
    assert!(directory.exists(Path::new("postings.idx")).unwrap());
    assert_eq!(0, directory.stats().read_count());
}

#[test]
fn tantivy_reader_queries_resident_files() {
    let temporary_directory = TempDir::new().unwrap();
    let mut writer = IndexWriterWrapper::create(
        temporary_directory.path(),
        "title",
        "english",
        true,
        true,
        0,
        0,
        "default",
    )
    .unwrap();
    writer
        .add_strings_batch(&["resident directory", "other content"])
        .unwrap();
    writer.commit().unwrap();
    drop(writer);

    let mut files = HashMap::new();
    for entry in std::fs::read_dir(temporary_directory.path()).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            files.insert(
                PathBuf::from(entry.file_name()),
                OwnedBytes::new(std::fs::read(entry.path()).unwrap()),
            );
        }
    }
    let directory = ResidentDirectory::new(files);
    let reader =
        IndexReaderWrapper::open(directory, "title", "english", ReloadPolicy::Manual).unwrap();
    reader.prepare_for_search().unwrap();

    assert_eq!(vec![0], reader.term_query("resident").unwrap());
}
