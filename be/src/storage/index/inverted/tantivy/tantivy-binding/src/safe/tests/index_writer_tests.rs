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

use tempfile::TempDir;

use crate::safe::{IndexReaderWrapper, IndexWriterWrapper};

#[test]
fn round_trip_create_add_commit_query() {
    let tmp = TempDir::new().expect("tempdir");
    let field = "title";

    let mut w =
        IndexWriterWrapper::create(tmp.path(), field, "english").expect("create writer");
    w.add_strings_batch(&["hello world", "tantivy ffi", "starrocks integration"])
        .expect("add batch");
    w.commit().expect("commit");
    drop(w);

    let r = IndexReaderWrapper::load(tmp.path(), field, "english").expect("load reader");
    let hits = r.term_query("tantivy").expect("query");
    assert_eq!(hits, vec![1u32], "term 'tantivy' should match doc_id=1");

    let none = r.term_query("nonexistent_term").expect("query");
    assert!(none.is_empty(), "unknown term must yield no hits");
}

#[test]
fn create_fails_when_index_already_exists() {
    // Contract: tantivy::Index::create_in_dir refuses to overwrite an existing
    // index, so BE callers MUST clean the temp dir before init. This test
    // pins that contract so a future tantivy version change is caught.
    let tmp = TempDir::new().expect("tempdir");
    let mut w =
        IndexWriterWrapper::create(tmp.path(), "f", "english").expect("first create");
    w.commit().expect("commit");
    drop(w);

    let msg = match IndexWriterWrapper::create(tmp.path(), "f", "english") {
        Ok(_) => panic!("second create over existing index must fail"),
        Err(e) => e.to_string(),
    };
    assert!(
        msg.to_lowercase().contains("already") || msg.to_lowercase().contains("exists"),
        "expected IndexAlreadyExists-shaped error, got: {msg}"
    );
}

#[test]
fn commit_is_single_use() {
    // `commit()` now consumes the inner IndexWriter (so it can call
    // `wait_merging_threads()`), so any second commit / add after the first
    // commit must error out instead of panicking on `Option::unwrap` or
    // silently no-op'ing.
    let tmp = TempDir::new().expect("tempdir");
    let mut w =
        IndexWriterWrapper::create(tmp.path(), "f", "english").expect("create");
    w.add_strings_batch(&["a", "b"]).expect("add ok");
    w.commit().expect("first commit");

    let add_err = w
        .add_strings_batch(&["c"])
        .expect_err("add after commit must fail");
    assert!(
        add_err.to_string().to_lowercase().contains("committed"),
        "expected 'already committed' error from add, got: {add_err}"
    );

    let commit_err = w.commit().expect_err("second commit must fail");
    assert!(
        commit_err.to_string().to_lowercase().contains("committed"),
        "expected 'already committed' error from commit, got: {commit_err}"
    );
}

#[test]
fn null_placeholders_preserve_doc_id_alignment() {
    let tmp = TempDir::new().expect("tempdir");
    let mut w = IndexWriterWrapper::create(tmp.path(), "f", "english").expect("create");
    // Row 0 = "alpha", row 1 = NULL placeholder, row 2 = "alpha", row 3 = NULL.
    w.add_strings_batch(&["alpha", "", "alpha", ""]).expect("add");
    w.commit().expect("commit");
    drop(w);

    let r = IndexReaderWrapper::load(tmp.path(), "f", "english").expect("load");
    let mut hits = r.term_query("alpha").expect("query");
    hits.sort_unstable();
    assert_eq!(hits, vec![0u32, 2u32], "rows 0 and 2 should match");
}
