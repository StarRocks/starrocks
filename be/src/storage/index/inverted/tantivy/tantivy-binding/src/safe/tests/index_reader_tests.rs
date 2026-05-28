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

use tantivy::directory::RamDirectory;
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::{Index, ReloadPolicy, TantivyDocument};
use tempfile::TempDir;

use crate::safe::{IndexReaderWrapper, IndexWriterWrapper};

fn build(values: &[&str]) -> TempDir {
    let tmp = TempDir::new().expect("tempdir");
    let mut w = IndexWriterWrapper::create(tmp.path(), "f", "english").expect("create");
    w.add_strings_batch(values).expect("add");
    w.commit().expect("commit");
    drop(w);
    tmp
}

#[test]
fn match_any() {
    let tmp = build(&["alpha beta", "gamma", "alpha gamma", "delta"]);
    let r = IndexReaderWrapper::load(tmp.path(), "f", "english").expect("load");
    let mut hits = r.match_any_query(&["beta", "delta"]).expect("query");
    hits.sort_unstable();
    assert_eq!(hits, vec![0u32, 3u32]);
}

#[test]
fn match_all() {
    let tmp = build(&["alpha beta gamma", "alpha gamma", "alpha beta"]);
    let r = IndexReaderWrapper::load(tmp.path(), "f", "english").expect("load");
    let hits = r.match_all_query(&["alpha", "beta"]).expect("query");
    assert_eq!(hits, vec![0u32, 2u32]);
}

#[test]
fn phrase_with_slop() {
    let tmp = build(&[
        "the quick brown fox",        // 0
        "the lazy brown dog",         // 1
        "quick fox jumps over",       // 2
    ]);
    let r = IndexReaderWrapper::load(tmp.path(), "f", "english").expect("load");
    // exact phrase
    assert_eq!(r.phrase_query(&["quick", "brown"], 0).expect("q"), vec![0u32]);
    // slop=1 lets "quick fox" match "quick brown fox" (gap of 1 between quick and fox)
    let mut hits = r.phrase_query(&["quick", "fox"], 1).expect("q");
    hits.sort_unstable();
    assert_eq!(hits, vec![0u32, 2u32]);
}

/// Round-trips through `open` with an in-memory `RamDirectory`. Validates
/// that the core constructor is genuinely Directory-agnostic (not just
/// MmapDirectory in disguise).
#[test]
fn open_works_with_ram_directory() {
    // Build an in-memory index using tantivy's native APIs (avoids touching
    // IndexWriterWrapper, which only targets local paths).
    let mut schema_builder = Schema::builder();
    let text_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(crate::safe::tokenizer::TOKENIZER_NAME)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    let text_field = schema_builder.add_text_field("f", text_options);
    let schema = schema_builder.build();

    let ram_dir = RamDirectory::create();
    let index = Index::create(ram_dir.clone(), schema.clone(), Default::default())
        .expect("create in ram");
    let analyzer = crate::safe::tokenizer::build("english").expect("english analyzer");
    index
        .tokenizers()
        .register(crate::safe::tokenizer::TOKENIZER_NAME, analyzer);

    let mut writer = index.writer_with_num_threads(1, 15_000_000).expect("writer");
    for v in ["alpha beta", "gamma", "alpha"] {
        let mut doc = TantivyDocument::default();
        doc.add_text(text_field, v);
        writer.add_document(doc).expect("add");
    }
    writer.commit().expect("commit");
    drop(writer);

    // Open via the unified core constructor with Manual reload (matches
    // compound-reader semantics; RamDirectory is read-only post-commit).
    let r = IndexReaderWrapper::open(ram_dir, "f", "english", ReloadPolicy::Manual)
        .expect("open ram");
    let mut hits = r.term_query("alpha").expect("query");
    hits.sort_unstable();
    assert_eq!(hits, vec![0u32, 2u32], "alpha appears in rows 0 and 2");
}
