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

//! Safe Rust wrapper around `tantivy::IndexWriter`.
//!
//! Schema layout: a single TEXT field. tantivy DocId is used as the BE row id
//! directly — the writer therefore MUST add documents in row order, including
//! empty-string placeholder docs for null rows. The BE side maintains a
//! separate Roaring null bitmap and serializes it as a `_starrocks_null_bitmap`
//! sidecar before pack time so the global doc-id alignment survives across
//! the temp-dir → .idx compound packing.

use std::path::Path;

use tantivy::schema::{FAST, IndexRecordOption, Field, Schema, TextFieldIndexing, TextOptions};
use tantivy::{Index, IndexWriter, TantivyDocument};

use crate::error::{Result, TantivyBindingError};
use crate::safe::tokenizer::{self, TOKENIZER_NAME};

/// Memory budget handed to tantivy's IndexWriter for one BE segment. tantivy
/// will spill to internal segments if buffered beyond this; the reader path
/// handles multi-segment via per-segment doc-id offsets.
const WRITER_MEMORY_BUDGET_BYTES: usize = 50 * 1024 * 1024;

/// Hardcode the tantivy writer to a single indexing worker thread for now.
/// Write concurrency is controlled by StarRocks-side parameters instead, so
/// we can avoid introducing the extra complexity of multi-threaded writes in
/// this binding layer. If higher per-writer throughput is needed in the
/// future, make this value configurable.
const WRITER_NUM_THREADS: usize = 1;

pub struct IndexWriterWrapper {
    // Held in an Option because `commit()` must take ownership of the underlying
    // `IndexWriter` to call `wait_merging_threads()` (which consumes `self`).
    // The wrapper is single-use: once `commit()` returns, this field is `None`
    // and any further `add_strings_batch` / `commit` call errors out.
    pub(crate) writer: Option<IndexWriter>,
    pub(crate) text_field: Field,
    // Explicit BE row id (segment-local insertion order). tantivy may split docs
    // across internal segments whose reader order is not insertion order, so the
    // row id must be stored, not inferred from segment offsets.
    row_id_field: Field,
    next_row_id: u64,
}

impl IndexWriterWrapper {
    /// Create a fresh tantivy index at `path` (must be empty/non-existent)
    /// with one TEXT field named `field_name` and the analyzer chain identified by `tokenizer_name`.
    pub fn create(path: &Path, field_name: &str, tokenizer_name: &str) -> Result<Self> {
        std::fs::create_dir_all(path)?;

        let analyzer = tokenizer::build(tokenizer_name)?;

        let mut schema_builder = Schema::builder();
        let text_options = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(TOKENIZER_NAME)
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        );
        let text_field = schema_builder.add_text_field(field_name, text_options);
        let row_id_field = schema_builder.add_u64_field("row_id", FAST);
        let schema = schema_builder.build();

        let index = Index::create_in_dir(path, schema)?;
        index.tokenizers().register(TOKENIZER_NAME, analyzer);
        let writer: IndexWriter =
            index.writer_with_num_threads(WRITER_NUM_THREADS, WRITER_MEMORY_BUDGET_BYTES)?;

        Ok(Self { writer: Some(writer), text_field, row_id_field, next_row_id: 0 })
    }

    /// Append `values.len()` documents in order. Use `""` (empty string) for rows that are null on the BE side;
    /// the BE caller is responsible for tracking a separate null bitmap so empty-string and null can be
    /// disambiguated at query time.
    pub fn add_strings_batch(&mut self, values: &[&str]) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| TantivyBindingError::Internal("writer already committed".to_string()))?;
        for v in values {
            let mut doc = TantivyDocument::default();
            doc.add_text(self.text_field, v);
            doc.add_u64(self.row_id_field, self.next_row_id);
            self.next_row_id += 1;
            writer.add_document(doc)?;
        }
        Ok(())
    }

    /// Commit pending docs to disk AND block until every in-flight merge spawned
    /// by tantivy's internal `LogMergePolicy` has fully completed.
    ///
    /// Why both: `IndexWriter::commit()` only flushes the indexing pipeline; it
    /// schedules merges into a background rayon pool and returns. tantivy's
    /// `IndexWriter::Drop` does NOT wait for those merge threads. If we let the
    /// writer drop while merges are in flight, the rayon workers keep running,
    /// open new tempfiles via `atomic_write` (`tempfile_in(parent_path)`) inside
    /// our `_temp_dir`, and race against the BE-side `remove_all(parent)` that
    /// runs right after `pack` consumes the entry. That race surfaces as
    /// `tantivy_tmp/<...>.ivt/.tmpXXXX: No such file or directory` ENOENT and
    /// cancels the broker load.
    ///
    /// `IndexWriter::wait_merging_threads(mut self)` consumes the writer and
    /// blocks on `merge_operations.wait_until_empty()`, which is the canonical
    /// "all background work has finished" barrier. After it returns, our temp
    /// dir is quiescent and safe to pack + delete.
    pub fn commit(&mut self) -> Result<()> {
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| TantivyBindingError::Internal("writer already committed".to_string()))?;
        writer.commit()?;
        writer.wait_merging_threads()?;
        Ok(())
    }
}

impl Drop for IndexWriterWrapper {
    fn drop(&mut self) {
        // Failure / abort path: the wrapper is being torn down without a
        // successful `commit()`. Take the writer out and explicitly
        // `wait_merging_threads()` so background rayon merges cannot outlive
        // us into the BE-side `remove_all(.ivt)` and race on the temp dir.
        // `IndexWriter::Drop` alone only signals shutdown — it does NOT join
        // the merge pool — so a bare drop would reopen the same race the
        // happy path closes.
        if let Some(writer) = self.writer.take() {
            let _ = writer.wait_merging_threads();
        }
    }
}