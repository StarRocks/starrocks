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

use tantivy::schema::{IndexRecordOption, Field, Schema, TextFieldIndexing, TextOptions};
use tantivy::{Index, IndexWriter, TantivyDocument};

use crate::error::Result;
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
    pub(crate) writer: IndexWriter,
    pub(crate) text_field: Field,
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
        let schema = schema_builder.build();

        let index = Index::create_in_dir(path, schema)?;
        index.tokenizers().register(TOKENIZER_NAME, analyzer);
        let writer: IndexWriter =
            index.writer_with_num_threads(WRITER_NUM_THREADS, WRITER_MEMORY_BUDGET_BYTES)?;

        Ok(Self { writer, text_field })
    }

    /// Append `values.len()` documents in order. Use `""` (empty string) for rows that are null on the BE side; 
    /// the BE caller is responsible for tracking a separate null bitmap so empty-string and null can be
    /// disambiguated at query time.
    pub fn add_strings_batch(&mut self, values: &[&str]) -> Result<()> {
        for v in values {
            let mut doc = TantivyDocument::default();
            doc.add_text(self.text_field, v);
            self.writer.add_document(doc)?;
        }
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        Ok(())
    }
}

impl Drop for IndexWriterWrapper {
    fn drop(&mut self) {
        // tantivy's IndexWriter Drop already handles in-flight indexing cleanly; nothing extra to do here.
    }
}