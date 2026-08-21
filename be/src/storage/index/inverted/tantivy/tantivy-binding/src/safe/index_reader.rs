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

//! Safe Rust wrapper around `tantivy::IndexReader`.
//!
//! Returns BE-side row ids (u32) directly from tantivy DocAddress. Multi-
//! segment indexes are handled by computing per-segment doc-id offsets so the
//! returned u32s are globally unique within the BE segment.
//!
//! `open<D: Directory>` is the single core constructor: any byte source
//! (mmap'ed local dir, compound `.idx` via `PullDirectory`, in-memory
//! `RamDirectory`) flows through it. `load(path)` is a thin convenience
//! over `open(MmapDirectory::open(path)?, ..., ReloadPolicy::OnCommitWithDelay)`.
//!
//! ReloadPolicy convention enforced by call sites (not by this type):
//!   - local writable index → `ReloadPolicy::OnCommitWithDelay`
//!   - compound `.idx` (read-only) → `ReloadPolicy::Manual`
//!     (keeps tantivy from spinning a background reload thread that would
//!     issue spurious reads against the RA file / BlockCache.)

use std::ffi::c_void;
use std::path::Path;

use tantivy::collector::{Collector, SegmentCollector, TopDocs};
use tantivy::columnar::Column;
use tantivy::directory::MmapDirectory;
use tantivy::query::{BooleanQuery, Occur, PhraseQuery, Query, RegexQuery, TermQuery};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::{
    Directory, DocSet, Index, IndexReader, InvertedIndexReader, ReloadPolicy, Score,
    SegmentOrdinal, SegmentReader, Term, COLLECT_BLOCK_BUFFER_LEN,
};

use crate::error::{Result, TantivyBindingError};

/// Block size for flushing collected row ids into the caller's bitmap.
const BITMAP_FLUSH_BLOCK: usize = 4096;

/// C callback that appends a block of BE row ids into the caller-owned bitmap
/// (the C++ side does `roaring::Roaring::addMany`).
/// `set_bitset` callback so tantivy hits stream straight into the result bitmap
/// without a `Vec<u32>` round-trip.
pub type SetBitmapFn = extern "C" fn(ctx: *mut c_void, ids: *const u32, len: usize);

/// Opaque bitmap pointer + append callback handed to the direct-bitmap
/// collector. Holds raw pointers, but the reader uses tantivy's single-threaded
/// query executor so the callback is only ever invoked serially — hence the
/// `Send`/`Sync` impls are sound.
#[derive(Clone, Copy)]
pub struct BitmapSink {
    pub ctx: *mut c_void,
    pub append: SetBitmapFn,
}

unsafe impl Send for BitmapSink {}
unsafe impl Sync for BitmapSink {}

impl BitmapSink {
    #[inline]
    fn flush(&self, ids: &[u32]) {
        if !ids.is_empty() {
            (self.append)(self.ctx, ids.as_ptr(), ids.len());
        }
    }
}

pub struct IndexReaderWrapper {
    pub(crate) _index: Index,
    pub(crate) reader: IndexReader,
    pub(crate) text_field: Field,
}

impl IndexReaderWrapper {
    /// Opens an index from any `tantivy::Directory`,
    /// registers the requested tokenizer, and builds an `IndexReader` with
    /// the given reload policy.
    pub fn open<D: Directory>(
        dir: D,
        field_name: &str,
        tokenizer_name: &str,
        reload_policy: ReloadPolicy,
    ) -> Result<Self> {
        Self::open_with_digest(dir, field_name, tokenizer_name, None, reload_policy)
    }

    pub fn open_with_digest<D: Directory>(
        dir: D,
        field_name: &str,
        analyzer_definition: &str,
        expected_digest: Option<&str>,
        reload_policy: ReloadPolicy,
    ) -> Result<Self> {
        let index = Index::open(dir)?;
        let schema = index.schema();
        let text_field = schema.get_field(field_name).map_err(|_| {
            TantivyBindingError::InvalidArgument(format!("field '{field_name}' not found in index"))
        })?;
        let analyzer =
            crate::safe::tokenizer::resolve(analyzer_definition, expected_digest)?.analyzer;
        index
            .tokenizers()
            .register(crate::safe::tokenizer::TOKENIZER_NAME, analyzer);

        let reader = index
            .reader_builder()
            .reload_policy(reload_policy)
            .try_into()?;
        Ok(Self {
            _index: index,
            reader,
            text_field,
        })
    }

    /// Convenience: open a tantivy index laid out as a local directory at
    /// `path` (i.e. via `MmapDirectory`) with `OnCommitWithDelay` reload.
    pub fn load(path: &Path, field_name: &str, tokenizer_name: &str) -> Result<Self> {
        let dir = MmapDirectory::open(path)?;
        Self::open(
            dir,
            field_name,
            tokenizer_name,
            ReloadPolicy::OnCommitWithDelay,
        )
    }

    pub fn load_with_digest(
        path: &Path,
        field_name: &str,
        analyzer_definition: &str,
        expected_digest: Option<&str>,
    ) -> Result<Self> {
        let dir = MmapDirectory::open(path)?;
        Self::open_with_digest(
            dir,
            field_name,
            analyzer_definition,
            expected_digest,
            ReloadPolicy::OnCommitWithDelay,
        )
    }

    /// Single-term query (also used for EQUAL_QUERY on a non-tokenized field).
    pub fn term_query(&self, term_text: &str) -> Result<Vec<u32>> {
        let term = Term::from_field_text(self.text_field, term_text);
        let query = TermQuery::new(term, IndexRecordOption::Basic);
        self.collect_doc_ids(&query)
    }

    /// MATCH_ANY: any of `terms` matches (BooleanQuery SHOULD).
    pub fn match_any_query(&self, terms: &[&str]) -> Result<Vec<u32>> {
        let subqueries: Vec<(Occur, Box<dyn Query>)> = terms
            .iter()
            .map(|t| {
                let term = Term::from_field_text(self.text_field, t);
                let q: Box<dyn Query> = Box::new(TermQuery::new(term, IndexRecordOption::Basic));
                (Occur::Should, q)
            })
            .collect();
        let bq = BooleanQuery::new(subqueries);
        self.collect_doc_ids(&bq)
    }

    /// MATCH_ALL: every term in `terms` must match (BooleanQuery MUST).
    pub fn match_all_query(&self, terms: &[&str]) -> Result<Vec<u32>> {
        let subqueries: Vec<(Occur, Box<dyn Query>)> = terms
            .iter()
            .map(|t| {
                let term = Term::from_field_text(self.text_field, t);
                let q: Box<dyn Query> = Box::new(TermQuery::new(term, IndexRecordOption::Basic));
                (Occur::Must, q)
            })
            .collect();
        let bq = BooleanQuery::new(subqueries);
        self.collect_doc_ids(&bq)
    }

    /// MATCH_WILDCARD: SQL `LIKE` / `MATCH` pattern over the term dictionary.
    /// The match target is the **term dictionary** of the field; on
    /// tokenized columns this means tokens, not the original text — by
    /// design, aligning with the builtin GIN wildcard semantics.
    pub fn wildcard_query(&self, pattern: &str) -> Result<Vec<u32>> {
        let regex = match like_pattern_to_regex(pattern) {
            Some(r) => r,
            None => return Ok(Vec::new()),
        };
        let query = RegexQuery::from_pattern(&regex, self.text_field)
            .map_err(|err| TantivyBindingError::Internal(format!("RegexQueryError: {err}")))?;
        self.collect_doc_ids(&query)
    }

    /// MATCH_ANY with BM25 relevance scores: returns `(row_id, score)` per hit.
    /// Uses `WithFreqs` so tantivy reads term frequencies → full BM25 (k1=1.2,
    /// b=0.75); the index already stores freqs+positions (see index_writer.rs).
    /// `limit > 0` prunes to the top-`limit` hits by score inside tantivy
    /// (see `collect_doc_ids_scored`); `limit == 0` returns every hit.
    /// `min_score`/`max_score` gate hits to the inclusive `[min, max]` score
    /// range at collect time (`NEG_INFINITY`/`INFINITY` = no bound), backing a
    /// `WHERE score() > c` predicate without materializing out-of-range rows.
    pub fn match_any_query_scored(
        &self,
        terms: &[&str],
        limit: usize,
        min_score: f32,
        max_score: f32,
    ) -> Result<Vec<(u32, f32)>> {
        let subqueries: Vec<(Occur, Box<dyn Query>)> = terms
            .iter()
            .map(|t| {
                let term = Term::from_field_text(self.text_field, t);
                let q: Box<dyn Query> =
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                (Occur::Should, q)
            })
            .collect();
        let bq = BooleanQuery::new(subqueries);
        self.collect_doc_ids_scored(&bq, limit, min_score, max_score)
    }

    /// MATCH_ALL with BM25 relevance scores: returns `(row_id, score)` per hit.
    pub fn match_all_query_scored(
        &self,
        terms: &[&str],
        limit: usize,
        min_score: f32,
        max_score: f32,
    ) -> Result<Vec<(u32, f32)>> {
        let subqueries: Vec<(Occur, Box<dyn Query>)> = terms
            .iter()
            .map(|t| {
                let term = Term::from_field_text(self.text_field, t);
                let q: Box<dyn Query> =
                    Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                (Occur::Must, q)
            })
            .collect();
        let bq = BooleanQuery::new(subqueries);
        self.collect_doc_ids_scored(&bq, limit, min_score, max_score)
    }

    /// MATCH_PHRASE: ordered terms with at most `slop` positional gaps.
    pub fn phrase_query(&self, terms: &[&str], slop: u32) -> Result<Vec<u32>> {
        self.phrase_query_with_positions(terms, None, slop)
    }

    pub fn phrase_query_with_positions(
        &self,
        terms: &[&str],
        positions: Option<&[u32]>,
        slop: u32,
    ) -> Result<Vec<u32>> {
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        if terms.len() == 1 {
            return self.term_query(terms[0]);
        }
        let mut pq =
            PhraseQuery::new_with_offset(self.phrase_terms_with_positions(terms, positions)?);
        pq.set_slop(slop);
        self.collect_doc_ids(&pq)
    }

    // ---- Direct-to-bitmap variants --------------------------
    // Instead of returning a Vec<u32> of matched row ids (which the C++ side
    // then sorts + addMany-s into a roaring — the dominant CPU/memory cost for
    // high-frequency terms), these stream matched BE row ids straight into the
    // caller's bitmap via `sink`, block by block, through one generic collector
    // that works for any tantivy Query (EQUAL/ANY/ALL/PHRASE/WILDCARD).

    fn collect_to_bitmap(&self, query: &dyn Query, sink: BitmapSink) -> Result<()> {
        let searcher = self.reader.searcher();
        searcher.search(query, &BitmapCollector { sink })?;
        Ok(())
    }

    /// EQUAL / single-term, streamed into `sink`.
    pub fn term_query_bitmap(&self, term_text: &str, sink: BitmapSink) -> Result<()> {
        let term = Term::from_field_text(self.text_field, term_text);
        self.collect_to_bitmap(&TermQuery::new(term, IndexRecordOption::Basic), sink)
    }

    /// MATCH_ANY (BooleanQuery SHOULD), streamed into `sink`.
    pub fn match_any_query_bitmap(&self, terms: &[&str], sink: BitmapSink) -> Result<()> {
        let subqueries: Vec<(Occur, Box<dyn Query>)> = terms
            .iter()
            .map(|t| {
                let term = Term::from_field_text(self.text_field, t);
                let q: Box<dyn Query> = Box::new(TermQuery::new(term, IndexRecordOption::Basic));
                (Occur::Should, q)
            })
            .collect();
        self.collect_to_bitmap(&BooleanQuery::new(subqueries), sink)
    }

    /// MATCH_ALL, streamed into `sink`. When every term is high-frequency (the
    /// rarest term's `doc_freq / num_docs >= min_df_ratio`), tantivy's leapfrog
    /// Intersection has no cheap lead and degrades to O(hits) seek; instead build
    /// a per-term doc-id bitset, AND them word-wise, and stream matched row ids
    /// (the Doris bitmap-AND path). Otherwise a selective term exists and the
    /// general collector (leapfrog + direct write) is already optimal. An absent
    /// MUST term short-circuits to empty.
    pub fn match_all_query_bitmap(
        &self,
        terms: &[&str],
        min_df_ratio: f64,
        sink: BitmapSink,
    ) -> Result<()> {
        if terms.is_empty() {
            return Ok(());
        }
        let searcher = self.reader.searcher();
        let num_docs = searcher.num_docs();
        let mut min_df = u64::MAX;
        for t in terms {
            let df = searcher.doc_freq(&Term::from_field_text(self.text_field, t))?;
            if df == 0 {
                return Ok(());
            }
            min_df = min_df.min(df);
        }
        let all_high =
            terms.len() >= 2 && num_docs > 0 && (min_df as f64 / num_docs as f64) >= min_df_ratio;
        if !all_high {
            let subqueries: Vec<(Occur, Box<dyn Query>)> = terms
                .iter()
                .map(|t| {
                    let term = Term::from_field_text(self.text_field, t);
                    let q: Box<dyn Query> =
                        Box::new(TermQuery::new(term, IndexRecordOption::Basic));
                    (Occur::Must, q)
                })
                .collect();
            return self.collect_to_bitmap(&BooleanQuery::new(subqueries), sink);
        }

        // Bitmap-AND path: all terms high-frequency, no selective lead.
        let tterms: Vec<Term> = terms
            .iter()
            .map(|t| Term::from_field_text(self.text_field, t))
            .collect();
        let mut out: Vec<u32> = Vec::with_capacity(BITMAP_FLUSH_BLOCK);
        for seg in searcher.segment_readers() {
            let max_doc = seg.max_doc();
            if max_doc == 0 {
                continue;
            }
            let inv = seg.inverted_index(self.text_field)?;
            let words = (max_doc as usize + 63) / 64;
            let mut acc = vec![0u64; words];
            if !fill_term_bitset(&inv, &tterms[0], &mut acc)? {
                continue;
            }
            let mut nonempty = acc.iter().any(|&w| w != 0);
            let mut tmp = vec![0u64; words];
            for term in &tterms[1..] {
                if !nonempty {
                    break;
                }
                for w in tmp.iter_mut() {
                    *w = 0;
                }
                if !fill_term_bitset(&inv, term, &mut tmp)? {
                    nonempty = false;
                    break;
                }
                nonempty = false;
                for i in 0..words {
                    acc[i] &= tmp[i];
                    nonempty |= acc[i] != 0;
                }
            }
            if !nonempty {
                continue;
            }

            // Resolve doc ids to BE row ids and stream via addMany. read_postings
            // does not apply deletes, so skip them here (and disable the
            // contiguous fast path when the segment has any deletes).
            let row_id = seg.fast_fields().u64("row_id")?;
            let alive = seg.alive_bitset();
            let base = row_id.values_for_doc(0).next();
            let last = row_id.values_for_doc(max_doc - 1).next();
            let contiguous = alive.is_none()
                && matches!((base, last), (Some(b), Some(l)) if l == b + (max_doc as u64 - 1));
            let base = base.unwrap_or(0) as u32;
            for_each_set_bit(&acc, max_doc, |doc| {
                if let Some(ab) = alive {
                    if ab.is_deleted(doc) {
                        return;
                    }
                }
                let rid = if contiguous {
                    base + doc
                } else {
                    match row_id.values_for_doc(doc).next() {
                        Some(r) => r as u32,
                        None => return,
                    }
                };
                out.push(rid);
                if out.len() >= BITMAP_FLUSH_BLOCK {
                    sink.flush(&out);
                    out.clear();
                }
            });
        }
        sink.flush(&out);
        Ok(())
    }

    /// MATCH_PHRASE, streamed into `sink`.
    pub fn phrase_query_bitmap(&self, terms: &[&str], slop: u32, sink: BitmapSink) -> Result<()> {
        self.phrase_query_bitmap_with_positions(terms, None, slop, sink)
    }

    pub fn phrase_query_bitmap_with_positions(
        &self,
        terms: &[&str],
        positions: Option<&[u32]>,
        slop: u32,
        sink: BitmapSink,
    ) -> Result<()> {
        if terms.is_empty() {
            return Ok(());
        }
        if terms.len() == 1 {
            return self.term_query_bitmap(terms[0], sink);
        }
        let mut pq =
            PhraseQuery::new_with_offset(self.phrase_terms_with_positions(terms, positions)?);
        pq.set_slop(slop);
        self.collect_to_bitmap(&pq, sink)
    }

    fn phrase_terms_with_positions(
        &self,
        terms: &[&str],
        positions: Option<&[u32]>,
    ) -> Result<Vec<(usize, Term)>> {
        if let Some(positions) = positions {
            if positions.len() != terms.len() {
                return Err(TantivyBindingError::InvalidArgument(
                    "phrase term and position counts differ".to_string(),
                ));
            }
        }
        let base = positions
            .and_then(|values| values.first())
            .copied()
            .unwrap_or(0);
        terms
            .iter()
            .enumerate()
            .map(|(index, term)| {
                let position = positions
                    .map(|values| values[index].checked_sub(base))
                    .unwrap_or(Some(index as u32))
                    .ok_or_else(|| {
                        TantivyBindingError::InvalidArgument(
                            "phrase token positions must be nondecreasing".to_string(),
                        )
                    })?;
                Ok((
                    position as usize,
                    Term::from_field_text(self.text_field, term),
                ))
            })
            .collect()
    }

    /// MATCH_WILDCARD, streamed into `sink`.
    pub fn wildcard_query_bitmap(&self, pattern: &str, sink: BitmapSink) -> Result<()> {
        let regex = match like_pattern_to_regex(pattern) {
            Some(r) => r,
            None => return Ok(()),
        };
        let query = RegexQuery::from_pattern(&regex, self.text_field)
            .map_err(|err| TantivyBindingError::Internal(format!("RegexQueryError: {err}")))?;
        self.collect_to_bitmap(&query, sink)
    }

    fn collect_doc_ids(&self, query: &dyn Query) -> Result<Vec<u32>> {
        let searcher = self.reader.searcher();
        Ok(searcher.search(query, &RowIdCollector)?)
    }

    /// Like `collect_doc_ids` but keeps the BM25 score per hit. Returns
    /// `(row_id, score)`; the caller sorts (top-N by score desc for a `score()`
    /// ORDER BY ... LIMIT). Scores are unordered/segment-interleaved on return.
    ///
    /// `limit == 0` scores *every* hit (used for `ORDER BY score() ASC`, where
    /// the highest-score pruning of `TopDocs` does not apply). `limit > 0` pushes
    /// the LIMIT into tantivy's `TopDocs`, which prunes to the best `limit` hits
    /// per segment (WAND/block-max) instead of scoring the full posting list —
    /// mirroring the vector ANN top-k path so cost is O(limit) not O(hits).
    ///
    /// `min_score`/`max_score` keep only hits with `min <= score <= max`
    /// (`NEG_INFINITY`/`INFINITY` = unbounded). On the `limit == 0` path the
    /// gate runs inside the collector (out-of-range hits never allocate a
    /// row_id); on the top-k path it filters the returned hits — correct
    /// because `ORDER BY score() DESC LIMIT n WHERE score()>c` wants the top-n
    /// that also pass the threshold, and the top-n by score subsumes them.
    fn collect_doc_ids_scored(
        &self,
        query: &dyn Query,
        limit: usize,
        min_score: f32,
        max_score: f32,
    ) -> Result<Vec<(u32, f32)>> {
        let searcher = self.reader.searcher();
        if limit == 0 {
            return Ok(searcher.search(
                query,
                &RowIdScoreCollector {
                    min_score,
                    max_score,
                },
            )?);
        }
        let top = searcher.search(query, &TopDocs::with_limit(limit))?;
        let mut out = Vec::with_capacity(top.len());
        // Cache the row_id fast field per segment; TopDocs returns hits grouped
        // loosely by segment, so this avoids re-opening the column per hit.
        let mut cur: Option<(SegmentOrdinal, Column<u64>)> = None;
        for (score, addr) in top {
            if score < min_score || score > max_score {
                continue;
            }
            if cur
                .as_ref()
                .map(|(ord, _)| *ord != addr.segment_ord)
                .unwrap_or(true)
            {
                let ff = searcher
                    .segment_reader(addr.segment_ord)
                    .fast_fields()
                    .u64("row_id")?;
                cur = Some((addr.segment_ord, ff));
            }
            if let Some((_, ff)) = &cur {
                if let Some(rid) = ff.values_for_doc(addr.doc_id).next() {
                    out.push((rid as u32, score));
                }
            }
        }
        Ok(out)
    }
}

// direct-to-bitmap collector: stream matched BE row ids straight
// into the caller's bitmap in blocks, instead of materializing/sorting a
// Vec<u32> and adding it in one shot. Generic over any tantivy Query, so one
// collector serves EQUAL/MATCH_ANY/MATCH_ALL/PHRASE/WILDCARD. Deleted docs are
// already filtered by tantivy's scorer before `collect`, so no alive-bitset
// handling is needed here.
struct BitmapCollector {
    sink: BitmapSink,
}

struct BitmapSegmentCollector {
    sink: BitmapSink,
    row_id: Column<u64>,
    // When this segment's tantivy doc ids map to a contiguous BE row-id range
    // [base, base+max_doc) (the single-threaded writer's usual layout, verified
    // by endpoints), row_id = base + doc with no per-doc fast-field lookup.
    // Otherwise fall back to `row_id.values_for_doc(doc)`.
    base: u32,
    contiguous: bool,
    buf: Vec<u32>,
}

impl Collector for BitmapCollector {
    type Fruit = ();
    type Child = BitmapSegmentCollector;

    fn for_segment(
        &self,
        _ord: SegmentOrdinal,
        seg: &SegmentReader,
    ) -> tantivy::Result<BitmapSegmentCollector> {
        let row_id = seg.fast_fields().u64("row_id")?;
        let max_doc = seg.max_doc();
        let base = row_id.values_for_doc(0).next();
        let last = if max_doc > 0 {
            row_id.values_for_doc(max_doc - 1).next()
        } else {
            None
        };
        let contiguous = max_doc > 0
            && matches!((base, last), (Some(b), Some(l)) if l == b + (max_doc as u64 - 1));
        Ok(BitmapSegmentCollector {
            sink: self.sink,
            row_id,
            base: base.unwrap_or(0) as u32,
            contiguous,
            buf: Vec::with_capacity(BITMAP_FLUSH_BLOCK),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, _segs: Vec<()>) -> tantivy::Result<()> {
        Ok(())
    }
}

impl BitmapSegmentCollector {
    #[inline]
    fn flush_if_full(&mut self) {
        if self.buf.len() >= BITMAP_FLUSH_BLOCK {
            self.sink.flush(&self.buf);
            self.buf.clear();
        }
    }
}

impl SegmentCollector for BitmapSegmentCollector {
    type Fruit = ();

    fn collect(&mut self, doc: u32, _score: Score) {
        if self.contiguous {
            self.buf.push(self.base + doc);
        } else if let Some(rid) = self.row_id.values_for_doc(doc).next() {
            self.buf.push(rid as u32);
        }
        self.flush_if_full();
    }

    fn collect_block(&mut self, docs: &[u32]) {
        if self.contiguous {
            let base = self.base;
            self.buf.extend(docs.iter().map(|&d| base + d));
        } else {
            for &d in docs {
                if let Some(rid) = self.row_id.values_for_doc(d).next() {
                    self.buf.push(rid as u32);
                }
            }
        }
        self.flush_if_full();
    }

    fn harvest(self) -> () {
        self.sink.flush(&self.buf);
    }
}

// Fill `bitset` (1 bit per doc id) with `term`'s postings in one segment via
// bulk `fill_buffer` decode. Returns `false` if the term has no postings here,
// so a MUST intersection is empty for the whole segment. Deletes are NOT applied
// by `read_postings`; the caller filters them at row-id resolution time.
fn fill_term_bitset(inv: &InvertedIndexReader, term: &Term, bitset: &mut [u64]) -> Result<bool> {
    let mut postings = match inv
        .read_postings(term, IndexRecordOption::Basic)
        .map_err(|e| TantivyBindingError::Internal(format!("read_postings failed: {e}")))?
    {
        Some(p) => p,
        None => return Ok(false),
    };
    let mut buf = [0u32; COLLECT_BLOCK_BUFFER_LEN];
    loop {
        let n = postings.fill_buffer(&mut buf);
        for &doc in &buf[..n] {
            bitset[(doc as usize) >> 6] |= 1u64 << (doc & 63);
        }
        if n < COLLECT_BLOCK_BUFFER_LEN {
            break;
        }
    }
    Ok(true)
}

// Invoke `f` for each set doc id (`< max_doc`) in `bitset`, ascending.
fn for_each_set_bit<F: FnMut(u32)>(bitset: &[u64], max_doc: u32, mut f: F) {
    for (wi, &word) in bitset.iter().enumerate() {
        let mut bits = word;
        while bits != 0 {
            let doc = (wi as u32) * 64 + bits.trailing_zeros();
            if doc >= max_doc {
                return;
            }
            f(doc);
            bits &= bits - 1;
        }
    }
}

// Resolve hits to BE row ids via the stored `row_id` fast field, not segment
// offsets: tantivy's internal segment order is not insertion order, so offset
// arithmetic mis-maps rows once a BE segment spills into >1 tantivy segment.
struct RowIdCollector;

struct RowIdSegmentCollector {
    row_id: Column<u64>,
    ids: Vec<u32>,
}

impl Collector for RowIdCollector {
    type Fruit = Vec<u32>;
    type Child = RowIdSegmentCollector;

    fn for_segment(
        &self,
        _ord: SegmentOrdinal,
        seg: &SegmentReader,
    ) -> tantivy::Result<RowIdSegmentCollector> {
        Ok(RowIdSegmentCollector {
            row_id: seg.fast_fields().u64("row_id")?,
            ids: Vec::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, segs: Vec<Vec<u32>>) -> tantivy::Result<Vec<u32>> {
        let mut out: Vec<u32> = segs.into_iter().flatten().collect();
        out.sort_unstable();
        Ok(out)
    }
}

impl SegmentCollector for RowIdSegmentCollector {
    type Fruit = Vec<u32>;

    fn collect(&mut self, doc: u32, _score: Score) {
        if let Some(rid) = self.row_id.values_for_doc(doc).next() {
            self.ids.push(rid as u32);
        }
    }

    fn harvest(self) -> Vec<u32> {
        self.ids
    }
}

/// Translate a SQL `LIKE` / `MATCH` pattern into a regex string suitable
/// for `tantivy::query::RegexQuery::from_pattern`.
///
/// Tantivy's `RegexQuery` matches a term in the field's term dictionary
/// **iff the regex matches the entire term string** (the underlying
/// `regex-automata` DFA is run in fullmatch mode), and zero-width anchors
/// like `^` / `$` are NOT supported. So we encode the SQL `LIKE` semantics
/// purely with `.*`:
///
///   * `%` and `*` are equivalent multi-char wildcards.
///   * Consecutive wildcards collapse to a single one.
///   * Literal segments pass through `regex::escape` so SQL literals
///     containing regex metacharacters (`.`, `+`, `(`, `?`, ...) match
///     verbatim.
///   * A pattern that starts with a wildcard prepends `.*`; ending with a
///     wildcard appends `.*`. Internal wildcards become `.*` between
///     literal segments.
///   * A pattern made entirely of wildcards translates to `.*`, matching
///     every term in the dictionary.
///   * An empty pattern returns `None`; callers should resolve to an empty
///     result set without constructing a `RegexQuery`.
pub(crate) fn like_pattern_to_regex(pattern: &str) -> Option<String> {
    if pattern.is_empty() {
        return None;
    }

    let bytes = pattern.as_bytes();
    let starts_with_wildcard = matches!(bytes[0], b'%' | b'*');
    let ends_with_wildcard = matches!(bytes[bytes.len() - 1], b'%' | b'*');

    let mut literals: Vec<&str> = Vec::new();
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        while cursor < bytes.len() && matches!(bytes[cursor], b'%' | b'*') {
            cursor += 1;
        }
        if cursor >= bytes.len() {
            break;
        }
        let start = cursor;
        while cursor < bytes.len() && !matches!(bytes[cursor], b'%' | b'*') {
            cursor += 1;
        }
        // Safe: split only on ASCII (`%` / `*`); byte range stays on a
        // UTF-8 boundary.
        literals.push(&pattern[start..cursor]);
    }

    if literals.is_empty() {
        // Pattern is wildcards only.
        return Some(".*".to_string());
    }

    let mut regex = String::new();
    if starts_with_wildcard {
        regex.push_str(".*");
    }
    for (i, lit) in literals.iter().enumerate() {
        if i > 0 {
            regex.push_str(".*");
        }
        regex.push_str(&regex::escape(lit));
    }
    if ends_with_wildcard {
        regex.push_str(".*");
    }
    Some(regex)
}

// Scored sibling of RowIdCollector: turns BM25 on (`requires_scoring = true`) and
// keeps the per-hit score next to the BE row id. Reuses the same `row_id`
// fast-field resolution so (row_id, score) stays correctly paired across tantivy
// segments. This is the core BM25 mechanism for a native `score()` function.
// `min_score`/`max_score` gate hits to `[min, max]` at collect time so a
// `WHERE score() > c` predicate prunes inside tantivy (no row_id lookup for
// out-of-range hits); unbounded ends are `NEG_INFINITY`/`INFINITY`.
struct RowIdScoreCollector {
    min_score: f32,
    max_score: f32,
}

struct RowIdScoreSegmentCollector {
    row_id: Column<u64>,
    hits: Vec<(u32, f32)>,
    min_score: f32,
    max_score: f32,
}

impl Collector for RowIdScoreCollector {
    type Fruit = Vec<(u32, f32)>;
    type Child = RowIdScoreSegmentCollector;

    fn for_segment(
        &self,
        _ord: SegmentOrdinal,
        seg: &SegmentReader,
    ) -> tantivy::Result<RowIdScoreSegmentCollector> {
        Ok(RowIdScoreSegmentCollector {
            row_id: seg.fast_fields().u64("row_id")?,
            hits: Vec::new(),
            min_score: self.min_score,
            max_score: self.max_score,
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, segs: Vec<Vec<(u32, f32)>>) -> tantivy::Result<Vec<(u32, f32)>> {
        Ok(segs.into_iter().flatten().collect())
    }
}

impl SegmentCollector for RowIdScoreSegmentCollector {
    type Fruit = Vec<(u32, f32)>;

    fn collect(&mut self, doc: u32, score: Score) {
        if score < self.min_score || score > self.max_score {
            return;
        }
        if let Some(rid) = self.row_id.values_for_doc(doc).next() {
            self.hits.push((rid as u32, score));
        }
    }

    fn harvest(self) -> Vec<(u32, f32)> {
        self.hits
    }
}
