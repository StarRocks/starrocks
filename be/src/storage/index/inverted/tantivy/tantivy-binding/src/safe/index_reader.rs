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

use std::path::Path;

use tantivy::collector::{Collector, SegmentCollector, TopDocs};
use tantivy::columnar::Column;
use tantivy::directory::MmapDirectory;
use tantivy::query::{BooleanQuery, Occur, PhraseQuery, Query, RegexQuery, TermQuery};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::{Directory, Index, IndexReader, ReloadPolicy, Score, SegmentOrdinal, SegmentReader, Term};

use crate::error::{Result, TantivyBindingError};

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
        let index = Index::open(dir)?;
        let schema = index.schema();
        let text_field = schema.get_field(field_name).map_err(|_| {
            TantivyBindingError::InvalidArgument(format!(
                "field '{field_name}' not found in index"
            ))
        })?;
        let analyzer = crate::safe::tokenizer::build(tokenizer_name)?;
        index
            .tokenizers()
            .register(crate::safe::tokenizer::TOKENIZER_NAME, analyzer);

        let reader = index.reader_builder().reload_policy(reload_policy).try_into()?;
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
        Self::open(dir, field_name, tokenizer_name, ReloadPolicy::OnCommitWithDelay)
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
        let query = RegexQuery::from_pattern(&regex, self.text_field).map_err(|err| {
            TantivyBindingError::Internal(format!("RegexQueryError: {err}"))
        })?;
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
                let q: Box<dyn Query> = Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
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
                let q: Box<dyn Query> = Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
                (Occur::Must, q)
            })
            .collect();
        let bq = BooleanQuery::new(subqueries);
        self.collect_doc_ids_scored(&bq, limit, min_score, max_score)
    }

    /// MATCH_PHRASE: ordered terms with at most `slop` positional gaps.
    pub fn phrase_query(&self, terms: &[&str], slop: u32) -> Result<Vec<u32>> {
        if terms.is_empty() {
            return Ok(Vec::new());
        }
        if terms.len() == 1 {
            return self.term_query(terms[0]);
        }
        let tantivy_terms: Vec<Term> = terms
            .iter()
            .map(|t| Term::from_field_text(self.text_field, t))
            .collect();
        let mut pq = PhraseQuery::new(tantivy_terms);
        pq.set_slop(slop);
        self.collect_doc_ids(&pq)
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
            return Ok(searcher.search(query, &RowIdScoreCollector { min_score, max_score })?);
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
            if cur.as_ref().map(|(ord, _)| *ord != addr.segment_ord).unwrap_or(true) {
                let ff = searcher.segment_reader(addr.segment_ord).fast_fields().u64("row_id")?;
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

    fn for_segment(&self, _ord: SegmentOrdinal, seg: &SegmentReader) -> tantivy::Result<RowIdSegmentCollector> {
        Ok(RowIdSegmentCollector { row_id: seg.fast_fields().u64("row_id")?, ids: Vec::new() })
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

    fn for_segment(&self, _ord: SegmentOrdinal, seg: &SegmentReader) -> tantivy::Result<RowIdScoreSegmentCollector> {
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
