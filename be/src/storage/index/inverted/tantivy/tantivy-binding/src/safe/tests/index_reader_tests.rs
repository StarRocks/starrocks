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
use tantivy::schema::{FAST, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
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
    // RowIdCollector resolves hits via the stored `row_id` fast field, so the
    // hand-built index must carry it (mirrors IndexWriterWrapper's schema).
    let row_id_field = schema_builder.add_u64_field("row_id", FAST);
    let schema = schema_builder.build();

    let ram_dir = RamDirectory::create();
    let index = Index::create(ram_dir.clone(), schema.clone(), Default::default())
        .expect("create in ram");
    let analyzer = crate::safe::tokenizer::build("english").expect("english analyzer");
    index
        .tokenizers()
        .register(crate::safe::tokenizer::TOKENIZER_NAME, analyzer);

    let mut writer = index.writer_with_num_threads(1, 15_000_000).expect("writer");
    for (i, v) in ["alpha beta", "gamma", "alpha"].iter().enumerate() {
        let mut doc = TantivyDocument::default();
        doc.add_text(text_field, *v);
        doc.add_u64(row_id_field, i as u64);
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

mod wildcard {
    use super::*;
    use crate::safe::index_reader::like_pattern_to_regex;

    #[test]
    fn pattern_translation_table() {
        // Empty pattern → None: caller must short-circuit to empty result
        assert_eq!(like_pattern_to_regex(""), None);

        // Pure wildcards (any of % / *, including consecutive runs) → ".*".
        for p in ["%", "*", "%%", "**", "%*", "*%", "%%*"] {
            assert_eq!(
                like_pattern_to_regex(p),
                Some(".*".to_string()),
                "pure wildcard `{p}`"
            );
        }

        // RegexQuery is fullmatch on each term, no `^`/`$` anchors —
        // boundaries are encoded with leading/trailing `.*`.
        assert_eq!(like_pattern_to_regex("foo"), Some("foo".to_string()));
        assert_eq!(like_pattern_to_regex("foo%"), Some("foo.*".to_string()));
        assert_eq!(like_pattern_to_regex("%foo"), Some(".*foo".to_string()));
        assert_eq!(like_pattern_to_regex("%foo%"), Some(".*foo.*".to_string()));

        // % and * are equivalent.
        assert_eq!(like_pattern_to_regex("*foo*"), like_pattern_to_regex("%foo%"));
        assert_eq!(like_pattern_to_regex("foo*"), like_pattern_to_regex("foo%"));

        // Consecutive wildcards collapse.
        assert_eq!(like_pattern_to_regex("%%foo%%"), Some(".*foo.*".to_string()));
        assert_eq!(like_pattern_to_regex("%*foo*%"), Some(".*foo.*".to_string()));

        // Multiple literal segments joined by `.*`.
        assert_eq!(like_pattern_to_regex("foo%bar"), Some("foo.*bar".to_string()));
        assert_eq!(
            like_pattern_to_regex("%foo%bar%"),
            Some(".*foo.*bar.*".to_string())
        );
        assert_eq!(
            like_pattern_to_regex("a%b*c"),
            Some("a.*b.*c".to_string())
        );

        // Regex metacharacters in literal segments must be escaped.
        let r = like_pattern_to_regex("a.b%").unwrap();
        assert!(r.contains(r"a\.b"), "regex `{r}` must escape `.`");
        let r = like_pattern_to_regex("(x)%").unwrap();
        assert!(r.contains(r"\(x\)"), "regex `{r}` must escape parens");
        let r = like_pattern_to_regex("%a+b%").unwrap();
        assert!(r.contains(r"a\+b"), "regex `{r}` must escape `+`");
    }

    /// Build an index where the field is non-tokenized (TOKENIZER_RAW),
    /// so the term dictionary is the original string verbatim — this
    /// gives wildcard the parser=none semantics.
    fn build_raw(values: &[&str]) -> TempDir {
        let tmp = TempDir::new().expect("tempdir");
        let mut w = IndexWriterWrapper::create(tmp.path(), "f", "raw").expect("create");
        w.add_strings_batch(values).expect("add");
        w.commit().expect("commit");
        drop(w);
        tmp
    }

    #[test]
    fn wildcard_substring_prefix_suffix() {
        // doc 0..6 covers literal substring / prefix / suffix variants.
        let tmp = build_raw(&[
            "foo",      // 0
            "foobar",   // 1
            "barfoo",   // 2
            "baz",      // 3
            "afoob",    // 4
            "FOO",      // 5 — different case, parser=none keeps it as-is
        ]);
        let r = IndexReaderWrapper::load(tmp.path(), "f", "raw").expect("load");

        // %foo%: any term containing `foo` (case-sensitive, parser=none).
        let mut hits = r.wildcard_query("%foo%").expect("substr");
        hits.sort_unstable();
        assert_eq!(hits, vec![0u32, 1, 2, 4]);

        // foo%: terms starting with `foo`.
        let mut hits = r.wildcard_query("foo%").expect("prefix");
        hits.sort_unstable();
        assert_eq!(hits, vec![0u32, 1]);

        // %foo: terms ending with `foo`.
        let mut hits = r.wildcard_query("%foo").expect("suffix");
        hits.sort_unstable();
        assert_eq!(hits, vec![0u32, 2]);

        // * is equivalent to %.
        let mut by_star = r.wildcard_query("*foo*").expect("star substr");
        let mut by_pct = r.wildcard_query("%foo%").expect("pct substr");
        by_star.sort_unstable();
        by_pct.sort_unstable();
        assert_eq!(by_star, by_pct);
    }

    #[test]
    fn wildcard_pure_wildcard_matches_all_terms() {
        let tmp = build_raw(&["a", "b", "c"]);
        let r = IndexReaderWrapper::load(tmp.path(), "f", "raw").expect("load");

        let mut hits = r.wildcard_query("%").expect("all via %");
        hits.sort_unstable();
        assert_eq!(hits, vec![0u32, 1, 2]);

        let mut hits = r.wildcard_query("*").expect("all via *");
        hits.sort_unstable();
        assert_eq!(hits, vec![0u32, 1, 2]);

        // Empty pattern → empty result, no panic.
        let hits = r.wildcard_query("").expect("empty");
        assert!(hits.is_empty());
    }

    #[test]
    fn wildcard_escapes_regex_metacharacters() {
        // The literal segment contains `.`, which must NOT act as a regex
        // any-char. So `a.b` should match `a.b` but not `axb`.
        let tmp = build_raw(&["a.b", "axb", "ab", "aab"]);
        let r = IndexReaderWrapper::load(tmp.path(), "f", "raw").expect("load");

        let mut hits = r.wildcard_query("a.b").expect("exact dot");
        hits.sort_unstable();
        assert_eq!(hits, vec![0u32], "regex `.` in pattern must be escaped");

        let mut hits = r.wildcard_query("a.b%").expect("dot prefix");
        hits.sort_unstable();
        assert_eq!(hits, vec![0u32]);
    }
}

#[test]
fn score_ranks_by_tf_and_length() {
    let tmp = build(&[
        "gif gif gif gif",   // 0: TF=4, highest
        "gif jpg png html",  // 1: TF=1, longer
        "html css json xml", // 2: no gif
        "gif",               // 3: TF=1, shortest
    ]);
    let r = IndexReaderWrapper::load(tmp.path(), "f", "english").expect("load");
    let mut hits = r.match_any_query_scored(&["gif"], 0, f32::NEG_INFINITY, f32::INFINITY).expect("scored query");
    // only the 3 docs containing 'gif' come back
    let rows: std::collections::BTreeSet<u32> = hits.iter().map(|(rid, _)| *rid).collect();
    assert_eq!(rows, [0u32, 1, 3].into_iter().collect(), "matched rows: {hits:?}");
    // every BM25 score is positive
    assert!(hits.iter().all(|(_, s)| *s > 0.0), "scores: {hits:?}");
    // rank by score desc → doc 0 (TF=4) is the top hit
    hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    assert_eq!(hits[0].0, 0u32, "highest TF ranks first: {hits:?}");
    let s = |rid: u32| hits.iter().find(|(r, _)| *r == rid).unwrap().1;
    assert!(s(0) > s(1), "TF=4 outranks TF=1: {hits:?}");
    assert!(s(3) > s(1), "shorter doc outranks longer for same TF: {hits:?}");
}

#[test]
fn score_rarer_term_scores_higher_idf() {
    // 'gif' in 3 docs (low IDF), 'png' in 1 (high IDF); all docs are a single
    // token, and both tokens are stemming-invariant under the english analyzer
    // (so the raw-term query — matching production's pre-tokenized input —
    // hits). This isolates IDF: same doc length, only term rarity differs.
    let tmp = build(&["gif", "gif", "gif", "png"]);
    let r = IndexReaderWrapper::load(tmp.path(), "f", "english").expect("load");
    let common = r.match_any_query_scored(&["gif"], 0, f32::NEG_INFINITY, f32::INFINITY).expect("q");
    let rare = r.match_any_query_scored(&["png"], 0, f32::NEG_INFINITY, f32::INFINITY).expect("q");
    assert_eq!(common.len(), 3, "gif in 3 docs: {common:?}");
    assert_eq!(rare.len(), 1, "png in 1 doc: {rare:?}");
    // rarer term → higher IDF → higher BM25
    assert!(
        rare[0].1 > common[0].1,
        "rarer 'png' {rare:?} should outscore common 'gif' {common:?}"
    );
}

#[test]
fn score_topk_pushdown_limits_hits() {
    // 4 docs contain 'gif' with decreasing TF; a top-2 pushdown must return only
    // the 2 highest-scoring rows (doc 0 then doc 1), not the full posting list.
    let tmp = build(&[
        "gif gif gif gif", // 0: TF=4
        "gif gif gif",      // 1: TF=3
        "gif gif",          // 2: TF=2
        "gif",              // 3: TF=1
    ]);
    let r = IndexReaderWrapper::load(tmp.path(), "f", "english").expect("load");
    let mut top = r.match_any_query_scored(&["gif"], 2, f32::NEG_INFINITY, f32::INFINITY).expect("top-k query");
    assert_eq!(top.len(), 2, "limit=2 returns exactly 2 rows: {top:?}");
    top.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    let rows: Vec<u32> = top.iter().map(|(rid, _)| *rid).collect();
    assert_eq!(rows, vec![0u32, 1], "top-2 by score are the two highest-TF docs: {top:?}");
    // limit=0 still returns the full posting list (all 4 hits).
    let full = r.match_any_query_scored(&["gif"], 0, f32::NEG_INFINITY, f32::INFINITY).expect("full query");
    assert_eq!(full.len(), 4, "limit=0 keeps every hit: {full:?}");
}

#[test]
fn score_min_max_gate_filters_hits() {
    // 4 docs with decreasing TF for 'gif' → strictly decreasing BM25 scores.
    // A min/max score gate must keep only hits whose score is in [min, max].
    let tmp = build(&[
        "gif gif gif gif", // 0: TF=4, highest score
        "gif gif gif",      // 1: TF=3
        "gif gif",          // 2: TF=2
        "gif",              // 3: TF=1, lowest score
    ]);
    let r = IndexReaderWrapper::load(tmp.path(), "f", "english").expect("load");
    let mut all = r.match_any_query_scored(&["gif"], 0, f32::NEG_INFINITY, f32::INFINITY).expect("all");
    all.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap()); // score desc: doc0..doc3
    // Pick a threshold strictly between doc 1's and doc 2's score.
    let cut = (all[1].1 + all[2].1) / 2.0;
    // min gate (collector path, limit=0): only docs 0 and 1 survive.
    let hi = r.match_any_query_scored(&["gif"], 0, cut, f32::INFINITY).expect("min gate");
    let hi_rows: std::collections::BTreeSet<u32> = hi.iter().map(|(rid, _)| *rid).collect();
    assert_eq!(hi_rows, [0u32, 1].into_iter().collect(), "min gate keeps top-2: {hi:?}");
    assert!(hi.iter().all(|(_, s)| *s >= cut), "all >= min: {hi:?}");
    // max gate: only docs 2 and 3 survive.
    let lo = r.match_any_query_scored(&["gif"], 0, f32::NEG_INFINITY, cut).expect("max gate");
    let lo_rows: std::collections::BTreeSet<u32> = lo.iter().map(|(rid, _)| *rid).collect();
    assert_eq!(lo_rows, [2u32, 3].into_iter().collect(), "max gate keeps bottom-2: {lo:?}");
    // min gate on the top-k path (limit>0) prunes the same way.
    let hi_topk = r.match_any_query_scored(&["gif"], 10, cut, f32::INFINITY).expect("topk min gate");
    let hi_topk_rows: std::collections::BTreeSet<u32> = hi_topk.iter().map(|(rid, _)| *rid).collect();
    assert_eq!(hi_topk_rows, [0u32, 1].into_iter().collect(), "top-k min gate keeps top-2: {hi_topk:?}");
}
