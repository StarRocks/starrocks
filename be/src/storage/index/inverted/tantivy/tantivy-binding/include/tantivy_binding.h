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

#ifndef TANTIVY_BINDING_H
#define TANTIVY_BINDING_H

#pragma once

#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <ostream>

namespace starrocks::tantivy_binding {

constexpr static const uint32_t SPEC_VERSION = 1;

constexpr static const uint32_t RUNTIME_ABI_VERSION = 1;

constexpr static const uintptr_t MAX_DEFINITION_BYTES = (64 * 1024);

constexpr static const uintptr_t MAX_PIPELINE_COMPONENTS = 16;

constexpr static const uintptr_t MAX_MAPPING_RULES = 256;

constexpr static const uintptr_t MAX_MAPPING_RULE_BYTES = 1024;

constexpr static const uintptr_t MAX_MAPPING_BYTES = (32 * 1024);

constexpr static const uintptr_t MAX_STOPWORDS = 1024;

constexpr static const uintptr_t MAX_STOPWORD_BYTES = 256;

constexpr static const uintptr_t MAX_STOPWORDS_BYTES = (32 * 1024);

constexpr static const uintptr_t MAX_INPUT_BYTES = (1024 * 1024);

constexpr static const uintptr_t MAX_OUTPUT_TOKENS = 1000000;

constexpr static const uintptr_t MAX_TOKEN_BYTES = (32 * 1024);

/**
 * Discriminator for `Value`. Keep numerically stable — C++ side switches on
 * the integer values.
 */
enum class ValueTag : uint8_t {
    None = 0,
    Ptr = 1,
};

/**
 * Tagged value carried inside `RustResult`. Only `Ptr` (a `*mut c_void`
 * handle) and `None` are used today; if a future FFI needs to return an
 * owned array or scalar, add a new variant rather than reviving the older
 * `Array` / `U64` tags. The C++ side already switches on `ValueTag`, so
 * adding new variants is a localized change.
 */
struct Value {
    ValueTag tag;
    void* ptr;
};

/**
 * Result handed across the FFI for any fallible call.
 *
 * On success: `success = true`, `value` carries data per its `tag`, `error`
 * is null. On failure: `success = false`, `value.tag = None`, `error` points
 * to a heap-allocated NUL-terminated UTF-8 string. **Always** call
 * `free_rust_result` exactly once on every result the FFI returned to you.
 */
struct RustResult {
    bool success;
    Value value;
    const char* error;
};

/**
 * Owned `Vec<u32>` flattened to a (ptr, len, cap) triple. The `u32` width
 * matches Roaring bitmap's element type so the C++ side can do
 * `roaring->addMany(arr.len, arr.ptr)` without intermediate casting. Must
 * be released via `tantivy_free_u32_array` (which reconstructs the `Vec`
 * and drops it).
 */
struct RustU32Array {
    uint32_t* ptr;
    uintptr_t len;
    uintptr_t cap;
};

/**
 * A `{ptr, len}` slice passed from C++ to Rust. Does not own the memory.
 */
struct FFISlice {
    const uint8_t* ptr;
    uintptr_t len;
};

/**
 * Owned `Vec<f32>` flattened to a (ptr, len, cap) triple, kept PARALLEL to a
 * `RustU32Array` of row ids: `scores[i]` is the BM25 relevance score of
 * `row_ids[i]`. Must be released via `tantivy_free_f32_array`.
 */
struct RustF32Array {
    float* ptr;
    uintptr_t len;
    uintptr_t cap;
};

/**
 * C callback that appends a block of BE row ids into the caller-owned bitmap
 * (the C++ side does `roaring::Roaring::addMany`).
 * `set_bitset` callback so tantivy hits stream straight into the result bitmap
 * without a `Vec<u32>` round-trip.
 */
using SetBitmapFn = uintptr_t(*)(void *ctx, const uint32_t *ids, uintptr_t len);

/**
 * Owned array of NUL-terminated C strings. Must be released via
 * `tantivy_free_string_array`.
 */
struct RustStringArray {
    char** ptr;
    uintptr_t len;
};

/**
 * One structured analyzer token. String pointers are owned by the containing
 * `RustTokenArray` and released by `tantivy_free_token_array`.
 */
struct RustToken {
  char *term;
  uintptr_t position;
  uintptr_t position_length;
  uintptr_t start_offset;
  uintptr_t end_offset;
  char *token_type;
};

/**
 * Owned structured token array returned by analyzer-detail FFI calls.
 */
struct RustTokenArray {
  RustToken *ptr;
  uintptr_t len;
};

/**
 * Submit a joinable task to the BE pool. Takes ownership of `task` (opaque
 * boxed closure). Returns an opaque handle to be passed to the join callback.
 */
using TantivyPoolSubmitFn = void* (*)(void* task);

/**
 * Submit a fire-and-forget task to the BE pool. Takes ownership of `task`.
 */
using TantivyPoolSubmitDetachedFn = void (*)(void* task);

/**
 * Block until the joinable task behind `handle` completes, then free `handle`.
 */
using TantivyPoolJoinFn = void (*)(void* handle);

extern "C" {

/**
 * Open an existing tantivy index at `path` and return a reader handle.
 * `field_name` must match the field used at write time.
 *
 * SAFETY: `path` and `field_name` must be valid NUL-terminated C strings.
 */
RustResult tantivy_load_index_reader(const char *path,
                                     const char *field_name,
                                     const char *tokenizer_name,
                                     const char *analyzer_digest);

/**
 * Open an index from a compound `.idx` file via PullDirectory.
 *
 * `ra_file_handle` is a C++ `RandomAccessFile*` (opaque pointer).
 * `file_table_json` is a NUL-terminated JSON string mapping filename to
 * `{"offset": u64, "length": u64}`.
 * `field_name` is the tantivy text field name.
 *
 * Returns a `IndexReaderWrapper*` in `RustResult.value.ptr`. The returned
 * handle is interchangeable with handles from `tantivy_load_index_reader`:
 * callers consume it via `tantivy_term_query` / `tantivy_match_query` /
 * `tantivy_match_all_query` / `tantivy_phrase_match_query` and release it
 * via `tantivy_free_index_reader`.
 *
 * SAFETY: `ra_file_handle` must be a valid pointer whose lifetime exceeds
 * the returned reader. `file_table_json` and `field_name` must be valid
 * NUL-terminated C strings.
 */
RustResult tantivy_open_compound_reader(void *ra_file_handle,
                                        const char *file_table_json,
                                        const char *field_name,
                                        const char *tokenizer_name,
                                        const char *analyzer_digest);

/**
 * Single-term query. Matching row ids are written into `*out`. Caller MUST
 * release `*out` via `tantivy_free_u32_array`.
 *
 * SAFETY: `reader` and `out` must be non-NULL; `term` must be NUL-terminated.
 */
RustResult tantivy_term_query(const void* reader, const uint8_t* term_ptr, uintptr_t term_len, RustU32Array* out);

/**
 * MATCH_ANY query: returns rows matching ANY of `terms`.
 *
 * SAFETY: `reader`, `out` non-NULL; `terms` is a `count`-array of NUL-
 * terminated C strings (or `count == 0`).
 */
RustResult tantivy_match_query(const void* reader, const FFISlice* terms, uintptr_t count, RustU32Array* out);

/**
 * MATCH_ALL query: returns rows matching ALL of `terms`.
 *
 * SAFETY: same as `tantivy_match_query`.
 */
RustResult tantivy_match_all_query(const void* reader, const FFISlice* terms, uintptr_t count, RustU32Array* out);

/**
 * MATCH_ANY query WITH BM25 scores. Fills two PARALLEL arrays:
 * `out_ids[i]` is a matching row id and `out_scores[i]` its BM25 score.
 * Caller MUST release `out_ids` via `tantivy_free_u32_array` and `out_scores`
 * via `tantivy_free_f32_array`.
 *
 * `limit > 0` pushes the SQL LIMIT into tantivy so only the top-`limit` hits by
 * score are returned (per segment); `limit == 0` returns every hit.
 *
 * `min_score`/`max_score` gate hits to the inclusive `[min, max]` BM25 range
 * (backing a `WHERE score() > c` predicate); pass `-INFINITY`/`+INFINITY` for
 * an unbounded end.
 *
 * SAFETY: `reader`, `out_ids`, `out_scores` non-NULL; `terms` is a `count`-
 * array of FFISlice (or `count == 0`).
 */
RustResult tantivy_match_query_scored(const void* reader, const FFISlice* terms, uintptr_t count, uint64_t limit,
                                      float min_score, float max_score, RustU32Array* out_ids,
                                      RustF32Array* out_scores);

/**
 * MATCH_ALL query WITH BM25 scores. Same parallel-array contract as
 * `tantivy_match_query_scored`.
 *
 * SAFETY: same as `tantivy_match_query_scored`.
 */
RustResult tantivy_match_all_query_scored(const void* reader, const FFISlice* terms, uintptr_t count, uint64_t limit,
                                          float min_score, float max_score, RustU32Array* out_ids,
                                          RustF32Array* out_scores);

/**
 * MATCH_PHRASE query: returns rows where `terms` appear in order with at
 * most `slop` positional gaps.
 *
 * SAFETY: same as `tantivy_match_query`.
 */
RustResult tantivy_phrase_match_query(const void *reader,
                                      const FFISlice *terms,
                                      uintptr_t count,
                                      const uint32_t *positions,
                                      uint32_t slop,
                                      RustU32Array *out);

/**
 * MATCH_WILDCARD query: returns rows whose indexed term matches the SQL
 * `LIKE` / `MATCH` pattern. `%` and `*` are equivalent multi-char wildcards
 *
 * SAFETY: `reader` and `out` must be non-NULL; `pattern_ptr` may be NULL
 * only when `pattern_len == 0`.
 */
RustResult tantivy_wildcard_query(const void* reader, const uint8_t* pattern_ptr, uintptr_t pattern_len,
                                  RustU32Array* out);

/**
 * EQUAL / single-term → bitmap. SAFETY: as `tantivy_term_query`; `ctx`/`append`
 * must be valid for the duration of the call.
 */
RustResult tantivy_term_query_bitmap(const void *reader,
                                     const uint8_t *term_ptr,
                                     uintptr_t term_len,
                                     uintptr_t limit,
                                     void *ctx,
                                     SetBitmapFn append);

/**
 * MATCH_ANY → bitmap. SAFETY: as `tantivy_match_query`.
 */
RustResult tantivy_match_query_bitmap(const void *reader,
                                      const FFISlice *terms,
                                      uintptr_t count,
                                      uintptr_t limit,
                                      void *ctx,
                                      SetBitmapFn append);

/**
 * MATCH_ALL → bitmap. SAFETY: as `tantivy_match_query`.
 */
RustResult tantivy_match_all_query_bitmap(const void *reader,
                                          const FFISlice *terms,
                                          uintptr_t count,
                                          double min_df_ratio,
                                          uintptr_t limit,
                                          void *ctx,
                                          SetBitmapFn append);

/**
 * MATCH_PHRASE → bitmap. SAFETY: as `tantivy_phrase_match_query`.
 */
RustResult tantivy_phrase_match_query_bitmap(const void *reader,
                                             const FFISlice *terms,
                                             uintptr_t count,
                                             const uint32_t *positions,
                                             uint32_t slop,
                                             uintptr_t limit,
                                             void *ctx,
                                             SetBitmapFn append);

/**
 * MATCH_WILDCARD → bitmap. SAFETY: as `tantivy_wildcard_query`.
 */
RustResult tantivy_wildcard_query_bitmap(const void *reader,
                                         const uint8_t *pattern_ptr,
                                         uintptr_t pattern_len,
                                         uintptr_t limit,
                                         void *ctx,
                                         SetBitmapFn append);

/**
 * Release a reader handle. Safe on NULL.
 *
 * SAFETY: `reader` must be NULL or have been returned by
 * `tantivy_load_index_reader` and not previously freed.
 */
void tantivy_free_index_reader(void* reader);

/**
 * Create a fresh tantivy index at `path` with one TEXT field named
 * `field_name` and the analyzer chain identified by `tokenizer`. Returns an
 * opaque writer handle in `RustResult.value.ptr`. Caller MUST release with
 * `tantivy_free_index_writer` and the result with `free_rust_result`.
 *
 * `support_phrase`: when true, term positions are stored (needed for phrase
 * queries). When false, only term frequencies are stored (smaller on disk).
 *
 * `support_bm25`: when true, per-document fieldnorms are stored (needed for
 * BM25 length normalization). When false, fieldnorms are omitted.
 *
 * `memory_budget_bytes`: memory budget for the IndexWriter. 0 uses the
 * compile-time default (256 MB).
 *
 * `num_threads`: number of tantivy indexing worker threads. 0 uses the
 * compile-time default (1). Workers run on the shared BE thread pool.
 *
 * `merge_policy`: NUL-terminated C string selecting the merge policy.
 * `"no_merge"` disables merging; anything else uses `LogMergePolicy`.
 * NULL is treated as `"default"`.
 *
 * SAFETY: `path`, `field_name`, `tokenizer` must be valid NUL-terminated
 * C strings. `merge_policy` may be NULL.
 */
RustResult tantivy_create_index_writer(const char *path,
                                       const char *field_name,
                                       const char *tokenizer,
                                       const char *analyzer_digest,
                                       bool support_phrase,
                                       bool support_bm25,
                                       uintptr_t memory_budget_bytes,
                                       uintptr_t num_threads,
                                       const char *merge_policy);

/**
 * Append a batch of UTF-8 strings as documents in order. `values_ptr` is an
 * array of `count` `FFISlice` structs. A slice with NULL ptr is treated as
 * an empty placeholder doc so doc-id alignment is preserved across null rows.
 *
 * SAFETY: `writer` must be a non-NULL writer handle; `values_ptr` must be a
 * non-NULL array of `count` `FFISlice` structs (or `count == 0`).
 */
RustResult tantivy_index_add_strings_batch(void* writer, const FFISlice* values_ptr, uintptr_t count);

/**
 * Flush queued docs to disk. After success, a freshly loaded reader will
 * observe the new docs.
 *
 * SAFETY: same as `tantivy_index_add_strings_batch`.
 */
RustResult tantivy_commit_index(void* writer);

/**
 * Release a writer handle. Safe on NULL.
 *
 * SAFETY: `writer` must be NULL or have been returned by
 * `tantivy_create_index_writer` and not previously freed.
 */
void tantivy_free_index_writer(void* writer);

/**
 * Release a `RustResult` and any owned content it carries.
 *
 * - On failure result, releases the heap-allocated error string.
 * - `Ptr` variant carries an opaque handle whose lifetime is managed
 *   separately (via the dedicated free helpers); `None` carries no memory.
 *
 * SAFETY: `result` must be a value previously produced by this crate. Calling
 * this twice on the same logical result is undefined behavior.
 */
void free_rust_result(RustResult result);

/**
 * Release a `RustU32Array` produced by a query FFI. Safe on a NULL/empty
 * array (treated as a no-op).
 *
 * SAFETY: `array` must be a value previously produced by `RustU32Array::from_vec`.
 */
void tantivy_free_u32_array(RustU32Array array);

/**
 * Release a `RustF32Array` produced by a scored query FFI. Safe on a
 * NULL/empty array (treated as a no-op).
 *
 * SAFETY: `array` must be a value previously produced by `RustF32Array::from_vec`.
 */
void tantivy_free_f32_array(RustF32Array array);

/**
 * Release a `RustStringArray` produced by `RustStringArray::from_strings`.
 *
 * SAFETY: `array` must have been produced by `RustStringArray::from_strings`
 * and not previously freed.
 */
void tantivy_free_string_array(RustStringArray array);

/**
 * Release a `RustTokenArray` produced by `RustTokenArray::from_tokens`.
 */
void tantivy_free_token_array(RustTokenArray array);

/**
 * Validate and canonicalize an analyzer definition or legacy tokenizer name.
 * Returns `[canonical_json, sha256_digest]` in `out`.
 */
RustResult tantivy_analyzer_canonicalize(const char *definition, RustStringArray *out);

/**
 * Create a reusable analyzer handle. `expected_digest` may be NULL/empty.
 */
RustResult tantivy_create_analyzer(const char *definition, const char *expected_digest);

void tantivy_free_analyzer(void *analyzer);

/**
 * Retain an analyzer by returning an independently owned handle that uses the
 * same immutable pipeline. The returned pointer must be released with
 * `tantivy_free_analyzer`.
 */
RustResult tantivy_retain_analyzer(const void *analyzer);

RustResult tantivy_analyzer_tokenize(const void *analyzer,
                                     const uint8_t *text_ptr,
                                     uintptr_t text_len,
                                     RustStringArray *out);

RustResult tantivy_analyzer_tokenize_detail(const void *analyzer,
                                            const uint8_t *text_ptr,
                                            uintptr_t text_len,
                                            RustTokenArray *out);

RustResult tantivy_tokenize(const char *tokenizer_name,
                            const uint8_t *text_ptr,
                            uintptr_t text_len,
                            RustStringArray *out);

/**
 * Run and drop a task previously handed to the BE pool. Called by the BE pool
 * on a pool thread (with the captured mem tracker already installed).
 *
 * SAFETY: `task` must be a pointer produced by this crate's spawner and not
 * previously run or dropped.
 */
void tantivy_binding_run_pool_task(void* task);

/**
 * Drop a queued task without running it. Called by the BE pool for tasks still
 * pending when the pool shuts down, so the boxed closure is not leaked.
 *
 * SAFETY: same as [`tantivy_binding_run_pool_task`].
 */
void tantivy_binding_drop_pool_task(void* task);

/**
 * Install the BE thread pool as tantivy's global spawner. Call once at BE
 * startup, after the pool exists and before any tantivy index writer is
 * created. A later call replaces the previous spawner.
 *
 * SAFETY: the three callbacks must be valid for the entire process lifetime
 * and implement the ownership contract described in this module.
 */
void tantivy_binding_init_thread_pool(TantivyPoolSubmitFn submit, TantivyPoolSubmitDetachedFn submit_detached,
                                      TantivyPoolJoinFn join);

} // extern "C"

} // namespace starrocks::tantivy_binding

#endif // TANTIVY_BINDING_H
