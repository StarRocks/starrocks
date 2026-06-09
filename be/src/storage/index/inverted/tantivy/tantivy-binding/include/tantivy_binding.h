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
#include <ostream>
#include <new>

namespace starrocks::tantivy_binding {

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
  void *ptr;
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
  const char *error;
};

/**
 * Owned `Vec<u32>` flattened to a (ptr, len, cap) triple. The `u32` width
 * matches Roaring bitmap's element type so the C++ side can do
 * `roaring->addMany(arr.len, arr.ptr)` without intermediate casting. Must
 * be released via `tantivy_free_u32_array` (which reconstructs the `Vec`
 * and drops it).
 */
struct RustU32Array {
  uint32_t *ptr;
  uintptr_t len;
  uintptr_t cap;
};

/**
 * A `{ptr, len}` slice passed from C++ to Rust. Does not own the memory.
 */
struct FFISlice {
  const uint8_t *ptr;
  uintptr_t len;
};

/**
 * Owned `Vec<f32>` flattened to a (ptr, len, cap) triple, kept PARALLEL to a
 * `RustU32Array` of row ids: `scores[i]` is the BM25 relevance score of
 * `row_ids[i]`. Must be released via `tantivy_free_f32_array`.
 */
struct RustF32Array {
  float *ptr;
  uintptr_t len;
  uintptr_t cap;
};

/**
 * Owned array of NUL-terminated C strings. Must be released via
 * `tantivy_free_string_array`.
 */
struct RustStringArray {
  char **ptr;
  uintptr_t len;
};

extern "C" {

/**
 * Open an existing tantivy index at `path` and return a reader handle.
 * `field_name` must match the field used at write time.
 *
 * SAFETY: `path` and `field_name` must be valid NUL-terminated C strings.
 */
RustResult tantivy_load_index_reader(const char *path,
                                     const char *field_name,
                                     const char *tokenizer_name);

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
                                        const char *tokenizer_name);

/**
 * Single-term query. Matching row ids are written into `*out`. Caller MUST
 * release `*out` via `tantivy_free_u32_array`.
 *
 * SAFETY: `reader` and `out` must be non-NULL; `term` must be NUL-terminated.
 */
RustResult tantivy_term_query(const void *reader,
                              const uint8_t *term_ptr,
                              uintptr_t term_len,
                              RustU32Array *out);

/**
 * MATCH_ANY query: returns rows matching ANY of `terms`.
 *
 * SAFETY: `reader`, `out` non-NULL; `terms` is a `count`-array of NUL-
 * terminated C strings (or `count == 0`).
 */
RustResult tantivy_match_query(const void *reader,
                               const FFISlice *terms,
                               uintptr_t count,
                               RustU32Array *out);

/**
 * MATCH_ALL query: returns rows matching ALL of `terms`.
 *
 * SAFETY: same as `tantivy_match_query`.
 */
RustResult tantivy_match_all_query(const void *reader,
                                   const FFISlice *terms,
                                   uintptr_t count,
                                   RustU32Array *out);

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
RustResult tantivy_match_query_scored(const void *reader,
                                      const FFISlice *terms,
                                      uintptr_t count,
                                      uint64_t limit,
                                      float min_score,
                                      float max_score,
                                      RustU32Array *out_ids,
                                      RustF32Array *out_scores);

/**
 * MATCH_ALL query WITH BM25 scores. Same parallel-array contract as
 * `tantivy_match_query_scored`.
 *
 * SAFETY: same as `tantivy_match_query_scored`.
 */
RustResult tantivy_match_all_query_scored(const void *reader,
                                          const FFISlice *terms,
                                          uintptr_t count,
                                          uint64_t limit,
                                          float min_score,
                                          float max_score,
                                          RustU32Array *out_ids,
                                          RustF32Array *out_scores);

/**
 * MATCH_PHRASE query: returns rows where `terms` appear in order with at
 * most `slop` positional gaps.
 *
 * SAFETY: same as `tantivy_match_query`.
 */
RustResult tantivy_phrase_match_query(const void *reader,
                                      const FFISlice *terms,
                                      uintptr_t count,
                                      uint32_t slop,
                                      RustU32Array *out);

/**
 * MATCH_WILDCARD query: returns rows whose indexed term matches the SQL
 * `LIKE` / `MATCH` pattern. `%` and `*` are equivalent multi-char wildcards
 *
 * SAFETY: `reader` and `out` must be non-NULL; `pattern_ptr` may be NULL
 * only when `pattern_len == 0`.
 */
RustResult tantivy_wildcard_query(const void *reader,
                                  const uint8_t *pattern_ptr,
                                  uintptr_t pattern_len,
                                  RustU32Array *out);

/**
 * Release a reader handle. Safe on NULL.
 *
 * SAFETY: `reader` must be NULL or have been returned by
 * `tantivy_load_index_reader` and not previously freed.
 */
void tantivy_free_index_reader(void *reader);

/**
 * Create a fresh tantivy index at `path` with one TEXT field named
 * `field_name` and the analyzer chain identified by `tokenizer`. Returns an
 * opaque writer handle in `RustResult.value.ptr`. Caller MUST release with
 * `tantivy_free_index_writer` and the result with `free_rust_result`.
 *
 * SAFETY: `path`, `field_name`, `tokenizer` must be valid NUL-terminated
 * C strings.
 */
RustResult tantivy_create_index_writer(const char *path,
                                       const char *field_name,
                                       const char *tokenizer);

/**
 * Append a batch of UTF-8 strings as documents in order. `values_ptr` is an
 * array of `count` `FFISlice` structs. A slice with NULL ptr is treated as
 * an empty placeholder doc so doc-id alignment is preserved across null rows.
 *
 * SAFETY: `writer` must be a non-NULL writer handle; `values_ptr` must be a
 * non-NULL array of `count` `FFISlice` structs (or `count == 0`).
 */
RustResult tantivy_index_add_strings_batch(void *writer,
                                           const FFISlice *values_ptr,
                                           uintptr_t count);

/**
 * Flush queued docs to disk. After success, a freshly loaded reader will
 * observe the new docs.
 *
 * SAFETY: same as `tantivy_index_add_strings_batch`.
 */
RustResult tantivy_commit_index(void *writer);

/**
 * Release a writer handle. Safe on NULL.
 *
 * SAFETY: `writer` must be NULL or have been returned by
 * `tantivy_create_index_writer` and not previously freed.
 */
void tantivy_free_index_writer(void *writer);

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

RustResult tantivy_tokenize(const char *tokenizer_name,
                            const uint8_t *text_ptr,
                            uintptr_t text_len,
                            RustStringArray *out);

}  // extern "C"

}  // namespace starrocks::tantivy_binding

#endif  // TANTIVY_BINDING_H
