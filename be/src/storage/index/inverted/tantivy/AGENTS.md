# AGENTS.md - Tantivy Full-Text Search Integration

> Module-specific guidelines for the BE ↔ tantivy Rust-C++ bridge.
> This module lets the BE drive a [tantivy](https://github.com/quickwit-oss/tantivy) inverted index
> through the GIN (Generalized Inverted Index) framework. It coexists with the CLucene and Builtin
> implementations under `be/src/storage/index/inverted/`.

## Architecture Overview

This module implements a Rust ↔ C++ FFI bridge so the BE can write and query a tantivy index
without process-level Rust toolchain coupling: the Rust crate is compiled to `libtantivy_binding.a`
(staticlib) and statically linked into `starrocks_be`. cbindgen generates a C header that the BE
includes directly.

### Read path

```
SQL: SELECT ... WHERE col MATCH_ANY 'fox'
         │
     ColumnExprPredicate::seek_inverted_index()
         │
     TantivyInvertedReader::query()                ← C++ plugin
         │  FFI
     tantivy_match_query() / tantivy_term_query()  ← Rust FFI thunk (unified entry)
         │
     PullDirectory → sr_random_access_read()       ← FFI callback to C++
         │
     RandomAccessFile::read_at_fully()             ← StarletFS / BlockCache transparent
         │
     S3 / COS / local disk
```

The same FFI entry points (`tantivy_term_query`, `tantivy_match_query`, …) serve both the local
directory reader (single-segment debug / unit-test path) and the production compound `.idx` reader.
Polymorphism happens inside `IndexReaderWrapper::open()` via the `tantivy::Directory` trait
(`MmapDirectory` for local, `PullDirectory` for compound).

### Write path

```
SegmentWriter::append_chunk()
  └→ ColumnWriter::append()
      └→ TantivyInvertedWriter::add_values()       ← FFI: tantivy_index_add_strings_batch
      └→ TantivyInvertedWriter::add_nulls()        ← Roaring null bitmap
SegmentWriter::finalize_columns()
  └→ TantivyInvertedWriter::finish_compound()
      ├─ FFI: tantivy_finish_index() → IndexWriter::commit()
      ├─ Serialize null bitmap as _starrocks_null_bitmap sidecar
      └─ Return CompoundIndexEntry (list of files + metadata)
  └→ CompoundIndexFileWriter::pack(entries) → single .idx file
  └→ Remove .ivt temp directories
```

## Compound `.idx` File Format

All tantivy index files for a segment are packed into a single `.idx` file (one per segment,
covering all tantivy-indexed columns). This avoids small file proliferation on object storage.

```
Header (offset 0):
  u32  magic     = "SRCB" (0x53524342)
  u32  version   = 1
  u32  num_indices
  [per-index entry × num_indices]:
    u8   kind         (1 = INVERTED)
    u8[3] reserved
    u64  index_id
    u32  suffix_len, bytes (utf-8)
    u32  num_files
    [per-file × num_files]:
      u32  name_len, bytes (utf-8)
      u64  offset     (absolute position in .idx)
      u64  length

Data region:
  [file bytes concatenated in pack order]
```

Path convention: `{rowset_id}_{segment_id}.idx` alongside the `.dat` segment file.

## Layout

```
be/src/storage/index/inverted/tantivy/
├── AGENTS.md                        ← this file
├── CMakeLists.txt                   ← invokes cargo build
├── tantivy_plugin.{h,cpp}           ← InvertedPlugin implementation
├── tantivy_inverted_writer.{h,cpp}  ← InvertedWriter (write + finish_compound)
├── tantivy_inverted_reader.{h,cpp}  ← InvertedReader (query via PullDirectory)
├── tantivy_ffi_guards.h             ← RAII wrappers for FFI handles
├── random_access_bridge.{h,cpp}     ← sr_random_access_read() FFI callback
└── tantivy-binding/                 ← Rust crate (staticlib)
    ├── Cargo.toml
    ├── rust-toolchain.toml          ← pinned to 1.88.0
    ├── cbindgen.toml
    ├── build.rs                     ← cbindgen invocation
    ├── include/tantivy_binding.h    ← generated C header
    └── src/
        ├── lib.rs
        ├── error.rs
        ├── safe/                    ← safe Rust wrappers, no FFI
        │   ├── mod.rs
        │   ├── index_writer.rs      ← IndexWriterWrapper (single-threaded writer)
        │   ├── index_reader.rs      ← IndexReaderWrapper (Directory-polymorphic)
        │   ├── pull_directory.rs    ← read-only Directory backed by C++ RandomAccessFile
        │   ├── tokenizer/           ← analyzer chain
        │   │   ├── mod.rs           ← build()/tokenize() factory
        │   │   ├── cjk_bigram.rs    ← Lucene-style CJK bigram
        │   │   └── jieba.rs         ← jieba-rs dictionary segmentation
        │   └── tests/               ← unit tests for the safe layer
        │       ├── mod.rs
        │       ├── index_reader_tests.rs
        │       ├── index_writer_tests.rs
        │       └── tokenizer_tests.rs
        └── ffi/                     ← C ABI shim, all `extern "C"` lives here
            ├── mod.rs
            ├── catch.rs             ← panic-safe FFI wrapper (catch_ffi)
            ├── handle.rs            ← create_binding/as_ref/free_binding
            ├── result.rs            ← RustResult / RustU32Array / FFISlice
            ├── lifecycle_c.rs       ← free helpers
            ├── index_writer_c.rs    ← write FFI (create/add/commit/free)
            └── index_reader_c.rs    ← read FFI: load_index_reader,
                                       open_compound_reader, term/match/phrase queries
```

The `safe/` and `ffi/` split is load-bearing: safe code never speaks `extern "C"`, and FFI code
never holds tantivy state directly — handles are erased to `*mut c_void` via `create_binding`.

## Build Commands

The Rust crate is built automatically as part of `./build.sh --be --enable-shared-data`:

1. `build.sh` calls `ensure_rust_toolchain` (installs rustup if cargo is missing).
2. `build.sh` calls `update_submodule contrib/tantivy ...` to ensure the tantivy source is
   checked out.
3. CMake reaches `add_subdirectory(.../inverted/tantivy)` and runs `cargo build --release`.
4. The resulting `libtantivy_binding.a` is statically linked into the `starrocks_be` binary.

`--enable-shared-data` is required at the project level; without it CN reports
`Backend service binary was not compiled with SHARED_DATA support!`.

For fast iteration on the Rust binding only:

```bash
cd be/src/storage/index/inverted/tantivy/tantivy-binding
cargo build --release
cargo test --release
```

Do **not** attempt to fix Rust toolchain, cargo mirror, or tantivy build environment issues
inside this directory — they are container/image-level concerns. Stop and surface the failure
to the user.

## Code Style

### C++ side
Follow the [BE-wide C++ guidelines](../../../../AGENTS.md): clang-format, 4-space indent,
120-char line limit, `#pragma once`, `Status` for error handling.

C++ files in this module must include the standard Apache 2.0 license header.

### Rust side
- **Toolchain**: pinned to `1.88.0` via `rust-toolchain.toml`.
- **Edition**: 2021.
- **Allocator**: `#[global_allocator] = System` to avoid double-jemalloc conflicts with the BE.
- **FFI conventions**:
  - Every `extern "C"` function is wrapped in `catch_ffi(|| { ... })` so panics never unwind
    across the FFI boundary.
  - Errors return `RustResult` with `success: false` and an owned C string in `error`; the
    caller must free with `free_rust_result`. Use `TantivyResultGuard` on the C++ side.
  - Out-arrays use `RustU32Array { ptr, len }`; the caller frees with `tantivy_free_u32_array`
    (RAII via `TantivyU32ArrayGuard`).
  - Strings come in as `*const c_char` (NUL-terminated) or `FFISlice { ptr, len }` (non-NUL).
- **No tests inline with production code**: keep `#[test]` functions in `safe/tests/` so the
  FFI shim and the safe wrappers stay tight. See `tasks.md` history under
  `dedupe-tantivy-ffi-and-extract-tests` for the rationale.

## Key Design Decisions

- **Single-threaded IndexWriter**: `writer_with_num_threads(1, ...)` is mandatory. tantivy's
  default multi-worker writer dispatches docs to workers via a channel, and each worker assigns
  doc ids inside its own internal segment — the resulting doc-id-to-insertion-order mapping
  breaks the `tantivy DocId == BE row id` contract. One worker keeps insertion order
  deterministic; throughput cost is acceptable because BE already parallelizes at the
  tablet/segment level. See `WRITER_NUM_THREADS` in `safe/index_writer.rs`.
- **Default single-threaded query executor**: we do not call
  `Index::set_default_multithread_executor()`, so query-time merging across tantivy internal
  segments happens on the calling thread. This is intentional for now — the BE scan layer
  already fans out across tablets/segments, and adding a tantivy-side thread pool would
  contend with that. Revisit when wide-shard latency-sensitive queries become a bottleneck.
- **System allocator**: `#[global_allocator] = System` avoids double-jemalloc conflicts with
  the BE process.
- **PullDirectory**: read-only `tantivy::Directory` implementation that bridges to C++
  `RandomAccessFile` via an FFI callback, transparently leveraging StarRocks BlockCache for
  shared-data mode. See `safe/pull_directory.rs`.
- **Unified reader handle**: both `tantivy_load_index_reader` (local mmap directory) and
  `tantivy_open_compound_reader` (PullDirectory over a `.idx`) return the same opaque
  `IndexReaderWrapper*`. All query FFI entries (`tantivy_term_query`, `tantivy_match_query`,
  `tantivy_match_all_query`, `tantivy_phrase_match_query`) accept either; release with
  `tantivy_free_index_reader`.
- **Null handling**: null rows get empty-string placeholder docs to preserve doc-id alignment;
  a separate Roaring `_starrocks_null_bitmap` sidecar is serialized into the compound `.idx`
  for `IS NULL` queries.

## Tokenizers

| DDL `parser=` | Rust tokenizer constant | Algorithm |
|---|---|---|
| `english` / `standard` | `TOKENIZER_ENGLISH` | English word splitting + lowercase + stopwords |
| `chinese` | `TOKENIZER_CJK` | CJK bigram (Lucene-style overlapping bigrams) |
| `jieba` | `TOKENIZER_JIEBA` | jieba-rs dictionary segmentation |
| `none` | `TOKENIZER_RAW` | No tokenization (entire input as one token) |

CJK bigram produces overlapping tokens, so `MATCH_PHRASE` requires slop proportional to the gap
between target characters (e.g. matching "字节...推荐" with 3 intermediate bigrams needs `~3`
slop). Phrase matching for CJK indexes is opt-in via:

```sql
INDEX idx (col) USING GIN (
    "imp_lib"="tantivy", "parser"="chinese", "support_phrase"="true"
)
```

`support_phrase` defaults to `"false"` and is tantivy-only.

## Query Types

| SQL syntax | InvertedIndexQueryType | Tantivy FFI | Tantivy query |
|------------|------------------------|-------------|---------------|
| `= 'word'` | `EQUAL_QUERY` | `tantivy_term_query` | `TermQuery` |
| `MATCH_ANY 'a b'` | `MATCH_ANY_QUERY` | `tantivy_match_query` | `BooleanQuery(SHOULD)` |
| `MATCH_ALL 'a b'` | `MATCH_ALL_QUERY` | `tantivy_match_all_query` | `BooleanQuery(MUST)` |
| `MATCH_PHRASE 'a b'` | `MATCH_PHRASE_QUERY` | `tantivy_phrase_match_query` | `PhraseQuery(slop)` |
| `IS NULL` | `query_null` | (no FFI) | Roaring null bitmap sidecar |

All non-`EQUAL` queries first call `tantivy_tokenize` on the C++ side to apply the same analyzer
chain used at write time.

## Testing

### Rust unit tests

```bash
cd be/src/storage/index/inverted/tantivy/tantivy-binding
cargo test --release
```

Tests live entirely under `src/safe/tests/`, organized one file per safe module
(`index_reader_tests.rs`, `index_writer_tests.rs`, `tokenizer_tests.rs`). Adding a `#[test]`
inline inside a production module is a code-review smell — move it to `tests/`.

### BE unit tests

```bash
# Tantivy-related GTests:
./run-be-ut.sh --gtest_filter \
    "Tantivy*:TantivySmoke*:TantivyPlugin*:TantivyCompoundReader*:SegmentWriterTantivy*"
```

### End-to-end regression

The `hackernews` table (~28.7M rows in `test_tantivy`) is the canonical E2E baseline. Two
queries to run after any non-trivial change:

```sql
USE test_tantivy;
SELECT COUNT(*) FROM hackernews WHERE title MATCH_ANY 'python';        -- expect 30825
SELECT COUNT(*) FROM hackernews WHERE title MATCH_ALL 'python java';   -- expect 393
```

For DDL/parser changes, also run a small CJK table to cover `chinese` / `jieba` parsers.

## Key Files to Know

| File | Purpose |
|------|---------|
| `tantivy_plugin.{h,cpp}` | `InvertedPlugin` factory; entry point from the inverted index registry |
| `tantivy_inverted_writer.{h,cpp}` | Per-column writer; drives `add_values` → `finish_compound` |
| `tantivy_inverted_reader.{h,cpp}` | Per-column reader; dispatches by query type, owns the `RandomAccessFile` for compound mode |
| `tantivy_ffi_guards.h` | RAII wrappers for `RustResult`, `RustU32Array`, writer/reader handles |
| `tantivy-binding/src/safe/index_writer.rs` | `IndexWriterWrapper` — schema, single-thread writer |
| `tantivy-binding/src/safe/index_reader.rs` | `IndexReaderWrapper` — Directory-polymorphic reader |
| `tantivy-binding/src/safe/pull_directory.rs` | Read-only `Directory` over C++ `RandomAccessFile` |
| `tantivy-binding/src/safe/tokenizer/mod.rs` | `build()` / `tokenize()` factory by tokenizer name |
| `tantivy-binding/src/ffi/index_writer_c.rs` | Write FFI (`tantivy_index_*`, `tantivy_finish_index`) |
| `tantivy-binding/src/ffi/index_reader_c.rs` | Read FFI (`tantivy_load_index_reader`, `tantivy_open_compound_reader`, query entries) |

## Debugging Tips

- **FFI error messages**: failures bubble back as `RustResult.error` strings. The C++ side
  prefixes them with `"tantivy: "` via `tantivy_status_from_error`. Search BE logs for
  `tantivy:` to find the originating Rust error.
- **Reproducing query bugs in Rust alone**: the `IndexReaderWrapper` tests under
  `safe/tests/index_reader_tests.rs` show how to build an in-memory `RamDirectory`, load it
  with the production wrapper, and assert query results — useful for bisecting a tantivy
  upgrade independently of the BE.
- **Doc-id alignment regressions**: if `IS NULL` results disagree with `MATCH_ANY '...'` row
  counts, suspect a missing empty-string placeholder or a multi-thread writer regression.
  `WRITER_NUM_THREADS` must stay at `1`.
- **Compound `.idx` corruption**: dump the header with `xxd -l 256 <file>.idx` and check the
  `SRCB` magic + version `1`. The reader uses `CompoundIndexFileReader::open()` which validates
  the header before handing files to `PullDirectory`.

## PR Guidelines

Follow the [BE PR guidelines](../../../../AGENTS.md#pr-guidelines-for-be-changes), with these
module-specific additions:

- [ ] If FFI signatures changed, regenerate `include/tantivy_binding.h` (this happens
  automatically via `build.rs`, but commit the regenerated header).
- [ ] If the schema or `.idx` format changed, bump the version in the compound header and add a
  migration note — existing segments must remain readable.
- [ ] Run `cargo test --release` in `tantivy-binding/` and the GTest filter above.
- [ ] For non-trivial changes, run the hackernews E2E baseline (`30825` / `393`).

## License Header

All C++ **and Rust** files in this module must carry the standard StarRocks Apache 2.0 license
header at the top of the file. Place it before any `//!` crate/module doc-comment, separated by
a blank line:

```rust
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

//! Module-level doc comment goes here.
```

This applies to every `*.rs` file under `tantivy-binding/`, including `build.rs`, `mod.rs`
files, and test files under `safe/tests/`.
