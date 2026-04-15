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

#pragma once

#include <mutex>
#include <string>
#include <vector>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "gen_cpp/InternalService_types.h"
#include "util/slice.h"

namespace starrocks {

class RuntimeState;

// Phase 2 of the rejected records system table feature.
//
// RejectedRecordWriter captures rows rejected during load (type mismatch,
// strict-mode filter, NOT NULL violation, partition miss, etc.) as structured
// JSON Lines in a per-fragment local file. A later BE-resident sync daemon
// (Phase 3) reads those files, batches them, and posts them to the FE
// `_statistics_.rejected_records` system table using a merge-commit Stream
// Load so N BEs write in a single transaction.
//
// Design goals for this class:
//   * Zero BRPC / FE interaction on the import hot path -- only a local
//     file append. A sync failure never back-pressures the load.
//   * One writer per fragment. Lifecycle matches RuntimeState.
//   * Best-effort: file-open failures silently fall back to a no-op so a
//     load never aborts because bad-row capture couldn't be persisted.
//   * JSON Lines format makes downstream ingestion trivial (one record per
//     line, each line a self-contained JSON object).
//
// Three entry methods are provided because the raw data available at
// different rejection points is different:
//   * append_from_chunk  -- used by Sink / Strict-mode / columnar scanner
//                           rejections, which have a Chunk with all column
//                           values present.
//   * append_from_slices -- used by CSV parse failures, where a Chunk row
//                           has not yet been appended but the per-column
//                           raw text slices are still available.
//   * append_raw         -- used when the row cannot be split into columns
//                           at all (e.g. CSV column-count mismatch); the
//                           raw line is stored under the reserved `_raw`
//                           key.
class RejectedRecordWriter {
public:
    // Build a writer for the given RuntimeState. `state` must outlive the
    // writer; the writer pulls target database / load label / txn id /
    // backend id from `state` at each append. The file is opened lazily on
    // the first successful append so an enabled-but-never-triggered writer
    // costs only the allocation of this object.
    explicit RejectedRecordWriter(RuntimeState* state);
    ~RejectedRecordWriter();

    RejectedRecordWriter(const RejectedRecordWriter&) = delete;
    RejectedRecordWriter& operator=(const RejectedRecordWriter&) = delete;

    // Append a rejected row whose column values are fully present in a Chunk.
    //
    // `col_names` must be parallel to `chunk`'s columns. If the two sizes
    // disagree, the writer falls back to positional column names (col_0,
    // col_1, ...) so a schema-mismatch bug never silently drops the record.
    //
    // `source_info` is an opaque JSON snippet the caller prepares (for
    // example `{"file": "...", "line": 42}` for file loads or
    // `{"topic": "...", "partition": 3, "offset": 9001}` for Routine Load);
    // pass "" to omit. The writer does not validate it -- callers supply
    // it as already-serialized JSON.
    void append_from_chunk(const Chunk& chunk, size_t row_idx, const std::vector<std::string>& col_names,
                           const std::string& error_code, const std::string& error_message,
                           const std::string& error_column, const std::string& source_info);

    // Append a rejected row whose per-column raw text values are available
    // but whose Chunk has not been finalized (e.g. CSV Scanner: parsing
    // column `j` failed, columns `[0, j)` are in the Chunk but columns
    // `[j, n)` are not). The caller supplies the raw Slices directly so we
    // capture the exact input rather than whatever the Chunk happens to
    // contain.
    void append_from_slices(const std::vector<Slice>& col_values, const std::vector<std::string>& col_names,
                            const std::string& error_code, const std::string& error_message,
                            const std::string& error_column, const std::string& source_info);

    // Append a rejected row that could not be split into columns at all.
    // `raw_text` is stored as `{"_raw": "<text>"}` in the raw_record column.
    void append_raw(const std::string& raw_text, const std::string& error_code, const std::string& error_message,
                    const std::string& error_column, const std::string& source_info);

    // Absolute path of the backing JSON Lines file, or empty string if the
    // path has not been resolved yet. The sync daemon picks up files
    // under this path's directory tree by `.jsonl` suffix.
    const std::string& file_path() const { return _file_path; }

    // Flush any buffered data to disk. No-op since every append
    // open-writes-closes, retained for callers that defensively flush
    // at fragment teardown.
    void flush();

    // --- JSON builders ---
    //
    // These are pure-functional helpers exposed publicly so unit tests can
    // exercise them without instantiating a writer (which otherwise needs a
    // RuntimeState backed by an ExecEnv + LoadPathMgr). They produce the
    // JSON text stored in the `raw_record` column of the system table.

    // Build the `raw_record` JSON object from a Chunk row.
    static std::string build_json_from_chunk(const Chunk& chunk, size_t row_idx,
                                             const std::vector<std::string>& col_names);

    // Build the `raw_record` JSON object from per-column raw text Slices.
    static std::string build_json_from_slices(const std::vector<Slice>& col_values,
                                              const std::vector<std::string>& col_names);

    // Build `{"_raw": "<raw_text>"}`.
    static std::string build_json_from_raw(const std::string& raw_text);

private:
    // Resolve the backing JSON Lines path for the current fragment.
    // Creates the parent directory tree on first call. Returns an empty
    // string on any failure; callers treat that as a best-effort no-op
    // so a single load never aborts because bad-row capture couldn't
    // be persisted.
    const std::string& resolve_file_path();

    // Serialize a single JSON Lines record and append it to the file. All
    // three public entry methods converge here after building the
    // `raw_record` JSON object.
    //
    // Open-write-close each call so the sync daemon can atomically
    // rename the file out from under us without creating fd-vs-path
    // race holes. At the rejection rates we ever expect (one-in-many-
    // thousand), the syscall overhead is not measurable against the
    // cost of the rejection itself; in return we get crash-safe
    // semantics (a record that makes it past append_serialized is
    // guaranteed to be durable in the filesystem, not sitting in our
    // own stream buffer).
    void append_serialized(const std::string& raw_record_json, const std::string& error_code,
                           const std::string& error_message, const std::string& error_column,
                           const std::string& source_info);

    RuntimeState* _state;
    std::mutex _file_lock;
    std::string _file_path;
    bool _path_resolved = false;
    bool _path_resolve_failed = false;
};

} // namespace starrocks
