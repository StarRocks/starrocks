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

#include <gtest/gtest.h>

#include <deque>
#include <future>
#include <utility>
#include <vector>

#include "common/status.h"
#include "fs/fs.h"
#include "gen_cpp/persistent_index.pb.h"

namespace starrocks {

class WritableFile;
class RandomAccessFile;
class Tablet;

namespace lake {
class TabletManager;
} // namespace lake

// Build the connection between input rowsets' rows and ouput rowsets' rows,
// and then write to file.
//
// WHY: During primary key compaction, we need to track which rows from input rowsets
// map to which rows in output rowsets. This mapping is critical for maintaining
// consistency in primary key tables when resolving conflicts across concurrent transactions.
//
// Format: [rssid & rowid list, row count (8 bytes), Checksum(4 bytes)]
//
class RowsMapperBuilder {
public:
    RowsMapperBuilder(const std::string& filename) : _filename(filename) {}
    ~RowsMapperBuilder() {}
    // append rssid rowids to file
    Status append(const std::vector<uint64_t>& rssid_rowids);
    Status finalize();
    // Returns FileInfo containing both path and size for remote storage support.
    // WHY: Remote storage (S3/HDFS) may need file size to avoid extra metadata calls,
    // improving performance during parallel pk index execution.
    FileInfo file_info() const;

private:
    Status _init();

private:
    std::string _filename;
    std::unique_ptr<WritableFile> _wfile;
    uint32_t _checksum = 0;
    uint64_t _row_count = 0;
};

// Iterator for rows mapper file.
//
// Two operating modes:
//
//   (1) Sequential fallback (no `prepare_segments` call): every `next_values`
//       does a direct `read_at_fully` on the iterator's RAF. Safe and simple
//       but pays per-call OSS RPC cost.
//
//   (2) Pipelined sub-chunk (after `prepare_segments(segment_row_counts)`):
//       caller pre-declares every segment's row count. The iterator splits each
//       segment into fixed-size sub-chunks (= config::lake_rows_mapper_sub_chunk_bytes,
//       never spanning a segment boundary) and submits the first K
//       (= config::lake_rows_mapper_read_parallelism) sub-chunks to
//       `pk_index_execution_thread_pool` immediately, each on its own
//       RandomAccessFile so the underlying file class never sees concurrent
//       access. `next_values(seg_n)` consumes every sub-chunk belonging to the
//       current segment in order — memcpy-concatenating into the caller's
//       buffer (the single-sub-chunk case keeps the zero-copy swap fast path).
//       After each pop the iterator submits the next sub-chunk so K reads stay
//       in flight.
//
// Memory bound (pipelined mode): K × sub_chunk_bytes (independent of segment
// size). Caller's output buffer additionally holds one full segment during
// consumption.
//
class RowsMapperIterator {
public:
    RowsMapperIterator() {}
    ~RowsMapperIterator();
    // Open file and prepare internal state.
    // IMPORTANT: Now accepts FileInfo instead of string to support both local and remote storage.
    // For remote storage (S3/HDFS), having file size upfront avoids expensive get_size() calls.
    Status open(const FileInfo& filename);
    // Switch the iterator into pipelined per-segment mode (optional). Caller
    // declares every following `next_values(fetch_cnt)` call's `fetch_cnt`
    // upfront in order. The iterator immediately submits the first
    // min(K, segments) parallel reads — one per declared segment, each on its
    // own RandomAccessFile. Sum of `segment_row_counts` must equal the file's
    // total row count.
    Status prepare_segments(const std::vector<size_t>& segment_row_counts);
    // get `fetch_cnt` rows and move to next position
    Status next_values(size_t fetch_cnt, std::vector<uint64_t>* rssid_rowids);
    // Must be called when iterator end.
    Status status();

private:
    inline static const size_t EACH_ROW_SIZE = 8; // 8 bytes

    // Submit the read for `_next_sub_chunk_to_submit` if there's still a sub-chunk
    // remaining. Each submission opens its own RandomAccessFile so concurrent
    // tasks never touch shared file-class state.
    Status _maybe_submit_next();
    // Block on every still-in-flight chunk. Called from the destructor (tasks
    // hold raw pointers into chunk-owned vectors) and from any error path that
    // wants to release pending reads cleanly.
    void _drain_in_flight();

    struct InFlightChunk {
        size_t segment_idx = 0;
        size_t row_offset_in_segment = 0; // first row's offset within its segment (rows)
        size_t row_count = 0;
        int64_t file_offset = 0;              // byte offset within the file's body
        std::vector<uint64_t> data;           // owned; SWAPPED into caller when segment is single-sub-chunk
        std::unique_ptr<RandomAccessFile> rf; // own RAF — no shared file-class state across tasks
        std::shared_future<Status> future;
    };

    struct SubChunkDescriptor {
        size_t segment_idx;
        size_t row_offset_in_segment; // rows
        size_t row_count;             // rows
        int64_t file_offset;          // bytes
    };

private:
    std::unique_ptr<RandomAccessFile> _rfile; // sequential-fallback handle; also used for footer reads
    std::string _path;                        // remembered so per-chunk RAFs can be opened in pipelined mode
    uint64_t _row_count = 0;
    // Point to the next position that we want to read
    uint64_t _pos = 0;
    // Current checksum and expected checksum, will check if mismatch when iterator end
    uint32_t _expected_checksum = 0;
    uint32_t _current_checksum = 0;

    // Pipelined sub-chunk mode state (populated by `prepare_segments`).
    std::vector<size_t> _segment_sizes;
    std::vector<SubChunkDescriptor> _sub_chunks;  // flat list, sub-chunks ordered by (segment, row_offset)
    std::vector<size_t> _segment_sub_chunk_begin; // begin[i..i+1) = sub-chunk range for segment i
    size_t _next_sub_chunk_to_submit = 0;         // index into _sub_chunks
    size_t _next_segment_to_serve = 0;            // index into _segment_sizes
    std::deque<InFlightChunk> _in_flight;         // FIFO of in-flight sub-chunks
    bool _pipelined = false;                      // true after a successful prepare_segments
};

// rows mapper file's name for lake table
// WHY: Lake tables support both local and remote storage strategies for mapper files.
// The choice depends on config::enable_pk_index_parallel_execution and performance tradeoffs.

// Local filesystem storage (legacy path, used when parallel execution is disabled)
// WHY: Uses local disk for faster I/O but limited to single-node access
StatusOr<std::string> lake_rows_mapper_filename(int64_t tablet_id, int64_t txn_id);

// Remote storage access for existing lcrm files
// WHY: During parallel pk index execution, multiple nodes may need to read the same mapper file.
// Storing on remote storage (S3/HDFS) enables distributed access without file copying.
StatusOr<std::string> lake_rows_mapper_filename(lake::TabletManager* mgr, int64_t tablet_id,
                                                const std::string& lcrm_file);

// Create new mapper file (chooses storage based on config)
// WHY: Automatically selects between local disk (fast, single-node) and remote storage
// (slower, multi-node accessible) based on enable_pk_index_parallel_execution config.
// This allows transparent switching between execution modes without code changes.
StatusOr<std::string> new_lake_rows_mapper_filename(lake::TabletManager* mgr, int64_t tablet_id, int64_t txn_id);

// rows mapper file's name for local table
std::string local_rows_mapper_filename(Tablet* tablet, const std::string& rowset_id);

} // namespace starrocks