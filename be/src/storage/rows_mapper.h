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

// Iterator for rows mapper file
// WHY: Streaming read of mapper file to avoid loading entire mapping into memory,
// which is critical for large compactions (can save GBs of memory).
class RowsMapperIterator {
public:
    RowsMapperIterator() {}
    ~RowsMapperIterator();
    // Open file and prepare internal state.
    // IMPORTANT: Now accepts FileInfo instead of string to support both local and remote storage.
    // For remote storage (S3/HDFS), having file size upfront avoids expensive get_size() calls.
    Status open(const FileInfo& filename);
    // get `fetch_cnt` rows and move to next position
    Status next_values(size_t fetch_cnt, std::vector<uint64_t>* rssid_rowids);
    // Must be called when iterator end.
    Status status();

private:
    inline static const size_t EACH_ROW_SIZE = 8; // 8 bytes

private:
    std::unique_ptr<RandomAccessFile> _rfile;
    uint64_t _row_count = 0;
    // Point to the next position that we want to read
    uint64_t _pos = 0;
    // Current checksum and expected checksum, will check if mismatch when iterator end
    uint32_t _expected_checksum = 0;
    uint32_t _current_checksum = 0;
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