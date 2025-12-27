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
#include "gen_cpp/persistent_index.pb.h"

namespace starrocks {

class WritableFile;
class RandomAccessFile;
class Tablet;

// Build the connection between input rowsets' rows and ouput rowsets' rows,
// and then write to file.
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

private:
    Status _init();

private:
    std::string _filename;
    std::unique_ptr<WritableFile> _wfile;
    uint32_t _checksum = 0;
    uint64_t _row_count = 0;
};

// Iterator for rows mapper file
class RowsMapperIterator {
public:
    RowsMapperIterator() {}
    ~RowsMapperIterator();
    // Open file, and prepare internal state
    Status open(const std::string& filename);
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
StatusOr<std::string> lake_rows_mapper_filename(int64_t tablet_id, int64_t txn_id);

// rows mapper file's name for lake table with subtask_id (for parallel compaction)
StatusOr<std::string> lake_rows_mapper_filename(int64_t tablet_id, int64_t txn_id, int32_t subtask_id);

// Get the row count from a rows mapper file. Returns 0 if file doesn't exist or is invalid.
StatusOr<uint64_t> lake_rows_mapper_row_count(int64_t tablet_id, int64_t txn_id);

// Get the total row count from multiple rows mapper files (for parallel compaction).
// Returns the sum of row counts from all subtask files that exist.
StatusOr<uint64_t> lake_rows_mapper_row_count(int64_t tablet_id, int64_t txn_id, int32_t subtask_count);

// Get the total row count from rows mapper files for specific successful subtask IDs.
// This is used for partial success scenarios where only a subset of subtasks succeeded.
StatusOr<uint64_t> lake_rows_mapper_row_count(int64_t tablet_id, int64_t txn_id,
                                              const std::vector<int32_t>& success_subtask_ids);

// Delete all rows mapper files for parallel compaction subtasks.
// This is called in publish_primary_compaction when light_publish is NOT used,
// to prevent disk space leaks. When light_publish IS used, MultiRowsMapperIterator
// handles file deletion via set_delete_files_on_close(true).
void delete_lake_rows_mapper_files(int64_t tablet_id, int64_t txn_id, int32_t subtask_count);

// rows mapper file's name for local table
std::string local_rows_mapper_filename(Tablet* tablet, const std::string& rowset_id);

// Iterator for reading multiple rows mapper files in order (for parallel compaction).
// Files are read in subtask_id order (0, 1, 2, ...) which matches the segment order
// in the merged txn_log.
//
// File deletion behavior:
// - By default, files are NOT deleted when the iterator is destroyed (_delete_files_on_close = false).
// - Call set_delete_files_on_close(true) to enable automatic file deletion in destructor.
// - The caller (e.g., PrimaryKeyCompactionConflictResolver::execute()) should enable this
//   after successfully opening files to ensure cleanup in both success and failure paths.
// - When light_publish is NOT used, publish_primary_compaction cleans up files explicitly
//   via delete_lake_rows_mapper_files() to prevent disk space leaks.
class MultiRowsMapperIterator {
public:
    MultiRowsMapperIterator() = default;
    ~MultiRowsMapperIterator();

    // Open multiple files based on tablet_id, txn_id, and subtask_count.
    // Files are opened in subtask_id order (0, 1, 2, ...).
    Status open(int64_t tablet_id, int64_t txn_id, int32_t subtask_count);

    // Open multiple files for specific successful subtask IDs.
    // This is used for partial success scenarios where only a subset of subtasks succeeded.
    // Files are opened in the order specified by success_subtask_ids.
    Status open(int64_t tablet_id, int64_t txn_id, const std::vector<int32_t>& success_subtask_ids);

    // Get `fetch_cnt` rows and move to next position.
    // Automatically crosses file boundaries when needed.
    Status next_values(size_t fetch_cnt, std::vector<uint64_t>* rssid_rowids);

    // Must be called when iterator ends to verify checksums.
    Status status();

    // Get total row count across all files
    uint64_t total_row_count() const { return _total_row_count; }

    // Set whether to delete files when the iterator is destroyed.
    // Default is false for MultiRowsMapperIterator (unlike RowsMapperIterator).
    void set_delete_files_on_close(bool delete_files) { _delete_files_on_close = delete_files; }

private:
    inline static const size_t EACH_ROW_SIZE = 8; // 8 bytes

    struct FileInfo {
        std::unique_ptr<RandomAccessFile> rfile;
        uint64_t row_count = 0;
        uint64_t pos = 0;
        uint32_t expected_checksum = 0;
        uint32_t current_checksum = 0;
    };

    std::vector<FileInfo> _files;
    size_t _current_file_idx = 0;
    uint64_t _total_row_count = 0;
    bool _delete_files_on_close = false; // Default: don't delete files
};

} // namespace starrocks