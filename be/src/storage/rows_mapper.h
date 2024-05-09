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

// rows mapper file's name for local table
std::string local_rows_mapper_filename(Tablet* tablet, const std::string& rowset_id);

} // namespace starrocks