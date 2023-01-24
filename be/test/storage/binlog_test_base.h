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

#include "gtest/gtest.h"
#include "storage/binlog_file_reader.h"
#include "storage/binlog_file_writer.h"
#include "testutil/assert.h"

namespace starrocks {

using BinlogFileMetaPBPtr = std::shared_ptr<BinlogFileMetaPB>;

struct TestLogEntryInfo {
    LogEntryPB log_entry;
    int64_t version;
    int64_t start_seq_id;
    int64_t end_seq_id;
    FileIdPB* file_id;
    int32_t start_row_id;
    int32_t num_rows;
    bool end_of_version;
    int64_t timestamp;
};

struct DupKeyVersionInfo {
    DupKeyVersionInfo() = default;
    DupKeyVersionInfo(int64_t ver, int32_t entries, int64_t rows_per_entry, int64_t ts)
            : version(ver), num_entries(entries), num_rows_per_entry(rows_per_entry), timestamp(ts) {}

    int64_t version;
    int64_t num_entries;
    int64_t num_rows_per_entry;
    int64_t timestamp;
};

// Iterate log entries from multiple binlog files
class BinlogFileMergeReader {
public:
    BinlogFileMergeReader(std::string storage_path, std::vector<BinlogFileMetaPBPtr>& file_metas);
    Status seek(int64_t version, int64_t seq_id);
    Status next();
    LogEntryInfo* log_entry();

private:
    Status _init_file_reader(BinlogFileMetaPBPtr& file_meta, int64_t version, int64_t seq_id);

    std::string _storage_path;
    std::vector<BinlogFileMetaPBPtr>& _file_metas;
    int32_t _next_file_index;
    std::shared_ptr<BinlogFileReader> _current_reader;
};

class BinlogTestBase : public testing::Test {
protected:
    void estimate_log_entry_size(LogEntryTypePB entry_type, int32_t* estimated_size);

    std::shared_ptr<TestLogEntryInfo> build_insert_segment_log_entry(int64_t version, int64_t rowset_id, int seg_index,
                                                                     int64_t start_seq_id, int64_t num_rows,
                                                                     bool end_of_version, int64_t timestamp);

    std::shared_ptr<TestLogEntryInfo> build_empty_rowset_log_entry(int64_t version, int64_t timestamp);

    // some methods for verification
    void verify_file_id(FileIdPB* expect_file_id, FileIdPB* actual_file_id);
    void verify_log_entry(LogEntryPB* expect, LogEntryPB* actual);
    void verify_log_entry_info(const std::shared_ptr<TestLogEntryInfo>& expect, LogEntryInfo* actual);
    void verify_file_meta(BinlogFileMetaPB* expect_file_meta,
                          const std::shared_ptr<BinlogFileMetaPB>& actual_file_meta);
    void verify_seek_and_next(const std::string& file_path, const std::shared_ptr<BinlogFileMetaPB>& file_meta,
                              int64_t seek_version, int64_t seek_seq_id,
                              std::vector<std::shared_ptr<TestLogEntryInfo>>& expected, int expected_first_entry_index);
    void verify_dup_key_multiple_versions(std::vector<DupKeyVersionInfo>& versions, std::string binlog_storage_path,
                                          std::vector<BinlogFileMetaPBPtr> file_metas);
};

} // namespace starrocks