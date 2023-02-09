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

#include "storage/binlog_test_base.h"

namespace starrocks {

BinlogFileMergeReader::BinlogFileMergeReader(std::string storage_path, std::vector<BinlogFileMetaPBPtr>& file_metas)
        : _storage_path(std::move(storage_path)), _file_metas(file_metas), _next_file_index(0) {}

Status BinlogFileMergeReader::seek(int64_t version, int64_t seq_id) {
    if (_next_file_index != 0) {
        return Status::InternalError("Should seek only once");
    }

    if (_next_file_index >= _file_metas.size()) {
        return Status::EndOfFile("Read all files");
    }

    _next_file_index = 1;
    return _init_file_reader(_file_metas[0], version, seq_id);
}

Status BinlogFileMergeReader::next() {
    Status st = _current_reader->next();
    if (st.is_end_of_file() && _next_file_index < _file_metas.size()) {
        BinlogFileMetaPBPtr file_meta = _file_metas[_next_file_index];
        _next_file_index += 1;
        return _init_file_reader(file_meta, file_meta->start_version(), file_meta->start_seq_id());
    }
    return st;
}

LogEntryInfo* BinlogFileMergeReader::log_entry() {
    return _current_reader->log_entry();
}

Status BinlogFileMergeReader::_init_file_reader(BinlogFileMetaPBPtr& file_meta, int64_t version, int64_t seq_id) {
    std::string file_path = BinlogUtil::binlog_file_path(_storage_path, file_meta->id());
    _current_reader = std::make_shared<BinlogFileReader>(file_path, file_meta);
    return _current_reader->seek(version, seq_id);
}

void BinlogTestBase::estimate_log_entry_size(LogEntryTypePB entry_type, int32_t* estimated_size) {
    // TODO support other types
    EXPECT_EQ(INSERT_RANGE_PB, entry_type);
    LogEntryPB entry;
    entry.set_entry_type(entry_type);
    InsertRangePB* insert_range = entry.mutable_insert_range_data();
    FileIdPB* file_id = insert_range->mutable_file_id();
    file_id->set_rowset_id(INT64_MAX);
    file_id->set_segment_index(INT32_MAX);
    insert_range->set_start_row_id(INT32_MAX);
    insert_range->set_num_rows(INT32_MAX);
    *estimated_size = entry.ByteSizeLong();
}

std::shared_ptr<TestLogEntryInfo> BinlogTestBase::build_insert_segment_log_entry(int64_t version, int64_t rowset_id,
                                                                                 int seg_index, int64_t start_seq_id,
                                                                                 int64_t num_rows, bool end_of_version,
                                                                                 int64_t timestamp) {
    std::shared_ptr<TestLogEntryInfo> entry_info = std::make_shared<TestLogEntryInfo>();
    LogEntryPB& log_entry = entry_info->log_entry;
    log_entry.set_entry_type(INSERT_RANGE_PB);
    InsertRangePB* data = log_entry.mutable_insert_range_data();
    FileIdPB* file_id = data->mutable_file_id();
    file_id->set_rowset_id(rowset_id);
    file_id->set_segment_index(seg_index);
    data->set_start_row_id(0);
    data->set_num_rows(num_rows);
    entry_info->version = version;
    entry_info->start_seq_id = start_seq_id;
    entry_info->end_seq_id = start_seq_id + num_rows - 1;
    entry_info->file_id = file_id;
    entry_info->start_row_id = 0;
    entry_info->num_rows = num_rows;
    entry_info->end_of_version = end_of_version;
    entry_info->timestamp = timestamp;
    return entry_info;
}

std::shared_ptr<TestLogEntryInfo> BinlogTestBase::build_empty_rowset_log_entry(int64_t version, int64_t timestamp) {
    std::shared_ptr<TestLogEntryInfo> entry_info = std::make_shared<TestLogEntryInfo>();
    LogEntryPB& log_entry = entry_info->log_entry;
    log_entry.set_entry_type(EMPTY_PB);
    entry_info->version = version;
    entry_info->start_seq_id = 0;
    entry_info->end_seq_id = -1;
    entry_info->end_of_version = true;
    entry_info->timestamp = timestamp;
    return entry_info;
}

void BinlogTestBase::verify_file_id(FileIdPB* expect_file_id, FileIdPB* actual_file_id) {
    if (expect_file_id == nullptr) {
        ASSERT_TRUE(actual_file_id == nullptr);
        return;
    }
    ASSERT_EQ(expect_file_id->rowset_id(), actual_file_id->rowset_id());
    ASSERT_EQ(expect_file_id->segment_index(), actual_file_id->segment_index());
}

void BinlogTestBase::verify_log_entry(LogEntryPB* expect, LogEntryPB* actual) {
    ASSERT_EQ(expect->entry_type(), actual->entry_type());
    if (expect->entry_type() == EMPTY_PB) {
        return;
    }
    // TODO support update and delete
    ASSERT_EQ(INSERT_RANGE_PB, expect->entry_type());
    ASSERT_TRUE(expect->has_insert_range_data());
    ASSERT_TRUE(actual->has_insert_range_data());
    InsertRangePB* expect_data = expect->mutable_insert_range_data();
    InsertRangePB* actual_data = actual->mutable_insert_range_data();
    verify_file_id(expect_data->mutable_file_id(), actual_data->mutable_file_id());
    ASSERT_EQ(expect_data->start_row_id(), actual_data->start_row_id());
    ASSERT_EQ(expect_data->num_rows(), actual_data->num_rows());
}

void BinlogTestBase::verify_log_entry_info(const std::shared_ptr<TestLogEntryInfo>& expect, LogEntryInfo* actual) {
    verify_log_entry(&(expect->log_entry), actual->log_entry);
    ASSERT_EQ(expect->version, actual->version);
    ASSERT_EQ(expect->start_seq_id, actual->start_seq_id);
    ASSERT_EQ(expect->end_seq_id, actual->end_seq_id);
    verify_file_id(expect->file_id, actual->file_id);
    ASSERT_EQ(expect->start_row_id, actual->start_row_id);
    ASSERT_EQ(expect->num_rows, actual->num_rows);
    ASSERT_EQ(expect->end_of_version, actual->end_of_version);
    ASSERT_EQ(expect->timestamp, actual->timestamp_in_us);
}

void BinlogTestBase::verify_file_meta(BinlogFileMetaPB* expect_file_meta,
                                      const std::shared_ptr<BinlogFileMetaPB>& actual_file_meta) {
    ASSERT_EQ(expect_file_meta->id(), actual_file_meta->id());
    ASSERT_EQ(expect_file_meta->start_version(), actual_file_meta->start_version());
    ASSERT_EQ(expect_file_meta->start_seq_id(), actual_file_meta->start_seq_id());
    ASSERT_EQ(expect_file_meta->start_timestamp_in_us(), actual_file_meta->start_timestamp_in_us());
    ASSERT_EQ(expect_file_meta->end_version(), actual_file_meta->end_version());
    ASSERT_EQ(expect_file_meta->end_seq_id(), actual_file_meta->end_seq_id());
    ASSERT_EQ(expect_file_meta->end_timestamp_in_us(), actual_file_meta->end_timestamp_in_us());
    ASSERT_EQ(expect_file_meta->num_pages(), actual_file_meta->num_pages());
    ASSERT_EQ(expect_file_meta->file_size(), actual_file_meta->file_size());

    std::unordered_set<int64_t> rowset_set;
    for (auto id : expect_file_meta->rowsets()) {
        auto pair = rowset_set.emplace(id);
        ASSERT_TRUE(pair.second);
    }

    ASSERT_EQ(expect_file_meta->rowsets_size(), actual_file_meta->rowsets_size());
    for (auto id : actual_file_meta->rowsets()) {
        ASSERT_EQ(1, rowset_set.erase(id));
    }
    ASSERT_TRUE(rowset_set.empty());
}

void BinlogTestBase::verify_seek_and_next(const std::string& file_path,
                                          const std::shared_ptr<BinlogFileMetaPB>& file_meta, int64_t seek_version,
                                          int64_t seek_seq_id, std::vector<std::shared_ptr<TestLogEntryInfo>>& expected,
                                          int expected_first_entry_index) {
    std::shared_ptr<BinlogFileReader> file_reader = std::make_shared<BinlogFileReader>(file_path, file_meta);
    Status st = file_reader->seek(seek_version, seek_seq_id);
    for (int i = expected_first_entry_index; i < expected.size(); i++) {
        ASSERT_TRUE(st.ok());
        std::shared_ptr<TestLogEntryInfo> expect_entry = expected[i];
        LogEntryInfo* actual_entry = file_reader->log_entry();
        ASSERT_TRUE(actual_entry != nullptr);
        verify_log_entry_info(expect_entry, actual_entry);
        st = file_reader->next();
    }
    ASSERT_TRUE(st.is_end_of_file());
}

void BinlogTestBase::verify_dup_key_multiple_versions(std::vector<DupKeyVersionInfo>& versions,
                                                      std::string binlog_storage_path,
                                                      std::vector<BinlogFileMetaPBPtr> file_metas) {
    if (versions.empty()) {
        ASSERT_TRUE(file_metas.empty());
    }
    std::shared_ptr<BinlogFileMergeReader> reader =
            std::make_shared<BinlogFileMergeReader>(binlog_storage_path, file_metas);
    Status st = reader->seek(versions[0].version, 0);
    for (auto& version_info : versions) {
        int64_t version = version_info.version;
        std::shared_ptr<TestLogEntryInfo> expect_entry;
        if (version_info.num_entries == 0) {
            ASSERT_TRUE(st.ok());
            expect_entry = build_empty_rowset_log_entry(version, version_info.timestamp);
            LogEntryInfo* actual_entry = reader->log_entry();
            verify_log_entry_info(expect_entry, actual_entry);
            st = reader->next();
        } else {
            int64_t start_seq_id = 0;
            for (int32_t n = 0; n < version_info.num_entries; n++) {
                ASSERT_TRUE(st.ok());
                bool end_of_version = (n + 1) == version_info.num_entries;
                expect_entry = build_insert_segment_log_entry(version, version, n, start_seq_id,
                                                              version_info.num_rows_per_entry, end_of_version,
                                                              version_info.timestamp);
                LogEntryInfo* actual_entry = reader->log_entry();
                verify_log_entry_info(expect_entry, actual_entry);
                st = reader->next();
                start_seq_id += version_info.num_rows_per_entry;
            }
        }
    }
    ASSERT_TRUE(st.is_end_of_file());
}

} // namespace starrocks