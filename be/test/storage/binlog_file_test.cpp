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

#include <gtest/gtest.h>

#include "fs/fs_util.h"
#include "gen_cpp/binlog.pb.h"
#include "storage/binlog_file_reader.h"
#include "storage/binlog_file_writer.h"
#include "testutil/assert.h"

namespace starrocks {

struct TestLogEntryInfo {
    LogEntryPB log_entry;
    int64_t version;
    int64_t start_seq_id;
    int64_t end_seq_id;
    FileIdPB* file_id;
    int32_t start_row_id;
    bool end_of_version;
};

class BinlogFileTest : public testing::Test {
public:
    void SetUp() override {
        fs::remove_all(_binlog_file_dir);
        fs::create_directories(_binlog_file_dir);
    }

    void TearDown() override { fs::remove_all(_binlog_file_dir); }

protected:
    std::shared_ptr<TestLogEntryInfo> _build_insert_segment_log_entry(int64_t version, RowsetId& rowset_id,
                                                                      int seg_index, int64_t start_seq_id,
                                                                      int64_t num_rows, bool end_of_version) {
        std::shared_ptr<TestLogEntryInfo> entry_info = std::make_shared<TestLogEntryInfo>();
        LogEntryPB& log_entry = entry_info->log_entry;
        log_entry.set_entry_type(INSERT_RANGE_PB);
        InsertRangePB* data = log_entry.mutable_insert_range_data();
        FileIdPB* file_id = data->mutable_file_id();
        RowsetIdPB* rowset_id_pb = file_id->mutable_rowset_id();
        rowset_id_pb->set_hi(rowset_id.hi);
        rowset_id_pb->set_hi(rowset_id.mi);
        rowset_id_pb->set_hi(rowset_id.lo);
        file_id->set_segment_index(seg_index);
        entry_info->version = version;
        entry_info->start_seq_id = start_seq_id;
        entry_info->end_seq_id = start_seq_id + num_rows - 1;
        entry_info->file_id = file_id;
        entry_info->start_row_id = 0;
        entry_info->end_of_version = end_of_version;
        return entry_info;
    }

    std::shared_ptr<TestLogEntryInfo> _build_empty_rowset_log_entry(int64_t version) {
        std::shared_ptr<TestLogEntryInfo> entry_info = std::make_shared<TestLogEntryInfo>();
        LogEntryPB& log_entry = entry_info->log_entry;
        log_entry.set_entry_type(EMPTY);
        entry_info->version = version;
        entry_info->start_seq_id = 0;
        entry_info->end_seq_id = 0;
        entry_info->end_of_version = true;
        return entry_info;
    }

    std::string _binlog_file_dir = "binlog_file_test";
};

void verify_rowset_id(RowsetIdPB* expect_rowset_id, RowsetIdPB* actual_rowset_id) {
    ASSERT_EQ(expect_rowset_id->hi(), actual_rowset_id->hi());
    ASSERT_EQ(expect_rowset_id->mi(), actual_rowset_id->mi());
    ASSERT_EQ(expect_rowset_id->lo(), actual_rowset_id->lo());
}

void verify_file_id(FileIdPB* expect_file_id, FileIdPB* actual_file_id) {
    verify_rowset_id(expect_file_id->mutable_rowset_id(), actual_file_id->mutable_rowset_id());
    ASSERT_EQ(expect_file_id->segment_index(), actual_file_id->segment_index());
}

void verify_log_entry(LogEntryPB* expect, LogEntryPB* actual) {
    ASSERT_EQ(expect->entry_type(), actual->entry_type());
    ASSERT_EQ(INSERT_RANGE_PB, expect->entry_type());
    ASSERT_TRUE(expect->has_insert_range_data());
    ASSERT_TRUE(actual->has_insert_range_data());
    InsertRangePB* expect_data = expect->mutable_insert_range_data();
    InsertRangePB* actual_data = actual->mutable_insert_range_data();
    verify_file_id(expect_data->mutable_file_id(), actual_data->mutable_file_id());
    ASSERT_EQ(expect_data->start_row_id(), actual_data->start_row_id());
    ASSERT_EQ(expect_data->num_rows(), actual_data->num_rows());
}

void verify_log_entry_info(std::shared_ptr<TestLogEntryInfo> expect, LogEntryInfo* actual) {
    verify_log_entry(&(expect->log_entry), actual->log_entry);
    ASSERT_EQ(expect->version, actual->version);
    ASSERT_EQ(expect->start_seq_id, actual->start_seq_id);
    ASSERT_EQ(expect->end_seq_id, actual->end_seq_id);
    verify_file_id(expect->file_id, actual->file_id);
    ASSERT_EQ(expect->start_row_id, actual->start_row_id);
    ASSERT_EQ(expect->end_of_version, actual->end_of_version);
}

void verify_seek_and_next(std::string file_path, std::shared_ptr<BinlogFileMetaPB> file_meta, int64_t seek_version,
                          int64_t seek_seq_id, std::vector<std::shared_ptr<TestLogEntryInfo>>& expected,
                          int expect_first_index) {
    std::shared_ptr<BinlogFileReader> file_reader = std::make_shared<BinlogFileReader>(file_path, file_meta);
    Status st = file_reader->seek(seek_version, seek_seq_id);
    for (int i = expect_first_index; i < expected.size(); i++) {
        ASSERT_TRUE(st.ok());
        std::shared_ptr<TestLogEntryInfo> expect_entry = expected[i];
        LogEntryInfo* actual_entry = file_reader->log_entry();
        ASSERT_TRUE(actual_entry != nullptr);
        verify_log_entry_info(expect_entry, actual_entry);
    }
}

void add_rowset_to_file_meta(BinlogFileMetaPB* file_meta, RowsetId& rowset_id) {
    RowsetIdPB* rowset = file_meta->add_rowsets();
    rowset->set_hi(rowset_id.hi);
    rowset->set_mi(rowset_id.mi);
    rowset->set_lo(rowset_id.lo);
}

RowsetId pb_to_rowset_id(const RowsetIdPB& rowset_id_pb) {
    RowsetId rowsetId;
    rowsetId.init(rowset_id_pb.hi(), rowset_id_pb.mi(), rowset_id_pb.lo());
    return rowsetId;
}

void verify_file_meta(BinlogFileMetaPB* expect_file_meta, std::shared_ptr<BinlogFileMetaPB> actual_file_meta) {
    ASSERT_EQ(expect_file_meta->id(), actual_file_meta->id());
    ASSERT_EQ(expect_file_meta->start_version(), actual_file_meta->start_version());
    ASSERT_EQ(expect_file_meta->start_seq_id(), actual_file_meta->start_seq_id());
    ASSERT_EQ(expect_file_meta->start_timestamp_in_us(), actual_file_meta->start_timestamp_in_us());
    ASSERT_EQ(expect_file_meta->end_version(), actual_file_meta->end_version());
    ASSERT_EQ(expect_file_meta->end_seq_id(), actual_file_meta->end_seq_id());
    ASSERT_EQ(expect_file_meta->end_timestamp_in_us(), actual_file_meta->end_timestamp_in_us());

    unordered_set<RowsetId, HashOfRowsetId> rowset_set;
    for (int i = 0; i < expect_file_meta->rowsets_size(); i++) {
        RowsetId rowset = pb_to_rowset_id(expect_file_meta->rowsets(i));
        auto pair = rowset_set.emplace(rowset);
        ASSERT_FALSE(pair.second);
    }

    for (int i = 0; i < actual_file_meta->rowsets_size(); i++) {
        RowsetId rowset = pb_to_rowset_id(expect_file_meta->rowsets(i));
        auto pair = rowset_set.emplace(rowset);
        ASSERT_EQ(1, rowset_set.erase(rowset));
    }
    ASSERT_TRUE(rowset_set.empty());
}

// Test for duplicate key, and there is only insert range
TEST_F(BinlogFileTest, test_duplicate_key) {
    CompressionTypePB compression_type = LZ4_FRAME;
    int32_t page_size = 100;
    int64_t file_id = 1;

    std::vector<std::shared_ptr<TestLogEntryInfo>> expect_entries;
    BinlogFileMetaPB expect_file_meta;
    expect_file_meta.set_id(file_id);
    expect_file_meta.set_num_pages(0);
    expect_file_meta.set_file_size(0);

    std::string file_path = BinlogFileWriter::binlog_file_path(_binlog_file_dir, file_id);
    std::shared_ptr<BinlogFileWriter> file_writer =
            std::make_shared<BinlogFileWriter>(file_id, file_path, page_size, compression_type);
    file_writer->init();
    std::shared_ptr<BinlogFileMetaPB> file_meta = std::make_shared<BinlogFileMetaPB>();
    RowsetId rowset_id;
    // a rowset contains multiple segments, and write to multiple pages
    rowset_id.init(1, 2, 3);
    ASSERT_OK(file_writer->prepare(1, rowset_id, 0, 1));
    ASSERT_OK(file_writer->add_insert_range(0, 0, 100));
    ASSERT_OK(file_writer->add_insert_range(1, 0, 50));
    ASSERT_OK(file_writer->add_insert_range(2, 0, 96));
    ASSERT_OK(file_writer->commit(true));

    expect_file_meta.set_start_version(1);
    expect_file_meta.set_start_seq_id(0);
    expect_file_meta.set_start_timestamp_in_us(1);
    expect_file_meta.set_end_version(1);
    expect_file_meta.set_end_seq_id(245);
    expect_file_meta.set_end_timestamp_in_us(1);
    expect_file_meta.set_num_pages(2);
    add_rowset_to_file_meta(&expect_file_meta, rowset_id);
    expect_entries.emplace_back(_build_insert_segment_log_entry(1, rowset_id, 0, 0, 100, false));
    expect_entries.emplace_back(_build_insert_segment_log_entry(1, rowset_id, 1, 100, 50, false));
    expect_entries.emplace_back(_build_insert_segment_log_entry(1, rowset_id, 2, 150, 96, true));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // a rowset contains one segment, and write to one page
    rowset_id.init(2, 2, 3);
    ASSERT_OK(file_writer->prepare(2, rowset_id, 0, 2));
    ASSERT_OK(file_writer->add_insert_range(0, 0, 32));
    ASSERT_OK(file_writer->commit(true));

    expect_file_meta.set_end_version(2);
    expect_file_meta.set_end_seq_id(0);
    expect_file_meta.set_end_timestamp_in_us(31);
    expect_file_meta.set_num_pages(3);
    add_rowset_to_file_meta(&expect_file_meta, rowset_id);
    expect_entries.emplace_back(_build_insert_segment_log_entry(2, rowset_id, 0, 0, 32, true));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // a rowset contains no segment
    rowset_id.init(3, 2, 3);
    ASSERT_OK(file_writer->prepare(3, rowset_id, 0, 3));
    ASSERT_OK(file_writer->add_empty());
    ASSERT_OK(file_writer->commit(true));

    expect_file_meta.set_end_version(3);
    expect_file_meta.set_end_seq_id(0);
    expect_file_meta.set_end_timestamp_in_us(3);
    expect_file_meta.set_num_pages(4);
    expect_entries.emplace_back(_build_empty_rowset_log_entry(3));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // a rowset contains multiple segments, but cross two binlog files, and commit with end_of_version false
    rowset_id.init(4, 2, 3);
    ASSERT_OK(file_writer->prepare(4, rowset_id, 0, 4));
    ASSERT_OK(file_writer->add_insert_range(0, 0, 40));
    ASSERT_OK(file_writer->add_insert_range(1, 0, 20));
    ASSERT_OK(file_writer->commit(false));

    expect_file_meta.set_end_version(4);
    expect_file_meta.set_end_seq_id(59);
    expect_file_meta.set_end_timestamp_in_us(4);
    expect_file_meta.set_num_pages(5);
    expect_entries.emplace_back(_build_insert_segment_log_entry(4, rowset_id, 0, 0, 40, false));
    expect_entries.emplace_back(_build_insert_segment_log_entry(4, rowset_id, 1, 40, 20, false));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // verify that seek to different positions and read until the end of file
    verify_seek_and_next(file_path, file_meta, 1, 0, expect_entries, 0);
    verify_seek_and_next(file_path, file_meta, 1, 50, expect_entries, 0);
    verify_seek_and_next(file_path, file_meta, 1, 99, expect_entries, 0);
    verify_seek_and_next(file_path, file_meta, 1, 100, expect_entries, 1);
    verify_seek_and_next(file_path, file_meta, 1, 130, expect_entries, 1);
    verify_seek_and_next(file_path, file_meta, 1, 149, expect_entries, 1);
    verify_seek_and_next(file_path, file_meta, 1, 150, expect_entries, 2);
    verify_seek_and_next(file_path, file_meta, 1, 245, expect_entries, 2);
    verify_seek_and_next(file_path, file_meta, 2, 0, expect_entries, 3);
    verify_seek_and_next(file_path, file_meta, 2, 25, expect_entries, 3);
    verify_seek_and_next(file_path, file_meta, 2, 32, expect_entries, 3);
    verify_seek_and_next(file_path, file_meta, 3, 0, expect_entries, 4);
    verify_seek_and_next(file_path, file_meta, 4, 0, expect_entries, 5);
    verify_seek_and_next(file_path, file_meta, 4, 25, expect_entries, 5);
    verify_seek_and_next(file_path, file_meta, 4, 32, expect_entries, 5);
    verify_seek_and_next(file_path, file_meta, 4, 40, expect_entries, 6);
    verify_seek_and_next(file_path, file_meta, 4, 59, expect_entries, 6);
}

TEST_F(BinlogFileTest, test_abort) {
    CompressionTypePB compression_type = SNAPPY;
    int32_t page_size = 100;
    int64_t file_id = 1;

    std::vector<std::shared_ptr<TestLogEntryInfo>> expect_entries;
    BinlogFileMetaPB expect_file_meta;
    expect_file_meta.set_id(file_id);
    expect_file_meta.set_num_pages(0);
    expect_file_meta.set_file_size(0);

    std::string file_path = BinlogFileWriter::binlog_file_path(_binlog_file_dir, file_id);
    std::shared_ptr<BinlogFileWriter> file_writer =
            std::make_shared<BinlogFileWriter>(file_id, file_path, page_size, compression_type);
    ASSERT_OK(file_writer->init());
    std::shared_ptr<BinlogFileMetaPB> file_meta = std::make_shared<BinlogFileMetaPB>();
    file_writer->copy_file_meta(file_meta.get());

    RowsetId rowset_id;
    rowset_id.init(1, 2, 3);
    // prepare failed, then abort
    ASSERT_OK(file_writer->prepare(1, rowset_id, 0, 1));
    ASSERT_OK(file_writer->abort());

    // write some pages but not committed, then abort
    rowset_id.init(2, 2, 3);
    ASSERT_OK(file_writer->prepare(2, rowset_id, 0, 2));
    ASSERT_OK(file_writer->add_insert_range(0, 0, 100));
    ASSERT_OK(file_writer->add_insert_range(1, 0, 50));
    ASSERT_OK(file_writer->add_insert_range(2, 0, 96));
    // ensue there is something durable, and will be truncated when aborting
    ASSERT_LE(file_writer->committed_file_size(), file_writer->file_size());
    ASSERT_OK(file_writer->abort());

    // write binlog successfully
    rowset_id.init(3, 2, 3);
    ASSERT_OK(file_writer->prepare(3, rowset_id, 0, 3));
    ASSERT_OK(file_writer->add_insert_range(0, 0, 3));
    ASSERT_OK(file_writer->commit(true));

    expect_file_meta.set_start_version(3);
    expect_file_meta.set_start_seq_id(0);
    expect_file_meta.set_start_timestamp_in_us(3);
    expect_file_meta.set_end_version(3);
    expect_file_meta.set_end_seq_id(2);
    expect_file_meta.set_end_timestamp_in_us(3);
    expect_file_meta.set_num_pages(1);
    add_rowset_to_file_meta(&expect_file_meta, rowset_id);
    expect_entries.emplace_back(_build_insert_segment_log_entry(3, rowset_id, 0, 0, 3, true));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // add some log entries, then abort
    rowset_id.init(4, 2, 3);
    ASSERT_OK(file_writer->prepare(4, rowset_id, 0, 4));
    ASSERT_OK(file_writer->add_insert_range(0, 0, 100));
    ASSERT_OK(file_writer->add_insert_range(1, 0, 50));
    ASSERT_OK(file_writer->add_insert_range(2, 0, 96));
    ASSERT_LE(file_writer->committed_file_size(), file_writer->file_size());
    ASSERT_OK(file_writer->abort());

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // write binlog successfully
    rowset_id.init(5, 2, 3);
    ASSERT_OK(file_writer->prepare(5, rowset_id, 0, 5));
    ASSERT_OK(file_writer->add_insert_range(0, 0, 40));
    ASSERT_OK(file_writer->add_insert_range(1, 0, 20));
    ASSERT_OK(file_writer->commit(false));

    expect_file_meta.set_end_version(5);
    expect_file_meta.set_end_seq_id(59);
    expect_file_meta.set_end_timestamp_in_us(5);
    expect_file_meta.set_num_pages(3);
    expect_entries.emplace_back(_build_insert_segment_log_entry(5, rowset_id, 0, 0, 40, false));
    expect_entries.emplace_back(_build_insert_segment_log_entry(5, rowset_id, 1, 40, 20, false));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    verify_seek_and_next(file_path, file_meta, 3, 0, expect_entries, 0);
}

// TODO add tests for primary key
TEST_F(BinlogFileTest, test_primary_key) {}

} // namespace starrocks