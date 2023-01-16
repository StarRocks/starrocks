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

#include <iostream>

#include "fs/fs_util.h"
#include "gen_cpp/binlog.pb.h"
#include "storage/binlog_file_reader.h"
#include "storage/binlog_file_writer.h"
#include "storage/binlog_util.h"
#include "testutil/assert.h"

namespace starrocks {

class BinlogFileTest : public testing::Test {
public:
    void SetUp() override {
        CHECK_OK(fs::remove_all(_binlog_file_dir));
        CHECK_OK(fs::create_directories(_binlog_file_dir));
        ASSIGN_OR_ABORT(_fs, FileSystem::CreateSharedFromString(_binlog_file_dir));
    }

    void TearDown() override { fs::remove_all(_binlog_file_dir); }

protected:
    std::shared_ptr<FileSystem> _fs;
    std::string _binlog_file_dir = "binlog_file_test";
};

void estimate_log_entry_size(LogEntryTypePB entry_type, int32_t* estimated_size) {
    // TODO support other types
    EXPECT_EQ(INSERT_RANGE_PB, entry_type);
    LogEntryPB entry;
    entry.set_entry_type(entry_type);
    InsertRangePB* insert_range = entry.mutable_insert_range_data();
    FileIdPB* file_id = insert_range->mutable_file_id();
    RowsetIdPB* rs_id = file_id->mutable_rowset_id();
    rs_id->set_hi(0);
    rs_id->set_mi(0);
    rs_id->set_lo(0);
    file_id->set_segment_index(0);
    insert_range->set_start_row_id(0);
    insert_range->set_num_rows(1);
    *estimated_size = entry.ByteSizeLong();
}

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

std::shared_ptr<TestLogEntryInfo> _build_insert_segment_log_entry(int64_t version, RowsetId& rowset_id, int seg_index,
                                                                  int64_t start_seq_id, int64_t num_rows,
                                                                  bool end_of_version, int64_t timestamp) {
    std::shared_ptr<TestLogEntryInfo> entry_info = std::make_shared<TestLogEntryInfo>();
    LogEntryPB& log_entry = entry_info->log_entry;
    log_entry.set_entry_type(INSERT_RANGE_PB);
    InsertRangePB* data = log_entry.mutable_insert_range_data();
    FileIdPB* file_id = data->mutable_file_id();
    RowsetIdPB* rowset_id_pb = file_id->mutable_rowset_id();
    rowset_id_pb->set_hi(rowset_id.hi);
    rowset_id_pb->set_mi(rowset_id.mi);
    rowset_id_pb->set_lo(rowset_id.lo);
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

std::shared_ptr<TestLogEntryInfo> _build_empty_rowset_log_entry(int64_t version, int64_t timestamp) {
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

void verify_rowset_id(RowsetIdPB* expect_rowset_id, RowsetIdPB* actual_rowset_id) {
    ASSERT_EQ(expect_rowset_id->hi(), actual_rowset_id->hi());
    ASSERT_EQ(expect_rowset_id->mi(), actual_rowset_id->mi());
    ASSERT_EQ(expect_rowset_id->lo(), actual_rowset_id->lo());
}

void verify_file_id(FileIdPB* expect_file_id, FileIdPB* actual_file_id) {
    if (expect_file_id == nullptr) {
        ASSERT_TRUE(actual_file_id == nullptr);
        return;
    }
    verify_rowset_id(expect_file_id->mutable_rowset_id(), actual_file_id->mutable_rowset_id());
    ASSERT_EQ(expect_file_id->segment_index(), actual_file_id->segment_index());
}

void verify_log_entry(LogEntryPB* expect, LogEntryPB* actual) {
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

void verify_log_entry_info(const std::shared_ptr<TestLogEntryInfo>& expect, LogEntryInfo* actual) {
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

void verify_seek_and_next(const std::string& file_path, std::shared_ptr<BinlogFileMetaPB> file_meta,
                          int64_t seek_version, int64_t seek_seq_id,
                          std::vector<std::shared_ptr<TestLogEntryInfo>>& expected, int expected_first_entry_index) {
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

void add_rowset_to_file_meta(BinlogFileMetaPB* file_meta, RowsetId& rowset_id) {
    RowsetIdPB* rowset = file_meta->add_rowsets();
    rowset->set_hi(rowset_id.hi);
    rowset->set_mi(rowset_id.mi);
    rowset->set_lo(rowset_id.lo);
}

void verify_file_meta(BinlogFileMetaPB* expect_file_meta, std::shared_ptr<BinlogFileMetaPB> actual_file_meta) {
    ASSERT_EQ(expect_file_meta->id(), actual_file_meta->id());
    ASSERT_EQ(expect_file_meta->start_version(), actual_file_meta->start_version());
    ASSERT_EQ(expect_file_meta->start_seq_id(), actual_file_meta->start_seq_id());
    ASSERT_EQ(expect_file_meta->start_timestamp_in_us(), actual_file_meta->start_timestamp_in_us());
    ASSERT_EQ(expect_file_meta->end_version(), actual_file_meta->end_version());
    ASSERT_EQ(expect_file_meta->end_seq_id(), actual_file_meta->end_seq_id());
    ASSERT_EQ(expect_file_meta->end_timestamp_in_us(), actual_file_meta->end_timestamp_in_us());
    ASSERT_EQ(expect_file_meta->num_pages(), actual_file_meta->num_pages());
    ASSERT_EQ(expect_file_meta->file_size(), actual_file_meta->file_size());

    std::unordered_set<RowsetId, HashOfRowsetId> rowset_set;
    for (int i = 0; i < expect_file_meta->rowsets_size(); i++) {
        RowsetId rowset_id;
        BinlogUtil::convert_pb_to_rowset_id(expect_file_meta->rowsets(i), &rowset_id);
        auto pair = rowset_set.emplace(rowset_id);
        ASSERT_TRUE(pair.second);
    }

    ASSERT_EQ(expect_file_meta->rowsets_size(), actual_file_meta->rowsets_size());
    for (int i = 0; i < actual_file_meta->rowsets_size(); i++) {
        RowsetId rowset_id;
        BinlogUtil::convert_pb_to_rowset_id(expect_file_meta->rowsets(i), &rowset_id);
        ASSERT_EQ(1, rowset_set.erase(rowset_id));
    }
    ASSERT_TRUE(rowset_set.empty());
}

// Test simple write/read for duplicate key, and there is only insert range
TEST_F(BinlogFileTest, test_basic_write_read) {
    CompressionTypePB compression_type = LZ4_FRAME;
    int32_t page_size = 50;
    int64_t file_id = 1;

    std::vector<std::shared_ptr<TestLogEntryInfo>> expect_entries;
    BinlogFileMetaPB expect_file_meta;
    expect_file_meta.set_id(file_id);
    expect_file_meta.set_num_pages(0);
    expect_file_meta.set_file_size(0);

    std::string file_path = BinlogUtil::binlog_file_path(_binlog_file_dir, file_id);
    std::shared_ptr<BinlogFileWriter> file_writer =
            std::make_shared<BinlogFileWriter>(file_id, file_path, page_size, compression_type);
    ASSERT_OK(file_writer->init());
    std::shared_ptr<BinlogFileMetaPB> file_meta = std::make_shared<BinlogFileMetaPB>();
    RowsetId rowset_id;
    // a rowset contains multiple segments, and write to multiple pages
    rowset_id.init(2, 1, 2, 3);
    ASSERT_OK(file_writer->begin(1, 0, 1));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 0), 0, 100));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 1), 0, 50));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 2), 0, 96));
    ASSERT_OK(file_writer->commit(true));

    expect_file_meta.set_start_version(1);
    expect_file_meta.set_start_seq_id(0);
    expect_file_meta.set_start_timestamp_in_us(1);
    expect_file_meta.set_end_version(1);
    expect_file_meta.set_end_seq_id(245);
    expect_file_meta.set_end_timestamp_in_us(1);
    expect_file_meta.set_num_pages(2);
    expect_file_meta.set_file_size(_fs->get_file_size(file_path).value());
    add_rowset_to_file_meta(&expect_file_meta, rowset_id);
    expect_entries.emplace_back(_build_insert_segment_log_entry(1, rowset_id, 0, 0, 100, false, 1));
    expect_entries.emplace_back(_build_insert_segment_log_entry(1, rowset_id, 1, 100, 50, false, 1));
    expect_entries.emplace_back(_build_insert_segment_log_entry(1, rowset_id, 2, 150, 96, true, 1));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // a rowset contains one segment, and write to one page
    rowset_id.init(2, 2, 2, 3);
    ASSERT_OK(file_writer->begin(2, 0, 2));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 0), 0, 32));
    ASSERT_OK(file_writer->commit(true));

    expect_file_meta.set_end_version(2);
    expect_file_meta.set_end_seq_id(31);
    expect_file_meta.set_end_timestamp_in_us(2);
    expect_file_meta.set_num_pages(3);
    expect_file_meta.set_file_size(_fs->get_file_size(file_path).value());
    add_rowset_to_file_meta(&expect_file_meta, rowset_id);
    expect_entries.emplace_back(_build_insert_segment_log_entry(2, rowset_id, 0, 0, 32, true, 2));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // a rowset contains no segment
    rowset_id.init(2, 3, 2, 3);
    ASSERT_OK(file_writer->begin(3, 0, 3));
    ASSERT_OK(file_writer->add_empty());
    ASSERT_OK(file_writer->commit(true));

    expect_file_meta.set_end_version(3);
    expect_file_meta.set_end_seq_id(-1);
    expect_file_meta.set_end_timestamp_in_us(3);
    expect_file_meta.set_num_pages(4);
    expect_file_meta.set_file_size(_fs->get_file_size(file_path).value());
    expect_entries.emplace_back(_build_empty_rowset_log_entry(3, 3));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // a rowset contains multiple segments, but cross two binlog files, and commit with end_of_version false
    rowset_id.init(2, 4, 2, 3);
    ASSERT_OK(file_writer->begin(4, 0, 4));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 0), 0, 40));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 1), 0, 20));
    ASSERT_OK(file_writer->commit(false));

    expect_file_meta.set_end_version(4);
    expect_file_meta.set_end_seq_id(59);
    expect_file_meta.set_end_timestamp_in_us(4);
    expect_file_meta.set_num_pages(5);
    expect_file_meta.set_file_size(_fs->get_file_size(file_path).value());
    add_rowset_to_file_meta(&expect_file_meta, rowset_id);
    expect_entries.emplace_back(_build_insert_segment_log_entry(4, rowset_id, 0, 0, 40, false, 4));
    expect_entries.emplace_back(_build_insert_segment_log_entry(4, rowset_id, 1, 40, 20, false, 4));

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
    verify_seek_and_next(file_path, file_meta, 2, 31, expect_entries, 3);
    verify_seek_and_next(file_path, file_meta, 3, 0, expect_entries, 4);
    verify_seek_and_next(file_path, file_meta, 4, 0, expect_entries, 5);
    verify_seek_and_next(file_path, file_meta, 4, 25, expect_entries, 5);
    verify_seek_and_next(file_path, file_meta, 4, 32, expect_entries, 5);
    verify_seek_and_next(file_path, file_meta, 4, 40, expect_entries, 6);
    verify_seek_and_next(file_path, file_meta, 4, 59, expect_entries, 6);
}

TEST_F(BinlogFileTest, test_basic_begin_commit_abort) {
    CompressionTypePB compression_type = SNAPPY;
    int32_t page_size = 50;
    int64_t file_id = 1;

    std::vector<std::shared_ptr<TestLogEntryInfo>> expect_entries;
    BinlogFileMetaPB expect_file_meta;
    expect_file_meta.set_id(file_id);
    expect_file_meta.set_num_pages(0);
    expect_file_meta.set_file_size(0);

    std::string file_path = BinlogUtil::binlog_file_path(_binlog_file_dir, file_id);
    std::shared_ptr<BinlogFileWriter> file_writer =
            std::make_shared<BinlogFileWriter>(file_id, file_path, page_size, compression_type);
    ASSERT_OK(file_writer->init());
    std::shared_ptr<BinlogFileMetaPB> file_meta = std::make_shared<BinlogFileMetaPB>();
    file_writer->copy_file_meta(file_meta.get());

    RowsetId rowset_id;
    rowset_id.init(2, 1, 2, 3);
    // prepare failed, then abort
    ASSERT_OK(file_writer->begin(1, 0, 1));
    ASSERT_OK(file_writer->abort());

    // write some pages but not committed, then abort
    rowset_id.init(2, 2, 2, 3);
    int64_t before_file_size = file_writer->file_size();
    ASSERT_OK(file_writer->begin(2, 0, 2));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 0), 0, 100));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 1), 0, 50));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 2), 0, 96));
    // ensue there is something durable, and will be truncated when aborting
    ASSERT_LE(before_file_size, file_writer->file_size());
    ASSERT_OK(file_writer->abort());

    // write binlog successfully
    rowset_id.init(2, 3, 2, 3);
    ASSERT_OK(file_writer->begin(3, 0, 3));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 0), 0, 3));
    ASSERT_OK(file_writer->commit(true));

    expect_file_meta.set_start_version(3);
    expect_file_meta.set_start_seq_id(0);
    expect_file_meta.set_start_timestamp_in_us(3);
    expect_file_meta.set_end_version(3);
    expect_file_meta.set_end_seq_id(2);
    expect_file_meta.set_end_timestamp_in_us(3);
    expect_file_meta.set_num_pages(1);
    expect_file_meta.set_file_size(_fs->get_file_size(file_path).value());
    add_rowset_to_file_meta(&expect_file_meta, rowset_id);
    expect_entries.emplace_back(_build_insert_segment_log_entry(3, rowset_id, 0, 0, 3, true, 3));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // add some log entries, then abort
    rowset_id.init(2, 4, 2, 3);
    before_file_size = file_writer->file_size();
    ASSERT_OK(file_writer->begin(4, 0, 4));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 0), 0, 100));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 1), 0, 50));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 2), 0, 96));
    ASSERT_LE(before_file_size, file_writer->file_size());
    ASSERT_OK(file_writer->abort());

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // write binlog successfully
    rowset_id.init(2, 5, 2, 3);
    ASSERT_OK(file_writer->begin(5, 0, 5));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 0), 0, 40));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 1), 0, 20));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 2), 0, 30));
    ASSERT_OK(file_writer->commit(false));

    expect_file_meta.set_end_version(5);
    expect_file_meta.set_end_seq_id(89);
    expect_file_meta.set_end_timestamp_in_us(5);
    expect_file_meta.set_num_pages(3);
    expect_file_meta.set_file_size(_fs->get_file_size(file_path).value());
    add_rowset_to_file_meta(&expect_file_meta, rowset_id);
    expect_entries.emplace_back(_build_insert_segment_log_entry(5, rowset_id, 0, 0, 40, false, 5));
    expect_entries.emplace_back(_build_insert_segment_log_entry(5, rowset_id, 1, 40, 20, false, 5));
    expect_entries.emplace_back(_build_insert_segment_log_entry(5, rowset_id, 2, 60, 30, false, 5));

    // test begin without commit/abort
    rowset_id.init(2, 6, 2, 3);
    ASSERT_OK(file_writer->begin(6, 0, 6));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 0), 0, 40));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 1), 0, 20));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    verify_seek_and_next(file_path, file_meta, 3, 0, expect_entries, 0);
}

TEST_F(BinlogFileTest, test_resize) {
    std::vector<std::shared_ptr<TestLogEntryInfo>> expect_entries;
    std::string file_path = BinlogUtil::binlog_file_path(_binlog_file_dir, 1);
    std::shared_ptr<BinlogFileWriter> file_writer = std::make_shared<BinlogFileWriter>(1, file_path, 50, LZ4_FRAME);
    ASSERT_OK(file_writer->init());

    RowsetId rowset_id;
    rowset_id.init(2, 1, 2, 3);
    ASSERT_OK(file_writer->begin(1, 0, 1));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 0), 0, 100));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 1), 0, 50));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 2), 0, 96));
    ASSERT_OK(file_writer->commit(true));
    std::shared_ptr<BinlogFileMetaPB> file_meta_1 = std::make_shared<BinlogFileMetaPB>();
    file_writer->copy_file_meta(file_meta_1.get());
    ASSERT_EQ(_fs->get_file_size(file_path).value(), file_meta_1->file_size());

    expect_entries.emplace_back(_build_insert_segment_log_entry(1, rowset_id, 0, 0, 100, false, 1));
    expect_entries.emplace_back(_build_insert_segment_log_entry(1, rowset_id, 1, 100, 50, false, 1));
    expect_entries.emplace_back(_build_insert_segment_log_entry(1, rowset_id, 2, 150, 96, true, 1));

    rowset_id.init(2, 2, 2, 3);
    ASSERT_OK(file_writer->begin(2, 0, 2));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 0), 0, 32));
    ASSERT_OK(file_writer->commit(true));
    std::shared_ptr<BinlogFileMetaPB> file_meta_2 = std::make_shared<BinlogFileMetaPB>();
    file_writer->copy_file_meta(file_meta_2.get());
    ASSERT_EQ(_fs->get_file_size(file_path).value(), file_meta_2->file_size());
    ASSERT_LE(file_meta_1->file_size(), file_meta_2->file_size());

    ASSERT_OK(file_writer->reset(file_meta_1.get()));
    ASSERT_EQ(_fs->get_file_size(file_path).value(), file_meta_1->file_size());
    verify_seek_and_next(file_path, file_meta_1, 1, 0, expect_entries, 0);

    rowset_id.init(2, 5, 2, 3);
    ASSERT_OK(file_writer->begin(5, 0, 5));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 0), 0, 40));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 1), 0, 20));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, 2), 0, 30));
    ASSERT_OK(file_writer->commit(false));
    std::shared_ptr<BinlogFileMetaPB> file_meta_3 = std::make_shared<BinlogFileMetaPB>();
    file_writer->copy_file_meta(file_meta_3.get());

    expect_entries.emplace_back(_build_insert_segment_log_entry(5, rowset_id, 0, 0, 40, false, 5));
    expect_entries.emplace_back(_build_insert_segment_log_entry(5, rowset_id, 1, 40, 20, false, 5));
    expect_entries.emplace_back(_build_insert_segment_log_entry(5, rowset_id, 2, 60, 30, false, 5));

    verify_seek_and_next(file_path, file_meta_3, 1, 0, expect_entries, 0);
}

struct VersionInfo {
    int64_t version;
    int32_t num_entries;
    int64_t num_rows_per_entry;
};

void verify_random_result(std::vector<VersionInfo>& versions, std::string& file_path,
                          std::shared_ptr<BinlogFileMetaPB> file_meta) {
    std::shared_ptr<BinlogFileReader> file_reader = std::make_shared<BinlogFileReader>(file_path, file_meta);
    Status st = file_reader->seek(versions[0].version, 0);
    for (auto& version_info : versions) {
        int64_t version = version_info.version;
        RowsetId rowset_id;
        rowset_id.init(2, version_info.version, 2, 3);
        std::shared_ptr<TestLogEntryInfo> expect_entry;
        if (version_info.num_entries == 0) {
            ASSERT_TRUE(st.ok());
            expect_entry = _build_empty_rowset_log_entry(version, version);
            LogEntryInfo* actual_entry = file_reader->log_entry();
            verify_log_entry_info(expect_entry, actual_entry);
            st = file_reader->next();
        } else {
            int64_t start_seq_id = 0;
            for (int32_t n = 0; n < version_info.num_entries; n++) {
                ASSERT_TRUE(st.ok());
                bool end_of_version = (n + 1) == version_info.num_entries;
                expect_entry = _build_insert_segment_log_entry(
                        version, rowset_id, n, start_seq_id, version_info.num_rows_per_entry, end_of_version, version);
                LogEntryInfo* actual_entry = file_reader->log_entry();
                verify_log_entry_info(expect_entry, actual_entry);
                st = file_reader->next();
                start_seq_id += version_info.num_rows_per_entry;
            }
        }
    }
    ASSERT_TRUE(st.is_end_of_file());
}

// Test generating large binlog file with random content for duplicate key
TEST_F(BinlogFileTest, test_random_write_read) {
    CompressionTypePB compression_type = NO_COMPRESSION;
    int32_t expect_file_size = 100 * 1024 * 1024;
    int32_t expect_num_versions = 1000;
    int32_t max_page_size = 32 * 1024;
    int32_t estimated_log_entry_size;
    estimate_log_entry_size(INSERT_RANGE_PB, &estimated_log_entry_size);
    int32_t avg_entries_per_version = expect_file_size / estimated_log_entry_size;

    std::string file_path = BinlogUtil::binlog_file_path(_binlog_file_dir, 1);
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(file_path));
    std::shared_ptr<BinlogFileWriter> file_writer =
            std::make_shared<BinlogFileWriter>(1, file_path, max_page_size, compression_type);
    ASSERT_OK(file_writer->init());
    std::vector<VersionInfo> versions;
    while (file_writer->file_size() < expect_file_size && versions.size() < expect_num_versions) {
        VersionInfo version_info;
        version_info.version = versions.size() + 1;
        // there is 1/20 to generate empty version
        version_info.num_entries = (std::rand() % 20 == 0) ? 0 : (std::rand() % avg_entries_per_version * 2 + 1);
        version_info.num_rows_per_entry = std::rand() % 100 + 1;
        versions.push_back(version_info);
        RowsetId rowset_id;
        rowset_id.init(2, version_info.version, 2, 3);
        ASSERT_OK(file_writer->begin(version_info.version, 0, version_info.version));
        if (version_info.num_entries == 0) {
            ASSERT_OK(file_writer->add_empty());
        } else {
            for (int32_t n = 0; n < version_info.num_entries; n++) {
                ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, n), 0,
                                                        version_info.num_rows_per_entry));
            }
        }
        ASSERT_OK(file_writer->commit(true));
    }
    ASSERT_OK(file_writer->close(true));

    std::shared_ptr<BinlogFileMetaPB> file_meta = std::make_shared<BinlogFileMetaPB>();
    file_writer->copy_file_meta(file_meta.get());
    ASSERT_EQ(versions[0].version, file_meta->start_version());
    ASSERT_EQ(0, file_meta->start_seq_id());
    ASSERT_EQ(versions.back().version, file_meta->end_version());
    ASSERT_EQ(versions.back().num_entries * versions.back().num_rows_per_entry - 1, file_meta->end_seq_id());
    verify_random_result(versions, file_path, file_meta);
}

// Test random and repeated begin-commit and begin-abort
TEST_F(BinlogFileTest, test_random_begin_commit_abort) {
    CompressionTypePB compression_type = LZ4_FRAME;
    int32_t max_page_size = 32 * 1024;
    int32_t estimated_log_entry_size;
    estimate_log_entry_size(INSERT_RANGE_PB, &estimated_log_entry_size);
    int32_t num_log_entries_per_page = max_page_size / estimated_log_entry_size;

    std::string file_path = BinlogUtil::binlog_file_path(_binlog_file_dir, 1);
    ASSIGN_OR_ABORT(auto fs, FileSystem::CreateSharedFromString(file_path));
    std::shared_ptr<BinlogFileWriter> file_writer =
            std::make_shared<BinlogFileWriter>(1, file_path, max_page_size, compression_type);
    ASSERT_OK(file_writer->init());
    std::vector<VersionInfo> versions;
    for (int i = 1; i <= 2000 || versions.empty(); i++) {
        VersionInfo version_info;
        version_info.version = i;
        int32_t num_pages = std::rand() % 5;
        version_info.num_entries = num_pages * num_log_entries_per_page;
        version_info.num_rows_per_entry = std::rand() % 100 + 1;
        RowsetId rowset_id;
        rowset_id.init(2, version_info.version, 2, 3);
        ASSERT_OK(file_writer->begin(version_info.version, 0, version_info.version));
        if (version_info.num_entries == 0) {
            ASSERT_OK(file_writer->add_empty());
        } else {
            for (int32_t n = 0; n < version_info.num_entries; n++) {
                ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(&rowset_id, n), 0,
                                                        version_info.num_rows_per_entry));
            }
        }
        bool abort = std::rand() % 3;
        if (abort) {
            ASSERT_OK(file_writer->abort());
        } else {
            ASSERT_OK(file_writer->commit(true));
            versions.push_back(version_info);
        }
    }
    ASSERT_OK(file_writer->close(true));

    std::shared_ptr<BinlogFileMetaPB> file_meta = std::make_shared<BinlogFileMetaPB>();
    file_writer->copy_file_meta(file_meta.get());
    ASSERT_EQ(versions[0].version, file_meta->start_version());
    ASSERT_EQ(0, file_meta->start_seq_id());
    ASSERT_EQ(versions.back().version, file_meta->end_version());
    ASSERT_EQ(versions.back().num_entries * versions.back().num_rows_per_entry - 1, file_meta->end_seq_id());
    verify_random_result(versions, file_path, file_meta);
}

// TODO add tests for primary key
TEST_F(BinlogFileTest, test_primary_key) {}

} // namespace starrocks