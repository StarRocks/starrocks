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
#include "storage/binlog_test_base.h"
#include "testutil/assert.h"

namespace starrocks {

class BinlogFileTest : public BinlogTestBase {
public:
    void SetUp() override {
        CHECK_OK(fs::remove_all(_binlog_file_dir));
        CHECK_OK(fs::create_directories(_binlog_file_dir));
        ASSIGN_OR_ABORT(_fs, FileSystem::CreateSharedFromString(_binlog_file_dir));
    }

    void TearDown() override { fs::remove_all(_binlog_file_dir); }

protected:
    void test_reset_and_reopen(bool is_reset);
    void test_load(bool append_meta);

    std::shared_ptr<FileSystem> _fs;
    std::string _binlog_file_dir = "binlog_file_test";
};

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
    ASSERT_OK(file_writer->begin(1, 0, 1));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(1, 0), 0, 100));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(1, 1), 0, 80));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(1, 2), 0, 60));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(1, 3), 0, 40));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(1, 4), 0, 20));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(1, 5), 0, 10));

    ASSERT_OK(file_writer->commit(true));

    expect_file_meta.set_start_version(1);
    expect_file_meta.set_start_seq_id(0);
    expect_file_meta.set_start_timestamp_in_us(1);
    expect_file_meta.set_end_version(1);
    expect_file_meta.set_end_seq_id(309);
    expect_file_meta.set_end_timestamp_in_us(1);
    expect_file_meta.set_version_eof(true);
    expect_file_meta.set_num_pages(2);
    expect_file_meta.set_file_size(_fs->get_file_size(file_path).value());
    expect_file_meta.add_rowsets(1);
    expect_entries.emplace_back(build_insert_segment_log_entry(1, 1, 0, 0, 100, false, 1));
    expect_entries.emplace_back(build_insert_segment_log_entry(1, 1, 1, 100, 80, false, 1));
    expect_entries.emplace_back(build_insert_segment_log_entry(1, 1, 2, 180, 60, false, 1));
    expect_entries.emplace_back(build_insert_segment_log_entry(1, 1, 3, 240, 40, false, 1));
    expect_entries.emplace_back(build_insert_segment_log_entry(1, 1, 4, 280, 20, false, 1));
    expect_entries.emplace_back(build_insert_segment_log_entry(1, 1, 5, 300, 10, true, 1));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // a rowset contains one segment, and write to one page
    ASSERT_OK(file_writer->begin(2, 0, 2));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(2, 0), 0, 32));
    ASSERT_OK(file_writer->commit(true));

    expect_file_meta.set_end_version(2);
    expect_file_meta.set_end_seq_id(31);
    expect_file_meta.set_end_timestamp_in_us(2);
    expect_file_meta.set_version_eof(true);
    expect_file_meta.set_num_pages(3);
    expect_file_meta.set_file_size(_fs->get_file_size(file_path).value());
    expect_file_meta.add_rowsets(2);
    expect_entries.emplace_back(build_insert_segment_log_entry(2, 2, 0, 0, 32, true, 2));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // a rowset contains no segment
    ASSERT_OK(file_writer->begin(3, 0, 3));
    ASSERT_OK(file_writer->add_empty());
    ASSERT_OK(file_writer->commit(true));

    expect_file_meta.set_end_version(3);
    expect_file_meta.set_end_seq_id(0);
    expect_file_meta.set_end_timestamp_in_us(3);
    expect_file_meta.set_version_eof(true);
    expect_file_meta.set_num_pages(4);
    expect_file_meta.set_file_size(_fs->get_file_size(file_path).value());
    expect_entries.emplace_back(build_empty_rowset_log_entry(3, 3));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // a rowset contains multiple segments, but cross two binlog files, and commit with end_of_version false
    ASSERT_OK(file_writer->begin(4, 0, 4));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(4, 0), 0, 40));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(4, 1), 0, 20));
    ASSERT_OK(file_writer->commit(false));

    expect_file_meta.set_end_version(4);
    expect_file_meta.set_end_seq_id(59);
    expect_file_meta.set_end_timestamp_in_us(4);
    expect_file_meta.set_version_eof(false);
    expect_file_meta.set_num_pages(5);
    expect_file_meta.set_file_size(_fs->get_file_size(file_path).value());
    expect_file_meta.add_rowsets(4);
    expect_entries.emplace_back(build_insert_segment_log_entry(4, 4, 0, 0, 40, false, 4));
    expect_entries.emplace_back(build_insert_segment_log_entry(4, 4, 1, 40, 20, false, 4));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // verify that seek to different positions and read until the end of file
    verify_seek_and_next(file_path, file_meta, 1, 0, expect_entries, 0);
    verify_seek_and_next(file_path, file_meta, 1, 50, expect_entries, 0);
    verify_seek_and_next(file_path, file_meta, 1, 99, expect_entries, 0);
    verify_seek_and_next(file_path, file_meta, 1, 100, expect_entries, 1);
    verify_seek_and_next(file_path, file_meta, 1, 150, expect_entries, 1);
    verify_seek_and_next(file_path, file_meta, 1, 179, expect_entries, 1);
    verify_seek_and_next(file_path, file_meta, 1, 180, expect_entries, 2);
    verify_seek_and_next(file_path, file_meta, 1, 200, expect_entries, 2);
    verify_seek_and_next(file_path, file_meta, 1, 239, expect_entries, 2);
    verify_seek_and_next(file_path, file_meta, 2, 0, expect_entries, 6);
    verify_seek_and_next(file_path, file_meta, 2, 25, expect_entries, 6);
    verify_seek_and_next(file_path, file_meta, 2, 31, expect_entries, 6);
    verify_seek_and_next(file_path, file_meta, 3, 0, expect_entries, 7);
    verify_seek_and_next(file_path, file_meta, 4, 0, expect_entries, 8);
    verify_seek_and_next(file_path, file_meta, 4, 25, expect_entries, 8);
    verify_seek_and_next(file_path, file_meta, 4, 32, expect_entries, 8);
    verify_seek_and_next(file_path, file_meta, 4, 40, expect_entries, 9);
    verify_seek_and_next(file_path, file_meta, 4, 59, expect_entries, 9);
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

    // prepare failed, then abort
    ASSERT_OK(file_writer->begin(1, 0, 1));
    ASSERT_OK(file_writer->abort());

    // write some pages but not committed, then abort
    int64_t before_file_size = file_writer->file_size();
    ASSERT_OK(file_writer->begin(2, 0, 2));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(2, 0), 0, 100));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(2, 1), 0, 50));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(2, 2), 0, 96));
    // ensue there is something durable, and will be truncated when aborting
    ASSERT_LE(before_file_size, file_writer->file_size());
    ASSERT_OK(file_writer->abort());

    // write binlog successfully
    ASSERT_OK(file_writer->begin(3, 0, 3));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(3, 0), 0, 3));
    ASSERT_OK(file_writer->commit(true));

    expect_file_meta.set_start_version(3);
    expect_file_meta.set_start_seq_id(0);
    expect_file_meta.set_start_timestamp_in_us(3);
    expect_file_meta.set_end_version(3);
    expect_file_meta.set_end_seq_id(2);
    expect_file_meta.set_end_timestamp_in_us(3);
    expect_file_meta.set_version_eof(true);
    expect_file_meta.set_num_pages(1);
    expect_file_meta.set_file_size(_fs->get_file_size(file_path).value());
    expect_file_meta.add_rowsets(3);
    expect_entries.emplace_back(build_insert_segment_log_entry(3, 3, 0, 0, 3, true, 3));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // add some log entries, then abort
    before_file_size = file_writer->file_size();
    ASSERT_OK(file_writer->begin(4, 0, 4));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(4, 0), 0, 100));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(4, 1), 0, 80));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(4, 2), 0, 60));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(4, 3), 0, 40));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(4, 4), 0, 20));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(4, 5), 0, 10));
    ASSERT_LE(before_file_size, file_writer->file_size());
    ASSERT_OK(file_writer->abort());

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    // write binlog successfully
    ASSERT_OK(file_writer->begin(5, 0, 5));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(5, 0), 0, 60));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(5, 1), 0, 50));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(5, 2), 0, 40));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(5, 3), 0, 30));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(5, 4), 0, 20));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(5, 5), 0, 10));
    ASSERT_OK(file_writer->commit(false));

    expect_file_meta.set_end_version(5);
    expect_file_meta.set_end_seq_id(209);
    expect_file_meta.set_end_timestamp_in_us(5);
    expect_file_meta.set_version_eof(false);
    expect_file_meta.set_num_pages(3);
    expect_file_meta.set_file_size(_fs->get_file_size(file_path).value());
    expect_file_meta.add_rowsets(5);
    expect_entries.emplace_back(build_insert_segment_log_entry(5, 5, 0, 0, 60, false, 5));
    expect_entries.emplace_back(build_insert_segment_log_entry(5, 5, 1, 60, 50, false, 5));
    expect_entries.emplace_back(build_insert_segment_log_entry(5, 5, 2, 110, 40, false, 5));
    expect_entries.emplace_back(build_insert_segment_log_entry(5, 5, 3, 150, 30, false, 5));
    expect_entries.emplace_back(build_insert_segment_log_entry(5, 5, 4, 180, 20, false, 5));
    expect_entries.emplace_back(build_insert_segment_log_entry(5, 5, 5, 200, 10, false, 5));

    // test begin without commit/abort
    ASSERT_OK(file_writer->begin(6, 0, 6));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(6, 0), 0, 40));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(6, 1), 0, 20));

    file_writer->copy_file_meta(file_meta.get());
    verify_file_meta(&expect_file_meta, file_meta);

    verify_seek_and_next(file_path, file_meta, 3, 0, expect_entries, 0);
}

void BinlogFileTest::test_reset_and_reopen(bool is_reset) {
    std::vector<std::shared_ptr<TestLogEntryInfo>> expect_entries;
    std::string file_path = BinlogUtil::binlog_file_path(_binlog_file_dir, 1);
    std::shared_ptr<BinlogFileWriter> file_writer = std::make_shared<BinlogFileWriter>(1, file_path, 50, LZ4_FRAME);
    ASSERT_OK(file_writer->init());

    // 1. write binlog for version 1 and 2
    ASSERT_OK(file_writer->begin(1, 0, 1));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(1, 0), 0, 100));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(1, 1), 0, 50));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(1, 2), 0, 96));
    ASSERT_OK(file_writer->commit(true));
    std::shared_ptr<BinlogFileMetaPB> file_meta_1 = std::make_shared<BinlogFileMetaPB>();
    file_writer->copy_file_meta(file_meta_1.get());
    ASSERT_EQ(file_meta_1->file_size(), _fs->get_file_size(file_path).value());
    expect_entries.emplace_back(build_insert_segment_log_entry(1, 1, 0, 0, 100, false, 1));
    expect_entries.emplace_back(build_insert_segment_log_entry(1, 1, 1, 100, 50, false, 1));
    expect_entries.emplace_back(build_insert_segment_log_entry(1, 1, 2, 150, 96, true, 1));

    ASSERT_OK(file_writer->begin(2, 0, 2));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(2, 0), 0, 32));
    ASSERT_OK(file_writer->commit(true));
    std::shared_ptr<BinlogFileMetaPB> file_meta_2 = std::make_shared<BinlogFileMetaPB>();
    file_writer->copy_file_meta(file_meta_2.get());
    ASSERT_EQ(_fs->get_file_size(file_path).value(), file_meta_2->file_size());
    ASSERT_LE(file_meta_1->file_size(), file_meta_2->file_size());
    expect_entries.emplace_back(build_insert_segment_log_entry(2, 2, 0, 0, 32, true, 2));

    verify_seek_and_next(file_path, file_meta_2, 1, 0, expect_entries, 0);

    // 2. reset/reopen the writer, and fallback to version 1
    if (is_reset) {
        ASSERT_OK(file_writer->reset(file_meta_1.get()));
    } else {
        ASSERT_OK(file_writer->close(true));
        auto status_or = BinlogFileWriter::reopen(1, file_path, 50, LZ4_FRAME, file_meta_1.get());
        ASSERT_OK(status_or.status());
        file_writer = status_or.value();
    }
    ASSERT_EQ(file_meta_1->file_size(), _fs->get_file_size(file_path).value());
    std::unordered_set<int64_t>& fallback_rowsets = file_writer->rowsets();
    ASSERT_EQ(fallback_rowsets.size(), file_meta_1->rowsets().size());
    for (auto id : file_meta_1->rowsets()) {
        ASSERT_EQ(1, fallback_rowsets.count(id));
    }
    expect_entries.pop_back();
    verify_seek_and_next(file_path, file_meta_1, 1, 0, expect_entries, 0);

    // 3. append new version 3
    ASSERT_OK(file_writer->begin(3, 0, 3));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(3, 0), 0, 40));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(3, 1), 0, 20));
    ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(3, 2), 0, 30));
    ASSERT_OK(file_writer->commit(false));
    std::shared_ptr<BinlogFileMetaPB> file_meta_3 = std::make_shared<BinlogFileMetaPB>();
    file_writer->copy_file_meta(file_meta_3.get());
    expect_entries.emplace_back(build_insert_segment_log_entry(3, 3, 0, 0, 40, false, 3));
    expect_entries.emplace_back(build_insert_segment_log_entry(3, 3, 1, 40, 20, false, 3));
    expect_entries.emplace_back(build_insert_segment_log_entry(3, 3, 2, 60, 30, false, 3));

    verify_seek_and_next(file_path, file_meta_3, 1, 0, expect_entries, 0);
}

TEST_F(BinlogFileTest, test_reset) {
    test_reset_and_reopen(true);
}

TEST_F(BinlogFileTest, test_reopen) {
    test_reset_and_reopen(false);
}

// Test generating large binlog file with random content for duplicate key
TEST_F(BinlogFileTest, DISABLED_test_random_write_read) {
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
    std::vector<DupKeyVersionInfo> versions;
    while (file_writer->file_size() < expect_file_size && versions.size() < expect_num_versions) {
        DupKeyVersionInfo version_info;
        version_info.version = versions.size() + 1;
        // there is 1/20 to generate empty version
        version_info.num_entries = (std::rand() % 20 == 0) ? 0 : (std::rand() % avg_entries_per_version * 2 + 1);
        version_info.num_rows_per_entry = std::rand() % 100 + 1;
        version_info.timestamp = version_info.version;
        versions.push_back(version_info);
        ASSERT_OK(file_writer->begin(version_info.version, 0, version_info.timestamp));
        if (version_info.num_entries == 0) {
            ASSERT_OK(file_writer->add_empty());
        } else {
            for (int32_t n = 0; n < version_info.num_entries; n++) {
                ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(version_info.version, n), 0,
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
    verify_dup_key_multiple_versions(versions, _binlog_file_dir, {file_meta});
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
    std::vector<DupKeyVersionInfo> versions;
    for (int i = 1; i <= 50 || versions.empty(); i++) {
        DupKeyVersionInfo version_info;
        version_info.version = i;
        int32_t num_pages = std::rand() % 5;
        version_info.num_entries = num_pages * num_log_entries_per_page;
        version_info.num_rows_per_entry = std::rand() % 100 + 1;
        version_info.timestamp = version_info.version;
        ASSERT_OK(file_writer->begin(version_info.version, 0, version_info.timestamp));
        if (version_info.num_entries == 0) {
            ASSERT_OK(file_writer->add_empty());
        } else {
            for (int32_t n = 0; n < version_info.num_entries; n++) {
                ASSERT_OK(file_writer->add_insert_range(RowsetSegInfo(version_info.version, n), 0,
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
    ASSERT_EQ(std::max(versions.back().num_entries * versions.back().num_rows_per_entry - 1, (int64_t)0),
              file_meta->end_seq_id());
    verify_dup_key_multiple_versions(versions, _binlog_file_dir, {file_meta});
}

// TODO add tests for primary key
TEST_F(BinlogFileTest, test_primary_key) {}

void BinlogFileTest::test_load(bool append_meta) {
    int64_t file_id = 1;
    std::string file_path = BinlogUtil::binlog_file_path(_binlog_file_dir, file_id);
    std::shared_ptr<BinlogFileWriter> file_writer =
            std::make_shared<BinlogFileWriter>(file_id, file_path, 1024 * 1024 * 1024, LZ4_FRAME);
    ASSERT_OK(file_writer->init());
    int32_t num_version = 10;
    int32_t num_segs_per_version = 10;
    int32_t num_rows_per_seg = 100;
    std::vector<BinlogFileMetaPBPtr> metas_for_each_page;
    BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
    for (int version = 1; version <= num_version; version++) {
        int64_t timestamp = version * 1000000;
        int64_t rowset_id = version;
        ASSERT_OK(file_writer->begin(version, 0, timestamp));
        if (version == 1) {
            file_meta->set_id(file_id);
            file_meta->set_start_version(version);
            file_meta->set_start_seq_id(0);
            file_meta->set_start_timestamp_in_us(timestamp);
            file_meta->set_num_pages(0);
        }
        file_meta->add_rowsets(rowset_id);
        for (int32_t seg_index = 0; seg_index < num_segs_per_version; seg_index++) {
            RowsetSegInfo info(rowset_id, seg_index);
            ASSERT_OK(file_writer->add_insert_range(info, 0, num_rows_per_seg));
            bool version_eof;
            if (seg_index + 1 < num_segs_per_version) {
                ASSERT_OK(file_writer->force_flush_page(false));
                version_eof = false;
            } else {
                ASSERT_OK(file_writer->commit(true));
                version_eof = true;
            }
            file_meta->set_end_version(version);
            file_meta->set_end_seq_id((seg_index + 1) * num_rows_per_seg - 1);
            file_meta->set_end_timestamp_in_us(timestamp);
            file_meta->set_version_eof(version_eof);
            file_meta->set_num_pages(file_meta->num_pages() + 1);
            file_meta->set_file_size(_fs->get_file_size(file_path).value());
            std::shared_ptr<BinlogFileMetaPB> meta = std::make_shared<BinlogFileMetaPB>();
            meta->CopyFrom(*file_meta);
            metas_for_each_page.push_back(meta);
        }
        std::shared_ptr<BinlogFileMetaPB> meta = std::make_shared<BinlogFileMetaPB>();
        file_writer->copy_file_meta(meta.get());
        verify_file_meta(meta.get(), file_meta);
    }
    ASSERT_OK(file_writer->close(append_meta));

    for (int32_t i = 0; i < metas_for_each_page.size(); i++) {
        auto& meta = metas_for_each_page[i];
        BinlogLsn maxLsn(meta->end_version(), meta->end_seq_id() + 1);
        auto status_or = BinlogFileReader::load_meta(file_id, file_path, maxLsn);
        ASSERT_OK(status_or.status());
        verify_file_meta(meta.get(), status_or.value());
    }
}

TEST_F(BinlogFileTest, test_load_with_append_meta) {
    test_load(true);
}

TEST_F(BinlogFileTest, test_load_without_append_meta) {
    test_load(false);
}

} // namespace starrocks