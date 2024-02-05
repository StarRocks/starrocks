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

#include "storage/binlog_builder.h"

#include <gtest/gtest.h>

#include "fs/fs_util.h"
#include "storage/binlog_test_base.h"
#include "testutil/assert.h"

namespace starrocks {

class ControlParams;
class ExpectBuildResult;
class ExpectDiscardResult;

class BinlogBuilderTest : public BinlogTestBase {
public:
    void SetUp() override {
        srand(GetCurrentTimeMicros());
        CHECK_OK(fs::remove_all(_binlog_file_dir));
        CHECK_OK(fs::create_directories(_binlog_file_dir));
        ASSIGN_OR_ABORT(_fs, FileSystem::CreateSharedFromString(_binlog_file_dir));
    }

    void TearDown() override { fs::remove_all(_binlog_file_dir); }

    void test_write_one_version(ControlParams control_params, ExpectBuildResult expect_result);
    void test_abort_one_version(int32_t num_files, bool start_with_active_writer);
    void test_discard_binlog_build_result(int64_t version, BinlogBuildResultPtr result,
                                          ExpectDiscardResult& expect_result);

protected:
    std::shared_ptr<FileSystem> _fs;
    std::string _binlog_file_dir = "binlog_builder_test";
};

// Control the behaviour of test
struct ControlParams {
    // the maximum number of entries of a version
    int32_t max_num_entries;
    // the maximum number of files to write in a version
    int32_t max_num_files;
    // whether there is an active writer for the builder
    bool start_with_active_writer;
    // whether to force the last file to reach the file size limit when commit
    bool force_to_reach_file_size_limit;
};

struct ExpectBuildResult {
    bool result_with_active_writer;
    // -1 indicates that don't verify the number of files
    int32_t num_files;
};

void BinlogBuilderTest::test_write_one_version(ControlParams control_params, ExpectBuildResult expect_result) {
    int64_t max_file_size = 1024 * 5;
    int32_t max_page_size = 256;
    int64_t next_file_id = 1;
    std::vector<DupKeyVersionInfo> version_info_vec;

    BinlogFileWriterPtr active_writer;
    BinlogFileMetaPBPtr active_meta;
    if (control_params.start_with_active_writer) {
        DupKeyVersionInfo version_1(1, 1, 100, 1);
        version_info_vec.push_back(version_1);
        std::string file_path = BinlogUtil::binlog_file_path(_binlog_file_dir, next_file_id);
        active_writer = std::make_shared<BinlogFileWriter>(next_file_id, file_path, max_page_size, LZ4_FRAME);
        ASSERT_OK(active_writer->init());
        ASSERT_OK(active_writer->begin(version_1.version, 0, version_1.timestamp));
        ASSERT_OK(active_writer->add_insert_range(RowsetSegInfo(1, 0), 0, version_1.num_rows_per_entry));
        ASSERT_OK(active_writer->commit(true));
        active_meta = std::make_shared<BinlogFileMetaPB>();
        active_writer->copy_file_meta(active_meta.get());
        next_file_id += 1;
    }

    BinlogBuilderParamsPtr param = std::make_shared<BinlogBuilderParams>();
    param->binlog_storage_path = _binlog_file_dir;
    param->max_file_size = max_file_size;
    param->max_page_size = max_page_size;
    param->compression_type = LZ4_FRAME;
    param->start_file_id = next_file_id;
    param->active_file_writer = active_writer;
    param->active_file_meta = active_meta;

    std::shared_ptr<BinlogBuilder> builder = std::make_shared<BinlogBuilder>(1, 2, 2, param);
    BinlogBuildResultPtr result = std::make_shared<BinlogBuildResult>();
    int32_t num_entries = 0;
    while (num_entries < control_params.max_num_entries) {
        if (builder->num_files() == control_params.max_num_files &&
            builder->current_write_file_size() > max_file_size - 2 * max_page_size) {
            break;
        }
        ASSERT_OK(builder->add_insert_range(RowsetSegInfo(2, num_entries), 0, 100));
        num_entries += 1;
    }
    // hack to let the file size reach the limit and close it
    if (control_params.force_to_reach_file_size_limit) {
        param->max_file_size = 1;
    }
    ASSERT_OK(builder->commit(result.get()));
    version_info_vec.push_back({2, num_entries, 100, 2});

    ASSERT_EQ(param.get(), result->params.get());
    int64_t start_file_id = control_params.start_with_active_writer ? next_file_id - 1 : next_file_id;
    ASSERT_EQ(start_file_id + expect_result.num_files, result->next_file_id);

    if (expect_result.result_with_active_writer) {
        ASSERT_TRUE(result->active_writer != nullptr);
    } else {
        ASSERT_TRUE(result->active_writer == nullptr);
    }

    ASSERT_EQ(expect_result.num_files, result->metas.size());

    // test file size limitation
    for (int i = 0; i + 1 < result->metas.size(); i++) {
        auto& meta = result->metas[i];
        ASSERT_EQ(start_file_id + i, meta->id());
        ASSERT_TRUE(max_file_size <= meta->file_size());
        // inaccurate upper limitation
        ASSERT_TRUE(meta->file_size() <= max_file_size + 2 * max_page_size);
    }

    verify_dup_key_multiple_versions(version_info_vec, _binlog_file_dir, result->metas);
}

TEST_F(BinlogBuilderTest, test_write_one_version_one_file) {
    ControlParams params{.max_num_entries = 2000,
                         .max_num_files = 1,
                         .start_with_active_writer = false,
                         .force_to_reach_file_size_limit = true};
    ExpectBuildResult result{.result_with_active_writer = false, .num_files = 1};
    test_write_one_version(params, result);
}

TEST_F(BinlogBuilderTest, test_write_one_version_multiple_files) {
    ControlParams params{.max_num_entries = INT32_MAX,
                         .max_num_files = 5,
                         .start_with_active_writer = false,
                         .force_to_reach_file_size_limit = true};
    ExpectBuildResult result{.result_with_active_writer = false, .num_files = 5};
    test_write_one_version(params, result);
}

TEST_F(BinlogBuilderTest, test_active_writer) {
    ControlParams params{.max_num_entries = INT32_MAX,
                         .max_num_files = 5,
                         .start_with_active_writer = true,
                         .force_to_reach_file_size_limit = false};
    ExpectBuildResult result{.result_with_active_writer = true, .num_files = 5};
    test_write_one_version(params, result);
}

void BinlogBuilderTest::test_abort_one_version(int32_t num_files, bool start_with_active_writer) {
    int64_t max_file_size = 1024 * 5;
    int32_t max_page_size = 64;
    int64_t next_file_id = 1;
    std::vector<DupKeyVersionInfo> version_info_vec;

    BinlogFileWriterPtr active_writer;
    BinlogFileMetaPBPtr active_meta;
    if (start_with_active_writer) {
        DupKeyVersionInfo version_1(1, 1, 100, 1);
        version_info_vec.push_back(version_1);
        std::string file_path = BinlogUtil::binlog_file_path(_binlog_file_dir, next_file_id);
        active_writer = std::make_shared<BinlogFileWriter>(next_file_id, file_path, max_page_size, LZ4_FRAME);
        ASSERT_OK(active_writer->init());
        ASSERT_OK(active_writer->begin(version_1.version, 0, version_1.version));
        ASSERT_OK(active_writer->add_insert_range(RowsetSegInfo(1, 0), 0, version_1.num_rows_per_entry));
        ASSERT_OK(active_writer->commit(true));
        active_meta = std::make_shared<BinlogFileMetaPB>();
        active_writer->copy_file_meta(active_meta.get());
        next_file_id += 1;
    }

    BinlogBuilderParamsPtr param = std::make_shared<BinlogBuilderParams>();
    param->binlog_storage_path = _binlog_file_dir;
    param->max_file_size = max_file_size;
    param->max_page_size = max_page_size;
    param->compression_type = LZ4_FRAME;
    param->start_file_id = next_file_id;
    param->active_file_writer = active_writer;
    param->active_file_meta = active_meta;

    std::shared_ptr<BinlogBuilder> builder = std::make_shared<BinlogBuilder>(1, 2, 2, param);
    BinlogBuildResultPtr result = std::make_shared<BinlogBuildResult>();
    if (num_files == 1) {
        for (int i = 0; i < 10; i++) {
            ASSERT_OK(builder->add_insert_range(RowsetSegInfo(2, i), 0, 100));
        }
    } else {
        int num_entries = 0;
        while (builder->num_files() < num_files) {
            ASSERT_OK(builder->add_insert_range(RowsetSegInfo(2, num_entries), 0, 100));
            num_entries += 1;
        }
    }
    builder->abort(result.get());
    ASSERT_EQ(param.get(), result->params.get());
    ASSERT_EQ(0, result->metas.size());
    int64_t first_delete_id;
    if (start_with_active_writer) {
        ASSERT_TRUE(result->active_writer != nullptr);
        BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
        result->active_writer->copy_file_meta(file_meta.get());
        verify_dup_key_multiple_versions(version_info_vec, _binlog_file_dir, {file_meta});
        first_delete_id = next_file_id + 1;
    } else {
        ASSERT_TRUE(result->active_writer == nullptr);
        first_delete_id = next_file_id;
    }

    for (int64_t id = first_delete_id; id < result->next_file_id; id++) {
        std::string path = BinlogUtil::binlog_file_path(_binlog_file_dir, id);
        ASSERT_TRUE(_fs->path_exists(path).is_not_found());
    }
}

TEST_F(BinlogBuilderTest, test_abort_one_version_one_file_with_active_writer) {
    test_abort_one_version(1, true);
}

TEST_F(BinlogBuilderTest, test_abort_one_version_one_file_without_active_writer) {
    test_abort_one_version(1, false);
}

TEST_F(BinlogBuilderTest, test_abort_one_version_multiple_files_with_active_writer) {
    test_abort_one_version(5, true);
}

TEST_F(BinlogBuilderTest, test_abort_one_version_multiple_files_without_active_writer) {
    test_abort_one_version(5, false);
}

TEST_F(BinlogBuilderTest, test_random_commit_abort_multiple_versions) {
    int32_t num_versions = 100;
    int64_t max_file_size = 5 * 1024;
    int32_t max_page_size = 64;
    std::vector<DupKeyVersionInfo> version_info_vec;
    std::map<int64_t, BinlogFileMetaPBPtr> metas;

    BinlogBuildResultPtr last_result;
    for (int version = 1; version <= num_versions; version++) {
        BinlogBuilderParamsPtr param = std::make_shared<BinlogBuilderParams>();
        param->binlog_storage_path = _binlog_file_dir;
        param->max_file_size = max_file_size;
        param->max_page_size = max_page_size;
        param->compression_type = LZ4_FRAME;
        if (last_result == nullptr) {
            param->start_file_id = 1;
        } else {
            param->start_file_id = last_result->next_file_id;
            if (last_result->active_writer != nullptr) {
                param->active_file_writer = last_result->active_writer;
                param->active_file_meta = std::make_shared<BinlogFileMetaPB>();
                last_result->active_writer->copy_file_meta(param->active_file_meta.get());
            }
        }

        std::shared_ptr<BinlogBuilder> builder = std::make_shared<BinlogBuilder>(1, version, version, param);
        bool is_empty = (std::rand() % 50) == 0;
        int32_t num_entries = 0;
        int32_t rows_per_entry = std::rand() % 100 + 1;
        if (is_empty) {
            builder->add_empty();
        } else {
            int32_t num_files = std::rand() % 5 + 1;
            while (num_entries < 5 || builder->num_files() < num_files) {
                ASSERT_OK(builder->add_insert_range(RowsetSegInfo(version, num_entries), 0, rows_per_entry));
                num_entries += 1;
            }
        }
        bool is_abort = (std::rand() % 5) == 0;
        BinlogBuildResultPtr result = std::make_shared<BinlogBuildResult>();
        if (is_abort) {
            builder->abort(result.get());
        } else {
            ASSERT_OK(builder->commit(result.get()));
            version_info_vec.push_back({version, num_entries, rows_per_entry, version});
        }
        for (auto& meta : result->metas) {
            metas[meta->id()] = meta;
        }
        last_result = result;
    }

    std::vector<BinlogFileMetaPBPtr> meta_vect;
    for (auto it = metas.begin(); it != metas.end(); it++) {
        meta_vect.push_back(it->second);
    }
    verify_dup_key_multiple_versions(version_info_vec, _binlog_file_dir, meta_vect);
}

struct ExpectDiscardResult {
    bool has_active_writer;
    std::vector<DupKeyVersionInfo> active_version_info_vect;
};

void BinlogBuilderTest::test_discard_binlog_build_result(int64_t version, BinlogBuildResultPtr result,
                                                         ExpectDiscardResult& expect_result) {
    BinlogFileWriterPtr active_writer = BinlogBuilder::discard_binlog_build_result(version, *result);

    if (expect_result.has_active_writer) {
        ASSERT_TRUE(active_writer != nullptr);
        BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
        active_writer->copy_file_meta(file_meta.get());
        verify_dup_key_multiple_versions(expect_result.active_version_info_vect, _binlog_file_dir, {file_meta});

        std::vector<DupKeyVersionInfo> new_version_info_vect(expect_result.active_version_info_vect);
        DupKeyVersionInfo new_version(1000, 1, 100, 1000);
        new_version_info_vect.push_back(new_version);
        ASSERT_OK(active_writer->begin(new_version.version, 0, new_version.timestamp));
        ASSERT_OK(active_writer->add_insert_range(RowsetSegInfo(new_version.version, 0), 0,
                                                  new_version.num_rows_per_entry));
        ASSERT_OK(active_writer->commit(true));
        BinlogFileMetaPBPtr new_file_meta = std::make_shared<BinlogFileMetaPB>();
        active_writer->copy_file_meta(new_file_meta.get());
        verify_dup_key_multiple_versions(new_version_info_vect, _binlog_file_dir, {new_file_meta});
    } else {
        ASSERT_TRUE(active_writer == nullptr);
    }

    int first_invalid_id = expect_result.has_active_writer ? 1 : 0;
    for (int i = first_invalid_id; i < result->metas.size(); i++) {
        auto& meta = result->metas[i];
        std::string path = BinlogUtil::binlog_file_path(_binlog_file_dir, meta->id());
        ASSERT_TRUE(_fs->path_exists(path).is_not_found());
    }
}

TEST_F(BinlogBuilderTest, test_discard_result_without_active_writer) {
    BinlogBuilderParamsPtr param = std::make_shared<BinlogBuilderParams>();
    param->binlog_storage_path = _binlog_file_dir;
    param->max_file_size = 1024 * 1024;
    param->max_page_size = 1024;
    param->compression_type = LZ4_FRAME;
    param->start_file_id = 1;
    param->active_file_writer = nullptr;
    param->active_file_meta = nullptr;

    BinlogBuildResultPtr result = std::make_shared<BinlogBuildResult>();
    result->params = param;
    result->next_file_id = 5;
    result->active_writer = nullptr;
    for (int64_t file_id = param->start_file_id; file_id < result->next_file_id; file_id++) {
        BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
        result->metas.push_back(file_meta);
        file_meta->set_id(file_id);
        std::string path = BinlogUtil::binlog_file_path(_binlog_file_dir, file_id);
        auto wf = _fs->new_writable_file(path);
        ASSERT_OK(wf.status());
        wf.value()->close();
        ASSERT_OK(_fs->path_exists(path));
    }

    ExpectDiscardResult expect_result;
    expect_result.has_active_writer = false;

    test_discard_binlog_build_result(2, result, expect_result);
}

TEST_F(BinlogBuilderTest, test_discard_result_with_active_writer) {
    BinlogBuilderParamsPtr param = std::make_shared<BinlogBuilderParams>();
    param->binlog_storage_path = _binlog_file_dir;
    param->max_file_size = 1024 * 1024;
    param->max_page_size = 1024;
    param->compression_type = LZ4_FRAME;
    param->start_file_id = 1;

    DupKeyVersionInfo version_info(1, 1, 100, 1);
    BinlogFileWriterPtr active_writer = std::make_shared<BinlogFileWriter>(
            param->start_file_id, BinlogUtil::binlog_file_path(_binlog_file_dir, param->start_file_id),
            param->max_file_size, param->compression_type);
    ASSERT_OK(active_writer->init());
    ASSERT_OK(active_writer->begin(version_info.version, 0, version_info.version));
    ASSERT_OK(active_writer->add_insert_range(RowsetSegInfo(version_info.version, 0), 0,
                                              version_info.num_rows_per_entry));
    ASSERT_OK(active_writer->commit(true));
    param->active_file_writer = active_writer;
    param->active_file_meta = std::make_shared<BinlogFileMetaPB>();
    active_writer->copy_file_meta(param->active_file_meta.get());

    BinlogBuildResultPtr result = std::make_shared<BinlogBuildResult>();
    result->params = param;
    result->next_file_id = 5;
    result->active_writer = nullptr;
    for (int64_t file_id = param->start_file_id + 1; file_id < result->next_file_id; file_id++) {
        BinlogFileMetaPBPtr file_meta = std::make_shared<BinlogFileMetaPB>();
        result->metas.push_back(file_meta);
        file_meta->set_id(file_id);
        std::string path = BinlogUtil::binlog_file_path(_binlog_file_dir, file_id);
        auto wf = _fs->new_writable_file(path);
        ASSERT_OK(wf.status());
        wf.value()->close();
        ASSERT_OK(_fs->path_exists(path));
    }

    ExpectDiscardResult expect_result;
    expect_result.has_active_writer = true;
    expect_result.active_version_info_vect.push_back(version_info);

    test_discard_binlog_build_result(2, result, expect_result);
}

} // namespace starrocks