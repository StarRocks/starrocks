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

#include "exec/hdfs_scanner/hdfs_scanner_json.h"

#include <google/protobuf/descriptor.pb.h>
#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "formats/parquet/parquet_test_util/util.h"
#include "storage/chunk_helper.h"

namespace starrocks {
class HdfsScannerJsonReaderTest : public testing::Test {
public:
    void SetUp() override;
    void TearDown() override;

    void create_random_access_file(const std::string& path);
    TupleDescriptor* create_tuple_descriptor();
    static std::string gen_check_str(size_t start, size_t end);
    static std::string gen_check_int(size_t start, size_t end);

protected:
    std::shared_ptr<FileSystem> _fs;
    OpenFileOptions _opts;
    HdfsScanStats _app_stats;
    HdfsScanStats _fs_stats;
    RuntimeState _runtime_state;
    ObjectPool _pool;

    std::shared_ptr<io::CacheInputStream> _cache_input_stream = nullptr;
    std::shared_ptr<io::SharedBufferedInputStream> _shared_buffered_input_stream = nullptr;
    std::unique_ptr<RandomAccessFile> _file;
};

void HdfsScannerJsonReaderTest::SetUp() {
    _opts.fs_stats = &_fs_stats;
    _opts.app_stats = &_app_stats;
}

void HdfsScannerJsonReaderTest::TearDown() {}

void HdfsScannerJsonReaderTest::create_random_access_file(const std::string& path) {
    ASSIGN_OR_ABORT(_fs, FileSystem::CreateSharedFromString(path));
    _opts.fs = _fs.get();
    _opts.path = path;
    _opts.file_size = _fs->get_file_size(path).value();
    ASSIGN_OR_ABORT(_file,
                    HdfsScanner::create_random_access_file(_shared_buffered_input_stream, _cache_input_stream, _opts));
}

TupleDescriptor* HdfsScannerJsonReaderTest::create_tuple_descriptor() {
    parquet::Utils::SlotDesc slots[] = {
            {"c1", TypeDescriptor::from_logical_type(TYPE_INT)},
            {"c2", TypeDescriptor::from_logical_type(TYPE_VARCHAR)},
            {""} // end
    };
    return parquet::Utils::create_tuple_descriptor(&_runtime_state, &_pool, slots);
}

std::string HdfsScannerJsonReaderTest::gen_check_str(size_t start, size_t end) {
    std::stringstream ss;
    ss << "[";
    for (size_t i = start; i <= end; i++) {
        ss << "'str_" << i << "'";
        if (i != end) {
            ss << ", ";
        }
    }
    ss << "]";
    return ss.str();
}

std::string HdfsScannerJsonReaderTest::gen_check_int(size_t start, size_t end) {
    std::stringstream ss;
    ss << "[";
    for (size_t i = start; i <= end; i++) {
        ss << i;
        if (i != end) {
            ss << ", ";
        }
    }
    ss << "]";
    return ss.str();
}

TEST_F(HdfsScannerJsonReaderTest, test_read_all_rows) {
    std::string path = "./be/test/exec/hdfs_scanner/test_data/2_cols_10_rows.json";

    create_random_access_file(path);
    auto* tuple_desc = create_tuple_descriptor();

    HdfsJsonReader json_reader(_file.get(), tuple_desc->slots());
    ASSERT_OK(json_reader.init());

    auto chunk = ChunkHelper::new_chunk(*tuple_desc, 0);
    EXPECT_STATUS(Status::EndOfFile(""), json_reader.next_record(chunk.get(), 4096));
    ASSERT_EQ(chunk->num_rows(), 10);
    ASSERT_EQ(chunk->get_column_by_slot_id(0)->debug_string(), "[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->debug_string(),
              "['str_1', 'str_2', 'str_3', 'str_4', 'str_5', 'str_6', 'str_7', 'str_8', 'str_9', 'str_10']");
}

TEST_F(HdfsScannerJsonReaderTest, test_read_more_rows) {
    std::string path = "./be/test/exec/hdfs_scanner/test_data/2_cols_10_rows.json";

    create_random_access_file(path);
    auto* tuple_desc = create_tuple_descriptor();

    HdfsJsonReader json_reader(_file.get(), tuple_desc->slots());
    ASSERT_OK(json_reader.init());

    auto chunk = ChunkHelper::new_chunk(*tuple_desc, 0);
    ASSERT_OK(json_reader.next_record(chunk.get(), 4));
    ASSERT_EQ(chunk->num_rows(), 4);
    ASSERT_EQ(chunk->get_column_by_slot_id(0)->debug_string(), "[1, 2, 3, 4]");
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->debug_string(), "['str_1', 'str_2', 'str_3', 'str_4']");

    chunk->reset();
    ASSERT_OK(json_reader.next_record(chunk.get(), 4));
    ASSERT_EQ(chunk->num_rows(), 4);
    ASSERT_EQ(chunk->get_column_by_slot_id(0)->debug_string(), "[5, 6, 7, 8]");
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->debug_string(), "['str_5', 'str_6', 'str_7', 'str_8']");

    chunk->reset();
    EXPECT_STATUS(Status::EndOfFile(""), json_reader.next_record(chunk.get(), 4));
    ASSERT_EQ(chunk->num_rows(), 2);
    ASSERT_EQ(chunk->get_column_by_slot_id(0)->debug_string(), "[9, 10]");
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->debug_string(), "['str_9', 'str_10']");
}

TEST_F(HdfsScannerJsonReaderTest, test_read_large_file) {
    std::string path = "./be/test/exec/hdfs_scanner/test_data/2_cols_150_rows.json";

    create_random_access_file(path);
    auto* tuple_desc = create_tuple_descriptor();

    HdfsJsonReader json_reader(_file.get(), tuple_desc->slots());
    ASSERT_OK(json_reader.init());

    auto chunk = ChunkHelper::new_chunk(*tuple_desc, 0);
    ASSERT_OK(json_reader.next_record(chunk.get(), 50));
    ASSERT_EQ(chunk->num_rows(), 50);

    ASSERT_EQ(chunk->get_column_by_slot_id(0)->debug_string(), gen_check_int(1, 50));
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->debug_string(), gen_check_str(1, 50));

    chunk->reset();
    ASSERT_OK(json_reader.next_record(chunk.get(), 50));
    ASSERT_EQ(chunk->num_rows(), 50);
    ASSERT_EQ(chunk->get_column_by_slot_id(0)->debug_string(), gen_check_int(51, 100));
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->debug_string(), gen_check_str(51, 100));

    chunk->reset();
    ASSERT_OK(json_reader.next_record(chunk.get(), 50));
    ASSERT_EQ(chunk->num_rows(), 50);
    ASSERT_EQ(chunk->get_column_by_slot_id(0)->debug_string(), gen_check_int(101, 150));
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->debug_string(), gen_check_str(101, 150));

    chunk->reset();
    EXPECT_STATUS(Status::EndOfFile(""), json_reader.next_record(chunk.get(), 50));
}

TEST_F(HdfsScannerJsonReaderTest, test_read_large_rows) {
    std::string path = "./be/test/exec/hdfs_scanner/test_data/2_cols_2_rows_large.json";

    create_random_access_file(path);
    auto* tuple_desc = create_tuple_descriptor();

    HdfsJsonReader json_reader(_file.get(), tuple_desc->slots());
    ASSERT_OK(json_reader.init());

    auto chunk = ChunkHelper::new_chunk(*tuple_desc, 0);
    EXPECT_STATUS(Status::NotSupported(""), json_reader.next_record(chunk.get(), 50));
}

TEST_F(HdfsScannerJsonReaderTest, test_read_wrong_order_json) {
    std::string path = "./be/test/exec/hdfs_scanner/test_data/3_cols_10_rows_wrong_order.json";

    create_random_access_file(path);
    auto* tuple_desc = create_tuple_descriptor();

    HdfsJsonReader json_reader(_file.get(), tuple_desc->slots());
    ASSERT_OK(json_reader.init());

    auto chunk = ChunkHelper::new_chunk(*tuple_desc, 0);
    EXPECT_STATUS(Status::EndOfFile(""), json_reader.next_record(chunk.get(), 50));
    ASSERT_EQ(chunk->num_rows(), 8);
    ASSERT_EQ(chunk->get_column_by_slot_id(0)->debug_string(), "[1, 2, 3, 4, 5, NULL, 7, NULL]");
    ASSERT_EQ(chunk->get_column_by_slot_id(1)->debug_string(),
              "['str_1', 'str_2', 'str_3', 'str_4', NULL, 'str_6', 'str_7', NULL]");
}
} // namespace starrocks