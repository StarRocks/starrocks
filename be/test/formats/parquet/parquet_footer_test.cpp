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

#include <filesystem>

#include "common/statusor.h"
#include "exec/hdfs_scanner.h"
#include "formats/parquet/file_reader.h"

namespace starrocks::parquet {

class ParquetFooterTest : public testing::Test {
public:
    ParquetFooterTest() { ctx.stats = &stats; }

protected:
    std::unique_ptr<RandomAccessFile> open_file(const std::string& file_path) {
        return *FileSystem::Default()->new_random_access_file(file_path);
    }

    HdfsScanStats stats;
    HdfsScannerContext ctx;
};

TEST_F(ParquetFooterTest, TestEmptyParquetFile) {
    const std::string file_path = "./be/test/formats/parquet/test_data/empty.parquet";
    auto file = open_file(file_path);
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(file_path));
    Status status = file_reader->init(&ctx);
    EXPECT_TRUE(status.is_corruption());
}

TEST_F(ParquetFooterTest, TestEncryptedParquetFile) {
    const std::string file_path = "./be/test/formats/parquet/test_data/encrypt_columns_and_footer.parquet.encrypted";
    auto file = open_file(file_path);
    auto file_reader =
            std::make_shared<FileReader>(config::vector_chunk_size, file.get(), std::filesystem::file_size(file_path));
    Status status = file_reader->init(&ctx);
    EXPECT_TRUE(status.is_not_supported());
}

} // namespace starrocks::parquet