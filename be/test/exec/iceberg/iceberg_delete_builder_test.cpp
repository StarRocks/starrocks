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

#include "exec/iceberg/iceberg_delete_builder.h"

#include <gtest/gtest.h>

#include "fs/fs.h"
#include "runtime/descriptor_helper.h"
#include "testutil/assert.h"

namespace starrocks {

class IcebergDeleteBuilderTest : public testing::Test {
public:
    IcebergDeleteBuilderTest() = default;
    ~IcebergDeleteBuilderTest() override = default;

protected:
    std::string _parquet_delete_path = "./be/test/exec/test_data/parquet_scanner/parquet_delete_file.parquet";
    std::string _parquet_data_path = "parquet_data_file.parquet";

    std::set<int64_t> _need_skip_rowids;
};

TEST_F(IcebergDeleteBuilderTest, TestParquetBuilder) {
    const DataCacheOptions data_cache_options{};
    std::unique_ptr<ParquetPositionDeleteBuilder> parquet_builder(
            new ParquetPositionDeleteBuilder(FileSystem::Default(), data_cache_options, _parquet_data_path));
    ASSERT_OK(parquet_builder->build(TQueryGlobals().time_zone, _parquet_delete_path, 845, &_need_skip_rowids));
    ASSERT_EQ(1, _need_skip_rowids.size());
}

} // namespace starrocks