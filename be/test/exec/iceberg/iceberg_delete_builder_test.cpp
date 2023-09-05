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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/exec/es_query_builder_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include "exec/iceberg//iceberg_delete_builder.h"

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
    std::unique_ptr<ParquetPositionDeleteBuilder> parquet_builder(
            new ParquetPositionDeleteBuilder(FileSystem::Default(), _parquet_data_path));
    ASSERT_OK(parquet_builder->build(TQueryGlobals().time_zone, _parquet_delete_path, 845, &_need_skip_rowids));
    ASSERT_EQ(1, _need_skip_rowids.size());
}

} // namespace starrocks