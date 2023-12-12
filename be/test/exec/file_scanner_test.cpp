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

#include "exec/file_scanner.h"

#include <gtest/gtest.h>

#include <limits>

#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

class FileScannerTest : public ::testing::Test {
    TBrokerScanRange create_scan_range(const std::vector<std::string>& file_names) {
        TBrokerScanRange scan_range;

        std::vector<TBrokerRangeDesc> ranges;
        ranges.resize(file_names.size());
        for (auto i = 0; i < file_names.size(); ++i) {
            TBrokerRangeDesc& range = ranges[i];
            range.__set_path(file_names[i]);
            range.start_offset = 0;
            range.size = std::numeric_limits<int64_t>::max();
            range.file_type = TFileType::FILE_LOCAL;
            range.__set_format_type(TFileFormatType::FORMAT_PARQUET);
        }
        scan_range.ranges = ranges;
        return scan_range;
    }

    void SetUp() override {
        std::string starrocks_home = getenv("STARROCKS_HOME");
        test_exec_dir = starrocks_home + "/be/test/exec";
    }

private:
    std::string test_exec_dir;
};

TEST_F(FileScannerTest, sample_schema) {
    {
        // sample 1 file.
        // file1: col1,int
        // result: col1,int
        const std::vector<std::pair<std::string, TypeDescriptor>> expected_schema = {
                {"col1", TypeDescriptor::from_logical_type(TYPE_BIGINT)}};

        RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
        auto scan_range = create_scan_range({test_exec_dir + "/test_data/parquet_data/merged_schema1.parquet",
                                             test_exec_dir + "/test_data/parquet_data/merged_schema2.parquet",
                                             test_exec_dir + "/test_data/parquet_data/merged_schema3.parquet"});

        scan_range.params.__set_schema_sample_file_count(1);

        std::vector<SlotDescriptor> schema;
        FileScanner::sample_schema(&state, scan_range, &schema);

        EXPECT_EQ(schema.size(), expected_schema.size());

        for (size_t i = 0; i < schema.size(); ++i) {
            EXPECT_EQ(schema[i].col_name(), expected_schema[i].first);
            EXPECT_TRUE(schema[i].type() == expected_schema[i].second)
                    << schema[i].col_name() << " got: " << schema[i].type().debug_string()
                    << " expect: " << expected_schema[i].second.debug_string();
        }
    }

    {
        // sample 2 file.
        // file1: col1,int
        // file3: col2,varchar
        // result: col1,int; col2,varchar
        const std::vector<std::pair<std::string, TypeDescriptor>> expected_schema = {
                {"col1", TypeDescriptor::from_logical_type(TYPE_BIGINT)},
                {"col2", TypeDescriptor::from_logical_type(TYPE_VARCHAR)}};

        RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
        auto scan_range = create_scan_range({test_exec_dir + "/test_data/parquet_data/merged_schema1.parquet",
                                             test_exec_dir + "/test_data/parquet_data/merged_schema2.parquet",
                                             test_exec_dir + "/test_data/parquet_data/merged_schema3.parquet"});

        scan_range.params.__set_schema_sample_file_count(2);

        std::vector<SlotDescriptor> schema;
        FileScanner::sample_schema(&state, scan_range, &schema);

        EXPECT_EQ(schema.size(), expected_schema.size());

        for (size_t i = 0; i < schema.size(); ++i) {
            EXPECT_EQ(schema[i].col_name(), expected_schema[i].first);
            EXPECT_TRUE(schema[i].type() == expected_schema[i].second)
                    << schema[i].col_name() << " got: " << schema[i].type().debug_string()
                    << " expect: " << expected_schema[i].second.debug_string();
        }
    }

    {
        // sample 3 file.
        // file1: col1,int
        // file2: col1,varchar; col2,int
        // file3: col2,varchar
        // result: col1,varchar; col2,varchar
        const std::vector<std::pair<std::string, TypeDescriptor>> expected_schema = {
                {"col1", TypeDescriptor::from_logical_type(TYPE_VARCHAR)},
                {"col2", TypeDescriptor::from_logical_type(TYPE_VARCHAR)}};

        RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
        auto scan_range = create_scan_range({test_exec_dir + "/test_data/parquet_data/merged_schema1.parquet",
                                             test_exec_dir + "/test_data/parquet_data/merged_schema2.parquet",
                                             test_exec_dir + "/test_data/parquet_data/merged_schema3.parquet"});

        scan_range.params.__set_schema_sample_file_count(3);

        std::vector<SlotDescriptor> schema;
        FileScanner::sample_schema(&state, scan_range, &schema);

        EXPECT_EQ(schema.size(), expected_schema.size());

        for (size_t i = 0; i < schema.size(); ++i) {
            EXPECT_EQ(schema[i].col_name(), expected_schema[i].first);
            EXPECT_TRUE(schema[i].type() == expected_schema[i].second)
                    << schema[i].col_name() << " got: " << schema[i].type().debug_string()
                    << " expect: " << expected_schema[i].second.debug_string();
        }
    }
}

} // namespace starrocks
