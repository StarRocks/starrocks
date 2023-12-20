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
        // file1: col1,int64
        // result: col1,BIGINT
        const std::vector<std::pair<std::string, TypeDescriptor>> expected_schema = {
                {"col1", TypeDescriptor::from_logical_type(TYPE_BIGINT)}};

        RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
        auto scan_range = create_scan_range({test_exec_dir + "/test_data/parquet_data/schema1.parquet",
                                             test_exec_dir + "/test_data/parquet_data/schema2.parquet",
                                             test_exec_dir + "/test_data/parquet_data/schema3.parquet"});

        scan_range.params.__set_schema_sample_file_count(1);

        std::vector<SlotDescriptor> schema;
        EXPECT_OK(FileScanner::sample_schema(&state, scan_range, &schema));

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
        // file1: col1,int64
        // file3: col2,int32; col3,int32; col4,float
        // result: col1,BIGINT; col2,INT; col3,INT; col4,FLOAT
        const std::vector<std::pair<std::string, TypeDescriptor>> expected_schema = {
                {"col1", TypeDescriptor::from_logical_type(TYPE_BIGINT)},
                {"col2", TypeDescriptor::from_logical_type(TYPE_INT)},
                {"col3", TypeDescriptor::from_logical_type(TYPE_INT)},
                {"col4", TypeDescriptor::from_logical_type(TYPE_FLOAT)}};

        RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
        auto scan_range = create_scan_range({test_exec_dir + "/test_data/parquet_data/schema1.parquet",
                                             test_exec_dir + "/test_data/parquet_data/schema2.parquet",
                                             test_exec_dir + "/test_data/parquet_data/schema3.parquet"});

        scan_range.params.__set_schema_sample_file_count(2);

        std::vector<SlotDescriptor> schema;
        EXPECT_OK(FileScanner::sample_schema(&state, scan_range, &schema));

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
        // file1: col1,int64
        // file2: col2,byte_array; col3,int64; col4,double
        // file3: col2,int32; col3,int32; col4,float
        // result: col1,BIGINT; col2,VARCHAR; col3,BIGINT; col4,DOUBLE
        const std::vector<std::pair<std::string, TypeDescriptor>> expected_schema = {
                {"col1", TypeDescriptor::from_logical_type(TYPE_BIGINT)},
                {"col2", TypeDescriptor::from_logical_type(TYPE_VARCHAR)},
                {"col3", TypeDescriptor::from_logical_type(TYPE_BIGINT)},
                {"col4", TypeDescriptor::from_logical_type(TYPE_DOUBLE)}};

        RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
        auto scan_range = create_scan_range({test_exec_dir + "/test_data/parquet_data/schema1.parquet",
                                             test_exec_dir + "/test_data/parquet_data/schema2.parquet",
                                             test_exec_dir + "/test_data/parquet_data/schema3.parquet"});

        scan_range.params.__set_schema_sample_file_count(3);

        std::vector<SlotDescriptor> schema;
        EXPECT_OK(FileScanner::sample_schema(&state, scan_range, &schema));

        EXPECT_EQ(schema.size(), expected_schema.size());

        for (size_t i = 0; i < schema.size(); ++i) {
            EXPECT_EQ(schema[i].col_name(), expected_schema[i].first);
            EXPECT_TRUE(schema[i].type() == expected_schema[i].second)
                    << schema[i].col_name() << " got: " << schema[i].type().debug_string()
                    << " expect: " << expected_schema[i].second.debug_string();
        }
    }

    {
        // sample 1 file.
        // file4: col1,int64; COL1,int64
        // result: duplicated column name
        RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
        auto scan_range = create_scan_range({test_exec_dir + "/test_data/parquet_data/schema4.parquet"});

        scan_range.params.__set_schema_sample_file_count(1);

        std::vector<SlotDescriptor> schema;
        auto st = FileScanner::sample_schema(&state, scan_range, &schema);
        //Identical names in upper/lower cases, file: [/work/starrocks-main/be/test/exec/test_data/parquet_data/schema4.parquet], column names: [col1] [COL1]"
        EXPECT_STATUS(Status::NotSupported(""), st);
    }

    {
        // sample 1 file.
        // file1: col1,int64
        // file4: col1,int64; COL1,int64
        // result: duplicated column name
        RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
        auto scan_range = create_scan_range({test_exec_dir + "/test_data/parquet_data/schema1.parquet",
                                             test_exec_dir + "/test_data/parquet_data/schema4.parquet"});

        scan_range.params.__set_schema_sample_file_count(2);

        std::vector<SlotDescriptor> schema;
        auto st = FileScanner::sample_schema(&state, scan_range, &schema);
        //Identical names in upper/lower cases, files: [/work/starrocks-main/be/test/exec/test_data/parquet_data/schema4.parquet] [/work/starrocks-main/be/test/exec/test_data/parquet_data/schema1.parquet], names: [COL1] [col1]"
        EXPECT_TRUE(st.is_not_supported());
    }
}

} // namespace starrocks
