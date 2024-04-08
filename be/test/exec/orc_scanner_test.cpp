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

#include <exec/orc_scanner.h>
#include <gtest/gtest.h>

#include "runtime/runtime_state.h"

namespace starrocks {

class ORCScannerTest : public ::testing::Test {
public:
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
            range.__set_format_type(TFileFormatType::FORMAT_ORC);
        }
        scan_range.ranges = ranges;
        return scan_range;
    }

    void SetUp() override {
        std::string starrocks_home = getenv("STARROCKS_HOME");
        test_exec_dir = starrocks_home + "/be/test/exec";
    }

    std::string test_exec_dir;
};

TEST_F(ORCScannerTest, get_schema) {
    RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
    auto scan_range = create_scan_range({test_exec_dir + "/test_data/orc_scanner/type_mismatch.orc"});

    scan_range.params.__set_schema_sample_file_count(1);

    ScannerCounter counter{};
    RuntimeProfile profile{"test"};
    std::unique_ptr<ORCScanner> scanner = std::make_unique<ORCScanner>(&state, &profile, scan_range, &counter, true);

    auto st = scanner->open();
    EXPECT_TRUE(st.ok());

    std::vector<SlotDescriptor> schemas;
    st = scanner->get_schema(&schemas);
    EXPECT_TRUE(st.ok());

    EXPECT_EQ("VARCHAR(1048576)", schemas[0].type().debug_string());

    ASSERT_GT(counter.file_read_count, 0);
    ASSERT_GT(counter.file_read_ns, 0);
}

} // namespace starrocks
