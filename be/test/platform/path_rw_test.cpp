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

#include "platform/path_rw.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "base/testutil/assert.h"

namespace starrocks {

class PathRwTest : public testing::Test {
public:
    void SetUp() override {
        _root = std::filesystem::absolute("./test_run/path_rw_test").string();
        std::filesystem::remove_all(_root);
        std::filesystem::create_directories(_root);
    }

    void TearDown() override { std::filesystem::remove_all(_root); }

protected:
    std::string _root;
};

TEST_F(PathRwTest, read_write_test_file_removes_probe_file) {
    const std::string test_file = _root + "/probe_file";

    ASSERT_OK(read_write_test_file(test_file));
    ASSERT_FALSE(std::filesystem::exists(test_file));
}

TEST_F(PathRwTest, check_datapath_rw_reports_path_availability) {
    ASSERT_TRUE(check_datapath_rw(_root));
    ASSERT_FALSE(check_datapath_rw(_root + "/missing"));
}

} // namespace starrocks
