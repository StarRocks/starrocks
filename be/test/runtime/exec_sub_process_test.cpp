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

#include <string>
#include <vector>

#include "common/logging.h"

namespace starrocks {
std::string lite_exec(const std::vector<std::string>& argv_vec, int timeout_ms = 1200000);

TEST(TestLiteFork, fork) {
    {
        std::vector<std::string> argv = {"/bin/echo", "hello", "world"};
        auto res = lite_exec(argv);
        ASSERT_EQ(res, "hello world\n");
    }
    {
        std::vector<std::string> argv = {"/bin/sleep", "3600"};
        auto res = lite_exec(argv, 1000);
        ASSERT_TRUE(res.find("timeout") != std::string::npos);
    }
}
} // namespace starrocks