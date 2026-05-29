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

#include "runtime/command_executor.h"

#include <gtest/gtest.h>

#include <string>

namespace starrocks {

TEST(CommandExecutorTest, not_support) {
    std::string result;
    EXPECT_TRUE(execute_command("set_config_11", "{}", &result).is_not_supported());
}

TEST(CommandExecutorTest, set_config_rejects_invalid_json_shape) {
    std::string result;
    auto st = execute_command("set_config", R"({"name":1,"value":"x"})", &result);
    EXPECT_TRUE(st.is_invalid_argument()) << st;
}

} // namespace starrocks
