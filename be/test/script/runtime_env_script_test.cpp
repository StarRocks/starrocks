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

#include "script/script.h"

namespace starrocks {

TEST(RuntimeEnvScriptTest, GlobalEnvAliasRemainsAvailable) {
    std::string result;
    ASSERT_TRUE(execute_script(R"(
        var globalTracker = GlobalEnv.GetInstance().process_mem_tracker()
        var runtimeTracker = RuntimeEnv.GetInstance().process_mem_tracker()
        System.print("ok")
    )",
                               result)
                        .ok());
    ASSERT_NE(result.find("ok"), std::string::npos) << result;
}

} // namespace starrocks
