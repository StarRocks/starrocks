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
#include <rapidjson/document.h>

#include <string>

namespace starrocks {

TEST(CommandExecutorTest, not_support) {
    std::string result;
    EXPECT_TRUE(execute_command("set_config_11", "{}", &result).is_not_supported());
}

TEST(CommandExecutorTest, show_config_basic) {
    std::string result;
    Status status = execute_command("show_config", "{}", &result);

    EXPECT_TRUE(status.ok());
    EXPECT_FALSE(result.empty());

    rapidjson::Document doc;
    doc.Parse(result.c_str());

    EXPECT_FALSE(doc.HasParseError());
    EXPECT_TRUE(doc.HasMember("configs"));
    EXPECT_TRUE(doc["configs"].IsArray());
    EXPECT_GT(doc["configs"].Size(), 0);

    const auto& config = doc["configs"][0];
    EXPECT_TRUE(config.HasMember("key"));
    EXPECT_TRUE(config.HasMember("value"));
    EXPECT_TRUE(config.HasMember("type"));
    EXPECT_TRUE(config.HasMember("is_mutable"));
}

TEST(CommandExecutorTest, show_config_with_pattern) {
    std::string pattern = "mem";
    std::string params = "{\"pattern\":\"" + pattern + "\"}";
    std::string result;

    Status status = execute_command("show_config", params, &result);
    EXPECT_TRUE(status.ok());

    rapidjson::Document doc;
    doc.Parse(result.c_str());

    EXPECT_TRUE(doc.HasMember("configs"));
    const auto& configs = doc["configs"];

    if (configs.Size() > 0) {
        std::string key = configs[0]["key"].GetString();
        EXPECT_NE(key.find(pattern), std::string::npos);
    }
}
} // namespace starrocks
