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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>

#include "common/config.h"
#include "util/logging.h"

namespace starrocks {

class UpdateLoggingTest : public testing::Test {
public:
    void SetUp() override {
        _saved_level = config::sys_log_level;
        _saved_minloglevel = FLAGS_minloglevel;
    }
    void TearDown() override {
        config::sys_log_level = _saved_level;
        FLAGS_minloglevel = _saved_minloglevel;
    }

private:
    std::string _saved_level;
    int32_t _saved_minloglevel = 0;
};

TEST_F(UpdateLoggingTest, test_each_severity_takes_effect) {
    const std::pair<const char*, int32_t> cases[] = {
            {"INFO", 0},
            {"WARNING", 1},
            {"ERROR", 2},
            {"FATAL", 3},
            // The severity is matched case-insensitively, so a lowercase spelling has to land on the
            // same level as its uppercase twin.
            {"warning", 1},
    };
    for (const auto& [level, expected] : cases) {
        config::sys_log_level = level;
        ASSERT_TRUE(update_logging().ok()) << level;
        EXPECT_EQ(expected, FLAGS_minloglevel) << level;
    }
}

TEST_F(UpdateLoggingTest, test_unknown_severity_is_rejected) {
    config::sys_log_level = "WARNING";
    ASSERT_TRUE(update_logging().ok());
    ASSERT_EQ(1, FLAGS_minloglevel);

    // A level nobody recognizes has to reach the caller as an error rather than silently leave the
    // process running at whatever level it happened to be at.
    config::sys_log_level = "WARN";
    Status st = update_logging();
    EXPECT_TRUE(st.is_invalid_argument()) << st;
    EXPECT_EQ(1, FLAGS_minloglevel);
}

} // namespace starrocks
