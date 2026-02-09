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

#include "util/logging.h"

#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

#include "common/config.h"

namespace starrocks {

class LoggingTest : public testing::Test {
protected:
    void SetUp() override {
        // Save original config values
        _original_verbose_modules = config::sys_log_verbose_modules;
        _original_verbose_level = config::sys_log_verbose_level;
    }

    void TearDown() override {
        // Restore original config values
        config::sys_log_verbose_modules = _original_verbose_modules;
        config::sys_log_verbose_level = _original_verbose_level;
        // Restore glog VLOG level settings
        update_vlog_conf();
    }

private:
    std::vector<std::string> _original_verbose_modules;
    int32_t _original_verbose_level;
};

// Test update_vlog_conf with empty config
TEST_F(LoggingTest, UpdateVerboseModulesEmpty) {
    config::sys_log_verbose_modules = std::vector<std::string>{};
    config::sys_log_verbose_level = 2;

    // Should not crash with empty config
    ASSERT_NO_THROW(update_vlog_conf());
}

// Test update_vlog_conf with single module
TEST_F(LoggingTest, UpdateVerboseModulesSingle) {
    config::sys_log_verbose_modules = std::vector<std::string>{"storage_engine"};
    config::sys_log_verbose_level = 2;

    ASSERT_NO_THROW(update_vlog_conf());
}

// Test update_vlog_conf with multiple modules
TEST_F(LoggingTest, UpdateVerboseModulesMultiple) {
    config::sys_log_verbose_modules = std::vector<std::string>{"storage_engine", "tablet_manager"};
    config::sys_log_verbose_level = 3;

    ASSERT_NO_THROW(update_vlog_conf());
}

// Test update_vlog_conf with wildcard patterns
TEST_F(LoggingTest, UpdateVerboseModulesWildcard) {
    config::sys_log_verbose_modules = std::vector<std::string>{"*"};
    config::sys_log_verbose_level = 2;

    ASSERT_NO_THROW(update_vlog_conf());
}

// Test update_vlog_conf handles empty strings in config
TEST_F(LoggingTest, UpdateVerboseModulesSkipsEmpty) {
    config::sys_log_verbose_modules = std::vector<std::string>{"storage_engine", "", "tablet_manager"};
    config::sys_log_verbose_level = 2;

    // Should skip empty strings without error
    ASSERT_NO_THROW(update_vlog_conf());
}

// Test update_vlog_conf with different verbose levels
TEST_F(LoggingTest, UpdateVerboseLevelOnly) {
    config::sys_log_verbose_modules = std::vector<std::string>{"storage_engine"};

    // Test with different verbose levels
    for (int level = 0; level <= 10; ++level) {
        config::sys_log_verbose_level = level;
        ASSERT_NO_THROW(update_vlog_conf());
    }
}

// Test dynamic update of verbose level without changing modules
TEST_F(LoggingTest, UpdateVerboseLevelDynamic) {
    config::sys_log_verbose_modules = std::vector<std::string>{"*"};
    config::sys_log_verbose_level = 1;
    ASSERT_NO_THROW(update_vlog_conf());

    // Change only the level
    config::sys_log_verbose_level = 5;
    ASSERT_NO_THROW(update_vlog_conf());

    // Change to higher level
    config::sys_log_verbose_level = 10;
    ASSERT_NO_THROW(update_vlog_conf());

    // Change to lower level
    config::sys_log_verbose_level = 0;
    ASSERT_NO_THROW(update_vlog_conf());
}

// Test verbose level clamping: negative values should be clamped to 0
TEST_F(LoggingTest, UpdateVerboseLevelClamp) {
    config::sys_log_verbose_modules = std::vector<std::string>{"storage_engine"};

    // Negative value should be clamped to 0
    config::sys_log_verbose_level = -5;
    ASSERT_NO_THROW(update_vlog_conf());
    EXPECT_EQ(static_cast<int32_t>(config::sys_log_verbose_level), 0);

    // Zero (boundary) should remain unchanged
    config::sys_log_verbose_level = 0;
    ASSERT_NO_THROW(update_vlog_conf());
    EXPECT_EQ(static_cast<int32_t>(config::sys_log_verbose_level), 0);

    // Positive values should remain unchanged
    config::sys_log_verbose_level = 10;
    ASSERT_NO_THROW(update_vlog_conf());
    EXPECT_EQ(static_cast<int32_t>(config::sys_log_verbose_level), 10);

    // Large positive value should remain unchanged (no max limit)
    config::sys_log_verbose_level = 200;
    ASSERT_NO_THROW(update_vlog_conf());
    EXPECT_EQ(static_cast<int32_t>(config::sys_log_verbose_level), 200);
}

// Test thread safety of update_vlog_conf
TEST_F(LoggingTest, UpdateVerboseModulesThreadSafety) {
    std::vector<std::thread> threads;
    const int num_threads = 10;
    const int iterations = 100;

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([i, iterations]() {
            for (int j = 0; j < iterations; ++j) {
                // Alternate between different configs
                if (j % 2 == 0) {
                    config::sys_log_verbose_modules = std::vector<std::string>{"storage_engine", "tablet_manager"};
                } else {
                    config::sys_log_verbose_modules = std::vector<std::string>{"*query*", "*compaction*"};
                }
                config::sys_log_verbose_level = (i + j) % 5;
                update_vlog_conf();
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
    // Should complete without crash or data corruption
}

} // namespace starrocks
