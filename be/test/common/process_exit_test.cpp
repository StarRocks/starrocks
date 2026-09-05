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

#include "common/process_exit.h"

#include <gtest/gtest.h>

#include <atomic>

#include "common/status.h"

namespace starrocks {

extern std::atomic<bool> k_starrocks_exit;
extern std::atomic<bool> k_starrocks_quick_exit;

using namespace ::testing;

class ProcessExitTest : public testing::Test {
    void TearDown() override {
        // restore the flags
        k_starrocks_exit.store(false);
        k_starrocks_quick_exit.store(false);
    }
};

TEST_F(ProcessExitTest, testExitFlag) {
    // no exit at all
    EXPECT_FALSE(process_exit_in_progress());
    EXPECT_FALSE(process_quick_exit_in_progress());

    // first time set exit, return true
    EXPECT_TRUE(set_process_exit());

    EXPECT_TRUE(process_exit_in_progress());
    EXPECT_FALSE(process_quick_exit_in_progress());

    // second time set exit, return false because it is already set to true
    EXPECT_FALSE(set_process_exit());

    // verify the exit status remain the same
    EXPECT_TRUE(process_exit_in_progress());
    EXPECT_FALSE(process_quick_exit_in_progress());
}

TEST_F(ProcessExitTest, testQuickExitFlag) {
    // no exit at all
    EXPECT_FALSE(process_exit_in_progress());
    EXPECT_FALSE(process_quick_exit_in_progress());

    // first time set exit, return true
    EXPECT_TRUE(set_process_quick_exit());

    EXPECT_TRUE(process_exit_in_progress());
    EXPECT_TRUE(process_quick_exit_in_progress());

    // second time set exit, return false because it is already set to true
    EXPECT_FALSE(set_process_quick_exit());

    // verify the exit status remain the same
    EXPECT_TRUE(process_exit_in_progress());
    EXPECT_TRUE(process_quick_exit_in_progress());
}
} // namespace starrocks
