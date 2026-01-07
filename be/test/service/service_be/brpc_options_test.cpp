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

#include <brpc/server.h>
#include <gtest/gtest.h>

#include "common/config.h"

namespace starrocks {

class BrpcOptionsTest : public testing::Test {};

// Test that brpc_idle_timeout_sec config has correct default value
TEST_F(BrpcOptionsTest, test_brpc_idle_timeout_sec_default) {
    // Default value should be -1 (disabled)
    EXPECT_EQ(-1, config::brpc_idle_timeout_sec);
}

// Test that brpc_idle_timeout_sec can be applied to brpc::ServerOptions
TEST_F(BrpcOptionsTest, test_brpc_idle_timeout_sec_apply_to_server_options) {
    brpc::ServerOptions options;
    options.idle_timeout_sec = config::brpc_idle_timeout_sec;
    EXPECT_EQ(config::brpc_idle_timeout_sec, options.idle_timeout_sec);
}

// Test that brpc_idle_timeout_sec can be set to custom values
TEST_F(BrpcOptionsTest, test_brpc_idle_timeout_sec_custom_values) {
    brpc::ServerOptions options;

    // Test with default value (-1, disabled)
    options.idle_timeout_sec = -1;
    EXPECT_EQ(-1, options.idle_timeout_sec);

    // Test with positive value (e.g., 300 seconds = 5 minutes)
    options.idle_timeout_sec = 300;
    EXPECT_EQ(300, options.idle_timeout_sec);

    // Test with zero (immediate timeout)
    options.idle_timeout_sec = 0;
    EXPECT_EQ(0, options.idle_timeout_sec);
}

} // namespace starrocks
