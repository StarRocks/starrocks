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

#include "base/brpc/disposable_closure.h"

#include <errno.h>
#include <gtest/gtest.h>

#include <string>
#include <string_view>

namespace starrocks {

TEST(DisposableClosureTest, RunsSuccessHandler) {
    auto* closure = new DisposableClosure<int, int>(7);
    closure->result = 42;

    bool success_called = false;
    bool failure_called = false;
    closure->addSuccessHandler([&](const int& ctx, const int& result) {
        success_called = true;
        EXPECT_EQ(7, ctx);
        EXPECT_EQ(42, result);
    });
    closure->addFailureHandler([&](const int&, std::string_view) { failure_called = true; });

    closure->Run();

    EXPECT_TRUE(success_called);
    EXPECT_FALSE(failure_called);
}

TEST(DisposableClosureTest, RunsFailureHandler) {
    auto* closure = new DisposableClosure<int, int>(11);
    closure->cntl.SetFailed(EINVAL, "bad request");

    bool success_called = false;
    bool failure_called = false;
    std::string failure_message;
    closure->addSuccessHandler([&](const int&, const int&) { success_called = true; });
    closure->addFailureHandler([&](const int& ctx, std::string_view message) {
        failure_called = true;
        EXPECT_EQ(11, ctx);
        failure_message = std::string(message);
    });

    closure->Run();

    EXPECT_FALSE(success_called);
    EXPECT_TRUE(failure_called);
    EXPECT_NE(std::string::npos, failure_message.find("brpc failed"));
    EXPECT_NE(std::string::npos, failure_message.find("bad request"));
}

} // namespace starrocks
