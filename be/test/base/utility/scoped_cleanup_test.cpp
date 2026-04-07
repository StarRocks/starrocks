// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "base/utility/scoped_cleanup.h"

#include <gtest/gtest.h>

#include <functional>
#include <type_traits>

namespace starrocks {

TEST(ScopedCleanup, TestCleanup) {
    int var = 0;
    {
        auto saved = var;
        auto cleanup = MakeScopedCleanup([&]() { var = saved; });
        var = 42;
    }
    ASSERT_EQ(0, var);
}

TEST(ScopedCleanup, TestCleanupMacro) {
    int var = 0;
    {
        auto saved = var;
        SCOPED_CLEANUP({ var = saved; });
        var = 42;
    }
    ASSERT_EQ(0, var);
}

TEST(ScopedCleanup, TestCancelCleanup) {
    int var = 0;
    {
        auto saved = var;
        auto cleanup = MakeScopedCleanup([&]() { var = saved; });
        var = 42;
        cleanup.cancel();
    }
    ASSERT_EQ(42, var);
}

TEST(ScopedCleanup, NotCopyable) {
    auto cleanup = MakeScopedCleanup([]() {});
    using CleanupType = decltype(cleanup);
    EXPECT_FALSE(std::is_copy_constructible_v<CleanupType>);
    EXPECT_FALSE(std::is_copy_assignable_v<CleanupType>);
}

TEST(ScopedCleanup, MoveAssignRunsExistingCleanup) {
    int var = 0;
    {
        auto first = MakeScopedCleanup<std::function<void()>>(std::function<void()>([&]() { var = 1; }));
        auto second = MakeScopedCleanup<std::function<void()>>(std::function<void()>([&]() { var = 2; }));
        first = std::move(second);
        ASSERT_EQ(1, var);
    }
    ASSERT_EQ(2, var);
}

TEST(ScopedCleanup, MoveAssignFromCancelledDoesNotRunNewCleanup) {
    int var = 0;
    {
        auto first = MakeScopedCleanup<std::function<void()>>(std::function<void()>([&]() { var = 1; }));
        auto second = MakeScopedCleanup<std::function<void()>>(std::function<void()>([&]() { var = 2; }));
        second.cancel();
        first = std::move(second);
        ASSERT_EQ(1, var);
    }
    ASSERT_EQ(1, var);
}

} // namespace starrocks
