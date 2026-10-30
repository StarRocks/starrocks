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

#include "base/concurrency/once.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "base/status.h"

namespace starrocks {

TEST(OnceTest, InvokeOnceRunsOnlyOnce) {
    OnceFlag flag;
    int call_count = 0;

    EXPECT_TRUE(invoke_once(flag, [&] { ++call_count; }));
    EXPECT_FALSE(invoke_once(flag, [&] { ++call_count; }));

    EXPECT_EQ(1, call_count);
    EXPECT_TRUE(invoked(flag));
}

TEST(OnceTest, InvokeOnceConcurrentCallersShareOneExecution) {
    OnceFlag flag;
    std::atomic<int> call_count{0};
    std::vector<std::thread> threads;
    threads.reserve(8);

    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] { invoke_once(flag, [&] { ++call_count; }); });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(1, call_count.load());
    EXPECT_TRUE(invoked(flag));
}

TEST(OnceTest, SuccessOnceRetriesAfterFailure) {
    OnceFlag flag;
    int call_count = 0;

    auto failed = success_once(flag, [&] {
        ++call_count;
        return Status::InternalError("first attempt failed");
    });
    ASSERT_FALSE(failed.ok());
    EXPECT_FALSE(invoked(flag));

    auto succeeded = success_once(flag, [&] {
        ++call_count;
        return Status::OK();
    });
    ASSERT_TRUE(succeeded.ok()) << succeeded.status().to_string();
    EXPECT_TRUE(succeeded.value());

    auto skipped = success_once(flag, [&] {
        ++call_count;
        return Status::OK();
    });
    ASSERT_TRUE(skipped.ok()) << skipped.status().to_string();
    EXPECT_FALSE(skipped.value());

    EXPECT_EQ(2, call_count);
    EXPECT_TRUE(invoked(flag));
}

} // namespace starrocks
