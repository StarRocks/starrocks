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

#include "storage/lake/pk_index_cache_policy.h"

#include <gtest/gtest.h>

#include <atomic>
#include <future>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "storage/lake/pk_index_publish_limiter.h"

namespace starrocks::lake {

TEST(PkIndexCachePolicyTest, equal_cycle_and_capped_weighted_shares) {
    std::vector<PkIndexCacheShare> shares(20, {100, 1, 0});
    allocate_pk_index_cache_shares(1840, &shares);
    for (const auto& share : shares) {
        EXPECT_EQ(92, share.target);
    }
    shares = {{10, 100, 0}, {1000, 3, 0}, {1000, 1, 0}};
    allocate_pk_index_cache_shares(410, &shares);
    EXPECT_EQ(10, shares[0].target);
    EXPECT_EQ(300, shares[1].target);
    EXPECT_EQ(100, shares[2].target);
    allocate_pk_index_cache_shares(0, &shares);
    for (const auto& share : shares) EXPECT_EQ(0, share.target);
    allocate_pk_index_cache_shares(10000, &shares);
    for (const auto& share : shares) EXPECT_EQ(share.demand, share.target);
}

TEST(PkIndexPublishLimiterTest, fifo_wakeup_timeout_and_cancellation) {
    PkIndexPublishLimiter limiter;
    auto concurrency = [] { return 1; };
    auto running = [] { return false; };
    ASSIGN_OR_ABORT(auto first, limiter.acquire(1000, concurrency, running));
    EXPECT_EQ(nullptr, limiter.try_acquire(1));
    EXPECT_TRUE(limiter.acquire(0, concurrency, running).status().is_time_out());
    EXPECT_TRUE(limiter.acquire(1000, concurrency, [] { return true; }).status().is_cancelled());
    std::promise<void> queued1;
    std::promise<void> queued2;
    std::atomic<int> queued{0};
    auto* sync = SyncPoint::GetInstance();
    sync->SetCallBack("PkIndexPublishLimiter::queued", [&](void*) {
        const int n = ++queued;
        if (n == 1) queued1.set_value();
        if (n == 2) queued2.set_value();
    });
    sync->EnableProcessing();
    DeferOp cleanup([&] {
        sync->DisableProcessing();
        sync->ClearAllCallBacks();
    });
    std::atomic<int> sequence{0};
    auto wait = [&]() -> int {
        auto result = limiter.acquire(5000, concurrency, running);
        if (!result.ok()) return -1;
        return sequence.fetch_add(1);
    };
    auto second = std::async(std::launch::async, wait);
    EXPECT_EQ(std::future_status::ready, queued1.get_future().wait_for(std::chrono::seconds(2)));
    auto third = std::async(std::launch::async, wait);
    EXPECT_EQ(std::future_status::ready, queued2.get_future().wait_for(std::chrono::seconds(2)));
    EXPECT_EQ(0, sequence.load());
    EXPECT_EQ(nullptr, limiter.try_acquire(1));
    first.reset();
    EXPECT_EQ(0, second.get());
    EXPECT_EQ(1, third.get());
    ASSERT_OK(limiter.acquire(0, concurrency, running).status());
    EXPECT_NE(nullptr, limiter.try_acquire(1));
}

} // namespace starrocks::lake
