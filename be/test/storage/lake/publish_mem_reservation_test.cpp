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

#include "storage/lake/publish_mem_reservation.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "runtime/mem_tracker.h"

namespace starrocks::lake {

static constexpr int64_t KB = 1024;

// --- Normal path: admit below limit, charge exactly, release on scope exit ----------------------
TEST(PublishMemReservationTest, NormalBelowLimit) {
    MemTracker t(1000 * KB, "p3-test", nullptr);
    std::atomic<bool> slot{false};
    {
        PublishMemReservation r(&t, 300 * KB, slot);
        EXPECT_TRUE(r.admitted());
        EXPECT_EQ(300 * KB, t.consumption());
        EXPECT_EQ(300 * KB, r.consumed_bytes());
    }
    EXPECT_EQ(0, t.consumption()); // symmetric release
}

// --- Normal path: exactly at the limit is allowed (consumption+estimate == limit) ---------------
TEST(PublishMemReservationTest, NormalAtLimitBoundary) {
    MemTracker t(1000 * KB, "p3-test", nullptr);
    std::atomic<bool> slot{false};
    {
        PublishMemReservation r(&t, 1000 * KB, slot);
        EXPECT_TRUE(r.admitted());
        EXPECT_EQ(1000 * KB, t.consumption());
    }
    EXPECT_EQ(0, t.consumption());
}

// --- Normal path: reject when it would exceed the limit; charge/release nothing -----------------
TEST(PublishMemReservationTest, NormalRejectedUnderContention) {
    MemTracker t(1000 * KB, "p3-test", nullptr);
    std::atomic<bool> slot{false};
    PublishMemReservation held(&t, 800 * KB, slot);
    ASSERT_TRUE(held.admitted());
    ASSERT_EQ(800 * KB, t.consumption());
    {
        PublishMemReservation r(&t, 300 * KB, slot); // 800 + 300 > 1000
        EXPECT_FALSE(r.admitted());
        EXPECT_EQ(0, r.consumed_bytes());
        EXPECT_EQ(800 * KB, t.consumption()); // rejected attempt changed nothing
    }
    EXPECT_EQ(800 * KB, t.consumption()); // rejected dtor released nothing
}

// --- Kill switch (limit == -1, pct==0): always admit; consume for observability ----------------
TEST(PublishMemReservationTest, KillSwitchUnlimited) {
    MemTracker t(-1, "p3-test", nullptr);
    std::atomic<bool> slot{false};
    const int64_t huge = 5LL * 1024 * 1024 * KB; // 5 GB
    {
        PublishMemReservation r(&t, huge, slot);
        EXPECT_TRUE(r.admitted());
        EXPECT_EQ(huge, t.consumption()); // observability on the -1 tracker
    }
    EXPECT_EQ(0, t.consumption());
}

// --- Pathological limit == 0: admit (gate disabled), never inflate negative --------------------
TEST(PublishMemReservationTest, PathologicalZeroLimitNoNegative) {
    MemTracker t(0, "p3-test", nullptr);
    std::atomic<bool> slot{false};
    {
        PublishMemReservation r(&t, 500 * KB, slot);
        EXPECT_TRUE(r.admitted());
        EXPECT_EQ(0, r.consumed_bytes()); // try_consume no-ops at limit 0
        EXPECT_EQ(0, t.consumption());
    }
    EXPECT_EQ(0, t.consumption()); // no negative inflation
}

// --- Oversized single-slot lifecycle: admit one, reject a 2nd, re-admit after release ----------
TEST(PublishMemReservationTest, OversizedSingleSlotLifecycle) {
    MemTracker t(1000 * KB, "p3-test", nullptr);
    std::atomic<bool> slot{false};
    {
        PublishMemReservation big1(&t, 5000 * KB, slot); // estimate > limit
        EXPECT_TRUE(big1.admitted());
        EXPECT_TRUE(big1.holds_oversized_slot());
        EXPECT_EQ(0, t.consumption()); // oversized does NOT charge the shared tracker
        EXPECT_TRUE(slot.load());
        {
            PublishMemReservation big2(&t, 6000 * KB, slot);
            EXPECT_FALSE(big2.admitted());
            EXPECT_FALSE(big2.holds_oversized_slot());
        }
        EXPECT_TRUE(slot.load()); // loser did not clear the winner's slot (N2)
        {
            PublishMemReservation norm(&t, 400 * KB, slot);
            EXPECT_TRUE(norm.admitted()); // normal traffic flows alongside an oversized
            EXPECT_EQ(400 * KB, t.consumption());
        }
    }
    EXPECT_FALSE(slot.load()); // slot freed after the winner destructs
    EXPECT_EQ(0, t.consumption());
    {
        PublishMemReservation big3(&t, 5000 * KB, slot);
        EXPECT_TRUE(big3.admitted()); // a later oversized can proceed (no permanent wedge)
    }
}

// --- Oversized under heavy concurrency: at most ONE admitted at a time (the TOCTOU race) -------
TEST(PublishMemReservationTest, OversizedConcurrencyRaceFree) {
    MemTracker t(1000 * KB, "p3-test", nullptr);
    std::atomic<bool> slot{false};
    std::atomic<int> live{0};
    std::atomic<int> max_live{0};
    std::atomic<long> total_ok{0};
    std::atomic<bool> go{false};
    const int kThreads = 64, kRounds = 2000;

    std::vector<std::thread> ths;
    for (int i = 0; i < kThreads; ++i) {
        ths.emplace_back([&] {
            while (!go.load(std::memory_order_acquire)) {
            }
            for (int r = 0; r < kRounds; ++r) {
                PublishMemReservation resv(&t, 9999 * KB, slot); // always oversized
                if (resv.admitted()) {
                    int n = live.fetch_add(1) + 1;
                    int prev = max_live.load();
                    while (n > prev && !max_live.compare_exchange_weak(prev, n)) {
                    }
                    std::this_thread::yield();
                    total_ok.fetch_add(1);
                    live.fetch_sub(1);
                }
            }
        });
    }
    go.store(true, std::memory_order_release);
    for (auto& th : ths) th.join();

    EXPECT_EQ(1, max_live.load()); // never two oversized admitted at once
    EXPECT_GT(total_ok.load(), 0); // some got through (not starved)
    EXPECT_FALSE(slot.load());     // no slot leak
    EXPECT_EQ(0, t.consumption()); // oversized never touched the shared tracker
}

// --- Normal-path concurrency: atomic reserve never overshoots the limit ------------------------
TEST(PublishMemReservationTest, NormalConcurrencyNeverOvershoots) {
    const int64_t limit = 100 * KB;
    MemTracker t(limit, "p3-test", nullptr);
    std::atomic<bool> slot{false};
    std::atomic<int64_t> peak{0};
    std::atomic<bool> go{false};
    const int kThreads = 32, kRounds = 5000;

    std::vector<std::thread> ths;
    for (int i = 0; i < kThreads; ++i) {
        ths.emplace_back([&] {
            while (!go.load(std::memory_order_acquire)) {
            }
            for (int r = 0; r < kRounds; ++r) {
                PublishMemReservation resv(&t, 30 * KB, slot);
                if (resv.admitted()) {
                    int64_t c = t.consumption();
                    int64_t p = peak.load();
                    while (c > p && !peak.compare_exchange_weak(p, c)) {
                    }
                    std::this_thread::yield();
                }
            }
        });
    }
    go.store(true, std::memory_order_release);
    for (auto& th : ths) th.join();

    EXPECT_LE(peak.load(), limit); // never exceeded the limit under contention
    EXPECT_EQ(0, t.consumption()); // symmetric under load
}

// --- nullptr tracker (gate not wired): admit, touch nothing ------------------------------------
TEST(PublishMemReservationTest, NullTrackerAlwaysAdmits) {
    std::atomic<bool> slot{false};
    PublishMemReservation r(nullptr, 500 * KB, slot);
    EXPECT_TRUE(r.admitted());
    EXPECT_EQ(0, r.consumed_bytes());
}

} // namespace starrocks::lake
