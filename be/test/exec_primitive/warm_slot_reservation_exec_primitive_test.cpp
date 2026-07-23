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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "exec_primitive/pipeline/scan/warm_slot_reservation.h"

namespace starrocks::pipeline {

TEST(WarmSlotReservationTest, ReserveUpToMaxInflightThenReject) {
    WarmSlotReservation slots;
    EXPECT_TRUE(slots.try_reserve(/*max_warm_inflight=*/2, /*data_reserved=*/0, /*io_task_cap=*/8));
    EXPECT_TRUE(slots.try_reserve(2, 0, 8));
    EXPECT_EQ(2, slots.running());
    // Third reservation exceeds max_warm_inflight and must leave the count unchanged.
    EXPECT_FALSE(slots.try_reserve(2, 0, 8));
    EXPECT_EQ(2, slots.running());
    slots.release();
    EXPECT_TRUE(slots.try_reserve(2, 0, 8));
    EXPECT_EQ(2, slots.running());
}

TEST(WarmSlotReservationTest, DataPlusWarmBoundedByCap) {
    WarmSlotReservation slots;
    // data holds 7 of 8 slots: exactly one warm reservation fits.
    EXPECT_TRUE(slots.try_reserve(4, /*data_reserved=*/7, /*io_task_cap=*/8));
    EXPECT_FALSE(slots.try_reserve(4, 7, 8));
    EXPECT_EQ(1, slots.running());
    // data at the cap: no warm at all.
    WarmSlotReservation full;
    EXPECT_FALSE(full.try_reserve(4, 8, 8));
    EXPECT_EQ(0, full.running());
}

TEST(WarmSlotReservationTest, DisableIsSticky) {
    WarmSlotReservation slots;
    EXPECT_TRUE(slots.try_reserve(4, 0, 8));
    slots.disable();
    EXPECT_TRUE(slots.disabled());
    EXPECT_FALSE(slots.try_reserve(4, 0, 8));
    // The already-held slot drains normally; new reservations stay rejected after the drain.
    slots.release();
    EXPECT_EQ(0, slots.running());
    EXPECT_FALSE(slots.try_reserve(4, 0, 8));
}

// The teardown race the class exists for: once disable() was called and the driver observed
// running() == 0 (pending_finish() returned false), a late try_reserve from an executor thread
// must never succeed -- otherwise a warm task re-arms after operator teardown began and runs
// on a destroyed operator. Hammer reserve/release from many threads, disable mid-flight, wait
// for the drain, then check no reservation succeeded after the drain was observed.
TEST(WarmSlotReservationTest, NoRearmAfterDrainObserved) {
    constexpr int kThreads = 8;
    constexpr int kMaxInflight = 4;
    constexpr int kCap = 16;

    WarmSlotReservation slots;
    std::atomic<bool> drained{false};
    std::atomic<int> rearms_after_drain{0};
    std::atomic<bool> stop{false};

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&]() {
            while (!stop.load()) {
                if (slots.try_reserve(kMaxInflight, /*data_reserved=*/0, kCap)) {
                    if (drained.load()) {
                        rearms_after_drain.fetch_add(1);
                    }
                    slots.release();
                }
            }
        });
    }

    // Let the workers contend for a while, then tear down.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    slots.disable();
    // The driver side: wait until the count drains to zero, then declare the drain observed.
    while (slots.running() != 0) {
        std::this_thread::yield();
    }
    drained.store(true);
    // Give the workers time to attempt (and fail) more reservations after the drain.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    stop.store(true);
    for (auto& w : workers) {
        w.join();
    }

    EXPECT_EQ(0, rearms_after_drain.load());
    EXPECT_EQ(0, slots.running());
}

// The bound the protocol guarantees: the number of concurrently HELD slots (successful
// reservations not yet released) never exceeds max_warm_inflight -- a fourth holder's
// fetch_add would observe prev + 1 > max and roll back. The raw running() counter may
// transiently overshoot while losers validate and roll back, so track holders explicitly;
// a check-then-add implementation lets extra holders through here.
TEST(WarmSlotReservationTest, HeldSlotsBoundedUnderConcurrency) {
    constexpr int kThreads = 8;
    constexpr int kMaxInflight = 3;
    constexpr int kDataReserved = 2;
    constexpr int kCap = 16; // loose cap so max_warm_inflight is the binding bound
    constexpr int kItersPerThread = 20000;

    WarmSlotReservation slots;
    std::atomic<int> holders{0};
    std::atomic<int> max_observed{0};

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&]() {
            for (int i = 0; i < kItersPerThread; ++i) {
                if (slots.try_reserve(kMaxInflight, kDataReserved, kCap)) {
                    int cur = holders.fetch_add(1) + 1;
                    int prev = max_observed.load();
                    while (cur > prev && !max_observed.compare_exchange_weak(prev, cur)) {
                    }
                    holders.fetch_sub(1);
                    slots.release();
                }
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }

    EXPECT_LE(max_observed.load(), kMaxInflight);
    EXPECT_EQ(0, slots.running());
}

} // namespace starrocks::pipeline
