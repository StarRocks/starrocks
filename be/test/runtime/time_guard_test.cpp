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

#include "runtime/time_guard.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "butil/resource_pool.h"

namespace starrocks {

// The TraceContext resource is allocated from a dedicated butil::ResourcePool. `item_num` is the
// monotonic high-water mark of constructed items: returning a resource does not decrease it, but a
// subsequent get_resource() on the same thread reuses the freed slot instead of constructing a new
// one. So on a single-threaded create/destroy loop, `item_num` stays flat when each guard returns
// its resource, and grows ~linearly when it leaks one per iteration.
static size_t allocated_trace_contexts() {
    return butil::describe_resources<SignalTimerGuard::TraceContext>().item_num;
}

// A long timeout means the timer is always unscheduled before it fires (the common fast path,
// where unschedule() returns 0). Before the fix, the destructor only returned the resource when
// unschedule() != 0, so every fast-path guard leaked one TraceContext.
TEST(SignalTimerGuardTest, fast_path_does_not_leak_resource) {
    constexpr int64_t kNeverFiresMs = 3600 * 1000; // 1 hour
    constexpr int kIterations = 200;

    // Warm up so the pool has constructed/freed at least one slot before we measure the baseline.
    { SignalTimerGuard guard(kNeverFiresMs); }

    const size_t before = allocated_trace_contexts();
    for (int i = 0; i < kIterations; ++i) {
        SignalTimerGuard guard(kNeverFiresMs);
        // guard destroyed here; timer removed before firing -> resource must be returned and the
        // next iteration must reuse it.
    }
    const size_t growth = allocated_trace_contexts() - before;

    // With the fix the freed slot is reused every iteration, so growth is ~0 (a small allowance
    // covers pool block bookkeeping). A leak would grow by ~kIterations.
    EXPECT_LT(growth, static_cast<size_t>(kIterations / 2))
            << "fast-path guard leaked TraceContext resources; item_num grew by " << growth << " over " << kIterations
            << " iterations";
}

// timeout <= 0 must be a no-op: nothing is scheduled and no resource is allocated.
TEST(SignalTimerGuardTest, disabled_allocates_nothing) {
    const size_t before = allocated_trace_contexts();
    for (int i = 0; i < 50; ++i) {
        SignalTimerGuard guard_zero(0);
        SignalTimerGuard guard_neg(-1);
    }
    EXPECT_EQ(before, allocated_trace_contexts());
}

// When the timer actually fires, dump_trace_info() runs and returns the resource itself; the
// destructor must not return it again (the pre-fix code double-returned it on res != 0). Exercising
// the fired path must neither crash nor corrupt the pool.
TEST(SignalTimerGuardTest, fired_path_runs_without_crash) {
    constexpr int64_t kFiresQuicklyMs = 1;
    for (int i = 0; i < 20; ++i) {
        {
            SignalTimerGuard guard(kFiresQuicklyMs);
        }
        // Give the global timer thread time to run the callback before the next iteration.
        std::this_thread::sleep_for(std::chrono::milliseconds(15));
    }
    // Let any in-flight callback settle. Reaching here without a crash/double-free is the assertion.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    SUCCEED();
}

} // namespace starrocks
