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

#include <sched.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <vector>

// Third-party benchmark harness
#include <benchmark/benchmark.h>

#include "base/concurrency/spinlock.h"
#include "base/logging.h"

namespace starrocks {

// Exact pre-optimization SpinLock preserved as a self-contained baseline for
// side-by-side A/B benchmarking on both x86_64 and ARM64:
// 1. try_lock() uses full-barrier __sync_bool_compare_and_swap without a relaxed load pre-check.
// 2. unlock() emits a full __sync_synchronize() barrier followed by a plain store.
// 3. slow_acquire() executes 70 consecutive pause/isb instructions blindly without
//    probing _locked inside the loop.
class LegacySpinLock {
public:
    LegacySpinLock() = default;

    void lock() {
        if (!try_lock()) {
            slow_acquire();
        }
    }

    void unlock() {
        __sync_synchronize();
        DCHECK(_locked);
        _locked = false;
    }

    inline bool try_lock() { return __sync_bool_compare_and_swap(&_locked, false, true); }

    void dcheck_locked() { DCHECK(_locked); }

private:
    __attribute__((noinline)) void slow_acquire() {
        while (true) {
            if (try_lock()) {
                return;
            }
            for (int i = 0; i < NUM_SPIN_CYCLES; ++i) {
#if (defined(__i386) || defined(__x86_64__))
                asm volatile("pause\n" : : : "memory");
#elif defined(__aarch64__)
                asm volatile("isb\n" ::: "memory");
#endif
            }
            if (try_lock()) {
                return;
            }
            sched_yield();
        }
    }

    static constexpr int NUM_SPIN_CYCLES = 70;
    bool _locked{false};
};

namespace {

void ApplyThreadCounts(benchmark::Benchmark* b) {
    b->Threads(1)
            ->Threads(2)
            ->Threads(4)
            ->Threads(8)
            ->Threads(16)
            ->Threads(32)
            ->Threads(64)
            ->Threads(128)
            ->UseRealTime();
}

inline uint64_t splitmix64_step(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

template <typename LockType>
struct alignas(64) SharedLockState {
    LockType lock;
    uint64_t value{0};
};

template <typename LockType>
void BM_Uncontended(benchmark::State& state) {
    LockType lock;
    uint64_t counter = 0;
    for (auto _ : state) {
        std::lock_guard<LockType> guard(lock);
        ++counter;
        benchmark::DoNotOptimize(counter);
    }
    state.SetItemsProcessed(state.iterations());
}

template <typename LockType>
void BM_ContendedCounter(benchmark::State& state) {
    static SharedLockState<LockType> shared;
    for (auto _ : state) {
        std::lock_guard<LockType> guard(shared.lock);
        ++shared.value;
        benchmark::DoNotOptimize(shared.value);
    }
    state.SetItemsProcessed(state.iterations());
}

template <typename LockType>
void BM_ContendedCriticalSection(benchmark::State& state) {
    static SharedLockState<LockType> shared;
    uint64_t unlocked_state = static_cast<uint64_t>(state.thread_index()) + 1;
    for (auto _ : state) {
        {
            std::lock_guard<LockType> guard(shared.lock);
            uint64_t s = shared.value;
            for (int i = 0; i < 4; ++i) {
                s = splitmix64_step(s);
            }
            shared.value = s;
            benchmark::DoNotOptimize(shared.value);
        }
        for (int i = 0; i < 4; ++i) {
            unlocked_state = splitmix64_step(unlocked_state);
        }
        benchmark::DoNotOptimize(unlocked_state);
    }
    state.SetItemsProcessed(state.iterations());
}

template <typename LockType>
void BM_AcquireLatency(benchmark::State& state) {
    static SharedLockState<LockType> shared;
    static constexpr size_t kMaxSamples = 1U << 16;
    static constexpr uint64_t kSampleMask = 63; // Sample 1 in 64 acquisitions.

    std::vector<uint64_t> samples;
    samples.reserve(kMaxSamples);
    uint64_t iter = 0;
    size_t overwrite_pos = 0;

    for (auto _ : state) {
        if ((iter++ & kSampleMask) == 0) {
            const auto start = std::chrono::steady_clock::now();
            shared.lock.lock();
            const auto acquired = std::chrono::steady_clock::now();
            ++shared.value;
            benchmark::DoNotOptimize(shared.value);
            shared.lock.unlock();

            const auto wait_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(acquired - start).count());
            if (samples.size() < kMaxSamples) {
                samples.push_back(wait_ns);
            } else {
                samples[overwrite_pos++ & (kMaxSamples - 1)] = wait_ns;
            }
        } else {
            std::lock_guard<LockType> guard(shared.lock);
            ++shared.value;
            benchmark::DoNotOptimize(shared.value);
        }
    }

    if (!samples.empty()) {
        std::sort(samples.begin(), samples.end());
        const auto percentile = [&samples](double q) -> double {
            const size_t idx = static_cast<size_t>(q * static_cast<double>(samples.size() - 1));
            return static_cast<double>(samples[idx]);
        };
        state.counters["p50_ns"] = benchmark::Counter(percentile(0.50), benchmark::Counter::kAvgThreads);
        state.counters["p99_ns"] = benchmark::Counter(percentile(0.99), benchmark::Counter::kAvgThreads);
        state.counters["p999_ns"] = benchmark::Counter(percentile(0.999), benchmark::Counter::kAvgThreads);
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK_TEMPLATE(BM_Uncontended, LegacySpinLock)->Threads(1)->UseRealTime();
BENCHMARK_TEMPLATE(BM_Uncontended, SpinLock)->Threads(1)->UseRealTime();

BENCHMARK_TEMPLATE(BM_ContendedCounter, LegacySpinLock)->Apply(ApplyThreadCounts);
BENCHMARK_TEMPLATE(BM_ContendedCounter, SpinLock)->Apply(ApplyThreadCounts);

BENCHMARK_TEMPLATE(BM_ContendedCriticalSection, LegacySpinLock)->Apply(ApplyThreadCounts);
BENCHMARK_TEMPLATE(BM_ContendedCriticalSection, SpinLock)->Apply(ApplyThreadCounts);

BENCHMARK_TEMPLATE(BM_AcquireLatency, LegacySpinLock)->Apply(ApplyThreadCounts);
BENCHMARK_TEMPLATE(BM_AcquireLatency, SpinLock)->Apply(ApplyThreadCounts);

} // namespace
} // namespace starrocks

BENCHMARK_MAIN();
