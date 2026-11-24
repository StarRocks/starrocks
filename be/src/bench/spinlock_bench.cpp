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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <vector>

// Third-party benchmark harness
#include <benchmark/benchmark.h>

#include "base/concurrency/spinlock.h"

namespace starrocks {
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
            ->Threads(256)
            ->Threads(512)
            ->Threads(1024)
            ->Threads(2048)
            ->UseRealTime();
}

inline uint64_t splitmix64_step(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

struct alignas(64) SharedLockState {
    SpinLock lock;
    uint64_t value{0};
};

void BM_SpinLock_Uncontended(benchmark::State& state) {
    SpinLock lock;
    uint64_t counter = 0;
    for (auto _ : state) {
        std::lock_guard<SpinLock> guard(lock);
        ++counter;
        benchmark::DoNotOptimize(counter);
    }
    state.SetItemsProcessed(state.iterations());
}

void BM_SpinLock_ContendedCounter(benchmark::State& state) {
    static SharedLockState shared;
    for (auto _ : state) {
        std::lock_guard<SpinLock> guard(shared.lock);
        ++shared.value;
        benchmark::DoNotOptimize(shared.value);
    }
    state.SetItemsProcessed(state.iterations());
}

void BM_SpinLock_ContendedCriticalSection(benchmark::State& state) {
    static SharedLockState shared;
    uint64_t unlocked_state = static_cast<uint64_t>(state.thread_index()) + 1;
    for (auto _ : state) {
        {
            std::lock_guard<SpinLock> guard(shared.lock);
            uint64_t s = shared.value;
            for (int i = 0; i < 4; ++i) {
                s = splitmix64_step(s);
            }
            shared.value = s;
            benchmark::DoNotOptimize(shared.value);
        }
        for (int i = 0; i < 64; ++i) {
            unlocked_state = splitmix64_step(unlocked_state);
        }
        benchmark::DoNotOptimize(unlocked_state);
    }
    state.SetItemsProcessed(state.iterations());
}

struct PooledLatencyAggregator {
    std::mutex mu;
    std::vector<uint64_t> pooled_samples;
    int completed_threads{0};
};

void BM_SpinLock_AcquireLatency(benchmark::State& state) {
    static SharedLockState shared;
    static PooledLatencyAggregator aggregator;
    static constexpr size_t kMaxSamples = 1U << 16;
    static constexpr uint64_t kSampleMask = 63; // Sample 1 in 64 acquisitions.

    std::vector<uint64_t> samples;
    samples.reserve(kMaxSamples);
    uint64_t iter = static_cast<uint64_t>(state.thread_index());
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
            std::lock_guard<SpinLock> guard(shared.lock);
            ++shared.value;
            benchmark::DoNotOptimize(shared.value);
        }
    }

    {
        std::lock_guard<std::mutex> lock(aggregator.mu);
        if (aggregator.completed_threads == 0) {
            aggregator.pooled_samples.clear();
        }
        aggregator.pooled_samples.insert(aggregator.pooled_samples.end(), samples.begin(), samples.end());
        ++aggregator.completed_threads;
        if (aggregator.completed_threads == state.threads()) {
            aggregator.completed_threads = 0;
            if (!aggregator.pooled_samples.empty()) {
                std::sort(aggregator.pooled_samples.begin(), aggregator.pooled_samples.end());
                const auto& pooled = aggregator.pooled_samples;
                const auto percentile = [&pooled](double q) -> double {
                    const size_t idx = static_cast<size_t>(q * static_cast<double>(pooled.size() - 1));
                    return static_cast<double>(pooled[idx]);
                };
                state.counters["p50_ns"] = benchmark::Counter(percentile(0.50));
                state.counters["p99_ns"] = benchmark::Counter(percentile(0.99));
                state.counters["p999_ns"] = benchmark::Counter(percentile(0.999));
            }
        }
    }
    state.SetItemsProcessed(state.iterations());
}

BENCHMARK(BM_SpinLock_Uncontended)->Threads(1)->UseRealTime();
BENCHMARK(BM_SpinLock_ContendedCounter)->Apply(ApplyThreadCounts);
BENCHMARK(BM_SpinLock_ContendedCriticalSection)->Apply(ApplyThreadCounts);
BENCHMARK(BM_SpinLock_AcquireLatency)->Apply(ApplyThreadCounts);

} // namespace
} // namespace starrocks

BENCHMARK_MAIN();
