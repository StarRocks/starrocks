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

#include <benchmark/benchmark.h>
#include <glog/logging.h>

#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

void ensure_global_env() {
    if (!GlobalEnv::is_init()) {
        auto env = GlobalEnv::GetInstance();
        env->_process_mem_tracker = std::make_shared<MemTracker>(-1, "allocator_bench_root");
        env->_is_init = true;
    }
}
// Shared tracker tree for all threads in the benchmark.
static MemTracker* shared_tracker() {
    // Root has no limit to avoid accidental limit hits.
    static MemTracker root_tracker(1024 * 1024 * 1024, "mem_tracker_bench_root");
    return &root_tracker;
}

// Tracker tree with no limits set (limit = -1).
static MemTracker* shared_unlimited_tracker() {
    static MemTracker root_tracker(-1, "mem_tracker_bench_unlimited_root");
    return &root_tracker;
}

constexpr int64_t kBytesPerOp = 256;
constexpr int kOpsPerIteration = 100000;

static void BM_memtracker_try_consume(benchmark::State& state) {
    MemTracker* tracker = shared_tracker();

    // Reset consumption before each run to keep iterations comparable.
    if (state.thread_index == 0) {
        tracker->set(0);
    }

    for (auto _ : state) {
        for (int i = 0; i < kOpsPerIteration; ++i) {
            auto* failed = tracker->try_consume(kBytesPerOp);
            // try_consume should always succeed because limits are disabled.
            DCHECK(failed == nullptr);
            tracker->release(kBytesPerOp);
        }
    }

    const double ops = static_cast<double>(state.iterations()) * kOpsPerIteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.counters["s_per_op"] = benchmark::Counter(
            kOpsPerIteration, benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}
static void BM_memtracker_try_consume_unlimited(benchmark::State& state) {
    MemTracker* tracker = shared_unlimited_tracker();

    // Reset consumption before each run to keep iterations comparable.
    if (state.thread_index == 0) {
        tracker->set(0);
        if (tracker->parent() != nullptr) {
            tracker->parent()->set(0);
        }
    }

    for (auto _ : state) {
        for (int i = 0; i < kOpsPerIteration; ++i) {
            auto* failed = tracker->try_consume(kBytesPerOp);
            DCHECK(failed == nullptr);
            tracker->release(kBytesPerOp);
        }
    }

    const double ops = static_cast<double>(state.iterations()) * kOpsPerIteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.counters["s_per_op"] = benchmark::Counter(
            kOpsPerIteration, benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

static void BM_current_thread_try_mem_consume_with_cache(benchmark::State& state) {
    for (auto _ : state) {
        CurrentThread& current = CurrentThread::current();
        CHECK(current.mem_tracker() == GlobalEnv::GetInstance()->process_mem_tracker());
        for (int i = 0; i < kOpsPerIteration; ++i) {
            bool ok = current.try_mem_consume(kBytesPerOp);
            DCHECK(ok);
            current.mem_release(kBytesPerOp);
        }
        current.mem_tracker_ctx_shift();
    }
    // Flush any cached counters before reporting.

    const double ops = static_cast<double>(state.iterations()) * kOpsPerIteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.counters["s_per_op"] = benchmark::Counter(
            kOpsPerIteration, benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

static void BM_current_thread_try_mem_consume_without_cache(benchmark::State& state) {
    for (auto _ : state) {
        CHECK(CurrentThread::mem_tracker() == GlobalEnv::GetInstance()->process_mem_tracker());
        for (int i = 0; i < kOpsPerIteration; ++i) {
            bool ok = CurrentThread::try_mem_consume_without_cache(kBytesPerOp);
            DCHECK(ok);
            CurrentThread::mem_release_without_cache(kBytesPerOp);
        }
    }

    const double ops = static_cast<double>(state.iterations()) * kOpsPerIteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.counters["s_per_op"] = benchmark::Counter(
            kOpsPerIteration, benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

static void process_args(benchmark::internal::Benchmark* b) {
    std::vector<int> threads_list = {1, 2, 4, 8, 16, 32, 64};

    for (auto threads : threads_list) {
        b->Threads(threads)->Iterations(10);
    }
}

BENCHMARK(BM_memtracker_try_consume)->Apply(process_args);
BENCHMARK(BM_memtracker_try_consume_unlimited)->Apply(process_args);
BENCHMARK(BM_current_thread_try_mem_consume_with_cache)->Apply(process_args);
BENCHMARK(BM_current_thread_try_mem_consume_without_cache)->Apply(process_args);

} // namespace starrocks
int main(int argc, char** argv) {
    starrocks::ensure_global_env();
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}
