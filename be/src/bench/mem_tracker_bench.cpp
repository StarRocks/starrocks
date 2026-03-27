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

static MemTracker* process_mem_tracker_provider() {
    return GlobalEnv::GetInstance()->process_mem_tracker();
}

void ensure_global_env() {
    if (!GlobalEnv::is_init()) {
        auto env = GlobalEnv::GetInstance();
        env->_process_mem_tracker = std::make_shared<MemTracker>(-1, "allocator_bench_root");
        CurrentThread::set_mem_tracker_source(&GlobalEnv::is_init, process_mem_tracker_provider);
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

// 3-level hierarchy: process -> pool -> query (reflects real usage)
static MemTracker* shared_hierarchy_3level_leaf() {
    static MemTracker process_tracker(1LL << 30, "bench_process");
    static MemTracker pool_tracker(512LL << 20, "bench_pool", &process_tracker);
    static MemTracker query_tracker(256LL << 20, "bench_query", &pool_tracker);
    return &query_tracker;
}

// Shared parent for contention benchmarks — each thread creates its own child.
static MemTracker* shared_contention_parent() {
    static MemTracker parent_tracker(1LL << 30, "bench_contention_parent");
    return &parent_tracker;
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

// ---- New benchmarks: consume()/release() path (the actual hot path) ----

// Benchmark consume/release on single-level tracker (baseline for CAS overhead)
static void BM_memtracker_consume(benchmark::State& state) {
    MemTracker* tracker = shared_tracker();
    if (state.thread_index == 0) {
        tracker->set(0);
    }

    for (auto _ : state) {
        for (int i = 0; i < kOpsPerIteration; ++i) {
            tracker->consume(kBytesPerOp);
            tracker->release(kBytesPerOp);
        }
    }

    const double ops = static_cast<double>(state.iterations()) * kOpsPerIteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.counters["s_per_op"] = benchmark::Counter(
            kOpsPerIteration, benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

// ---- New benchmarks: multi-level hierarchy ----

// consume/release on 3-level hierarchy (process -> pool -> query)
static void BM_memtracker_consume_hierarchy_3level(benchmark::State& state) {
    MemTracker* tracker = shared_hierarchy_3level_leaf();
    if (state.thread_index == 0) {
        tracker->set(0);
        tracker->parent()->set(0);
        tracker->parent()->parent()->set(0);
    }

    for (auto _ : state) {
        for (int i = 0; i < kOpsPerIteration; ++i) {
            tracker->consume(kBytesPerOp);
            tracker->release(kBytesPerOp);
        }
    }

    const double ops = static_cast<double>(state.iterations()) * kOpsPerIteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.counters["s_per_op"] = benchmark::Counter(
            kOpsPerIteration, benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

// try_consume/release on 3-level hierarchy
static void BM_memtracker_try_consume_hierarchy_3level(benchmark::State& state) {
    MemTracker* tracker = shared_hierarchy_3level_leaf();
    if (state.thread_index == 0) {
        tracker->set(0);
        tracker->parent()->set(0);
        tracker->parent()->parent()->set(0);
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

// ---- New benchmark: shared parent contention ----

// Multiple threads each with their own child tracker, all rolling up to a shared parent.
// This is the real contention pattern: N queries sharing the query_pool tracker.
static void BM_memtracker_shared_parent_contention(benchmark::State& state) {
    MemTracker* parent = shared_contention_parent();
    // Each thread creates its own child tracker.
    thread_local std::unique_ptr<MemTracker> tl_child;
    if (!tl_child || tl_child->parent() != parent) {
        tl_child = std::make_unique<MemTracker>(-1, "bench_child_" + std::to_string(state.thread_index), parent);
    }

    if (state.thread_index == 0) {
        parent->set(0);
    }
    tl_child->set(0);

    for (auto _ : state) {
        for (int i = 0; i < kOpsPerIteration; ++i) {
            tl_child->consume(kBytesPerOp);
            tl_child->release(kBytesPerOp);
        }
    }

    const double ops = static_cast<double>(state.iterations()) * kOpsPerIteration;
    state.SetItemsProcessed(static_cast<int64_t>(ops));
    state.counters["s_per_op"] = benchmark::Counter(
            kOpsPerIteration, benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

// ---- New benchmark: peak-stable scenario (CAS skip validation) ----

// Pre-establish a high peak, then do consume/release pairs below it.
// Measures the benefit of skipping the CAS loop when value < peak.
static void BM_memtracker_consume_peak_stable(benchmark::State& state) {
    MemTracker* tracker = shared_tracker();
    if (state.thread_index == 0) {
        // Establish a high peak, then reset current to 0
        tracker->set(static_cast<int64_t>(kBytesPerOp) * kOpsPerIteration);
        tracker->set(0);
    }

    for (auto _ : state) {
        for (int i = 0; i < kOpsPerIteration; ++i) {
            tracker->consume(kBytesPerOp);
            tracker->release(kBytesPerOp);
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
BENCHMARK(BM_memtracker_consume)->Apply(process_args);
BENCHMARK(BM_memtracker_consume_hierarchy_3level)->Apply(process_args);
BENCHMARK(BM_memtracker_try_consume_hierarchy_3level)->Apply(process_args);
BENCHMARK(BM_memtracker_shared_parent_contention)->Apply(process_args);
BENCHMARK(BM_memtracker_consume_peak_stable)->Apply(process_args);

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
