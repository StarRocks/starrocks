// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#include <benchmark/benchmark.h>

#include <atomic>
#include <thread>
#include <vector>

#include "exec/workgroup/lock_free_scan_task_queue.h"
#include "exec/workgroup/scan_task_queue.h"

namespace starrocks::workgroup {

static ScanTask make_bench_task(int priority) {
    ScanTask task([](YieldContext&) {});
    task.priority = priority;
    return task;
}

static void BM_ThreadArgs(benchmark::internal::Benchmark* b) {
    for (int threads : {1, 2, 4, 8, 16, 32, 64}) {
        b->Arg(threads);
    }
}

// Pre-fill count: large enough that queue never empties during take+put.
// Keep prefill small to avoid penalizing old queue's O(N) _adjust_priority_if_needed,
// but large enough that take() never blocks (prefill >> num_threads).
static constexpr int PREFILL = 1000;
// Ops per thread per benchmark iteration.
static constexpr int OPS_PER_THREAD = 5000;

// ---------------------------------------------------------------------------
// Scenario 1: Sustained Mixed take+put (queue always has data)
//
// Queue is pre-filled ONCE before the benchmark loop. Each iteration just
// measures the take+put hot path. The queue is always non-empty because
// every take is immediately followed by a put.
// ---------------------------------------------------------------------------

static void BM_LockFreeScanTaskQueue_SustainedMixed(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    LockFreeScanTaskQueue queue(num_threads);
    for (int i = 0; i < PREFILL; ++i) {
        queue.force_put(make_bench_task(i % 21), 0);
    }

    int64_t cumulative_ops = 0;
    for (auto _ : state) {
        std::atomic<int64_t> total_ops{0};
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &total_ops, t]() {
                int64_t local_ops = 0;
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    ScanTask task;
                    if (queue.try_take(task)) {
                        queue.force_put(std::move(task), t);
                        local_ops++;
                    }
                }
                total_ops.fetch_add(local_ops, std::memory_order_relaxed);
            });
        }

        for (auto& th : threads) th.join();
        cumulative_ops += total_ops.load();
    }
    state.SetItemsProcessed(cumulative_ops);
}

BENCHMARK(BM_LockFreeScanTaskQueue_SustainedMixed)->Apply(BM_ThreadArgs);

static void BM_PriorityScanTaskQueue_SustainedMixed(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    PriorityScanTaskQueue queue(PREFILL * 2);
    for (int i = 0; i < PREFILL; ++i) {
        queue.force_put(make_bench_task(i % 21));
    }

    int64_t cumulative_ops = 0;
    for (auto _ : state) {
        std::atomic<int64_t> total_ops{0};
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &total_ops]() {
                int64_t local_ops = 0;
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    auto result = queue.take();
                    if (result.ok()) {
                        queue.force_put(std::move(result).value());
                        local_ops++;
                    }
                }
                total_ops.fetch_add(local_ops, std::memory_order_relaxed);
            });
        }

        for (auto& th : threads) th.join();
        cumulative_ops += total_ops.load();
    }
}

BENCHMARK(BM_PriorityScanTaskQueue_SustainedMixed)->Apply(BM_ThreadArgs);

// ---------------------------------------------------------------------------
// Scenario 2: Pure enqueue contention
//
// N threads each enqueue OPS_PER_THREAD items concurrently.
// Queue is drained after each iteration to prevent unbounded growth.
// ---------------------------------------------------------------------------

static void BM_LockFreeScanTaskQueue_EnqueueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    for (auto _ : state) {
        LockFreeScanTaskQueue queue(num_threads);

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, t]() {
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    queue.force_put(make_bench_task(i % 21), t);
                }
            });
        }

        for (auto& th : threads) th.join();

        state.PauseTiming();
        ScanTask drain;
        while (queue.try_take(drain)) {}
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * num_threads * OPS_PER_THREAD);
}

BENCHMARK(BM_LockFreeScanTaskQueue_EnqueueOnly)->Apply(BM_ThreadArgs);

static void BM_PriorityScanTaskQueue_EnqueueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    for (auto _ : state) {
        PriorityScanTaskQueue queue(num_threads * OPS_PER_THREAD + 1);

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue]() {
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    queue.force_put(make_bench_task(i % 21));
                }
            });
        }

        for (auto& th : threads) th.join();

        state.PauseTiming();
        queue.close();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * num_threads * OPS_PER_THREAD);
}

BENCHMARK(BM_PriorityScanTaskQueue_EnqueueOnly)->Apply(BM_ThreadArgs);

} // namespace starrocks::workgroup

BENCHMARK_MAIN();
