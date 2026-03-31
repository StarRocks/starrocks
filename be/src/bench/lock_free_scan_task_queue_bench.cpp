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

#include <atomic>
#include <thread>
#include <vector>

#include "exec/workgroup/lock_free_scan_task_queue.h"
#include "exec/workgroup/scan_task_queue.h"

namespace starrocks::workgroup {

// ---------------------------------------------------------------------------
// Helper: create a lightweight ScanTask suitable for benchmarking.
// ---------------------------------------------------------------------------

static ScanTask make_bench_task(int priority) {
    ScanTask task([](YieldContext&) {});
    task.priority = priority;
    return task;
}

// ---------------------------------------------------------------------------
// Thread-count parameterization: 1, 2, 4, 8, 16, 32, 64
// ---------------------------------------------------------------------------

static void BM_ThreadArgs(benchmark::internal::Benchmark* b) {
    for (int threads : {1, 2, 4, 8, 16, 32, 64}) {
        b->Arg(threads);
    }
}

// ---------------------------------------------------------------------------
// BM_LockFreeScanTaskQueue_Mixed
//
// N threads concurrently do try_take + force_put (simulates ScanExecutor
// worker loop). Pre-fill with N*1000 tasks, each thread does 1000
// iterations of take/put.
// ---------------------------------------------------------------------------

static void BM_LockFreeScanTaskQueue_Mixed(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int items_per_thread = 1000;
    const int total_items = num_threads * items_per_thread;

    for (auto _ : state) {
        LockFreeScanTaskQueue queue(num_threads);

        // Pre-fill the queue.
        for (int i = 0; i < total_items; ++i) {
            queue.force_put(make_bench_task(i % LockFreeScanTaskQueue::NUM_PRIORITY_LEVELS));
        }

        // Launch N threads, each doing mixed take/put.
        std::atomic<int> ops_done{0};
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &ops_done, t, items_per_thread]() {
                for (int i = 0; i < items_per_thread; ++i) {
                    ScanTask task;
                    if (queue.try_take(task)) {
                        queue.force_put(std::move(task), t);
                        ops_done.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        // Drain remaining tasks.
        ScanTask drain;
        while (queue.try_take(drain)) {
        }
    }

    state.SetItemsProcessed(state.iterations() * total_items);
}

BENCHMARK(BM_LockFreeScanTaskQueue_Mixed)->Apply(BM_ThreadArgs);

// ---------------------------------------------------------------------------
// BM_PriorityScanTaskQueue_Mixed
//
// Same mixed-contention scenario but using the old mutex-based
// PriorityScanTaskQueue. Pre-fill so that take() never blocks.
// ---------------------------------------------------------------------------

static void BM_PriorityScanTaskQueue_Mixed(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int items_per_thread = 1000;
    const int total_items = num_threads * items_per_thread;

    for (auto _ : state) {
        PriorityScanTaskQueue queue(100000);

        // Pre-fill the queue so take() will not block.
        for (int i = 0; i < total_items; ++i) {
            queue.force_put(make_bench_task(i % LockFreeScanTaskQueue::NUM_PRIORITY_LEVELS));
        }

        // Launch N threads, each doing mixed take/put.
        std::atomic<int> ops_done{0};
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &ops_done, items_per_thread]() {
                for (int i = 0; i < items_per_thread; ++i) {
                    auto result = queue.take();
                    if (result.ok()) {
                        queue.force_put(std::move(result).value());
                        ops_done.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        // Drain remaining tasks.
        queue.close();
    }

    state.SetItemsProcessed(state.iterations() * total_items);
}

BENCHMARK(BM_PriorityScanTaskQueue_Mixed)->Apply(BM_ThreadArgs);

// ---------------------------------------------------------------------------
// BM_LockFreeScanTaskQueue_ProducerConsumer
//
// P producer threads + C consumer threads (disjoint pools).
// Simulates Driver Executor -> ScanExecutor pattern.
// Producers use force_put, consumers use try_take.
// Uses half the thread count for producers and half for consumers
// (minimum 1 of each).
// ---------------------------------------------------------------------------

static void BM_LockFreeScanTaskQueue_ProducerConsumer(benchmark::State& state) {
    const int total_threads = static_cast<int>(state.range(0));
    const int num_producers = std::max(1, total_threads / 2);
    const int num_consumers = std::max(1, total_threads - num_producers);
    const int items_per_producer = 1000;
    const int total_items = num_producers * items_per_producer;

    for (auto _ : state) {
        LockFreeScanTaskQueue queue(num_consumers);

        std::atomic<bool> producers_done{false};
        std::atomic<int> consumed{0};
        std::vector<std::thread> threads;
        threads.reserve(total_threads);

        // Producer threads.
        for (int p = 0; p < num_producers; ++p) {
            threads.emplace_back([&queue, items_per_producer]() {
                for (int i = 0; i < items_per_producer; ++i) {
                    queue.force_put(make_bench_task(i % LockFreeScanTaskQueue::NUM_PRIORITY_LEVELS));
                }
            });
        }

        // Consumer threads.
        for (int c = 0; c < num_consumers; ++c) {
            threads.emplace_back([&queue, &producers_done, &consumed, total_items]() {
                while (true) {
                    ScanTask task;
                    if (queue.try_take(task)) {
                        consumed.fetch_add(1, std::memory_order_relaxed);
                    } else if (producers_done.load(std::memory_order_acquire) &&
                               consumed.load(std::memory_order_relaxed) >= total_items) {
                        break;
                    }
                }
            });
        }

        // Wait for producers to finish.
        for (int i = 0; i < num_producers; ++i) {
            threads[i].join();
        }
        producers_done.store(true, std::memory_order_release);

        // Wait for consumers to finish.
        for (int i = num_producers; i < static_cast<int>(threads.size()); ++i) {
            threads[i].join();
        }

        // Drain any remaining tasks.
        ScanTask drain;
        while (queue.try_take(drain)) {
        }
    }

    state.SetItemsProcessed(state.iterations() * total_items);
}

BENCHMARK(BM_LockFreeScanTaskQueue_ProducerConsumer)->Apply(BM_ThreadArgs);

// ---------------------------------------------------------------------------
// BM_LockFreeScanTaskQueue_EnqueueOnly
//
// N threads each force_put 1000 tasks concurrently.
// Measures pure enqueue throughput and contention.
// ---------------------------------------------------------------------------

static void BM_LockFreeScanTaskQueue_EnqueueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int items_per_thread = 1000;
    const int total_items = num_threads * items_per_thread;

    for (auto _ : state) {
        LockFreeScanTaskQueue queue(num_threads);

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, t, items_per_thread]() {
                for (int i = 0; i < items_per_thread; ++i) {
                    queue.force_put(make_bench_task(i % LockFreeScanTaskQueue::NUM_PRIORITY_LEVELS), t);
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        // Drain the queue.
        ScanTask drain;
        while (queue.try_take(drain)) {
        }
    }

    state.SetItemsProcessed(state.iterations() * total_items);
}

BENCHMARK(BM_LockFreeScanTaskQueue_EnqueueOnly)->Apply(BM_ThreadArgs);

// ---------------------------------------------------------------------------
// BM_PriorityScanTaskQueue_EnqueueOnly
//
// Same enqueue-only scenario but using the old mutex-based queue.
// ---------------------------------------------------------------------------

static void BM_PriorityScanTaskQueue_EnqueueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int items_per_thread = 1000;
    const int total_items = num_threads * items_per_thread;

    for (auto _ : state) {
        PriorityScanTaskQueue queue(100000);

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, items_per_thread]() {
                for (int i = 0; i < items_per_thread; ++i) {
                    queue.force_put(make_bench_task(i % LockFreeScanTaskQueue::NUM_PRIORITY_LEVELS));
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        // Drain the queue.
        queue.close();
    }

    state.SetItemsProcessed(state.iterations() * total_items);
}

BENCHMARK(BM_PriorityScanTaskQueue_EnqueueOnly)->Apply(BM_ThreadArgs);

} // namespace starrocks::workgroup

BENCHMARK_MAIN();
