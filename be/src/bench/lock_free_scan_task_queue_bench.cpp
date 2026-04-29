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

#include <algorithm>
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
    for (int threads : {1, 2, 4, 8, 16, 32}) {
        b->Arg(threads);
    }
}

static constexpr int PREFILL_MIN = 4096;
static constexpr int PREFILL_PER_THREAD = 256;
static constexpr int OPS_PER_THREAD = 5000;

static int prefill_count(int num_threads) {
    return std::max(PREFILL_MIN, num_threads * PREFILL_PER_THREAD);
}

// ---------------------------------------------------------------------------
// Scenario 1: Sustained Mixed take+put (queue always has data)
//
// Matches ScanExecutor::worker_thread() pattern:
//   take → task.run() → force_put (resubmit)
// Queue is pre-filled ONCE before the loop.
// ---------------------------------------------------------------------------

static void BM_LockFreeScanTaskQueue_SustainedMixed(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int fill_count = prefill_count(num_threads);

    LockFreeScanTaskQueue queue(num_threads);
    for (int i = 0; i < fill_count; ++i) {
        queue.force_put(make_bench_task(i % 21), i % num_threads);
    }

    int64_t cumulative_ops = 0;
    int64_t cumulative_failed_ops = 0;
    for (auto _ : state) {
        state.PauseTiming();
        std::vector<int64_t> local_ops(num_threads, 0);
        std::vector<int64_t> local_fail_ops(num_threads, 0);
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &local_ops, &local_fail_ops, &start, t]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                int64_t ops = 0;
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    ScanTask task;
                    if (queue.try_take(task, t)) {
                        queue.force_put(std::move(task), t);
                        ++ops;
                    } else {
                        ++local_fail_ops[t];
                    }
                }
                local_ops[t] = ops;
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();

        for (int t = 0; t < num_threads; ++t) {
            cumulative_ops += local_ops[t];
            cumulative_failed_ops += local_fail_ops[t];
        }
    }
    state.SetItemsProcessed(cumulative_ops);
    state.counters["fail_ops"] = benchmark::Counter(cumulative_failed_ops);
}

BENCHMARK(BM_LockFreeScanTaskQueue_SustainedMixed)->Apply(BM_ThreadArgs)->UseRealTime();

// Same as BM_LockFreeScanTaskQueue_SustainedMixed, but producers always use
// implicit producer path (force_put without worker_id), while consumers still
// use explicit consumer tokens (try_take with worker_id).
static void BM_LockFreeScanTaskQueue_SustainedMixed_ImplicitProducerExplicitConsumer(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int fill_count = prefill_count(num_threads);

    LockFreeScanTaskQueue queue(num_threads);
    for (int i = 0; i < fill_count; ++i) {
        queue.force_put(make_bench_task(i % 21));
    }

    int64_t cumulative_ops = 0;
    int64_t cumulative_failed_ops = 0;
    for (auto _ : state) {
        state.PauseTiming();
        std::vector<int64_t> local_ops(num_threads, 0);
        std::vector<int64_t> local_fail_ops(num_threads, 0);
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &local_ops, &local_fail_ops, &start, t]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                int64_t ops = 0;
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    ScanTask task;
                    if (queue.try_take(task, t)) {
                        queue.force_put(std::move(task));
                        ++ops;
                    } else {
                        ++local_fail_ops[t];
                    }
                }
                local_ops[t] = ops;
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();

        for (int t = 0; t < num_threads; ++t) {
            cumulative_ops += local_ops[t];
            cumulative_failed_ops += local_fail_ops[t];
        }
    }
    state.SetItemsProcessed(cumulative_ops);
    state.counters["fail_ops"] = benchmark::Counter(cumulative_failed_ops);
}

BENCHMARK(BM_LockFreeScanTaskQueue_SustainedMixed_ImplicitProducerExplicitConsumer)
        ->Apply(BM_ThreadArgs)
        ->UseRealTime();

static void BM_PriorityScanTaskQueue_SustainedMixed(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int fill_count = prefill_count(num_threads);

    PriorityScanTaskQueue queue(fill_count * 2);
    for (int i = 0; i < fill_count; ++i) {
        queue.force_put(make_bench_task(i % 21));
    }

    int64_t cumulative_ops = 0;
    int64_t cumulative_failed_ops = 0;
    for (auto _ : state) {
        state.PauseTiming();
        std::vector<int64_t> local_ops(num_threads, 0);
        std::vector<int64_t> local_fail_ops(num_threads, 0);
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &local_ops, &local_fail_ops, &start, t]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                int64_t ops = 0;
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    ScanTask task;
                    if (queue.try_take(&task)) {
                        queue.force_put(std::move(task));
                        ++ops;
                    } else {
                        ++local_fail_ops[t];
                    }
                }
                local_ops[t] = ops;
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();

        for (int t = 0; t < num_threads; ++t) {
            cumulative_ops += local_ops[t];
            cumulative_failed_ops += local_fail_ops[t];
        }
    }
    state.SetItemsProcessed(cumulative_ops);
    state.counters["fail_ops"] = benchmark::Counter(cumulative_failed_ops);
}

BENCHMARK(BM_PriorityScanTaskQueue_SustainedMixed)->Apply(BM_ThreadArgs)->UseRealTime();

// ---------------------------------------------------------------------------
// Scenario 2: Pure enqueue contention
// ---------------------------------------------------------------------------

static void BM_LockFreeScanTaskQueue_EnqueueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    for (auto _ : state) {
        state.PauseTiming();
        LockFreeScanTaskQueue queue(num_threads);
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &start, t]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    queue.force_put(make_bench_task(i % 21), t);
                }
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();

        state.PauseTiming();
        ScanTask drain;
        while (queue.try_take(drain)) {
        }
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * num_threads * OPS_PER_THREAD);
}

BENCHMARK(BM_LockFreeScanTaskQueue_EnqueueOnly)->Apply(BM_ThreadArgs)->UseRealTime();

// Enqueue-only with implicit producer path.
static void BM_LockFreeScanTaskQueue_EnqueueOnly_ImplicitProducer(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    for (auto _ : state) {
        state.PauseTiming();
        LockFreeScanTaskQueue queue(num_threads);
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &start]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    queue.force_put(make_bench_task(i % 21));
                }
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();

        state.PauseTiming();
        ScanTask drain;
        int drain_worker = 0;
        while (queue.try_take(drain, drain_worker)) {
            drain_worker = (drain_worker + 1) % num_threads;
        }
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * num_threads * OPS_PER_THREAD);
}

BENCHMARK(BM_LockFreeScanTaskQueue_EnqueueOnly_ImplicitProducer)->Apply(BM_ThreadArgs)->UseRealTime();

static void BM_PriorityScanTaskQueue_EnqueueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    for (auto _ : state) {
        state.PauseTiming();
        PriorityScanTaskQueue queue(num_threads * OPS_PER_THREAD + 1);
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &start]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    queue.force_put(make_bench_task(i % 21));
                }
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();

        state.PauseTiming();
        queue.close();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * num_threads * OPS_PER_THREAD);
}

BENCHMARK(BM_PriorityScanTaskQueue_EnqueueOnly)->Apply(BM_ThreadArgs)->UseRealTime();

// ---------------------------------------------------------------------------
// Scenario 3: Pure dequeue contention
//
// Pre-fill the queue, then N threads all compete to dequeue.
// Isolates the try_take hot path.
// ---------------------------------------------------------------------------

static void BM_LockFreeScanTaskQueue_DequeueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int total_ops = num_threads * OPS_PER_THREAD;

    for (auto _ : state) {
        state.PauseTiming();
        LockFreeScanTaskQueue queue(num_threads);
        for (int i = 0; i < total_ops; ++i) {
            queue.force_put(make_bench_task(i % 21), i % num_threads);
        }

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &start, t]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                ScanTask task;
                while (queue.try_take(task, t)) {
                }
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();
    }

    state.SetItemsProcessed(state.iterations() * total_ops);
}

BENCHMARK(BM_LockFreeScanTaskQueue_DequeueOnly)->Apply(BM_ThreadArgs)->UseRealTime();

// Dequeue-only with implicit producer prefill + explicit consumer tokens.
static void BM_LockFreeScanTaskQueue_DequeueOnly_ImplicitProducerExplicitConsumer(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int total_ops = num_threads * OPS_PER_THREAD;

    for (auto _ : state) {
        state.PauseTiming();
        LockFreeScanTaskQueue queue(num_threads);
        for (int i = 0; i < total_ops; ++i) {
            queue.force_put(make_bench_task(i % 21));
        }

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &start, t]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                ScanTask task;
                while (queue.try_take(task, t)) {
                }
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();
    }

    state.SetItemsProcessed(state.iterations() * total_ops);
}

BENCHMARK(BM_LockFreeScanTaskQueue_DequeueOnly_ImplicitProducerExplicitConsumer)->Apply(BM_ThreadArgs)->UseRealTime();

static void BM_PriorityScanTaskQueue_DequeueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int total_ops = num_threads * OPS_PER_THREAD;

    for (auto _ : state) {
        state.PauseTiming();
        PriorityScanTaskQueue queue(total_ops + 1);
        for (int i = 0; i < total_ops; ++i) {
            queue.force_put(make_bench_task(i % 21));
        }

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &start]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                ScanTask task;
                while (queue.try_take(&task)) {
                }
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();
    }

    state.SetItemsProcessed(state.iterations() * total_ops);
}

BENCHMARK(BM_PriorityScanTaskQueue_DequeueOnly)->Apply(BM_ThreadArgs)->UseRealTime();

} // namespace starrocks::workgroup

BENCHMARK_MAIN();
