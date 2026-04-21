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
#include <memory>
#include <thread>
#include <vector>

#include "exec/pipeline/empty_set_operator.h"
#include "exec/pipeline/lock_free_driver_queue.h"
#include "exec/pipeline/pipeline_driver_queue.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/pipeline_metrics.h"

namespace starrocks::pipeline {

static std::vector<PipelineDriver*> make_simple_drivers(int count, int level_stride) {
    std::vector<PipelineDriver*> drivers;
    drivers.reserve(count);
    auto source_factory = std::make_shared<EmptySetOperatorFactory>(1, 1);
    for (int i = 0; i < count; ++i) {
        Operators ops;
        ops.emplace_back(source_factory->create(1, i));
        auto* d = new PipelineDriver(std::move(ops), nullptr, nullptr, nullptr, i);
        d->set_driver_queue_level(static_cast<size_t>(i % level_stride));
        drivers.push_back(d);
    }
    return drivers;
}

static void free_drivers(std::vector<PipelineDriver*>& drivers) {
    for (auto* d : drivers) {
        delete d;
    }
    drivers.clear();
}

static void BM_ThreadArgs(benchmark::internal::Benchmark* b) {
    for (int threads : {1, 2, 4, 8, 16, 32}) {
        b->Arg(threads);
    }
}

// Keep prefill moderate: large enough to prevent empty queue,
// small enough to be fair to old queue's heap operations.
static constexpr int PREFILL_MIN = 4096;
static constexpr int PREFILL_PER_THREAD = 256;
static constexpr int OPS_PER_THREAD = 5000;
static constexpr int FAIL_SIMULATION_INTERVAL = 16;
static constexpr int64_t CPU_COST_SIMULATION_NS = 100000000; // 100ms

static int prefill_count(int num_threads) {
    return std::max(PREFILL_MIN, num_threads * PREFILL_PER_THREAD);
}

// ---------------------------------------------------------------------------
// Scenario 1: Sustained Mixed take+put_back (queue always has data)
//
// Drivers and queue created ONCE before the loop.
// Each iteration only measures the take+put_back hot path.
// ---------------------------------------------------------------------------

static void BM_LockFreeDriverQueue_SustainedMixed(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int fill_count = prefill_count(num_threads);

    auto drivers = make_simple_drivers(fill_count, LockFreeDriverQueue::QUEUE_SIZE);

    LockFreeDriverQueue queue(num_threads);
    for (int i = 0; i < fill_count; ++i) {
        queue.put_back(drivers[i], i % num_threads);
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
                int64_t local_ops_count = 0;
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    DriverRawPtr driver = nullptr;
                    if (queue.try_take(driver, t)) {
                        int64_t cpu_cost = ((i % FAIL_SIMULATION_INTERVAL) == 0) ? CPU_COST_SIMULATION_NS
                                                                                 : (CPU_COST_SIMULATION_NS / 10);
                        driver->driver_acct().update_last_time_spent(cpu_cost);
                        queue.update_statistics(static_cast<int>(driver->get_driver_queue_level()), cpu_cost);
                        queue.put_back(driver, t);
                        ++local_ops_count;
                    } else {
                        ++local_fail_ops[t];
                    }
                }
                local_ops[t] = local_ops_count;
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
    free_drivers(drivers);
}

BENCHMARK(BM_LockFreeDriverQueue_SustainedMixed)->Apply(BM_ThreadArgs)->UseRealTime();

static void BM_QuerySharedDriverQueue_SustainedMixed(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int fill_count = prefill_count(num_threads);

    auto drivers = make_simple_drivers(fill_count, QuerySharedDriverQueue::QUEUE_SIZE);

    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());
    for (int i = 0; i < fill_count; ++i) {
        queue.put_back(drivers[i]);
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
                int64_t local_ops_count = 0;
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    auto result = queue.take(false);
                    if (result.ok() && result.value() != nullptr) {
                        auto* driver = result.value();
                        int64_t cpu_cost = ((i % FAIL_SIMULATION_INTERVAL) == 0) ? CPU_COST_SIMULATION_NS
                                                                                 : (CPU_COST_SIMULATION_NS / 10);
                        driver->driver_acct().update_last_time_spent(cpu_cost);
                        queue.update_statistics(driver);
                        queue.put_back(driver);
                        ++local_ops_count;
                    } else {
                        ++local_fail_ops[t];
                    }
                }
                local_ops[t] = local_ops_count;
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
    free_drivers(drivers);
}

BENCHMARK(BM_QuerySharedDriverQueue_SustainedMixed)->Apply(BM_ThreadArgs)->UseRealTime();

// ---------------------------------------------------------------------------
// Scenario 2: Pure enqueue contention
// ---------------------------------------------------------------------------

static void BM_LockFreeDriverQueue_EnqueueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    std::vector<std::vector<PipelineDriver*>> all_drivers(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        all_drivers[t] = make_simple_drivers(OPS_PER_THREAD, LockFreeDriverQueue::QUEUE_SIZE);
    }

    for (auto _ : state) {
        state.PauseTiming();
        LockFreeDriverQueue queue(num_threads);

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &all_drivers, &start, t]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    auto* driver = all_drivers[t][i];
                    queue.put_back(driver, t);
                }
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();

        state.PauseTiming();
        DriverRawPtr drain = nullptr;
        while (queue.try_take(drain)) {
        }
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * num_threads * OPS_PER_THREAD);
    for (auto& v : all_drivers) free_drivers(v);
}

BENCHMARK(BM_LockFreeDriverQueue_EnqueueOnly)->Apply(BM_ThreadArgs)->UseRealTime();

static void BM_QuerySharedDriverQueue_EnqueueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    std::vector<std::vector<PipelineDriver*>> all_drivers(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        all_drivers[t] = make_simple_drivers(OPS_PER_THREAD, QuerySharedDriverQueue::QUEUE_SIZE);
    }

    PipelineExecutorMetrics metrics;

    for (auto _ : state) {
        state.PauseTiming();
        QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &all_drivers, &start, t]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    queue.put_back(all_drivers[t][i]);
                }
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();

        state.PauseTiming();
        while (true) {
            auto r = queue.take(false);
            if (!r.ok() || r.value() == nullptr) break;
        }
        queue.close();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * num_threads * OPS_PER_THREAD);
    for (auto& v : all_drivers) free_drivers(v);
}

BENCHMARK(BM_QuerySharedDriverQueue_EnqueueOnly)->Apply(BM_ThreadArgs)->UseRealTime();

// ---------------------------------------------------------------------------
// Scenario 3: Pure dequeue contention
//
// Pre-fill the queue, then N threads all compete to dequeue.
// Isolates the try_take / take hot path.
// ---------------------------------------------------------------------------

static void BM_LockFreeDriverQueue_DequeueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int total_ops = num_threads * OPS_PER_THREAD;

    auto drivers = make_simple_drivers(total_ops, LockFreeDriverQueue::QUEUE_SIZE);

    for (auto _ : state) {
        state.PauseTiming();
        LockFreeDriverQueue queue(num_threads);
        for (int i = 0; i < total_ops; ++i) {
            queue.put_back(drivers[i], i % num_threads);
        }

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};
        std::atomic<int64_t> total_dequeued{0};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &start, &total_dequeued, t]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                int64_t local_count = 0;
                DriverRawPtr driver = nullptr;
                while (queue.try_take(driver, t)) {
                    ++local_count;
                }
                total_dequeued.fetch_add(local_count, std::memory_order_relaxed);
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();
    }

    state.SetItemsProcessed(state.iterations() * total_ops);
    free_drivers(drivers);
}

BENCHMARK(BM_LockFreeDriverQueue_DequeueOnly)->Apply(BM_ThreadArgs)->UseRealTime();

static void BM_QuerySharedDriverQueue_DequeueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int total_ops = num_threads * OPS_PER_THREAD;

    auto drivers = make_simple_drivers(total_ops, QuerySharedDriverQueue::QUEUE_SIZE);
    PipelineExecutorMetrics metrics;

    for (auto _ : state) {
        state.PauseTiming();
        QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());
        for (int i = 0; i < total_ops; ++i) {
            queue.put_back(drivers[i]);
        }

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        std::atomic<bool> start{false};
        std::atomic<int64_t> total_dequeued{0};

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &start, &total_dequeued]() {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                int64_t local_count = 0;
                while (true) {
                    auto r = queue.take(false);
                    if (!r.ok() || r.value() == nullptr) break;
                    ++local_count;
                }
                total_dequeued.fetch_add(local_count, std::memory_order_relaxed);
            });
        }
        state.ResumeTiming();
        start.store(true, std::memory_order_release);

        for (auto& th : threads) th.join();

        state.PauseTiming();
        queue.close();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * total_ops);
    free_drivers(drivers);
}

BENCHMARK(BM_QuerySharedDriverQueue_DequeueOnly)->Apply(BM_ThreadArgs)->UseRealTime();

} // namespace starrocks::pipeline

BENCHMARK_MAIN();
