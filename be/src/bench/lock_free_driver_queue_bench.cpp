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

#include "exec/pipeline/lock_free_driver_queue.h"
#include "exec/pipeline/pipeline_driver_queue.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/pipeline_metrics.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {

// ---------------------------------------------------------------------------
// Mock operator and driver helpers
// ---------------------------------------------------------------------------

class BenchMockOp final : public SourceOperator {
public:
    BenchMockOp() : SourceOperator(nullptr, 1, "bench_mock", 1, false, 0) {}
    ~BenchMockOp() override = default;

    bool has_output() const override { return true; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return true; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState*) override { return nullptr; }
    Status push_chunk(RuntimeState*, const ChunkPtr&) override { return Status::OK(); }
};

static Operators bench_gen_ops() {
    Operators ops;
    ops.emplace_back(std::make_shared<BenchMockOp>());
    return ops;
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
// BM_LockFreeDriverQueue_Mixed
//
// N threads concurrently do try_take -> put_back cycles on a pre-filled
// LockFreeDriverQueue. This is the real-world hot path where executor
// threads simultaneously dequeue and re-enqueue drivers.
// ---------------------------------------------------------------------------

static void BM_LockFreeDriverQueue_Mixed(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int items_per_thread = 1000;
    const int total_items = num_threads * items_per_thread;

    // Pre-create drivers outside the benchmark loop.
    QueryContext query_ctx;
    std::vector<DriverPtr> drivers;
    drivers.reserve(total_items);
    for (int i = 0; i < total_items; ++i) {
        auto d = std::make_shared<PipelineDriver>(bench_gen_ops(), &query_ctx, nullptr, nullptr, -1);
        d->set_driver_queue_level(i % LockFreeDriverQueue::QUEUE_SIZE);
        drivers.push_back(std::move(d));
    }

    for (auto _ : state) {
        LockFreeDriverQueue queue(num_threads);

        // Pre-fill the queue with all drivers.
        for (int i = 0; i < total_items; ++i) {
            queue.put_back(drivers[i].get());
        }

        // Launch N threads, each doing mixed take/put_back.
        std::atomic<int> ops_done{0};
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &ops_done, t, items_per_thread]() {
                for (int i = 0; i < items_per_thread; ++i) {
                    DriverRawPtr driver = nullptr;
                    if (queue.try_take(driver)) {
                        queue.put_back(driver, t);
                        ops_done.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        // Drain remaining drivers to leave queue empty.
        DriverRawPtr drain = nullptr;
        while (queue.try_take(drain)) {
        }
    }

    state.SetItemsProcessed(state.iterations() * total_items);
}

BENCHMARK(BM_LockFreeDriverQueue_Mixed)->Apply(BM_ThreadArgs);

// ---------------------------------------------------------------------------
// BM_QuerySharedDriverQueue_Mixed
//
// Same mixed-contention scenario but using the old mutex-based
// QuerySharedDriverQueue.
// ---------------------------------------------------------------------------

static void BM_QuerySharedDriverQueue_Mixed(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int items_per_thread = 1000;
    const int total_items = num_threads * items_per_thread;

    // Pre-create drivers outside the benchmark loop.
    QueryContext query_ctx;
    std::vector<DriverPtr> drivers;
    drivers.reserve(total_items);
    for (int i = 0; i < total_items; ++i) {
        auto d = std::make_shared<PipelineDriver>(bench_gen_ops(), &query_ctx, nullptr, nullptr, -1);
        d->set_driver_queue_level(i % QuerySharedDriverQueue::QUEUE_SIZE);
        drivers.push_back(std::move(d));
    }

    for (auto _ : state) {
        QuerySharedDriverQueue queue(nullptr);

        // Pre-fill the queue with all drivers.
        for (int i = 0; i < total_items; ++i) {
            queue.put_back(drivers[i].get());
        }

        // Launch N threads, each doing mixed take/put_back.
        std::atomic<int> ops_done{0};
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &ops_done, items_per_thread]() {
                for (int i = 0; i < items_per_thread; ++i) {
                    auto result = queue.take(false);
                    if (result.ok() && result.value() != nullptr) {
                        queue.put_back(result.value());
                        ops_done.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        // Drain remaining drivers.
        while (true) {
            auto result = queue.take(false);
            if (!result.ok() || result.value() == nullptr) break;
        }

        queue.close();
    }

    state.SetItemsProcessed(state.iterations() * total_items);
}

BENCHMARK(BM_QuerySharedDriverQueue_Mixed)->Apply(BM_ThreadArgs);

// ---------------------------------------------------------------------------
// BM_LockFreeDriverQueue_EnqueueOnly
//
// N threads each enqueue 1000 drivers concurrently (no dequeue).
// Measures pure enqueue throughput and contention.
// ---------------------------------------------------------------------------

static void BM_LockFreeDriverQueue_EnqueueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int items_per_thread = 1000;
    const int total_items = num_threads * items_per_thread;

    // Pre-create drivers outside the benchmark loop.
    QueryContext query_ctx;
    std::vector<std::vector<DriverPtr>> all_drivers(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        all_drivers[t].reserve(items_per_thread);
        for (int i = 0; i < items_per_thread; ++i) {
            auto d = std::make_shared<PipelineDriver>(bench_gen_ops(), &query_ctx, nullptr, nullptr, -1);
            d->set_driver_queue_level((t * items_per_thread + i) % LockFreeDriverQueue::QUEUE_SIZE);
            all_drivers[t].push_back(std::move(d));
        }
    }

    for (auto _ : state) {
        LockFreeDriverQueue queue(num_threads);

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &all_drivers, t, items_per_thread]() {
                for (int i = 0; i < items_per_thread; ++i) {
                    queue.put_back(all_drivers[t][i].get(), t);
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        // Drain the queue.
        DriverRawPtr drain = nullptr;
        while (queue.try_take(drain)) {
        }
    }

    state.SetItemsProcessed(state.iterations() * total_items);
}

BENCHMARK(BM_LockFreeDriverQueue_EnqueueOnly)->Apply(BM_ThreadArgs);

// ---------------------------------------------------------------------------
// BM_QuerySharedDriverQueue_EnqueueOnly
//
// Same enqueue-only scenario but using the old mutex-based queue.
// ---------------------------------------------------------------------------

static void BM_QuerySharedDriverQueue_EnqueueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));
    const int items_per_thread = 1000;
    const int total_items = num_threads * items_per_thread;

    // Pre-create drivers outside the benchmark loop.
    QueryContext query_ctx;
    std::vector<std::vector<DriverPtr>> all_drivers(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        all_drivers[t].reserve(items_per_thread);
        for (int i = 0; i < items_per_thread; ++i) {
            auto d = std::make_shared<PipelineDriver>(bench_gen_ops(), &query_ctx, nullptr, nullptr, -1);
            d->set_driver_queue_level((t * items_per_thread + i) % QuerySharedDriverQueue::QUEUE_SIZE);
            all_drivers[t].push_back(std::move(d));
        }
    }

    for (auto _ : state) {
        QuerySharedDriverQueue queue(nullptr);

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &all_drivers, t, items_per_thread]() {
                for (int i = 0; i < items_per_thread; ++i) {
                    queue.put_back(all_drivers[t][i].get());
                }
            });
        }

        for (auto& th : threads) {
            th.join();
        }

        // Drain the queue.
        while (true) {
            auto result = queue.take(false);
            if (!result.ok() || result.value() == nullptr) break;
        }

        queue.close();
    }

    state.SetItemsProcessed(state.iterations() * total_items);
}

BENCHMARK(BM_QuerySharedDriverQueue_EnqueueOnly)->Apply(BM_ThreadArgs);

} // namespace starrocks::pipeline

BENCHMARK_MAIN();
