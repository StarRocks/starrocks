// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

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

static void BM_ThreadArgs(benchmark::internal::Benchmark* b) {
    for (int threads : {1, 2, 4, 8, 16, 32, 64}) {
        b->Arg(threads);
    }
}

// Keep prefill moderate: large enough to prevent empty queue,
// small enough to be fair to old queue's heap operations.
static constexpr int PREFILL = 1000;
static constexpr int OPS_PER_THREAD = 5000;

// ---------------------------------------------------------------------------
// Scenario 1: Sustained Mixed take+put_back (queue always has data)
//
// Drivers and queue created ONCE before the loop.
// Each iteration only measures the take+put_back hot path.
// ---------------------------------------------------------------------------

static void BM_LockFreeDriverQueue_SustainedMixed(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    // Pre-create drivers and queue once.
    QueryContext query_ctx;
    std::vector<DriverPtr> drivers;
    drivers.reserve(PREFILL);
    for (int i = 0; i < PREFILL; ++i) {
        auto d = std::make_shared<PipelineDriver>(bench_gen_ops(), &query_ctx, nullptr, nullptr, -1);
        d->set_driver_queue_level(i % LockFreeDriverQueue::QUEUE_SIZE);
        drivers.push_back(std::move(d));
    }

    LockFreeDriverQueue queue(num_threads);
    for (int i = 0; i < PREFILL; ++i) {
        queue.put_back(drivers[i].get(), 0);
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
                    DriverRawPtr driver = nullptr;
                    if (queue.try_take(driver, t)) {
                        queue.put_back(driver, t);
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

BENCHMARK(BM_LockFreeDriverQueue_SustainedMixed)->Apply(BM_ThreadArgs);

static void BM_QuerySharedDriverQueue_SustainedMixed(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    QueryContext query_ctx;
    std::vector<DriverPtr> drivers;
    drivers.reserve(PREFILL);
    for (int i = 0; i < PREFILL; ++i) {
        auto d = std::make_shared<PipelineDriver>(bench_gen_ops(), &query_ctx, nullptr, nullptr, -1);
        d->set_driver_queue_level(i % QuerySharedDriverQueue::QUEUE_SIZE);
        drivers.push_back(std::move(d));
    }

    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());
    for (int i = 0; i < PREFILL; ++i) {
        queue.put_back(drivers[i].get());
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
                    auto result = queue.take(false);
                    if (result.ok() && result.value() != nullptr) {
                        queue.put_back(result.value());
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

BENCHMARK(BM_QuerySharedDriverQueue_SustainedMixed)->Apply(BM_ThreadArgs);

// ---------------------------------------------------------------------------
// Scenario 2: Pure enqueue contention
// ---------------------------------------------------------------------------

static void BM_LockFreeDriverQueue_EnqueueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    QueryContext query_ctx;
    std::vector<std::vector<DriverPtr>> all_drivers(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        all_drivers[t].reserve(OPS_PER_THREAD);
        for (int i = 0; i < OPS_PER_THREAD; ++i) {
            auto d = std::make_shared<PipelineDriver>(bench_gen_ops(), &query_ctx, nullptr, nullptr, -1);
            d->set_driver_queue_level(i % LockFreeDriverQueue::QUEUE_SIZE);
            all_drivers[t].push_back(std::move(d));
        }
    }

    for (auto _ : state) {
        LockFreeDriverQueue queue(num_threads);

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &all_drivers, t]() {
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    queue.put_back(all_drivers[t][i].get(), t);
                }
            });
        }

        for (auto& th : threads) th.join();

        state.PauseTiming();
        DriverRawPtr drain = nullptr;
        while (queue.try_take(drain)) {}
        state.ResumeTiming();
    }

    state.SetItemsProcessed(state.iterations() * num_threads * OPS_PER_THREAD);
}

BENCHMARK(BM_LockFreeDriverQueue_EnqueueOnly)->Apply(BM_ThreadArgs);

static void BM_QuerySharedDriverQueue_EnqueueOnly(benchmark::State& state) {
    const int num_threads = static_cast<int>(state.range(0));

    QueryContext query_ctx;
    std::vector<std::vector<DriverPtr>> all_drivers(num_threads);
    for (int t = 0; t < num_threads; ++t) {
        all_drivers[t].reserve(OPS_PER_THREAD);
        for (int i = 0; i < OPS_PER_THREAD; ++i) {
            auto d = std::make_shared<PipelineDriver>(bench_gen_ops(), &query_ctx, nullptr, nullptr, -1);
            d->set_driver_queue_level(i % QuerySharedDriverQueue::QUEUE_SIZE);
            all_drivers[t].push_back(std::move(d));
        }
    }

    PipelineExecutorMetrics metrics;

    for (auto _ : state) {
        QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&queue, &all_drivers, t]() {
                for (int i = 0; i < OPS_PER_THREAD; ++i) {
                    queue.put_back(all_drivers[t][i].get());
                }
            });
        }

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
}

BENCHMARK(BM_QuerySharedDriverQueue_EnqueueOnly)->Apply(BM_ThreadArgs);

} // namespace starrocks::pipeline

BENCHMARK_MAIN();
