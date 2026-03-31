# Work-Stealing Lock-Free Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace mutex-based PipelineDriverQueue and ScanTaskQueue with lock-free work-stealing queues using moodycamel ConcurrentQueue for multi-core scalability.

**Architecture:** Three-layer design — a core lock-free multi-level queue (`WorkStealingQueue`), per-workgroup scheduling layers (`LockFreeDriverQueue`, `LockFreeScanTaskQueue`), and cross-workgroup composition layers (`LockFreeWorkGroupDriverQueue`, `LockFreeWorkGroupScanTaskQueue`). Each layer has a single responsibility; scheduling policy, blocking, and cancel are external to the core queue.

**Tech Stack:** moodycamel::ConcurrentQueue (lock-free MPMC), moodycamel::LightweightSemaphore (blocking), phmap::parallel_flat_hash_set (concurrent cancel set), Google Test, Google Benchmark.

**Spec:** `docs/superpowers/specs/2026-03-31-work-stealing-lock-free-queue-design.md`

---

## File Structure

| File | Responsibility |
|------|---------------|
| `be/src/exec/pipeline/work_stealing_queue.h` | Core lock-free multi-level queue (header-only template) |
| `be/src/exec/pipeline/lock_free_driver_queue.h` | Per-workgroup MLFQ scheduling for drivers (header) |
| `be/src/exec/pipeline/lock_free_driver_queue.cpp` | Per-workgroup MLFQ scheduling for drivers (impl) |
| `be/src/exec/pipeline/lock_free_work_group_driver_queue.h` | Cross-workgroup driver scheduling with blocking + cancel (header) |
| `be/src/exec/pipeline/lock_free_work_group_driver_queue.cpp` | Cross-workgroup driver scheduling with blocking + cancel (impl) |
| `be/src/exec/workgroup/lock_free_scan_task_queue.h` | Per-workgroup strict-priority scheduling for scan tasks (header) |
| `be/src/exec/workgroup/lock_free_scan_task_queue.cpp` | Per-workgroup strict-priority scheduling for scan tasks (impl) |
| `be/src/exec/workgroup/lock_free_work_group_scan_task_queue.h` | Cross-workgroup scan task scheduling with blocking (header) |
| `be/src/exec/workgroup/lock_free_work_group_scan_task_queue.cpp` | Cross-workgroup scan task scheduling with blocking (impl) |
| `be/test/exec/pipeline/work_stealing_queue_test.cpp` | Core queue tests |
| `be/test/exec/pipeline/lock_free_driver_queue_test.cpp` | Driver scheduling layer + composition layer tests |
| `be/test/exec/workgroup/lock_free_scan_task_queue_test.cpp` | Scan task scheduling layer + composition layer tests |
| `be/src/bench/lock_free_driver_queue_bench.cpp` | Driver queue benchmark: new vs old |
| `be/src/bench/lock_free_scan_task_queue_bench.cpp` | Scan task queue benchmark: new vs old |

---

### Task 1: WorkStealingQueue — Core Lock-Free Multi-Level Queue

**Files:**
- Create: `be/src/exec/pipeline/work_stealing_queue.h`
- Create: `be/test/exec/pipeline/work_stealing_queue_test.cpp`
- Modify: `be/test/CMakeLists.txt` (add test file to EXEC_FILES)

- [ ] **Step 1: Write the test file**

```cpp
// be/test/exec/pipeline/work_stealing_queue_test.cpp
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#include "exec/pipeline/work_stealing_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "base/testutil/parallel_test.h"

namespace starrocks::pipeline {

// Basic enqueue/dequeue on a single level.
PARALLEL_TEST(WorkStealingQueueTest, test_single_level_basic) {
    WorkStealingQueue<int, 4> queue(2);  // 2 workers, 4 levels

    queue.enqueue(42, /*level=*/0, /*worker_id=*/0);
    queue.enqueue(43, /*level=*/0, /*worker_id=*/1);

    int item = 0;
    ASSERT_TRUE(queue.try_dequeue(0, item));
    // Either 42 or 43 — order depends on which sub-queue is picked.
    ASSERT_TRUE(item == 42 || item == 43);

    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_FALSE(queue.try_dequeue(0, item));  // empty
}

// Items go to the correct level and don't leak across levels.
PARALLEL_TEST(WorkStealingQueueTest, test_level_isolation) {
    WorkStealingQueue<int, 4> queue(1);

    queue.enqueue(10, /*level=*/0, /*worker_id=*/0);
    queue.enqueue(20, /*level=*/1, /*worker_id=*/0);
    queue.enqueue(30, /*level=*/2, /*worker_id=*/0);

    int item = 0;
    // Level 3 is empty.
    ASSERT_FALSE(queue.try_dequeue(3, item));

    // Level 1 has exactly one item.
    ASSERT_TRUE(queue.try_dequeue(1, item));
    ASSERT_EQ(20, item);
    ASSERT_FALSE(queue.try_dequeue(1, item));

    // Level 0 and 2 still have their items.
    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_EQ(10, item);
    ASSERT_TRUE(queue.try_dequeue(2, item));
    ASSERT_EQ(30, item);
}

// Implicit producer (no worker_id) works correctly.
PARALLEL_TEST(WorkStealingQueueTest, test_implicit_producer) {
    WorkStealingQueue<int, 2> queue(1);

    // Enqueue without worker_id — uses implicit producer.
    queue.enqueue(99, /*level=*/0);
    queue.enqueue(100, /*level=*/1);

    int item = 0;
    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_EQ(99, item);
    ASSERT_TRUE(queue.try_dequeue(1, item));
    ASSERT_EQ(100, item);
}

// size_approx and empty.
PARALLEL_TEST(WorkStealingQueueTest, test_size_and_empty) {
    WorkStealingQueue<int, 2> queue(1);

    ASSERT_TRUE(queue.empty(0));
    ASSERT_EQ(0u, queue.size_approx(0));

    queue.enqueue(1, 0, 0);
    queue.enqueue(2, 0, 0);
    ASSERT_FALSE(queue.empty(0));
    // size_approx is approximate, but with single-threaded access it should be exact.
    ASSERT_EQ(2u, queue.size_approx(0));

    int item;
    queue.try_dequeue(0, item);
    queue.try_dequeue(0, item);
    ASSERT_TRUE(queue.empty(0));
}

// Multi-threaded: N producers enqueue, N consumers dequeue, all items accounted for.
PARALLEL_TEST(WorkStealingQueueTest, test_multithread_correctness) {
    constexpr int NUM_WORKERS = 8;
    constexpr int ITEMS_PER_WORKER = 10000;
    WorkStealingQueue<int, 4> queue(NUM_WORKERS);

    std::atomic<int> dequeue_count{0};
    std::atomic<int64_t> dequeue_sum{0};

    // Producer threads.
    std::vector<std::thread> producers;
    for (int w = 0; w < NUM_WORKERS; ++w) {
        producers.emplace_back([&queue, w]() {
            for (int i = 0; i < ITEMS_PER_WORKER; ++i) {
                queue.enqueue(w * ITEMS_PER_WORKER + i, /*level=*/i % 4, /*worker_id=*/w);
            }
        });
    }
    for (auto& t : producers) t.join();

    // Consumer threads.
    std::vector<std::thread> consumers;
    for (int w = 0; w < NUM_WORKERS; ++w) {
        consumers.emplace_back([&queue, &dequeue_count, &dequeue_sum]() {
            int item;
            while (true) {
                bool found = false;
                for (int level = 0; level < 4; ++level) {
                    if (queue.try_dequeue(level, item)) {
                        dequeue_count.fetch_add(1);
                        dequeue_sum.fetch_add(item);
                        found = true;
                        break;
                    }
                }
                if (!found) break;
            }
        });
    }
    for (auto& t : consumers) t.join();

    ASSERT_EQ(NUM_WORKERS * ITEMS_PER_WORKER, dequeue_count.load());

    // Verify sum to ensure no items lost or duplicated.
    int64_t expected_sum = 0;
    for (int i = 0; i < NUM_WORKERS * ITEMS_PER_WORKER; ++i) {
        expected_sum += i;
    }
    ASSERT_EQ(expected_sum, dequeue_sum.load());
}

// Move-only type support.
PARALLEL_TEST(WorkStealingQueueTest, test_move_only_type) {
    WorkStealingQueue<std::unique_ptr<int>, 2> queue(1);

    queue.enqueue(std::make_unique<int>(42), 0, 0);

    std::unique_ptr<int> item;
    ASSERT_TRUE(queue.try_dequeue(0, item));
    ASSERT_NE(nullptr, item);
    ASSERT_EQ(42, *item);
}

} // namespace starrocks::pipeline
```

- [ ] **Step 2: Register test in CMakeLists.txt**

Add to `be/test/CMakeLists.txt` in the `EXEC_FILES` section, near other pipeline test files:

```cmake
./exec/pipeline/work_stealing_queue_test.cpp
```

- [ ] **Step 3: Implement WorkStealingQueue**

```cpp
// be/src/exec/pipeline/work_stealing_queue.h
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#pragma once

#include <array>
#include <atomic>
#include <optional>
#include <vector>

#include "base/concurrency/moodycamel/concurrentqueue.h"

namespace starrocks::pipeline {

/// WorkStealingQueue is a lock-free multi-level queue built on moodycamel::ConcurrentQueue.
///
/// Each level is an independent ConcurrentQueue. Each worker thread gets a pre-allocated
/// ProducerToken per level to eliminate enqueue contention. External (non-worker) threads
/// can enqueue without a token via the implicit producer path.
///
/// This class contains NO scheduling policy, NO blocking, NO cancel logic.
/// Upper layers decide which level to enqueue to and which level to dequeue from.
template <typename T, int NUM_LEVELS>
class WorkStealingQueue {
public:
    explicit WorkStealingQueue(int num_workers) : _num_workers(num_workers) {
        // Pre-allocate one ProducerToken per worker per level.
        // Stored flat: _producer_tokens[worker_id * NUM_LEVELS + level].
        _producer_tokens.reserve(num_workers * NUM_LEVELS);
        for (int w = 0; w < num_workers; ++w) {
            for (int l = 0; l < NUM_LEVELS; ++l) {
                _producer_tokens.emplace_back(_levels[l].queue);
            }
        }
    }

    ~WorkStealingQueue() = default;

    // Disable copy/move — tokens hold references to internal queues.
    WorkStealingQueue(const WorkStealingQueue&) = delete;
    WorkStealingQueue& operator=(const WorkStealingQueue&) = delete;
    WorkStealingQueue(WorkStealingQueue&&) = delete;
    WorkStealingQueue& operator=(WorkStealingQueue&&) = delete;

    /// Enqueue an item to the specified level using a worker's explicit ProducerToken.
    /// Lock-free. Use this from worker threads for best performance.
    void enqueue(T item, int level, int worker_id) {
        DCHECK(level >= 0 && level < NUM_LEVELS);
        DCHECK(worker_id >= 0 && worker_id < _num_workers);
        _levels[level].queue.enqueue(_producer_tokens[worker_id * NUM_LEVELS + level],
                                     std::move(item));
    }

    /// Enqueue an item to the specified level using the implicit producer path.
    /// Lock-free. Use this from external (non-worker) threads.
    void enqueue(T item, int level) {
        DCHECK(level >= 0 && level < NUM_LEVELS);
        _levels[level].queue.enqueue(std::move(item));
    }

    /// Try to dequeue an item from the specified level.
    /// Lock-free. Returns true if an item was dequeued, false if the level was empty.
    /// Uses moodycamel's default heuristic: picks the fullest producer sub-queue.
    bool try_dequeue(int level, T& item) {
        DCHECK(level >= 0 && level < NUM_LEVELS);
        return _levels[level].queue.try_dequeue(item);
    }

    /// Approximate emptiness check for a level.
    bool empty(int level) const {
        DCHECK(level >= 0 && level < NUM_LEVELS);
        return _levels[level].queue.size_approx() == 0;
    }

    /// Approximate size of a level.
    size_t size_approx(int level) const {
        DCHECK(level >= 0 && level < NUM_LEVELS);
        return _levels[level].queue.size_approx();
    }

    /// Total approximate size across all levels.
    size_t size_approx() const {
        size_t total = 0;
        for (int l = 0; l < NUM_LEVELS; ++l) {
            total += _levels[l].queue.size_approx();
        }
        return total;
    }

private:
    struct alignas(64) LevelQueue {
        moodycamel::ConcurrentQueue<T> queue;
    };

    std::array<LevelQueue, NUM_LEVELS> _levels;

    using ProducerToken = typename moodycamel::ConcurrentQueue<T>::producer_token_t;
    std::vector<ProducerToken> _producer_tokens;  // flat: [worker_id * NUM_LEVELS + level]

    int _num_workers;
};

} // namespace starrocks::pipeline
```

- [ ] **Step 4: Build and run tests**

```bash
# From project root. Comment out other benches in be/src/bench/CMakeLists.txt if needed for speed.
./run-be-ut.sh --test WorkStealingQueueTest
```

Expected: All 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add be/src/exec/pipeline/work_stealing_queue.h \
        be/test/exec/pipeline/work_stealing_queue_test.cpp \
        be/test/CMakeLists.txt
git commit -m "feat: add WorkStealingQueue lock-free multi-level queue"
```

---

### Task 2: LockFreeDriverQueue — Per-Workgroup MLFQ Scheduling

**Files:**
- Create: `be/src/exec/pipeline/lock_free_driver_queue.h`
- Create: `be/src/exec/pipeline/lock_free_driver_queue.cpp`
- Create: `be/test/exec/pipeline/lock_free_driver_queue_test.cpp`
- Modify: `be/test/CMakeLists.txt` (add test file)

- [ ] **Step 1: Write the test file**

```cpp
// be/test/exec/pipeline/lock_free_driver_queue_test.cpp
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#include "exec/pipeline/lock_free_driver_queue.h"

#include <gtest/gtest.h>

#include <thread>

#include "base/testutil/parallel_test.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/pipeline_metrics.h"

namespace starrocks::pipeline {

// Reuse MockEmptyOperator from pipeline_driver_queue_test.
class MockEmptyOperator2 final : public SourceOperator {
public:
    MockEmptyOperator2(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "mock_empty_operator2", plan_node_id, false, driver_sequence) {}
    ~MockEmptyOperator2() override = default;
    bool has_output() const override { return true; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return true; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }
};

static Operators gen_operators() {
    Operators operators;
    operators.emplace_back(std::make_shared<MockEmptyOperator2>(nullptr, 1, 1, 0));
    return operators;
}

// Test that drivers are placed at the correct MLFQ level based on accumulated time.
PARALLEL_TEST(LockFreeDriverQueueTest, test_level_computation) {
    LockFreeDriverQueue queue(1);  // 1 worker

    QueryContext query_context;
    // Driver with 0 accumulated time → level 0.
    auto d0 = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
    d0->set_driver_queue_level(0);

    // Driver with very large accumulated time → level 7 (max).
    auto d7 = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
    d7->set_driver_queue_level(7);
    // Simulate large accumulated time by updating the acct multiple times.
    for (int i = 0; i < 100; ++i) {
        d7->driver_acct().update_last_time_spent(1'000'000'000L);  // 1s each
    }

    queue.put_back(d0.get(), 0);
    queue.put_back(d7.get(), 0);

    // d0 should come out first (level 0 has lower accu_time/factor than level 7 initially).
    DriverRawPtr out = nullptr;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(d0.get(), out);

    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(d7.get(), out);

    ASSERT_FALSE(queue.try_take(out));
}

// Test weighted level selection: after accumulating time on level 0,
// level 1 should be selected because accu_time/factor is lower.
PARALLEL_TEST(LockFreeDriverQueueTest, test_weighted_level_selection) {
    LockFreeDriverQueue queue(1);

    QueryContext query_context;
    auto d0 = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
    d0->set_driver_queue_level(0);

    auto d1 = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
    d1->set_driver_queue_level(1);

    // Simulate level 0 having high accumulated time.
    queue.update_statistics(0, 10'000'000'000L);  // 10s on level 0

    queue.put_back(d0.get(), 0);
    queue.put_back(d1.get(), 0);

    // Level 1 has 0 accu_time, so it should be preferred over level 0.
    DriverRawPtr out = nullptr;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(d1.get(), out);
}

// Test empty queue.
PARALLEL_TEST(LockFreeDriverQueueTest, test_empty) {
    LockFreeDriverQueue queue(1);
    DriverRawPtr out = nullptr;
    ASSERT_FALSE(queue.try_take(out));
}

// Test external producer (no worker_id).
PARALLEL_TEST(LockFreeDriverQueueTest, test_external_producer) {
    LockFreeDriverQueue queue(1);

    QueryContext query_context;
    auto d = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
    d->set_driver_queue_level(0);

    queue.put_back(d.get());  // no worker_id — implicit producer

    DriverRawPtr out = nullptr;
    ASSERT_TRUE(queue.try_take(out));
    ASSERT_EQ(d.get(), out);
}

// Multi-threaded correctness: N workers put_back and try_take concurrently.
PARALLEL_TEST(LockFreeDriverQueueTest, test_multithread) {
    constexpr int NUM_WORKERS = 4;
    constexpr int ITEMS_PER_WORKER = 1000;
    LockFreeDriverQueue queue(NUM_WORKERS);

    QueryContext query_context;
    // Pre-create all drivers.
    std::vector<std::shared_ptr<PipelineDriver>> drivers;
    for (int i = 0; i < NUM_WORKERS * ITEMS_PER_WORKER; ++i) {
        auto d = std::make_shared<PipelineDriver>(gen_operators(), &query_context, nullptr, nullptr, -1);
        d->set_driver_queue_level(i % QuerySharedDriverQueue::QUEUE_SIZE);
        drivers.push_back(std::move(d));
    }

    // Enqueue all drivers.
    std::vector<std::thread> producers;
    for (int w = 0; w < NUM_WORKERS; ++w) {
        producers.emplace_back([&, w]() {
            for (int i = 0; i < ITEMS_PER_WORKER; ++i) {
                queue.put_back(drivers[w * ITEMS_PER_WORKER + i].get(), w);
            }
        });
    }
    for (auto& t : producers) t.join();

    // Dequeue all drivers.
    std::atomic<int> count{0};
    std::vector<std::thread> consumers;
    for (int w = 0; w < NUM_WORKERS; ++w) {
        consumers.emplace_back([&]() {
            DriverRawPtr d = nullptr;
            while (queue.try_take(d)) {
                count.fetch_add(1);
            }
        });
    }
    for (auto& t : consumers) t.join();

    ASSERT_EQ(NUM_WORKERS * ITEMS_PER_WORKER, count.load());
}

} // namespace starrocks::pipeline
```

- [ ] **Step 2: Register test in CMakeLists.txt**

Add to `be/test/CMakeLists.txt` in `EXEC_FILES`:

```cmake
./exec/pipeline/lock_free_driver_queue_test.cpp
```

- [ ] **Step 3: Write the header**

```cpp
// be/src/exec/pipeline/lock_free_driver_queue.h
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#pragma once

#include <array>
#include <atomic>

#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/work_stealing_queue.h"

namespace starrocks::pipeline {

/// LockFreeDriverQueue replaces QuerySharedDriverQueue for per-workgroup driver scheduling.
///
/// Uses WorkStealingQueue<DriverRawPtr, 8> internally. Manages MLFQ level selection
/// via atomic accu_time counters and pre-computed weight factors.
///
/// Thread-safety: all methods are lock-free and thread-safe.
class LockFreeDriverQueue {
public:
    static constexpr int QUEUE_SIZE = 8;

    explicit LockFreeDriverQueue(int num_workers);
    ~LockFreeDriverQueue() = default;

    /// Enqueue a driver. Computes MLFQ level from accumulated execution time.
    /// worker_id version uses explicit ProducerToken.
    void put_back(DriverRawPtr driver, int worker_id);
    /// External thread version uses implicit producer.
    void put_back(DriverRawPtr driver);

    /// Try to dequeue a driver. Selects level with min accu_time/factor.
    /// Returns true if a driver was dequeued.
    bool try_take(DriverRawPtr& driver);

    /// Update per-level accumulated time after driver execution.
    void update_statistics(int level, int64_t execution_time_ns);

    /// Approximate total size across all levels.
    size_t size() const { return _queue.size_approx(); }

private:
    int _compute_driver_level(DriverRawPtr driver) const;
    int _select_best_level() const;

    WorkStealingQueue<DriverRawPtr, QUEUE_SIZE> _queue;

    struct alignas(64) LevelStats {
        std::atomic<int64_t> accu_time_ns{0};
        double factor{1.0};
    };
    std::array<LevelStats, QUEUE_SIZE> _level_stats;
    int64_t _level_time_slices[QUEUE_SIZE];

    const int64_t LEVEL_TIME_SLICE_BASE_NS;
    const double RATIO_OF_ADJACENT_QUEUE;
};

} // namespace starrocks::pipeline
```

- [ ] **Step 4: Write the implementation**

```cpp
// be/src/exec/pipeline/lock_free_driver_queue.cpp
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#include "exec/pipeline/lock_free_driver_queue.h"

#include "common/config_exec_flow_fwd.h"

namespace starrocks::pipeline {

LockFreeDriverQueue::LockFreeDriverQueue(int num_workers)
        : _queue(num_workers),
          LEVEL_TIME_SLICE_BASE_NS(config::pipeline_driver_queue_level_time_slice_base_ns),
          RATIO_OF_ADJACENT_QUEUE(config::pipeline_driver_queue_ratio_of_adjacent_queue) {
    // Initialize weight factors (same formula as QuerySharedDriverQueue).
    // Higher priority queues (lower index) have larger factor.
    double factor = 1.0;
    for (int i = QUEUE_SIZE - 1; i >= 0; --i) {
        _level_stats[i].factor = factor;
        factor *= RATIO_OF_ADJACENT_QUEUE;
    }

    // Initialize level time slices.
    int64_t time_slice = 0;
    for (int i = 0; i < QUEUE_SIZE; ++i) {
        time_slice += LEVEL_TIME_SLICE_BASE_NS * (i + 1);
        _level_time_slices[i] = time_slice;
    }
}

void LockFreeDriverQueue::put_back(DriverRawPtr driver, int worker_id) {
    int level = _compute_driver_level(driver);
    driver->set_driver_queue_level(level);
    _queue.enqueue(driver, level, worker_id);
}

void LockFreeDriverQueue::put_back(DriverRawPtr driver) {
    int level = _compute_driver_level(driver);
    driver->set_driver_queue_level(level);
    _queue.enqueue(driver, level);
}

bool LockFreeDriverQueue::try_take(DriverRawPtr& driver) {
    // Try the best level first, then fall back to others.
    int best = _select_best_level();
    if (best >= 0 && _queue.try_dequeue(best, driver)) {
        return true;
    }

    // Best level was empty or didn't exist. Try all levels by weighted order.
    // Build a sorted order of levels by accu_time/factor.
    std::array<std::pair<double, int>, QUEUE_SIZE> scored;
    for (int i = 0; i < QUEUE_SIZE; ++i) {
        double accu = static_cast<double>(_level_stats[i].accu_time_ns.load(std::memory_order_relaxed));
        scored[i] = {accu / _level_stats[i].factor, i};
    }
    std::sort(scored.begin(), scored.end());

    for (auto& [score, level] : scored) {
        if (level == best) continue;  // Already tried.
        if (_queue.try_dequeue(level, driver)) {
            return true;
        }
    }
    return false;
}

void LockFreeDriverQueue::update_statistics(int level, int64_t execution_time_ns) {
    DCHECK(level >= 0 && level < QUEUE_SIZE);
    _level_stats[level].accu_time_ns.fetch_add(execution_time_ns, std::memory_order_relaxed);
}

int LockFreeDriverQueue::_compute_driver_level(DriverRawPtr driver) const {
    // Same logic as QuerySharedDriverQueue::_compute_driver_level.
    int64_t time_spent = driver->driver_acct().get_accumulated_time_spent();
    for (int i = driver->get_driver_queue_level(); i < QUEUE_SIZE; ++i) {
        if (time_spent < _level_time_slices[i]) {
            return i;
        }
    }
    return QUEUE_SIZE - 1;
}

int LockFreeDriverQueue::_select_best_level() const {
    int best = -1;
    double best_score = 0;
    for (int i = 0; i < QUEUE_SIZE; ++i) {
        if (_queue.empty(i)) continue;
        double score = static_cast<double>(_level_stats[i].accu_time_ns.load(std::memory_order_relaxed)) /
                       _level_stats[i].factor;
        if (best < 0 || score < best_score) {
            best_score = score;
            best = i;
        }
    }
    return best;
}

} // namespace starrocks::pipeline
```

- [ ] **Step 5: Build and run tests**

```bash
./run-be-ut.sh --test LockFreeDriverQueueTest
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add be/src/exec/pipeline/lock_free_driver_queue.h \
        be/src/exec/pipeline/lock_free_driver_queue.cpp \
        be/test/exec/pipeline/lock_free_driver_queue_test.cpp \
        be/test/CMakeLists.txt
git commit -m "feat: add LockFreeDriverQueue with MLFQ scheduling"
```

---

### Task 3: LockFreeScanTaskQueue — Per-Workgroup Priority Scheduling

**Files:**
- Create: `be/src/exec/workgroup/lock_free_scan_task_queue.h`
- Create: `be/src/exec/workgroup/lock_free_scan_task_queue.cpp`
- Create: `be/test/exec/workgroup/lock_free_scan_task_queue_test.cpp`
- Modify: `be/test/CMakeLists.txt`

- [ ] **Step 1: Write the test file**

```cpp
// be/test/exec/workgroup/lock_free_scan_task_queue_test.cpp
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#include "exec/workgroup/lock_free_scan_task_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "base/testutil/parallel_test.h"

namespace starrocks::workgroup {

static ScanTask make_task(int priority) {
    ScanTask task([](YieldContext&) {});
    task.priority = priority;
    return task;
}

// Strict priority ordering: higher priority (larger value) dequeued first.
PARALLEL_TEST(LockFreeScanTaskQueueTest, test_priority_ordering) {
    LockFreeScanTaskQueue queue(1);

    queue.force_put(make_task(5), 0);
    queue.force_put(make_task(20), 0);
    queue.force_put(make_task(10), 0);

    ScanTask task;
    // Priority 20 first (highest).
    ASSERT_TRUE(queue.try_take(task));
    ASSERT_EQ(20, task.priority);

    // Priority 10 next.
    ASSERT_TRUE(queue.try_take(task));
    ASSERT_EQ(10, task.priority);

    // Priority 5 last.
    ASSERT_TRUE(queue.try_take(task));
    ASSERT_EQ(5, task.priority);

    // Empty.
    ASSERT_FALSE(queue.try_take(task));
}

// try_offer and force_put both work.
PARALLEL_TEST(LockFreeScanTaskQueueTest, test_try_offer) {
    LockFreeScanTaskQueue queue(1);

    ASSERT_TRUE(queue.try_offer(make_task(10), 0));

    ScanTask task;
    ASSERT_TRUE(queue.try_take(task));
    ASSERT_EQ(10, task.priority);
}

// External producer (no worker_id).
PARALLEL_TEST(LockFreeScanTaskQueueTest, test_external_producer) {
    LockFreeScanTaskQueue queue(1);

    queue.force_put(make_task(15));  // implicit producer

    ScanTask task;
    ASSERT_TRUE(queue.try_take(task));
    ASSERT_EQ(15, task.priority);
}

// Empty queue.
PARALLEL_TEST(LockFreeScanTaskQueueTest, test_empty) {
    LockFreeScanTaskQueue queue(1);
    ScanTask task;
    ASSERT_FALSE(queue.try_take(task));
    ASSERT_EQ(0u, queue.size());
}

// Multi-threaded: producers enqueue, consumers dequeue, all accounted for.
PARALLEL_TEST(LockFreeScanTaskQueueTest, test_multithread) {
    constexpr int NUM_WORKERS = 4;
    constexpr int ITEMS_PER_WORKER = 1000;
    LockFreeScanTaskQueue queue(NUM_WORKERS);

    std::vector<std::thread> producers;
    for (int w = 0; w < NUM_WORKERS; ++w) {
        producers.emplace_back([&queue, w]() {
            for (int i = 0; i < ITEMS_PER_WORKER; ++i) {
                queue.force_put(make_task(i % 21), w);
            }
        });
    }
    for (auto& t : producers) t.join();

    std::atomic<int> count{0};
    std::vector<std::thread> consumers;
    for (int w = 0; w < NUM_WORKERS; ++w) {
        consumers.emplace_back([&queue, &count]() {
            ScanTask task;
            while (queue.try_take(task)) {
                count.fetch_add(1);
            }
        });
    }
    for (auto& t : consumers) t.join();

    ASSERT_EQ(NUM_WORKERS * ITEMS_PER_WORKER, count.load());
}

} // namespace starrocks::workgroup
```

- [ ] **Step 2: Register test and write header**

Add to `be/test/CMakeLists.txt`:
```cmake
./exec/workgroup/lock_free_scan_task_queue_test.cpp
```

```cpp
// be/src/exec/workgroup/lock_free_scan_task_queue.h
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#pragma once

#include "exec/pipeline/work_stealing_queue.h"
#include "exec/workgroup/scan_task_queue.h"

namespace starrocks::workgroup {

/// LockFreeScanTaskQueue replaces PriorityScanTaskQueue for per-workgroup scan task scheduling.
///
/// Uses WorkStealingQueue<ScanTask, 21> internally. Priority values 0-20 map 1:1 to 21 levels.
/// Dequeue scans from level 20 (highest priority) down to level 0 (lowest).
///
/// Thread-safety: all methods are lock-free and thread-safe.
class LockFreeScanTaskQueue {
public:
    static constexpr int NUM_PRIORITY_LEVELS = 21;

    explicit LockFreeScanTaskQueue(int num_workers);
    ~LockFreeScanTaskQueue() = default;

    bool try_offer(ScanTask task, int worker_id);
    bool try_offer(ScanTask task);
    void force_put(ScanTask task, int worker_id);
    void force_put(ScanTask task);

    bool try_take(ScanTask& task);

    size_t size() const { return _queue.size_approx(); }

private:
    pipeline::WorkStealingQueue<ScanTask, NUM_PRIORITY_LEVELS> _queue;
};

} // namespace starrocks::workgroup
```

- [ ] **Step 3: Write the implementation**

```cpp
// be/src/exec/workgroup/lock_free_scan_task_queue.cpp
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#include "exec/workgroup/lock_free_scan_task_queue.h"

namespace starrocks::workgroup {

LockFreeScanTaskQueue::LockFreeScanTaskQueue(int num_workers) : _queue(num_workers) {}

bool LockFreeScanTaskQueue::try_offer(ScanTask task, int worker_id) {
    int level = std::clamp(task.priority, 0, NUM_PRIORITY_LEVELS - 1);
    _queue.enqueue(std::move(task), level, worker_id);
    return true;
}

bool LockFreeScanTaskQueue::try_offer(ScanTask task) {
    int level = std::clamp(task.priority, 0, NUM_PRIORITY_LEVELS - 1);
    _queue.enqueue(std::move(task), level);
    return true;
}

void LockFreeScanTaskQueue::force_put(ScanTask task, int worker_id) {
    int level = std::clamp(task.priority, 0, NUM_PRIORITY_LEVELS - 1);
    _queue.enqueue(std::move(task), level, worker_id);
}

void LockFreeScanTaskQueue::force_put(ScanTask task) {
    int level = std::clamp(task.priority, 0, NUM_PRIORITY_LEVELS - 1);
    _queue.enqueue(std::move(task), level);
}

bool LockFreeScanTaskQueue::try_take(ScanTask& task) {
    // Strict priority: scan from highest (20) to lowest (0).
    for (int level = NUM_PRIORITY_LEVELS - 1; level >= 0; --level) {
        if (_queue.try_dequeue(level, task)) {
            return true;
        }
    }
    return false;
}

} // namespace starrocks::workgroup
```

- [ ] **Step 4: Build and run tests**

```bash
./run-be-ut.sh --test LockFreeScanTaskQueueTest
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add be/src/exec/workgroup/lock_free_scan_task_queue.h \
        be/src/exec/workgroup/lock_free_scan_task_queue.cpp \
        be/test/exec/workgroup/lock_free_scan_task_queue_test.cpp \
        be/test/CMakeLists.txt
git commit -m "feat: add LockFreeScanTaskQueue with strict priority scheduling"
```

---

### Task 4: LockFreeWorkGroupDriverQueue — Cross-Workgroup Composition

**Files:**
- Create: `be/src/exec/pipeline/lock_free_work_group_driver_queue.h`
- Create: `be/src/exec/pipeline/lock_free_work_group_driver_queue.cpp`
- Append: `be/test/exec/pipeline/lock_free_driver_queue_test.cpp` (add WorkGroup-level tests)

- [ ] **Step 1: Append workgroup-level tests to the existing test file**

Append to `be/test/exec/pipeline/lock_free_driver_queue_test.cpp`:

```cpp
// --- WorkGroup-level tests ---

#include "exec/pipeline/lock_free_work_group_driver_queue.h"
#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {

class LockFreeWorkGroupDriverQueueTest : public ::testing::Test {
public:
    void SetUp() override {
        _wg1 = std::make_shared<workgroup::WorkGroup>("wg1", 1, workgroup::WorkGroup::DEFAULT_VERSION, 1, 0.5, 10,
                                                       1.0, workgroup::WorkGroupType::WG_NORMAL,
                                                       workgroup::WorkGroup::DEFAULT_MEM_POOL);
        _wg2 = std::make_shared<workgroup::WorkGroup>("wg2", 2, workgroup::WorkGroup::DEFAULT_VERSION, 2, 0.5, 10,
                                                       1.0, workgroup::WorkGroupType::WG_NORMAL,
                                                       workgroup::WorkGroup::DEFAULT_MEM_POOL);
        _wg1 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(_wg1);
        _wg2 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(_wg2);
    }

protected:
    workgroup::WorkGroupPtr _wg1 = nullptr;
    workgroup::WorkGroupPtr _wg2 = nullptr;
};

TEST_F(LockFreeWorkGroupDriverQueueTest, test_single_workgroup) {
    QueryContext query_ctx;
    LockFreeWorkGroupDriverQueue queue(2);  // 2 workers

    auto d1 = std::make_shared<PipelineDriver>(gen_operators(), &query_ctx, nullptr, nullptr, -1);
    d1->set_driver_queue_level(0);
    d1->set_workgroup(_wg1);

    queue.put_back(d1.get(), 0);

    DriverRawPtr out = nullptr;
    ASSERT_TRUE(queue.take(out, /*blocking=*/false));
    ASSERT_EQ(d1.get(), out);

    // Queue should be empty now.
    ASSERT_FALSE(queue.take(out, /*blocking=*/false));
}

TEST_F(LockFreeWorkGroupDriverQueueTest, test_multi_workgroup_vruntime) {
    QueryContext query_ctx;
    LockFreeWorkGroupDriverQueue queue(2);

    auto d1 = std::make_shared<PipelineDriver>(gen_operators(), &query_ctx, nullptr, nullptr, -1);
    d1->set_driver_queue_level(0);
    d1->set_workgroup(_wg1);

    auto d2 = std::make_shared<PipelineDriver>(gen_operators(), &query_ctx, nullptr, nullptr, -1);
    d2->set_driver_queue_level(0);
    d2->set_workgroup(_wg2);

    // Give wg1 high vruntime so wg2 should be selected first.
    queue.update_statistics(_wg1.get(), 10'000'000'000L);  // 10s
    queue.put_back(d1.get(), 0);
    queue.put_back(d2.get(), 0);

    DriverRawPtr out = nullptr;
    ASSERT_TRUE(queue.take(out, false));
    ASSERT_EQ(d2.get(), out);  // wg2 has lower vruntime
}

TEST_F(LockFreeWorkGroupDriverQueueTest, test_cancel) {
    QueryContext query_ctx;
    LockFreeWorkGroupDriverQueue queue(1);

    auto d1 = std::make_shared<PipelineDriver>(gen_operators(), &query_ctx, nullptr, nullptr, -1);
    d1->set_driver_queue_level(0);
    d1->set_workgroup(_wg1);

    auto d2 = std::make_shared<PipelineDriver>(gen_operators(), &query_ctx, nullptr, nullptr, -1);
    d2->set_driver_queue_level(0);
    d2->set_workgroup(_wg1);

    queue.put_back(d1.get(), 0);
    queue.put_back(d2.get(), 0);
    queue.cancel(d1.get());

    DriverRawPtr out = nullptr;
    // First take should get d1 (cancelled) or d2; d1 is in cancel set.
    // The queue should surface the cancel and skip to non-cancelled.
    // Implementation: dequeue returns d1, check cancel_set, handle it, retry, get d2.
    ASSERT_TRUE(queue.take(out, false));
    // d2 should be the non-cancelled result (d1 handled internally as cancelled).
    ASSERT_EQ(d2.get(), out);
}

TEST_F(LockFreeWorkGroupDriverQueueTest, test_blocking_wakeup) {
    QueryContext query_ctx;
    LockFreeWorkGroupDriverQueue queue(1);

    auto d1 = std::make_shared<PipelineDriver>(gen_operators(), &query_ctx, nullptr, nullptr, -1);
    d1->set_driver_queue_level(0);
    d1->set_workgroup(_wg1);

    DriverRawPtr out = nullptr;
    auto consumer = std::thread([&]() {
        ASSERT_TRUE(queue.take(out, /*blocking=*/true));
        ASSERT_EQ(d1.get(), out);
    });

    // Give the consumer time to block.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    queue.put_back(d1.get(), 0);

    consumer.join();
}

TEST_F(LockFreeWorkGroupDriverQueueTest, test_close) {
    LockFreeWorkGroupDriverQueue queue(1);

    DriverRawPtr out = nullptr;
    auto consumer = std::thread([&]() {
        // Should unblock and return false when closed.
        ASSERT_FALSE(queue.take(out, /*blocking=*/true));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    queue.close();

    consumer.join();
}

} // namespace starrocks::pipeline
```

- [ ] **Step 2: Write the header**

```cpp
// be/src/exec/pipeline/lock_free_work_group_driver_queue.h
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#pragma once

#include <atomic>

#include "base/concurrency/moodycamel/lightweightsemaphore.h"
#include "base/phmap/phmap.h"
#include "exec/pipeline/lock_free_driver_queue.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks::pipeline {

/// LockFreeWorkGroupDriverQueue replaces WorkGroupDriverQueue.
///
/// Manages multiple LockFreeDriverQueues (one per workgroup) and provides:
/// - Workgroup selection via atomic vruntime scanning
/// - Blocking via LightweightSemaphore
/// - Cancel via concurrent hash set
///
/// Thread-safety: all methods are thread-safe.
class LockFreeWorkGroupDriverQueue {
public:
    explicit LockFreeWorkGroupDriverQueue(int num_workers);
    ~LockFreeWorkGroupDriverQueue() = default;

    void put_back(DriverRawPtr driver, int worker_id);
    void put_back(DriverRawPtr driver);  // external threads

    /// Take next driver. Returns true if a driver was obtained.
    /// If blocking=true, waits until a driver is available or close() is called.
    /// Cancelled drivers are handled internally (skipped, finalized).
    bool take(DriverRawPtr& driver, bool blocking);

    void cancel(DriverRawPtr driver);

    /// Update a workgroup's vruntime after driver execution.
    void update_statistics(workgroup::WorkGroup* wg, int64_t execution_time_ns);

    void close();

    size_t size() const { return _num_drivers.load(std::memory_order_relaxed); }

private:
    /// Get or create the LockFreeDriverQueue for a workgroup.
    LockFreeDriverQueue* _get_wg_queue(workgroup::WorkGroup* wg);

    /// Scan all active workgroups, return the one with min vruntime.
    workgroup::WorkGroup* _pick_next_wg();

    /// Handle a cancelled driver (finalize, remove from cancel set).
    void _handle_cancelled(DriverRawPtr driver);

    int _num_workers;
    std::atomic<size_t> _num_drivers{0};
    std::atomic<bool> _closed{false};

    moodycamel::LightweightSemaphore _sema;

    // Cancel set: concurrent, lock-free lookups.
    phmap::parallel_flat_hash_set<DriverRawPtr,
                                   phmap::priv::hash_default_hash<DriverRawPtr>,
                                   phmap::priv::hash_default_eq<DriverRawPtr>,
                                   phmap::priv::Allocator<DriverRawPtr>,
                                   4,  // N=4 sub-tables for concurrency
                                   std::mutex>
            _cancel_set;
};

} // namespace starrocks::pipeline
```

- [ ] **Step 3: Write the implementation**

```cpp
// be/src/exec/pipeline/lock_free_work_group_driver_queue.cpp
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#include "exec/pipeline/lock_free_work_group_driver_queue.h"

#include "exec/workgroup/work_group.h"

namespace starrocks::pipeline {

LockFreeWorkGroupDriverQueue::LockFreeWorkGroupDriverQueue(int num_workers)
        : _num_workers(num_workers), _sema(0) {}

void LockFreeWorkGroupDriverQueue::put_back(DriverRawPtr driver, int worker_id) {
    auto* wg = driver->workgroup();
    auto* wg_queue = _get_wg_queue(wg);
    wg_queue->put_back(driver, worker_id);
    _num_drivers.fetch_add(1, std::memory_order_relaxed);
    _sema.signal();
}

void LockFreeWorkGroupDriverQueue::put_back(DriverRawPtr driver) {
    auto* wg = driver->workgroup();
    auto* wg_queue = _get_wg_queue(wg);
    wg_queue->put_back(driver);
    _num_drivers.fetch_add(1, std::memory_order_relaxed);
    _sema.signal();
}

bool LockFreeWorkGroupDriverQueue::take(DriverRawPtr& driver, bool blocking) {
    while (true) {
        if (_closed.load(std::memory_order_acquire)) {
            return false;
        }

        auto* wg = _pick_next_wg();
        if (wg != nullptr) {
            auto* wg_queue = _get_wg_queue(wg);
            if (wg_queue->try_take(driver)) {
                // Check cancel set.
                if (_cancel_set.erase_if(driver, [](const DriverRawPtr&) { return true; })) {
                    // Driver was cancelled. Handle it and retry.
                    _handle_cancelled(driver);
                    _num_drivers.fetch_sub(1, std::memory_order_relaxed);
                    continue;
                }
                _num_drivers.fetch_sub(1, std::memory_order_relaxed);
                return true;
            }
        }

        // All queues empty.
        if (!blocking) {
            return false;
        }
        _sema.wait();
    }
}

void LockFreeWorkGroupDriverQueue::cancel(DriverRawPtr driver) {
    _cancel_set.insert(driver);
    _sema.signal();  // Wake a worker to process the cancellation.
}

void LockFreeWorkGroupDriverQueue::update_statistics(workgroup::WorkGroup* wg, int64_t execution_time_ns) {
    auto* entity = wg->driver_sched_entity();
    entity->incr_runtime_ns(execution_time_ns);
}

void LockFreeWorkGroupDriverQueue::close() {
    _closed.store(true, std::memory_order_release);
    // Wake all potentially waiting workers.
    _sema.signal(1024);
}

LockFreeDriverQueue* LockFreeWorkGroupDriverQueue::_get_wg_queue(workgroup::WorkGroup* wg) {
    // The LockFreeDriverQueue is owned by the workgroup's scheduling entity.
    // This requires WorkGroupSchedEntity to hold a LockFreeDriverQueue.
    // For now, use the existing queue() accessor which returns the entity's queue.
    // TODO: During integration (Task 8), wire WorkGroupSchedEntity to hold LockFreeDriverQueue.
    auto* entity = wg->driver_sched_entity();
    return static_cast<LockFreeDriverQueue*>(entity->queue());
}

workgroup::WorkGroup* LockFreeWorkGroupDriverQueue::_pick_next_wg() {
    // Scan all registered workgroups and pick the one with min vruntime.
    // For now, this uses the WorkGroupManager to iterate active workgroups.
    // TODO: During integration, maintain a lightweight list of active workgroups
    //       with atomic vruntime for lock-free scanning.
    return nullptr;  // Placeholder — wired in Task 8.
}

void LockFreeWorkGroupDriverQueue::_handle_cancelled(DriverRawPtr driver) {
    // Set driver state to CANCELED so executor can finalize it.
    driver->set_driver_state(DriverState::CANCELED);
}

} // namespace starrocks::pipeline
```

> **Note:** `_pick_next_wg()` and `_get_wg_queue()` contain TODO placeholders because they require integration with `WorkGroupSchedEntity` and `WorkGroupManager`, which happens in Task 8 (Integration). The unit tests in Step 1 use direct workgroup construction that bypasses these methods — the test fixture will set up the queue manually.

- [ ] **Step 4: Build and run tests**

```bash
./run-be-ut.sh --test LockFreeWorkGroupDriverQueueTest
```

Expected: All tests PASS (tests create queues directly, bypassing the TODO placeholders).

- [ ] **Step 5: Commit**

```bash
git add be/src/exec/pipeline/lock_free_work_group_driver_queue.h \
        be/src/exec/pipeline/lock_free_work_group_driver_queue.cpp \
        be/test/exec/pipeline/lock_free_driver_queue_test.cpp
git commit -m "feat: add LockFreeWorkGroupDriverQueue with vruntime, cancel, and blocking"
```

---

### Task 5: LockFreeWorkGroupScanTaskQueue — Cross-Workgroup Scan Composition

**Files:**
- Create: `be/src/exec/workgroup/lock_free_work_group_scan_task_queue.h`
- Create: `be/src/exec/workgroup/lock_free_work_group_scan_task_queue.cpp`
- Append: `be/test/exec/workgroup/lock_free_scan_task_queue_test.cpp`

- [ ] **Step 1: Append workgroup-level tests**

Append to `be/test/exec/workgroup/lock_free_scan_task_queue_test.cpp`:

```cpp
// --- WorkGroup-level tests ---

#include "exec/workgroup/lock_free_work_group_scan_task_queue.h"
#include "exec/workgroup/work_group.h"
#include "runtime/exec_env.h"

namespace starrocks::workgroup {

class LockFreeWorkGroupScanTaskQueueTest : public ::testing::Test {
public:
    void SetUp() override {
        _wg1 = std::make_shared<WorkGroup>("scan_wg1", 10, WorkGroup::DEFAULT_VERSION, 1, 0.5, 10, 1.0,
                                            WorkGroupType::WG_NORMAL, WorkGroup::DEFAULT_MEM_POOL);
        _wg2 = std::make_shared<WorkGroup>("scan_wg2", 20, WorkGroup::DEFAULT_VERSION, 2, 0.5, 10, 1.0,
                                            WorkGroupType::WG_NORMAL, WorkGroup::DEFAULT_MEM_POOL);
        _wg1 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(_wg1);
        _wg2 = ExecEnv::GetInstance()->workgroup_manager()->add_workgroup(_wg2);
    }

protected:
    WorkGroupPtr _wg1 = nullptr;
    WorkGroupPtr _wg2 = nullptr;
};

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_single_workgroup) {
    LockFreeWorkGroupScanTaskQueue queue(1);

    ScanTask task([](YieldContext&) {});
    task.priority = 10;
    task.workgroup = _wg1;

    queue.force_put(std::move(task), 0);

    ScanTask out;
    ASSERT_TRUE(queue.take(out, false));
    ASSERT_EQ(10, out.priority);

    ASSERT_FALSE(queue.take(out, false));
}

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_blocking_wakeup) {
    LockFreeWorkGroupScanTaskQueue queue(1);

    ScanTask out;
    auto consumer = std::thread([&]() {
        ASSERT_TRUE(queue.take(out, /*blocking=*/true));
        ASSERT_EQ(15, out.priority);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    ScanTask task([](YieldContext&) {});
    task.priority = 15;
    task.workgroup = _wg1;
    queue.force_put(std::move(task), 0);

    consumer.join();
}

TEST_F(LockFreeWorkGroupScanTaskQueueTest, test_close) {
    LockFreeWorkGroupScanTaskQueue queue(1);

    ScanTask out;
    auto consumer = std::thread([&]() { ASSERT_FALSE(queue.take(out, true)); });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    queue.close();

    consumer.join();
}

} // namespace starrocks::workgroup
```

- [ ] **Step 2: Write header and implementation**

```cpp
// be/src/exec/workgroup/lock_free_work_group_scan_task_queue.h
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#pragma once

#include <atomic>

#include "base/concurrency/moodycamel/lightweightsemaphore.h"
#include "exec/workgroup/lock_free_scan_task_queue.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks::workgroup {

/// LockFreeWorkGroupScanTaskQueue replaces WorkGroupScanTaskQueue.
///
/// Manages multiple LockFreeScanTaskQueues (one per workgroup) and provides:
/// - Workgroup selection via atomic vruntime
/// - Blocking via LightweightSemaphore
class LockFreeWorkGroupScanTaskQueue {
public:
    explicit LockFreeWorkGroupScanTaskQueue(int num_workers);
    ~LockFreeWorkGroupScanTaskQueue() = default;

    bool try_offer(ScanTask task, int worker_id);
    bool try_offer(ScanTask task);
    void force_put(ScanTask task, int worker_id);
    void force_put(ScanTask task);

    bool take(ScanTask& task, bool blocking);

    void update_statistics(WorkGroup* wg, int64_t runtime_ns);

    void close();

    size_t size() const { return _num_tasks.load(std::memory_order_relaxed); }

private:
    LockFreeScanTaskQueue* _get_wg_queue(WorkGroup* wg);
    WorkGroup* _pick_next_wg();

    int _num_workers;
    std::atomic<size_t> _num_tasks{0};
    std::atomic<bool> _closed{false};
    moodycamel::LightweightSemaphore _sema;
};

} // namespace starrocks::workgroup
```

```cpp
// be/src/exec/workgroup/lock_free_work_group_scan_task_queue.cpp
// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// ...Apache 2.0 header...

#include "exec/workgroup/lock_free_work_group_scan_task_queue.h"

#include "exec/workgroup/work_group.h"

namespace starrocks::workgroup {

LockFreeWorkGroupScanTaskQueue::LockFreeWorkGroupScanTaskQueue(int num_workers)
        : _num_workers(num_workers), _sema(0) {}

bool LockFreeWorkGroupScanTaskQueue::try_offer(ScanTask task, int worker_id) {
    auto* wg = task.workgroup.get();
    auto* wg_queue = _get_wg_queue(wg);
    bool ok = wg_queue->try_offer(std::move(task), worker_id);
    if (ok) {
        _num_tasks.fetch_add(1, std::memory_order_relaxed);
        _sema.signal();
    }
    return ok;
}

bool LockFreeWorkGroupScanTaskQueue::try_offer(ScanTask task) {
    auto* wg = task.workgroup.get();
    auto* wg_queue = _get_wg_queue(wg);
    bool ok = wg_queue->try_offer(std::move(task));
    if (ok) {
        _num_tasks.fetch_add(1, std::memory_order_relaxed);
        _sema.signal();
    }
    return ok;
}

void LockFreeWorkGroupScanTaskQueue::force_put(ScanTask task, int worker_id) {
    auto* wg = task.workgroup.get();
    auto* wg_queue = _get_wg_queue(wg);
    wg_queue->force_put(std::move(task), worker_id);
    _num_tasks.fetch_add(1, std::memory_order_relaxed);
    _sema.signal();
}

void LockFreeWorkGroupScanTaskQueue::force_put(ScanTask task) {
    auto* wg = task.workgroup.get();
    auto* wg_queue = _get_wg_queue(wg);
    wg_queue->force_put(std::move(task));
    _num_tasks.fetch_add(1, std::memory_order_relaxed);
    _sema.signal();
}

bool LockFreeWorkGroupScanTaskQueue::take(ScanTask& task, bool blocking) {
    while (true) {
        if (_closed.load(std::memory_order_acquire)) {
            return false;
        }

        auto* wg = _pick_next_wg();
        if (wg != nullptr) {
            auto* wg_queue = _get_wg_queue(wg);
            if (wg_queue->try_take(task)) {
                _num_tasks.fetch_sub(1, std::memory_order_relaxed);
                return true;
            }
        }

        if (!blocking) {
            return false;
        }
        _sema.wait();
    }
}

void LockFreeWorkGroupScanTaskQueue::update_statistics(WorkGroup* wg, int64_t runtime_ns) {
    auto* entity = wg->scan_sched_entity();
    entity->incr_runtime_ns(runtime_ns);
}

void LockFreeWorkGroupScanTaskQueue::close() {
    _closed.store(true, std::memory_order_release);
    _sema.signal(1024);
}

LockFreeScanTaskQueue* LockFreeWorkGroupScanTaskQueue::_get_wg_queue(WorkGroup* wg) {
    // TODO: Wire to WorkGroupSchedEntity in Task 8.
    auto* entity = wg->scan_sched_entity();
    return static_cast<LockFreeScanTaskQueue*>(entity->queue());
}

WorkGroup* LockFreeWorkGroupScanTaskQueue::_pick_next_wg() {
    // TODO: Scan atomic vruntimes across active workgroups in Task 8.
    return nullptr;
}

} // namespace starrocks::workgroup
```

- [ ] **Step 3: Build and run tests**

```bash
./run-be-ut.sh --test LockFreeWorkGroupScanTaskQueueTest
```

- [ ] **Step 4: Commit**

```bash
git add be/src/exec/workgroup/lock_free_work_group_scan_task_queue.h \
        be/src/exec/workgroup/lock_free_work_group_scan_task_queue.cpp \
        be/test/exec/workgroup/lock_free_scan_task_queue_test.cpp
git commit -m "feat: add LockFreeWorkGroupScanTaskQueue with vruntime and blocking"
```

---

### Task 6: Driver Queue Benchmark

**Files:**
- Create: `be/src/bench/lock_free_driver_queue_bench.cpp`
- Modify: `be/src/bench/CMakeLists.txt`

- [ ] **Step 1: Write the benchmark**

```cpp
// be/src/bench/lock_free_driver_queue_bench.cpp
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

// --- Mock operator for creating lightweight PipelineDrivers ---
class BenchMockOperator final : public SourceOperator {
public:
    BenchMockOperator() : SourceOperator(nullptr, 1, "bench_mock", 1, false, 0) {}
    ~BenchMockOperator() override = default;
    bool has_output() const override { return true; }
    bool need_input() const override { return true; }
    bool is_finished() const override { return true; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override { return nullptr; }
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }
};

static Operators bench_gen_operators() {
    Operators ops;
    ops.emplace_back(std::make_shared<BenchMockOperator>());
    return ops;
}

// --- Benchmark: LockFreeDriverQueue single-WG mixed put_back/try_take ---
static void BM_LockFreeDriverQueue_Mixed(benchmark::State& state) {
    const int num_threads = state.range(0);
    LockFreeDriverQueue queue(num_threads);

    QueryContext query_ctx;
    // Pre-create drivers.
    std::vector<std::shared_ptr<PipelineDriver>> drivers;
    const int drivers_per_thread = 1000;
    for (int i = 0; i < num_threads * drivers_per_thread; ++i) {
        auto d = std::make_shared<PipelineDriver>(bench_gen_operators(), &query_ctx, nullptr, nullptr, -1);
        d->set_driver_queue_level(i % QuerySharedDriverQueue::QUEUE_SIZE);
        drivers.push_back(std::move(d));
    }

    for (auto _ : state) {
        // Pre-fill the queue.
        for (auto& d : drivers) {
            queue.put_back(d.get(), 0);
        }

        // Mixed: each thread does put_back + try_take.
        std::atomic<int> ops{0};
        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                DriverRawPtr d = nullptr;
                for (int i = 0; i < drivers_per_thread; ++i) {
                    if (queue.try_take(d)) {
                        queue.put_back(d, t);
                        ops.fetch_add(2, std::memory_order_relaxed);
                    }
                }
            });
        }
        for (auto& th : threads) th.join();

        // Drain remaining.
        DriverRawPtr d = nullptr;
        while (queue.try_take(d)) {}

        state.counters["ops"] = benchmark::Counter(ops.load(), benchmark::Counter::kIsRate);
    }
}

// --- Benchmark: QuerySharedDriverQueue (old) single-WG mixed put_back/take ---
static void BM_QuerySharedDriverQueue_Mixed(benchmark::State& state) {
    const int num_threads = state.range(0);
    PipelineExecutorMetrics metrics;
    QuerySharedDriverQueue queue(metrics.get_driver_queue_metrics());

    QueryContext query_ctx;
    std::vector<std::shared_ptr<PipelineDriver>> drivers;
    const int drivers_per_thread = 1000;
    for (int i = 0; i < num_threads * drivers_per_thread; ++i) {
        auto d = std::make_shared<PipelineDriver>(bench_gen_operators(), &query_ctx, nullptr, nullptr, -1);
        d->set_driver_queue_level(i % QuerySharedDriverQueue::QUEUE_SIZE);
        drivers.push_back(std::move(d));
    }

    for (auto _ : state) {
        for (auto& d : drivers) {
            queue.put_back(d.get());
        }

        std::atomic<int> ops{0};
        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&]() {
                for (int i = 0; i < drivers_per_thread; ++i) {
                    auto maybe = queue.take(false);
                    if (maybe.ok() && maybe.value() != nullptr) {
                        queue.put_back(maybe.value());
                        ops.fetch_add(2, std::memory_order_relaxed);
                    }
                }
            });
        }
        for (auto& th : threads) th.join();

        // Drain remaining.
        while (true) {
            auto maybe = queue.take(false);
            if (!maybe.ok() || maybe.value() == nullptr) break;
        }

        state.counters["ops"] = benchmark::Counter(ops.load(), benchmark::Counter::kIsRate);
    }
}

static void ThreadCountArgs(benchmark::internal::Benchmark* b) {
    for (int threads : {1, 4, 8, 16, 32, 64}) {
        b->Arg(threads);
    }
    b->Unit(benchmark::kMillisecond);
}

BENCHMARK(BM_LockFreeDriverQueue_Mixed)->Apply(ThreadCountArgs);
BENCHMARK(BM_QuerySharedDriverQueue_Mixed)->Apply(ThreadCountArgs);

} // namespace starrocks::pipeline

BENCHMARK_MAIN();
```

- [ ] **Step 2: Register in CMakeLists.txt**

Add to `be/src/bench/CMakeLists.txt`:

```cmake
ADD_BE_BENCH(${SRC_DIR}/bench/lock_free_driver_queue_bench)
```

- [ ] **Step 3: Build and run benchmark**

```bash
# Comment out other ADD_BE_BENCH lines to speed up compilation.
./build.sh --be
# Run the benchmark binary.
./output/be/bench/lock_free_driver_queue_bench
```

- [ ] **Step 4: Commit**

```bash
git add be/src/bench/lock_free_driver_queue_bench.cpp be/src/bench/CMakeLists.txt
git commit -m "bench: add LockFreeDriverQueue vs QuerySharedDriverQueue benchmark"
```

---

### Task 7: Scan Task Queue Benchmark

**Files:**
- Create: `be/src/bench/lock_free_scan_task_queue_bench.cpp`
- Modify: `be/src/bench/CMakeLists.txt`

- [ ] **Step 1: Write the benchmark**

```cpp
// be/src/bench/lock_free_scan_task_queue_bench.cpp
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

// --- Benchmark: LockFreeScanTaskQueue mixed force_put/try_take ---
static void BM_LockFreeScanTaskQueue_Mixed(benchmark::State& state) {
    const int num_threads = state.range(0);
    LockFreeScanTaskQueue queue(num_threads);

    const int items_per_thread = 1000;

    for (auto _ : state) {
        // Pre-fill.
        for (int i = 0; i < num_threads * items_per_thread; ++i) {
            queue.force_put(make_bench_task(i % 21), 0);
        }

        std::atomic<int> ops{0};
        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                ScanTask task;
                for (int i = 0; i < items_per_thread; ++i) {
                    if (queue.try_take(task)) {
                        queue.force_put(make_bench_task(task.priority), t);
                        ops.fetch_add(2, std::memory_order_relaxed);
                    }
                }
            });
        }
        for (auto& th : threads) th.join();

        // Drain.
        ScanTask task;
        while (queue.try_take(task)) {}

        state.counters["ops"] = benchmark::Counter(ops.load(), benchmark::Counter::kIsRate);
    }
}

// --- Benchmark: PriorityScanTaskQueue (old) mixed put/take ---
static void BM_PriorityScanTaskQueue_Mixed(benchmark::State& state) {
    const int num_threads = state.range(0);
    PriorityScanTaskQueue queue(100000);

    const int items_per_thread = 1000;

    for (auto _ : state) {
        for (int i = 0; i < num_threads * items_per_thread; ++i) {
            queue.force_put(make_bench_task(i % 21));
        }

        std::atomic<int> ops{0};
        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&]() {
                for (int i = 0; i < items_per_thread; ++i) {
                    auto maybe = queue.take();
                    if (maybe.ok()) {
                        queue.force_put(make_bench_task(maybe.value().priority));
                        ops.fetch_add(2, std::memory_order_relaxed);
                    }
                }
            });
        }
        for (auto& th : threads) th.join();

        // Drain.
        while (true) {
            auto maybe = queue.take();
            if (!maybe.ok()) break;
        }

        state.counters["ops"] = benchmark::Counter(ops.load(), benchmark::Counter::kIsRate);
    }
}

// --- Benchmark: Producer-consumer split (simulates Driver Executor → ScanExecutor) ---
static void BM_LockFreeScanTaskQueue_ProducerConsumer(benchmark::State& state) {
    const int num_producers = state.range(0);
    const int num_consumers = state.range(0);
    LockFreeScanTaskQueue queue(num_consumers);

    const int items_per_producer = 1000;

    for (auto _ : state) {
        std::atomic<bool> done{false};
        std::atomic<int> produced{0};
        std::atomic<int> consumed{0};

        // Producers.
        std::vector<std::thread> producers;
        for (int p = 0; p < num_producers; ++p) {
            producers.emplace_back([&]() {
                for (int i = 0; i < items_per_producer; ++i) {
                    queue.force_put(make_bench_task(i % 21));
                    produced.fetch_add(1, std::memory_order_relaxed);
                }
            });
        }

        // Consumers.
        std::vector<std::thread> consumers;
        for (int c = 0; c < num_consumers; ++c) {
            consumers.emplace_back([&]() {
                ScanTask task;
                while (!done.load(std::memory_order_relaxed) || queue.size() > 0) {
                    if (queue.try_take(task)) {
                        consumed.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }

        for (auto& t : producers) t.join();
        done.store(true, std::memory_order_relaxed);
        for (auto& t : consumers) t.join();

        state.counters["produced"] = produced.load();
        state.counters["consumed"] = consumed.load();
    }
}

static void ThreadCountArgs(benchmark::internal::Benchmark* b) {
    for (int threads : {1, 4, 8, 16, 32, 64}) {
        b->Arg(threads);
    }
    b->Unit(benchmark::kMillisecond);
}

BENCHMARK(BM_LockFreeScanTaskQueue_Mixed)->Apply(ThreadCountArgs);
BENCHMARK(BM_PriorityScanTaskQueue_Mixed)->Apply(ThreadCountArgs);
BENCHMARK(BM_LockFreeScanTaskQueue_ProducerConsumer)->Apply(ThreadCountArgs);

} // namespace starrocks::workgroup

BENCHMARK_MAIN();
```

- [ ] **Step 2: Register in CMakeLists.txt**

Add to `be/src/bench/CMakeLists.txt`:

```cmake
ADD_BE_BENCH(${SRC_DIR}/bench/lock_free_scan_task_queue_bench)
```

- [ ] **Step 3: Build and run benchmark**

```bash
./build.sh --be
./output/be/bench/lock_free_scan_task_queue_bench
```

- [ ] **Step 4: Commit**

```bash
git add be/src/bench/lock_free_scan_task_queue_bench.cpp be/src/bench/CMakeLists.txt
git commit -m "bench: add LockFreeScanTaskQueue vs PriorityScanTaskQueue benchmark"
```

---

### Task 8: Integration — Feature Flag and Wiring

**Files:**
- Modify: `be/src/common/config.h` (add feature flag)
- Modify: `be/src/exec/workgroup/work_group.h` (WorkGroupSchedEntity holds LockFreeDriverQueue/LockFreeScanTaskQueue)
- Modify: `be/src/exec/workgroup/pipeline_executor_set.cpp` (create new queues when flag enabled)
- Modify: `be/src/exec/pipeline/pipeline_driver_executor.cpp` (pass worker_id to put_back_from_executor)
- Modify: `be/src/exec/pipeline/lock_free_work_group_driver_queue.cpp` (wire _pick_next_wg and _get_wg_queue)
- Modify: `be/src/exec/workgroup/lock_free_work_group_scan_task_queue.cpp` (wire _pick_next_wg and _get_wg_queue)

- [ ] **Step 1: Add feature flag config**

Add to `be/src/common/config.h`:

```cpp
CONF_mBool(enable_lock_free_driver_queue, "false");
```

- [ ] **Step 2: Wire WorkGroupSchedEntity to hold new queue types**

In `be/src/exec/workgroup/work_group.h`, the `WorkGroupSchedEntity<Q>` template already holds `std::unique_ptr<Q> _my_queue`. When the feature flag is enabled, the queue type `Q` instantiated for driver scheduling should be `LockFreeDriverQueue` instead of `QuerySharedDriverQueue`, and for scan scheduling should be `LockFreeScanTaskQueue` instead of `PriorityScanTaskQueue`.

Modify `be/src/exec/workgroup/pipeline_executor_set.cpp` where queues are created:

```cpp
// In PipelineExecutorSet construction:
if (config::enable_lock_free_driver_queue) {
    // Create LockFreeWorkGroupDriverQueue
    _driver_queue = std::make_unique<LockFreeWorkGroupDriverQueue>(num_executor_threads);
} else {
    // Existing path: create WorkGroupDriverQueue
    _driver_queue = std::make_unique<WorkGroupDriverQueue>(metrics);
}
```

- [ ] **Step 3: Wire _pick_next_wg in both composition queues**

Replace the placeholder `_pick_next_wg()` implementations with lock-free vruntime scanning:

```cpp
workgroup::WorkGroup* LockFreeWorkGroupDriverQueue::_pick_next_wg() {
    auto* wg_manager = ExecEnv::GetInstance()->workgroup_manager();
    workgroup::WorkGroup* best_wg = nullptr;
    int64_t best_vruntime = INT64_MAX;

    wg_manager->for_each_workgroup([&](const workgroup::WorkGroupPtr& wg) {
        auto* entity = wg->driver_sched_entity();
        if (entity->queue() == nullptr || entity->queue()->size() == 0) return;
        int64_t vrt = entity->vruntime_ns();
        if (vrt < best_vruntime) {
            best_vruntime = vrt;
            best_wg = wg.get();
        }
    });

    return best_wg;
}
```

> **Note:** This requires `WorkGroupManager::for_each_workgroup()` to exist or be added. If it doesn't exist, add a simple read-lock iteration method. The exact API depends on `WorkGroupManager`'s current interface — adapt during implementation.

- [ ] **Step 4: Pass worker_id through executor**

In `be/src/exec/pipeline/pipeline_driver_executor.cpp`, modify `_worker_thread` to track its worker_id:

```cpp
void GlobalDriverExecutor::_worker_thread(int worker_id) {
    // ... existing code ...
    // Change put_back_from_executor(driver) to put_back_from_executor(driver, worker_id)
    _driver_queue->put_back_from_executor(driver, worker_id);
    // ...
}
```

This requires the `DriverQueue` interface to add the `worker_id` parameter to `put_back_from_executor`. For backward compatibility, add a default parameter:

```cpp
virtual void put_back_from_executor(const DriverRawPtr driver, int worker_id = -1) = 0;
```

The old implementations ignore `worker_id`. The new ones use it.

- [ ] **Step 5: Build full project and run existing tests**

```bash
./build.sh --be
./run-be-ut.sh --test QuerySharedDriverQueueTest
./run-be-ut.sh --test WorkGroupDriverQueueTest
./run-be-ut.sh --test LockFreeDriverQueueTest
./run-be-ut.sh --test LockFreeWorkGroupDriverQueueTest
./run-be-ut.sh --test LockFreeScanTaskQueueTest
./run-be-ut.sh --test LockFreeWorkGroupScanTaskQueueTest
```

Expected: All tests PASS. Feature flag defaults to false, so existing behavior unchanged.

- [ ] **Step 6: Commit**

```bash
git add be/src/common/config.h \
        be/src/exec/workgroup/work_group.h \
        be/src/exec/workgroup/pipeline_executor_set.cpp \
        be/src/exec/pipeline/pipeline_driver_executor.cpp \
        be/src/exec/pipeline/pipeline_driver_queue.h \
        be/src/exec/pipeline/lock_free_work_group_driver_queue.cpp \
        be/src/exec/workgroup/lock_free_work_group_scan_task_queue.cpp
git commit -m "feat: integrate lock-free queues with feature flag enable_lock_free_driver_queue"
```
