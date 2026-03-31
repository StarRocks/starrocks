# Work-Stealing Lock-Free Queue Design

## Background

PipelineDriverQueue and ScanTaskQueue are responsible for task scheduling in StarRocks' query execution engine. Both rely on a single coarse-grained mutex (`_global_mutex`) protecting all state, causing scalability bottlenecks under high core counts (64+). This design replaces them with a lock-free, work-stealing queue architecture based on moodycamel's `ConcurrentQueue`.

### Current Pain Points

- **QuerySharedDriverQueue**: Single `_global_mutex` on all put_back/take operations. 8-level MLFQ with high contention.
- **WorkGroupDriverQueue**: Single `_global_mutex` for take/update_statistics. Already uses moodycamel for batching put_back, but still serializes consumers.
- **PriorityScanTaskQueue**: Mutex-protected `BlockingPriorityQueue<ScanTask>`.
- **WorkGroupScanTaskQueue**: Single `_global_mutex` on all operations.

### Design Goals

- Eliminate global mutex from the hot path (enqueue/dequeue)
- Scale linearly with core count
- Preserve scheduling semantics with relaxed fairness (approximate MLFQ, eventual vruntime consistency)
- Shared abstraction for both PipelineDriverQueue and ScanTaskQueue

## Architecture Overview

Three-layer design with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────────┐
│  Composition Layer (组合层)                                      │
│  LockFreeWorkGroupDriverQueue / LockFreeWorkGroupScanTaskQueue  │
│  - Workgroup selection (atomic vruntime)                        │
│  - Blocking (LightweightSemaphore)                              │
│  - Cancel (ConcurrentHashSet)                                   │
│  - Owns one scheduling-layer queue per workgroup                │
├─────────────────────────────────────────────────────────────────┤
│  Scheduling Layer (调度层)                                       │
│  LockFreeDriverQueue / LockFreeScanTaskQueue                    │
│  - Level selection policy (accu_time/factor or strict priority) │
│  - Per-level statistics (atomic counters)                       │
│  - One instance per workgroup                                   │
├─────────────────────────────────────────────────────────────────┤
│  Core Layer (核心层)                                             │
│  WorkStealingQueue<T, NUM_LEVELS>                               │
│  - Multi-level lock-free queue                                  │
│  - Per-worker ProducerTokens for enqueue                        │
│  - Default dequeue behavior (load-balanced, picks fullest)      │
│  - No scheduling policy, no blocking, no cancel                 │
└─────────────────────────────────────────────────────────────────┘
```

## Layer 1: WorkStealingQueue (Core)

A pure lock-free multi-level queue. Each level is a `moodycamel::ConcurrentQueue`. No scheduling policy, no blocking, no cancel, no workgroup awareness.

### Interface

```cpp
template <typename T, int NUM_LEVELS>
class WorkStealingQueue {
public:
    explicit WorkStealingQueue(int num_workers);

    // Worker threads: use explicit ProducerToken (high-frequency, low contention)
    void enqueue(T item, int level, int worker_id);

    // External threads: use implicit producer (low-frequency, auto thread-pool sized)
    void enqueue(T item, int level);

    // Dequeue from specified level. Returns true if item was dequeued.
    // Uses moodycamel default behavior: picks the fullest producer sub-queue.
    bool try_dequeue(int level, T& item);

    // Approximate emptiness check
    bool empty(int level) const;
    size_t size_approx(int level) const;

    void close();

private:
    struct alignas(64) LevelQueue {
        moodycamel::ConcurrentQueue<T> queue;
    };

    std::array<LevelQueue, NUM_LEVELS> _levels;

    // tokens[worker_id][level] - pre-allocated at construction
    std::vector<std::array<moodycamel::ProducerToken, NUM_LEVELS>> _producer_tokens;

    std::atomic<bool> _closed{false};
};
```

### Key Design Decisions

**Per-worker ProducerToken**: Each worker thread gets a pre-allocated `ProducerToken` for every level. This creates per-worker sub-queues inside the ConcurrentQueue, eliminating enqueue contention between workers.

**Implicit producer for external threads**: External producers (PipelineDriverPoller, brpc handler threads, bthread TimerThread, Driver Executor threads for ScanTaskQueue) use the no-token `enqueue()` overload. moodycamel auto-creates an implicit producer per thread. This is safe because all external producers come from bounded thread pools.

**Load-balanced dequeue**: `try_dequeue()` uses moodycamel's default tokenless dequeue, which heuristically picks the fullest producer sub-queue. This provides implicit work-stealing / load-balancing without local-first affinity.

**Cache-line alignment**: Each `LevelQueue` is aligned to 64 bytes to prevent false sharing between levels.

### External Producer Thread Analysis

| Queue | External Producer | Thread Source | Bounded? |
|-------|-------------------|---------------|----------|
| PipelineDriverQueue | PipelineDriverPoller | Dedicated thread | Yes (1 per executor) |
| PipelineDriverQueue | Fragment init (submit) | brpc handler pool | Yes (pool-sized) |
| PipelineDriverQueue | EventScheduler callbacks | brpc/bthread timer/IO threads | Yes (pool-sized) |
| ScanTaskQueue | Driver Executor workers | GlobalDriverExecutor pool | Yes (configured) |

## Layer 2: Scheduling (Per-Workgroup)

### LockFreeDriverQueue (replaces QuerySharedDriverQueue)

One instance per workgroup. Manages MLFQ scheduling over a `WorkStealingQueue<DriverRawPtr, 8>`.

```cpp
class LockFreeDriverQueue {
public:
    explicit LockFreeDriverQueue(int num_workers);

    void put_back(DriverRawPtr driver, int worker_id);
    void put_back(DriverRawPtr driver);  // external threads

    bool try_take(DriverRawPtr& driver);

    void update_statistics(int level, uint64_t execution_time_ns);

private:
    int _compute_level(DriverRawPtr driver);

    WorkStealingQueue<DriverRawPtr, 8> _queue;

    struct alignas(64) LevelStats {
        std::atomic<uint64_t> accu_time_ns{0};
        double factor;  // pre-computed: RATIO^(QUEUE_SIZE-1-i)
    };
    std::array<LevelStats, 8> _level_stats;
};
```

**MLFQ Level Computation** (`_compute_level`):
Same as current logic. Based on driver's accumulated execution time:
- Level i if `accumulated_time < (i+1) * LEVEL_TIME_SLICE_BASE_NS`
- Base slice: 200ms (configurable via `pipeline_driver_queue_level_time_slice_base_ns`)

**Level Selection** (`try_take`):
1. Scan 8 `_level_stats[i].accu_time_ns / factor` values (8 atomic relaxed loads)
2. Pick level with minimum weighted time
3. Call `_queue.try_dequeue(min_level, driver)`
4. If empty, try remaining levels in ascending weighted-time order
5. Return false if all levels empty

**Statistics Update** (`update_statistics`):
- `_level_stats[level].accu_time_ns.fetch_add(delta_ns, std::memory_order_relaxed)`
- Called by upper layer after driver execution completes

**Relaxed Fairness**: Multiple workers may read slightly stale `accu_time` values, causing minor deviations from strict weighted fairness. This is acceptable — overall fairness is preserved statistically.

### LockFreeScanTaskQueue (replaces PriorityScanTaskQueue)

One instance per workgroup. Manages strict priority scheduling over a `WorkStealingQueue<ScanTask, 21>`.

```cpp
class LockFreeScanTaskQueue {
public:
    explicit LockFreeScanTaskQueue(int num_workers);

    bool try_offer(ScanTask task, int worker_id);
    bool try_offer(ScanTask task);       // external threads
    void force_put(ScanTask task, int worker_id);
    void force_put(ScanTask task);       // external threads

    bool try_take(ScanTask& task);

private:
    WorkStealingQueue<ScanTask, 21> _queue;
};
```

**Priority Mapping**: ScanTask priority values (0-20) map 1:1 to 21 levels. Priority 20 is the highest (level index 20), priority 0 is the lowest (level index 0).

**Level Selection** (`try_take`):
Strict priority scan from level 20 (highest) down to level 0 (lowest). Return first non-empty.

**try_offer vs force_put**: `try_offer` may fail if the underlying ConcurrentQueue cannot allocate. `force_put` always succeeds (allocates as needed). This matches the current `PriorityScanTaskQueue` semantics.

## Layer 3: Composition (Cross-Workgroup)

### LockFreeWorkGroupDriverQueue (replaces WorkGroupDriverQueue)

```cpp
class LockFreeWorkGroupDriverQueue : public DriverQueue {
public:
    void put_back(DriverRawPtr driver) override;
    void put_back_from_executor(DriverRawPtr driver, int worker_id) override;
    bool take(DriverRawPtr& driver, bool blocking) override;
    void cancel(DriverRawPtr driver) override;
    void update_statistics(DriverRawPtr driver) override;

private:
    WorkGroupDriverSchedEntity* _pick_next_wg();

    moodycamel::LightweightSemaphore _sema;
    ConcurrentHashSet<DriverRawPtr> _cancel_set;
};
```

### Blocking Strategy

Uses `moodycamel::LightweightSemaphore` (user-space spin + futex fallback) as a unified notification mechanism across all workgroup queues.

- **Signal**: Every `put_back()` calls `_sema.signal()` after enqueue
- **Wait**: Workers call `_sema.wait()` only after all workgroups' queues return empty from `try_take()`
- **Spurious wakeups**: Possible but harmless — worker simply re-scans and waits again

### Cancel Strategy

Uses a lock-free concurrent set for lazy cancel checking. Implementation options: `tbb::concurrent_hash_map<DriverRawPtr, bool>`, a sharded `std::unordered_set` with per-shard spinlock, or a simple lock-free linked list (cancel is rare, set is typically small). The exact choice should be determined during implementation based on available dependencies.

- **cancel(driver)**: `_cancel_set.insert(driver)` + `_sema.signal()`
- **take() after dequeue**: Check `_cancel_set.contains(driver)` — if true, handle cancellation (finalize driver), remove from set, and continue loop
- **Priority**: Cancelled drivers are processed as soon as dequeued. Unlike the current design which has a dedicated `pending_cancel_queue`, cancelled drivers are not explicitly prioritized. This is acceptable under relaxed fairness — they will be processed promptly since any dequeue encounters them.

Note: ScanTaskQueue has no cancel mechanism (handled at execution level), so `LockFreeWorkGroupScanTaskQueue` does not need a cancel set.

### Workgroup Selection

Replaces `std::set<WorkGroupSchedEntity*>` sorted by vruntime with lock-free scanning.

**`_pick_next_wg()` logic**:
1. Iterate all active `WorkGroupDriverSchedEntity` pointers
2. Read each entity's `atomic<uint64_t> vruntime_ns` (relaxed load)
3. Return entity with minimum vruntime
4. If no active workgroup, return nullptr

**Vruntime update**: After driver execution, `update_statistics()` does:
- `vruntime_ns.fetch_add(execution_time_ns / cpu_weight, relaxed)`

**Relaxed fairness**: Concurrent vruntime reads may see stale values. Two workers might both pick the same workgroup simultaneously. This causes temporary unfairness but self-corrects as the winning workgroup's vruntime advances.

### LockFreeWorkGroupScanTaskQueue (replaces WorkGroupScanTaskQueue)

Symmetric structure, same patterns:
- `LockFreeScanTaskQueue` per workgroup (21 levels, strict priority)
- `LightweightSemaphore` for blocking
- Workgroup selection by atomic vruntime
- No cancel set needed

## Data Flow Diagram

### put_back (enqueue) path

```
Driver execution complete (worker thread)
  │
  ├─ worker_id known
  │
  ▼
LockFreeWorkGroupDriverQueue::put_back_from_executor(driver, worker_id)
  │
  ├─ wg = driver->workgroup()
  ├─ level = wg_queue->_compute_level(driver)     // O(1)
  ├─ wg_queue->_queue.enqueue(driver, level, worker_id)  // lock-free, explicit token
  ├─ _sema.signal()                                 // wake one waiting worker
  │
  ▼
Done (no mutex acquired)
```

### take (dequeue) path

```
Worker thread needs next driver
  │
  ▼
LockFreeWorkGroupDriverQueue::take(driver, blocking=true)
  │
  ├─ loop:
  │   ├─ wg = _pick_next_wg()              // scan atomic vruntimes, pick min
  │   │
  │   ├─ if wg found:
  │   │   ├─ wg_queue->try_take(driver)    // scan accu_time/factor, try_dequeue
  │   │   ├─ if got driver:
  │   │   │   ├─ check _cancel_set         // lazy cancel check
  │   │   │   ├─ if cancelled: handle, continue loop
  │   │   │   └─ return true
  │   │   └─ if empty: try next workgroup
  │   │
  │   ├─ all workgroups empty:
  │   │   └─ _sema.wait()                  // block until signal
  │   │
  │   └─ continue loop
  │
  ▼
Driver returned (no mutex acquired on hot path)
```

## Concurrency Analysis

### Lock-Free Guarantees

| Operation | Lock-free? | Mechanism |
|-----------|:----------:|-----------|
| enqueue (worker) | Yes | ConcurrentQueue + explicit ProducerToken |
| enqueue (external) | Yes | ConcurrentQueue + implicit producer |
| try_dequeue | Yes | ConcurrentQueue default dequeue |
| accu_time read/update | Yes | atomic relaxed load/fetch_add |
| vruntime read/update | Yes | atomic relaxed load/fetch_add |
| level selection | Yes | scan array of atomics |
| workgroup selection | Yes | scan array of atomics |
| cancel insert/check | Yes | ConcurrentHashSet |
| blocking wait/signal | Mostly | LightweightSemaphore (spin + futex) |

### Contention Points

1. **Same-level enqueue from same worker**: Zero contention (dedicated ProducerToken → dedicated sub-queue)
2. **Same-level enqueue from different workers**: Zero contention (separate sub-queues)
3. **Same-level dequeue from multiple workers**: Minimal contention (CAS on block head pointer; moodycamel picks different sub-queues heuristically)
4. **Semaphore signal/wait**: Minimal (user-space spin in LightweightSemaphore, falls back to futex under high contention)
5. **Cancel set**: Minimal (lock-free concurrent hash set; cancel is rare operation)

### Relaxed Fairness Implications

| Behavior | Current (mutex-based) | New (lock-free) |
|----------|----------------------|-----------------|
| MLFQ level selection | Strictly consistent | Eventually consistent (stale accu_time reads) |
| Workgroup vruntime | Strictly consistent | Eventually consistent (stale vruntime reads) |
| Cancel priority | Dedicated fast-path queue | Lazy check after dequeue |
| Cross-level fairness | Exact weighted formula | Same formula, approximate values |

All relaxations are bounded: atomic values converge within one scheduling cycle. No starvation is possible because level/workgroup selection still uses the same weighted formulas.

## Integration Plan

### DriverQueue Interface Adaptation

The existing `DriverQueue` base class interface needs minor changes:

```cpp
class DriverQueue {
public:
    // Add worker_id parameter to enable per-worker token usage
    virtual void put_back_from_executor(DriverRawPtr driver, int worker_id) = 0;
    virtual bool take(DriverRawPtr& driver, bool blocking) = 0;
    // ... rest unchanged
};
```

### Worker Thread Adaptation

`GlobalDriverExecutor::_worker_thread()` needs to pass its worker_id:

```cpp
void GlobalDriverExecutor::_worker_thread(int worker_id) {
    while (!_is_closed) {
        DriverRawPtr driver = nullptr;
        if (!_driver_queue->take(driver, /*blocking=*/true)) {
            continue;
        }
        // execute driver...
        _driver_queue->put_back_from_executor(driver, worker_id);
    }
}
```

### Backward Compatibility

- New classes implement the same `DriverQueue` / `ScanTaskQueue` interfaces
- Can be enabled via a feature flag config (e.g., `enable_lock_free_driver_queue`)
- Old and new implementations coexist during transition
- Rollback: flip the flag

## File Layout

```
be/src/exec/pipeline/
├── work_stealing_queue.h              // WorkStealingQueue<T, NUM_LEVELS>
├── lock_free_driver_queue.h           // LockFreeDriverQueue
├── lock_free_driver_queue.cpp
├── lock_free_work_group_driver_queue.h    // LockFreeWorkGroupDriverQueue
├── lock_free_work_group_driver_queue.cpp

be/src/exec/workgroup/
├── lock_free_scan_task_queue.h        // LockFreeScanTaskQueue
├── lock_free_scan_task_queue.cpp
├── lock_free_work_group_scan_task_queue.h  // LockFreeWorkGroupScanTaskQueue
├── lock_free_work_group_scan_task_queue.cpp

be/test/exec/pipeline/
├── work_stealing_queue_test.cpp
├── lock_free_driver_queue_test.cpp

be/test/exec/workgroup/
├── lock_free_scan_task_queue_test.cpp
```

## Configuration

| Config | Default | Description |
|--------|---------|-------------|
| `enable_lock_free_driver_queue` | false | Feature flag to enable new lock-free queue |
| `pipeline_driver_queue_level_time_slice_base_ns` | 200000000 (200ms) | MLFQ base time slice (unchanged) |
| `pipeline_driver_queue_ratio_of_adjacent_queue` | 1.2 | MLFQ level ratio (unchanged) |

## Testing Strategy

1. **Unit tests for WorkStealingQueue**: Multi-threaded enqueue/dequeue correctness, level isolation, empty behavior
2. **Unit tests for LockFreeDriverQueue**: MLFQ level computation, weighted level selection, statistics accuracy
3. **Unit tests for LockFreeScanTaskQueue**: Priority ordering (21 levels), try_offer/force_put semantics
4. **Unit tests for composition layer**: Workgroup vruntime selection, cancel semantics, semaphore blocking/wakeup
5. **Stress tests**: High-contention scenarios with 64+ threads, measuring throughput and fairness
6. **Integration**: Run existing pipeline/scan test suites with feature flag enabled

## Benchmark

Benchmark code in `be/src/bench/`, using Google Benchmark framework (consistent with existing benchmarks in the codebase).

### Benchmark Targets

#### 1. ScanTaskQueue Benchmark (`be/src/bench/lock_free_scan_task_queue_bench.cpp`)

Compare `LockFreeScanTaskQueue` + `LockFreeWorkGroupScanTaskQueue` vs current `PriorityScanTaskQueue` + `WorkGroupScanTaskQueue`.

**Scenarios**:

| Scenario | Description | Measures |
|----------|-------------|----------|
| Single-WG enqueue throughput | N producer threads enqueue to 1 workgroup | Enqueue ops/sec, scalability with thread count |
| Single-WG dequeue throughput | N consumer threads dequeue from 1 workgroup | Dequeue ops/sec, scalability with thread count |
| Single-WG mixed put/take | N threads both enqueue and dequeue (simulates ScanExecutor workers) | Total ops/sec under contention |
| Multi-WG mixed put/take | N threads, M workgroups, mixed enqueue/dequeue | Cross-workgroup scheduling overhead |
| Producer-consumer split | P producer threads + C consumer threads (simulates Driver Executor → ScanExecutor pattern) | Throughput when producer/consumer are disjoint pools |

**Thread counts**: 1, 4, 8, 16, 32, 64, 128 (to show scalability curve)

**Workgroup counts** (for multi-WG): 1, 2, 4, 8

#### 2. PipelineDriverQueue Benchmark (`be/src/bench/lock_free_driver_queue_bench.cpp`)

Compare `LockFreeDriverQueue` + `LockFreeWorkGroupDriverQueue` vs current `QuerySharedDriverQueue` + `WorkGroupDriverQueue`.

**Scenarios**:

| Scenario | Description | Measures |
|----------|-------------|----------|
| Single-WG enqueue throughput | N worker threads put_back drivers | Enqueue ops/sec |
| Single-WG dequeue throughput | N worker threads take drivers | Dequeue ops/sec |
| Single-WG mixed put_back/take | N threads simulate executor loop: take → execute → put_back | End-to-end scheduling latency and throughput |
| Multi-WG mixed put_back/take | N threads, M workgroups with different cpu_weight | Workgroup fairness + throughput |
| Mixed with external producers | N executor workers + 1 poller thread + P external submit threads | Contention between internal and external producers |
| Cancel under load | N threads mixed put_back/take + periodic cancel | Cancel overhead impact on throughput |

**Thread counts**: 1, 4, 8, 16, 32, 64, 128

**Workgroup counts** (for multi-WG): 1, 2, 4, 8

### Key Metrics

Each benchmark reports:
- **Throughput**: ops/sec (operations per second)
- **Latency**: p50, p99, p999 per-operation latency (ns)
- **Scalability**: throughput ratio relative to single-thread baseline
- **Fairness** (for multi-WG): actual CPU time distribution vs expected (based on cpu_weight)

### Mock Objects

Benchmarks use lightweight mock objects to avoid pulling in full pipeline/scan dependencies:
- `MockDriver`: Minimal struct with workgroup pointer, accumulated execution time, and MLFQ level
- `MockScanTask`: Minimal struct with priority value and workgroup pointer
- `MockWorkGroup`: Minimal struct with cpu_weight, vruntime counter, and queue pointers

### Build Tips

To speed up benchmark iteration, temporarily comment out other benchmark cases in `be/src/bench/CMakeLists.txt` so only the new benchmark files are compiled.

### File Layout

```
be/src/bench/
├── lock_free_scan_task_queue_bench.cpp
├── lock_free_driver_queue_bench.cpp
```
