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

#pragma once

#include <any>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <set>
#include <unordered_set>

#include "common/statusor.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/blocking_priority_queue.hpp"
#include "util/race_detect.h"
#include "util/runtime_profile.h"

namespace starrocks::workgroup {

struct ScanTaskGroup {
    int64_t runtime_ns = 0;
    int sub_queue_level = 0;
};

#define TO_NEXT_STAGE(yield_point) yield_point++;

struct YieldContext {
    YieldContext() = default;
    YieldContext(size_t total_yield_point_cnt) : total_yield_point_cnt(total_yield_point_cnt) {}

    ~YieldContext() = default;

    DISALLOW_COPY(YieldContext);
    YieldContext(YieldContext&&) = default;
    YieldContext& operator=(YieldContext&&) = default;

    bool is_finished() const { return yield_point >= total_yield_point_cnt; }
    void set_finished() {
        yield_point = total_yield_point_cnt = 0;
        task_context_data.reset();
    }

    std::any task_context_data;
    size_t yield_point{};
    size_t total_yield_point_cnt{};
    const WorkGroup* wg = nullptr;
    // used to record the runtime information of a single call in order to decide whether to trigger yield.
    // It needs to be reset every time when the task is executed.
    int64_t time_spent_ns = 0;
    bool need_yield = false;
};

struct ScanTask {
public:
    using WorkFunction = std::function<void(YieldContext&)>;
    using YieldFunction = std::function<void(ScanTask&&)>;

    ScanTask() : ScanTask(nullptr, nullptr) {}
    explicit ScanTask(WorkFunction work_function) : workgroup(nullptr), work_function(std::move(work_function)) {}
    ScanTask(WorkGroup* workgroup, WorkFunction work_function)
            : workgroup(workgroup), work_function(std::move(work_function)) {}
    ScanTask(WorkGroup* workgroup, WorkFunction work_function, YieldFunction yield_function)
            : workgroup(workgroup),
              work_function(std::move(work_function)),
              yield_function(std::move(yield_function)) {}
    ~ScanTask() = default;

    DISALLOW_COPY(ScanTask);
    // Enable move constructor and assignment.
    ScanTask(ScanTask&&) = default;
    ScanTask& operator=(ScanTask&&) = default;

    bool operator<(const ScanTask& rhs) const { return priority < rhs.priority; }
    ScanTask& operator++() {
        priority += 2;
        return *this;
    }

    void run() { work_function(work_context); }

    bool is_finished() const { return work_context.is_finished(); }

    bool has_yield_function() const { return yield_function != nullptr; }

    void execute_yield_function() {
        DCHECK(yield_function != nullptr) << "yield function must be set";
        yield_function(std::move(*this));
    }

    const YieldContext& get_work_context() const { return work_context; }

public:
    WorkGroup* workgroup;
    YieldContext work_context;
    WorkFunction work_function;
    YieldFunction yield_function;
    int priority = 0;
    std::shared_ptr<ScanTaskGroup> task_group = nullptr;
    RuntimeProfile::HighWaterMarkCounter* peak_scan_task_queue_size_counter = nullptr;
};

/// There are two types of ScanTaskQueue:
/// - WorkGroupScanTaskQueue, which is a two-level queue.
///   - The first level selects the workgroup with the shortest execution time.
///   - The second level selects an appropriate task using either PriorityScanTaskQueue.
/// - PriorityScanTaskQueue, which prioritizes scan tasks with lower committed times.
class ScanTaskQueue {
public:
    ScanTaskQueue() = default;
    virtual ~ScanTaskQueue() = default;

    virtual void close() = 0;

    virtual StatusOr<ScanTask> take() = 0;
    virtual bool try_offer(ScanTask task) = 0;
    virtual void force_put(ScanTask task) = 0;

    virtual size_t size() const = 0;
    bool empty() const { return size() == 0; }

    virtual void update_statistics(ScanTask& task, int64_t runtime_ns) = 0;
    virtual bool should_yield(const WorkGroup* wg, int64_t unaccounted_runtime_ns) const = 0;
};

class PriorityScanTaskQueue final : public ScanTaskQueue {
public:
    explicit PriorityScanTaskQueue(size_t max_elements);
    ~PriorityScanTaskQueue() override = default;

    void close() override { _queue.shutdown(); }

    StatusOr<ScanTask> take() override;
    bool try_offer(ScanTask task) override;
    void force_put(ScanTask task) override;

    size_t size() const override { return _queue.get_size(); }

    void update_statistics(ScanTask& task, int64_t runtime_ns) override {}
    bool should_yield(const WorkGroup* wg, int64_t unaccounted_runtime_ns) const override { return false; }

private:
    BlockingPriorityQueue<ScanTask> _queue;
};

class WorkGroupScanTaskQueue final : public ScanTaskQueue {
public:
    WorkGroupScanTaskQueue(ScanSchedEntityType sched_entity_type) : _sched_entity_type(sched_entity_type) {}
    ~WorkGroupScanTaskQueue() override = default;

    void close() override;

    StatusOr<ScanTask> take() override;
    bool try_offer(ScanTask task) override;
    void force_put(ScanTask task) override;

    size_t size() const override { return _num_tasks.load(std::memory_order_acquire); }

    void update_statistics(ScanTask& task, int64_t runtime_ns) override;
    bool should_yield(const WorkGroup* wg, int64_t unaccounted_runtime_ns) const override;

private:
    /// These methods should be guarded by the outside _global_mutex.
    WorkGroupScanSchedEntity* _pick_next_wg() const;
    // _update_min_wg is invoked when an entity is enqueued or dequeued from _wg_entities.
    void _update_min_wg();
    void _enqueue_workgroup(WorkGroupScanSchedEntity* wg_entity);
    void _dequeue_workgroup(WorkGroupScanSchedEntity* wg_entity);

    // The ideal runtime of a work group is the weighted average of the schedule period.
    int64_t _ideal_runtime_ns(WorkGroupScanSchedEntity* wg_entity) const;

    WorkGroupScanSchedEntity* _sched_entity(WorkGroup* wg);
    const WorkGroupScanSchedEntity* _sched_entity(const WorkGroup* wg) const;

private:
    static constexpr int64_t SCHEDULE_PERIOD_PER_WG_NS = 100'000'000;

    struct WorkGroupScanSchedEntityComparator {
        using WorkGroupScanSchedEntityPtr = WorkGroupScanSchedEntity*;
        bool operator()(const WorkGroupScanSchedEntityPtr& lhs_ptr, const WorkGroupScanSchedEntityPtr& rhs_ptr) const;
    };
    using WorkgroupSet = std::set<WorkGroupScanSchedEntity*, WorkGroupScanSchedEntityComparator>;

    const ScanSchedEntityType _sched_entity_type;

    mutable std::mutex _global_mutex;
    std::condition_variable _cv;
    std::condition_variable _cv_for_borrowed_cpus;
    bool _is_closed = false;

    // Contains the workgroups which include the tasks ready to be run.
    // Entities are sorted by vruntime in set.
    // MUST guarantee the entity is not in set, when updating its vruntime.
    WorkgroupSet _wg_entities;

    size_t _sum_cpu_weight = 0;

    // Cache the minimum entity, used to check should_yield() without lock.
    std::atomic<WorkGroupScanSchedEntity*> _min_wg_entity = nullptr;

    std::atomic<size_t> _num_tasks = 0;
};

std::unique_ptr<ScanTaskQueue> create_scan_task_queue();

} // namespace starrocks::workgroup
