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
    const workgroup::WorkGroup* wg = nullptr;
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
            : workgroup(workgroup), work_function(std::move(work_function)), yield_function(std::move(yield_function)) {}
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

    bool has_yield_function() const {
        return yield_function != nullptr;
    }

    void execute_yield_function() {
        DCHECK(yield_function != nullptr) << "yield function must be set";
        yield_function(std::move(*this));
    }
    

public:
    WorkGroup* workgroup;
    YieldContext work_context;
    WorkFunction work_function;
    YieldFunction yield_function;
    int priority = 0;
    std::shared_ptr<ScanTaskGroup> task_group = nullptr;
    RuntimeProfile::HighWaterMarkCounter* peak_scan_task_queue_size_counter = nullptr;
};

/// There are three types of ScanTaskQueue:
/// - WorkGroupScanTaskQueue, which is a two-level queue.
///   - The first level selects the workgroup with the shortest execution time.
///   - The second level selects an appropriate task using either PriorityScanTaskQueue or MultiLevelFeedScanTaskQueue.
/// - PriorityScanTaskQueue, which prioritizes scan tasks with lower committed times.
/// - MultiLevelFeedScanTaskQueue, which prioritizes scan tasks with shorter execution time.
///   It is advisable to use MultiLevelFeedScanTaskQueue when scan tasks from large queries may impact those from small queries.
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

class MultiLevelFeedScanTaskQueue final : public ScanTaskQueue {
public:
    MultiLevelFeedScanTaskQueue();
    ~MultiLevelFeedScanTaskQueue() override = default;

    void close() override;

    StatusOr<ScanTask> take() override;
    bool try_offer(ScanTask task) override;
    void force_put(ScanTask task) override;

    size_t size() const override { return _num_tasks; }

    void update_statistics(ScanTask& task, int64_t runtime_ns) override;
    bool should_yield(const WorkGroup* wg, int64_t unaccounted_runtime_ns) const override { return false; }

    static constexpr int NUM_QUEUES = 8;
    static double ratio_of_adjacent_queue() { return config::pipeline_scan_queue_ratio_of_adjacent_queue; }

private:
    int _compute_queue_level(const ScanTask& task) const;

    struct SubQueue {
        void incr_cost_ns(int64_t delta) { cost_ns += delta; }
        double normalized_cost() const { return cost_ns / factor_for_normal; }

        std::queue<ScanTask> queue;

        int64_t level_time_slice = 0;
        double factor_for_normal = 0;
        int64_t cost_ns = 0;
    };

    // The time slice of the i-th level is (i+1)*LEVEL_TIME_SLICE_BASE ns,
    // so when a driver's execution time exceeds 0.1s, 0.3s, 0.6s, 1.0s, 1.5s, 2.1s, 2.8s, 3.7s.
    // it will move to next level.
    const int64_t LEVEL_TIME_SLICE_BASE_NS = config::pipeline_scan_queue_level_time_slice_base_ns;
    const double RATIO_OF_ADJACENT_QUEUE = ratio_of_adjacent_queue();

    mutable std::mutex _global_mutex;
    std::condition_variable _cv;
    bool _is_closed = false;

    SubQueue _queues[NUM_QUEUES];
    std::atomic<size_t> _num_tasks = 0;
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
    enum SchedEntityType { OLAP, CONNECTOR };

    WorkGroupScanTaskQueue(SchedEntityType sched_entity_type) : _sched_entity_type(sched_entity_type) {}
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
    workgroup::WorkGroupScanSchedEntity* _take_next_wg();
    // _update_min_wg is invoked when an entity is enqueued or dequeued from _wg_entities.
    void _update_min_wg();
    // Apply hard bandwidth control to non-short-query workgroups, when there are queries of the short-query workgroup.
    bool _throttled(const workgroup::WorkGroupScanSchedEntity* wg_entity, int64_t unaccounted_runtime_ns = 0) const;
    // _update_bandwidth_control_period resets period_end_ns and period_usage_ns, when a new period comes.
    // It is invoked when taking a task to execute or an executed task is finished.
    void _update_bandwidth_control_period();
    void _enqueue_workgroup(workgroup::WorkGroupScanSchedEntity* wg_entity);
    void _dequeue_workgroup(workgroup::WorkGroupScanSchedEntity* wg_entity);

    int64_t _bandwidth_quota_ns() const;
    // The ideal runtime of a work group is the weighted average of the schedule period.
    int64_t _ideal_runtime_ns(workgroup::WorkGroupScanSchedEntity* wg_entity) const;

    workgroup::WorkGroupScanSchedEntity* _sched_entity(workgroup::WorkGroup* wg);
    const workgroup::WorkGroupScanSchedEntity* _sched_entity(const workgroup::WorkGroup* wg) const;

private:
    static constexpr int64_t SCHEDULE_PERIOD_PER_WG_NS = 100'000'000;
    static constexpr int64_t BANDWIDTH_CONTROL_PERIOD_NS = 100'000'000;

    struct WorkGroupScanSchedEntityComparator {
        using WorkGroupScanSchedEntityPtr = workgroup::WorkGroupScanSchedEntity*;
        bool operator()(const WorkGroupScanSchedEntityPtr& lhs_ptr, const WorkGroupScanSchedEntityPtr& rhs_ptr) const;
    };
    using WorkgroupSet = std::set<workgroup::WorkGroupScanSchedEntity*, WorkGroupScanSchedEntityComparator>;

    const SchedEntityType _sched_entity_type;

    mutable std::mutex _global_mutex;
    std::condition_variable _cv;
    bool _is_closed = false;

    // Contains the workgroups which include the tasks ready to be run.
    // Entities are sorted by vruntime in set.
    // MUST guarantee the entity is not in set, when updating its vruntime.
    WorkgroupSet _wg_entities;

    size_t _sum_cpu_limit = 0;

    // Cache the minimum entity, used to check should_yield() without lock.
    std::atomic<workgroup::WorkGroupScanSchedEntity*> _min_wg_entity = nullptr;

    // Hard bandwidth control to non-short-query workgroups.
    // - The control period is 100ms, and the total quota of non-short-query workgroups is 100ms*(vCPUs-rt_wg.cpu_limit).
    // - The non-short-query workgroups cannot be executed in the current period, if their usage exceeds quota.
    // - When a new period comes, penalize the non-short-query workgroups according to the previous bandwidth usage.
    //     - If usage <= quota, don't penalize it.
    //     - If quota < usage <= 2*quota, set the new usage to `usage-quota`.
    //     - Otherwise, set the new usage to quota to prevent them from being executed in the new period.
    // Whether to apply the bandwidth control is decided by whether there are queries of the short-query workgroup.
    // - If there are queries of the short-query workgroup, apply the control.
    // - Otherwise, don't apply the control.
    int64_t _bandwidth_control_period_end_ns = 0;
    std::atomic<int64_t> _bandwidth_usage_ns = 0;

    std::atomic<size_t> _num_tasks = 0;
};

std::unique_ptr<ScanTaskQueue> create_scan_task_queue();

} // namespace starrocks::workgroup
