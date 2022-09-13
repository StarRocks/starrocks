// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <set>
#include <unordered_set>

#include "common/statusor.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/blocking_priority_queue.hpp"

namespace starrocks::workgroup {

struct ScanTask {
public:
    using WorkFunction = std::function<void()>;

    ScanTask() : ScanTask(nullptr, nullptr) {}
    explicit ScanTask(WorkFunction work_function) : workgroup(nullptr), work_function(std::move(work_function)) {}
    ScanTask(WorkGroup* workgroup, WorkFunction work_function)
            : workgroup(workgroup), work_function(std::move(work_function)) {}
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

public:
    WorkGroup* workgroup;
    WorkFunction work_function;
    int priority = 0;
};

class ScanTaskQueue {
public:
    ScanTaskQueue() = default;
    virtual ~ScanTaskQueue() = default;

    virtual void close() = 0;

    virtual StatusOr<ScanTask> take() = 0;
    virtual bool try_offer(ScanTask task) = 0;

    virtual size_t size() const = 0;
    bool empty() const { return size() == 0; }

    virtual void update_statistics(WorkGroup* wg, int64_t runtime_ns) = 0;
    virtual bool should_yield(const WorkGroup* wg, int64_t unaccounted_runtime_ns) const = 0;
};

class PriorityScanTaskQueue final : public ScanTaskQueue {
public:
    explicit PriorityScanTaskQueue(size_t max_elements);
    ~PriorityScanTaskQueue() override = default;

    void close() override { _queue.shutdown(); }

    StatusOr<ScanTask> take() override;
    bool try_offer(ScanTask task) override;

    size_t size() const override { return _queue.get_size(); }

    void update_statistics(WorkGroup* wg, int64_t runtime_ns) override {}
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

    size_t size() const override { return _num_tasks.load(std::memory_order_acquire); }

    void update_statistics(WorkGroup* wg, int64_t runtime_ns) override;
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
        bool operator()(const WorkGroupScanSchedEntityPtr& lhs, const WorkGroupScanSchedEntityPtr& rhs) const;
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

    // Cache the minimum vruntime and entity, used to check should_yield() without lock.
    std::atomic<int64_t> _min_vruntime_ns = std::numeric_limits<int64_t>::max();
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

} // namespace starrocks::workgroup
