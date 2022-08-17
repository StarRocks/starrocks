// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <unordered_set>

#include "common/statusor.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/blocking_priority_queue.hpp"

namespace starrocks::workgroup {

struct ScanTask {
public:
    using WorkFunction = std::function<void(int)>;

    ScanTask() : ScanTask(nullptr, nullptr) {}
    explicit ScanTask(WorkFunction work_function) : workgroup(nullptr), work_function(std::move(work_function)) {}
    ScanTask(WorkGroupPtr workgroup, WorkFunction work_function)
            : workgroup(std::move(workgroup)), work_function(std::move(work_function)) {}
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
    WorkGroupPtr workgroup;
    WorkFunction work_function;
    int priority = 0;
};

class ScanTaskQueue {
public:
    ScanTaskQueue() = default;
    virtual ~ScanTaskQueue() = default;

    virtual void close() = 0;

    virtual StatusOr<ScanTask> take(int worker_id) = 0;
    virtual bool try_offer(ScanTask task) = 0;

    virtual size_t size() const = 0;
};

class PriorityScanTaskQueue final : public ScanTaskQueue {
public:
    explicit PriorityScanTaskQueue(size_t max_elements);
    ~PriorityScanTaskQueue() override = default;

    void close() override { _queue.shutdown(); }

    StatusOr<ScanTask> take(int worker_id) override;
    bool try_offer(ScanTask task) override;

    size_t size() const override { return _queue.get_size(); }

private:
    BlockingPriorityQueue<ScanTask> _queue;
};

class FifoScanTaskQueue final : public ScanTaskQueue {
public:
    FifoScanTaskQueue() = default;
    ~FifoScanTaskQueue() override = default;

    // This method does nothing.
    void close() override {}

    StatusOr<ScanTask> take(int worker_id) override;
    bool try_offer(ScanTask task) override;

    size_t size() const override { return _queue.size(); }

private:
    std::queue<ScanTask> _queue;
};

class ScanTaskQueueWithWorkGroup final : public ScanTaskQueue {
public:
    explicit ScanTaskQueueWithWorkGroup(ScanExecutorType type) : _type(type) {}
    ~ScanTaskQueueWithWorkGroup() override = default;

    void close() override;

    StatusOr<ScanTask> take(int worker_id) override;
    bool try_offer(ScanTask task) override;

    size_t size() const override { return _total_task_num.load(std::memory_order_acquire); }

private:
    // Calculate the actual cpu used by all wg
    void _cal_wg_cpu_real_use_ratio();

    // _maybe_adjust_weight and _select_next_wg are guarded by the ourside _global_mutex.
    void _maybe_adjust_weight();
    WorkGroupPtr _select_next_wg(int worker_id);

    static constexpr int MAX_SCHEDULE_NUM_PERIOD = 512;

    std::mutex _global_mutex;
    std::condition_variable _cv;

    const ScanExecutorType _type;
    bool _is_closed = false;

    std::unordered_set<WorkGroupPtr> _ready_wgs;
    std::atomic<size_t> _total_task_num = 0;

    // Adjust select factor of each wg after every `min(MAX_SCHEDULE_NUM_PERIOD, num_tasks)` schedule times.
    int _remaining_schedule_num_period = 0;
};

} // namespace starrocks::workgroup
