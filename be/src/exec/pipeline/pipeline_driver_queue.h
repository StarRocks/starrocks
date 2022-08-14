// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <queue>

#include "exec/pipeline/pipeline_driver.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/factory_method.h"

namespace starrocks {
namespace pipeline {

class DriverQueue;
using DriverQueuePtr = std::unique_ptr<DriverQueue>;

class DriverQueue {
public:
    virtual ~DriverQueue() = default;
    virtual void close() = 0;

    virtual void put_back(const DriverRawPtr driver) = 0;
    virtual void put_back(const std::vector<DriverRawPtr>& drivers) = 0;
    // *from_executor* means that the executor thread puts the driver back to the queue.
    virtual void put_back_from_executor(const DriverRawPtr driver) = 0;
    virtual void put_back_from_executor(const std::vector<DriverRawPtr>& drivers) = 0;

    virtual StatusOr<DriverRawPtr> take(int worker_id) = 0;
    virtual void cancel(DriverRawPtr driver) = 0;

    // Update statistics of the driver's workgroup,
    // when yielding the driver in the executor thread.
    virtual void update_statistics(const DriverRawPtr driver) = 0;

    virtual size_t size() const = 0;
    bool empty() { return size() == 0; }

protected:
    // The time slice of the i-th level is (i+1)*LEVEL_TIME_SLICE_BASE ns,
    // so when a driver's execution time exceeds 0.2s, 0.6s, 1.2s, 2s, 3s, 4.2s, 5.6s, and 7.2s,
    // it will move to next level.
    static constexpr int64_t LEVEL_TIME_SLICE_BASE_NS = 200'000'000L;
};

// SubQuerySharedDriverQueue is used to store the driver waiting to be executed.
// It guarantees the following characteristics:
// 1. the running drivers are executed in FIFO order.
// 2. the cancelled drivers have the highest priority and are executed first.
// The cancelled drivers are all equal and the order of execution is not guaranteed,
// this may not be a big problem because the finalize operation is fast enough.
//
// We use some data structures to maintain the above properties:
// 1. std::deque<DriverRawPtr> queue
//   store the drivers added by PipelineDriverPoller, cancelled driver are added to the head and the others are added to the tail
// 2. std::queue<DriverRawPtr> pending_cancel_queue
//   store the drivers that are already in `queue` but cancelled, the drivers will only be added when the query is cancelled externally
// 3. std::unordered_set<DriverRawPtr> cancelled_set
//   record drivers that have been taken in pending_cancel_queue
//
// When taking a driver from SubQuerySharedDriverQueue, we try to take it from `pending_cancel_queue` first.
// if `pending_cancel_queue` is not empty, we take it from the head and record it in `cancelled_set`.
// Otherwise, we take it from `queue`.
// It should be noted that the driver in `queue` may already be taken from `pending_cancel_queue`,
// we should ignore such drivers and try to get the next one.
class SubQuerySharedDriverQueue {
public:
    void update_accu_time(const DriverRawPtr driver) {
        _accu_consume_time.fetch_add(driver->driver_acct().get_last_time_spent());
    }

    double accu_time_after_divisor() { return _accu_consume_time.load() / factor_for_normal; }

    void put(const DriverRawPtr driver);
    void cancel(const DriverRawPtr driver);
    DriverRawPtr take();
    inline bool empty() const { return driver_number == 0; }

    inline size_t size() const { return driver_number; }

    std::deque<DriverRawPtr> queue;
    std::queue<DriverRawPtr> pending_cancel_queue;
    std::unordered_set<DriverRawPtr> cancelled_set;
    size_t driver_number = 0;

    // factor for normalization
    double factor_for_normal = 0;

private:
    std::atomic<int64_t> _accu_consume_time = 0;
};

class QuerySharedDriverQueue : public FactoryMethod<DriverQueue, QuerySharedDriverQueue> {
    friend class FactoryMethod<DriverQueue, QuerySharedDriverQueue>;

public:
    QuerySharedDriverQueue();
    ~QuerySharedDriverQueue() override = default;
    void close() override;
    void put_back(const DriverRawPtr driver) override;
    void put_back(const std::vector<DriverRawPtr>& drivers) override;
    void put_back_from_executor(const DriverRawPtr driver) override;
    void put_back_from_executor(const std::vector<DriverRawPtr>& drivers) override;

    void update_statistics(const DriverRawPtr driver) override;

    // Return cancelled status, if the queue is closed.
    StatusOr<DriverRawPtr> take(int worker_id) override;

    void cancel(DriverRawPtr driver) override;

    size_t size() const override;

    static constexpr size_t QUEUE_SIZE = 8;
    static constexpr double RATIO_OF_ADJACENT_QUEUE = 1.2;

private:
    // When the driver at the i-th level costs _level_time_slices[i],
    // it will move to (i+1)-th level.
    int _compute_driver_level(const DriverRawPtr driver) const;

private:
    SubQuerySharedDriverQueue _queues[QUEUE_SIZE];
    // The time slice of the i-th level is (i+1)*LEVEL_TIME_SLICE_BASE ns.
    int64_t _level_time_slices[QUEUE_SIZE];

    mutable std::mutex _global_mutex;
    std::condition_variable _cv;
    bool _is_closed = false;
};

// All the QuerySharedDriverQueueWithoutLock's methods MUST be guarded by the outside lock.
class QuerySharedDriverQueueWithoutLock : public FactoryMethod<DriverQueue, QuerySharedDriverQueueWithoutLock> {
    friend class FactoryMethod<DriverQueue, QuerySharedDriverQueueWithoutLock>;

public:
    QuerySharedDriverQueueWithoutLock();
    ~QuerySharedDriverQueueWithoutLock() override = default;
    void close() override {}

    void put_back(const DriverRawPtr driver) override;
    void put_back(const std::vector<DriverRawPtr>& drivers) override;
    void put_back_from_executor(const DriverRawPtr driver) override;
    void put_back_from_executor(const std::vector<DriverRawPtr>& drivers) override;

    // Always return non-nullable value.
    StatusOr<DriverRawPtr> take(int worker_id) override;

    void cancel(DriverRawPtr driver) override;

    void update_statistics(const DriverRawPtr driver) override;

    size_t size() const override { return _size; }

private:
    void _put_back(const DriverRawPtr driver);
    // When the driver at the i-th level costs _level_time_slices[i],
    // it will move to (i+1)-th level.
    int _compute_driver_level(const DriverRawPtr driver) const;

private:
    static constexpr size_t QUEUE_SIZE = 8;
    static constexpr double RATIO_OF_ADJACENT_QUEUE = 1.2;

    SubQuerySharedDriverQueue _queues[QUEUE_SIZE];
    // The time slice of the i-th level is (i+1)*LEVEL_TIME_SLICE_BASE ns.
    int64_t _level_time_slices[QUEUE_SIZE];

    size_t _size = 0;
};

// DriverQueueWithWorkGroup contains two levels of queues.
// The first level is the work group queue, and the second level is the driver queue in a work group.
class DriverQueueWithWorkGroup : public FactoryMethod<DriverQueue, DriverQueueWithWorkGroup> {
    friend class FactoryMethod<DriverQueue, DriverQueueWithWorkGroup>;

public:
    ~DriverQueueWithWorkGroup() override = default;
    void close() override;

    void put_back(const DriverRawPtr driver) override;
    void put_back(const std::vector<DriverRawPtr>& drivers) override;
    // When the driver's workgroup is not in the workgroup queue
    // and the driver isn't from a executor thread (that is, from the poller or new driver),
    // the workgroup's vruntime is adjusted to workgroup_queue.min_vruntime-ideal_runtime/2,
    // to avoid sloping too much time to this workgroup.
    void put_back_from_executor(const DriverRawPtr driver) override;
    void put_back_from_executor(const std::vector<DriverRawPtr>& drivers) override;

    // Return cancelled status, if the queue is closed.
    // Firstly, select the work group with the minimum vruntime.
    // Secondly, select the proper driver from the driver queue of this work group.
    StatusOr<DriverRawPtr> take(int worker_id) override;

    void cancel(DriverRawPtr driver) override;

    void update_statistics(const DriverRawPtr driver) override;

    size_t size() const override;

private:
    // The schedule period is equal to SCHEDULE_PERIOD_PER_WG_NS * num_workgroups.
    static constexpr int64_t SCHEDULE_PERIOD_PER_WG_NS = 200'1000'1000;

    // This method should be guarded by the outside _global_mutex.
    template <bool from_executor>
    void _put_back(const DriverRawPtr driver);
    // This method should be guarded by the outside _global_mutex.
    workgroup::WorkGroup* _find_min_owner_wg(int worker_id);
    workgroup::WorkGroup* _find_min_wg();
    // The ideal runtime of a work group is the weighted average of the schedule period.
    int64_t _ideal_runtime_ns(workgroup::WorkGroup* wg);

    mutable std::mutex _global_mutex;
    std::condition_variable _cv;
    // _ready_wgs contains the workgroups which include the drivers need to be run.
    std::unordered_set<workgroup::WorkGroup*> _ready_wgs;
    size_t _sum_cpu_limit = 0;

    bool _is_closed = false;
};

} // namespace pipeline
} // namespace starrocks