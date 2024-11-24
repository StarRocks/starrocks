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

#include <queue>

#include "exec/pipeline/pipeline_driver.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/factory_method.h"

namespace starrocks::pipeline {

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

    virtual StatusOr<DriverRawPtr> take(const bool block) = 0;
    virtual void cancel(DriverRawPtr driver) = 0;

    // Update statistics of the driver's workgroup,
    // when yielding the driver in the executor thread.
    virtual void update_statistics(const DriverRawPtr driver) = 0;

    virtual size_t size() const = 0;
    bool empty() const { return size() == 0; }

    virtual bool should_yield(const DriverRawPtr driver, int64_t unaccounted_runtime_ns) const = 0;
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
    DriverRawPtr take(const bool block);
    inline bool empty() const { return num_drivers == 0; }

    inline size_t size() const { return num_drivers; }

    std::deque<DriverRawPtr> queue;
    std::queue<DriverRawPtr> pending_cancel_queue;
    std::unordered_set<DriverRawPtr> cancelled_set;
    size_t num_drivers = 0;

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

    void update_statistics(const DriverRawPtr driver) override;

    // Return cancelled status, if the queue is closed.
    StatusOr<DriverRawPtr> take(const bool block) override;

    void cancel(DriverRawPtr driver) override;

    size_t size() const override;

    bool should_yield(const DriverRawPtr driver, int64_t unaccounted_runtime_ns) const override { return false; }

    static double ratio_of_adjacent_queue() { return config::pipeline_driver_queue_ratio_of_adjacent_queue; }
    static constexpr size_t QUEUE_SIZE = 8;

private:
    // When the driver at the i-th level costs _level_time_slices[i],
    // it will move to (i+1)-th level.
    int _compute_driver_level(const DriverRawPtr driver) const;

private:
    // The time slice of the i-th level is (i+1)*LEVEL_TIME_SLICE_BASE ns,
    // so when a driver's execution time exceeds 0.2s, 0.6s, 1.2s, 2.0s, 3.0s, 4.2s, 5.6s, 7.4s.
    // it will move to next level.
    const int64_t LEVEL_TIME_SLICE_BASE_NS = config::pipeline_driver_queue_level_time_slice_base_ns;
    const double RATIO_OF_ADJACENT_QUEUE = ratio_of_adjacent_queue();

    SubQuerySharedDriverQueue _queues[QUEUE_SIZE];
    // The time slice of the i-th level is (i+1)*LEVEL_TIME_SLICE_BASE ns.
    int64_t _level_time_slices[QUEUE_SIZE];

    size_t _num_drivers = 0;

    mutable std::mutex _global_mutex;
    std::condition_variable _cv;
    bool _is_closed = false;
};

// WorkGroupDriverQueue contains two levels of queues.
// The first level is the work group queue, and the second level is the driver queue in a work group.
class WorkGroupDriverQueue : public FactoryMethod<DriverQueue, WorkGroupDriverQueue> {
    friend class FactoryMethod<DriverQueue, WorkGroupDriverQueue>;

public:
    ~WorkGroupDriverQueue() override = default;
    void close() override;

    void put_back(const DriverRawPtr driver) override;
    void put_back(const std::vector<DriverRawPtr>& drivers) override;
    // When the driver's workgroup is not in the workgroup queue
    // and the driver isn't from an executor thread (that is, from the poller or new driver),
    // the workgroup's vruntime is adjusted to workgroup_queue.min_vruntime-ideal_runtime/2,
    // to avoid sloping too much time to this workgroup.
    void put_back_from_executor(const DriverRawPtr driver) override;

    // Return cancelled status, if the queue is closed.
    // Firstly, select the work group with the minimum vruntime.
    // Secondly, select the proper driver from the driver queue of this work group.
    StatusOr<DriverRawPtr> take(const bool block) override;

    void cancel(DriverRawPtr driver) override;

    void update_statistics(const DriverRawPtr driver) override;

    size_t size() const override;

    bool should_yield(const DriverRawPtr driver, int64_t unaccounted_runtime_ns) const override;

private:
    /// These methods should be guarded by the outside _global_mutex.
    template <bool from_executor>
    void _put_back(const DriverRawPtr driver);
    workgroup::WorkGroupDriverSchedEntity* _pick_next_wg() const;
    // _update_min_wg is invoked when an entity is enqueued or dequeued from _wg_entities.
    void _update_min_wg();
    template <bool from_executor>
    void _enqueue_workgroup(workgroup::WorkGroupDriverSchedEntity* wg_entity);
    void _dequeue_workgroup(workgroup::WorkGroupDriverSchedEntity* wg_entity);

    // The ideal runtime of a work group is the weighted average of the schedule period.
    int64_t _ideal_runtime_ns(workgroup::WorkGroupDriverSchedEntity* wg_entity) const;

private:
    static constexpr int64_t SCHEDULE_PERIOD_PER_WG_NS = 100'000'000;

    struct WorkGroupDriverSchedEntityComparator {
        using WorkGroupDriverSchedEntityPtr = workgroup::WorkGroupDriverSchedEntity*;
        bool operator()(const WorkGroupDriverSchedEntityPtr& lhs_ptr,
                        const WorkGroupDriverSchedEntityPtr& rhs_ptr) const;
    };
    using WorkgroupSet = std::set<workgroup::WorkGroupDriverSchedEntity*, WorkGroupDriverSchedEntityComparator>;

    mutable std::mutex _global_mutex;
    std::condition_variable _cv;
    std::condition_variable _cv_for_borrowed_cpus;
    bool _is_closed = false;

    // Contains the workgroups which include the drivers ready to be run.
    // Entities are sorted by vruntime in set.
    // MUST guarantee the entity is not in set, when updating its vruntime.
    WorkgroupSet _wg_entities;

    size_t _sum_cpu_weight = 0;

    size_t _num_drivers = 0;

    // Cache the minimum entity, used to check should_yield() without lock.
    std::atomic<workgroup::WorkGroupDriverSchedEntity*> _min_wg_entity = nullptr;
};

} // namespace starrocks::pipeline
