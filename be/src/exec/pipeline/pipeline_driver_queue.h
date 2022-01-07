// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <queue>

#include "exec/pipeline/pipeline_driver.h"
#include "util/factory_method.h"

namespace starrocks {

namespace workgroup {
class WorkGroup;
}

namespace pipeline {

class DriverQueue;
using DriverQueuePtr = std::unique_ptr<DriverQueue>;

class DriverQueue {
public:
    virtual ~DriverQueue() = default;
    virtual void close() = 0;

    virtual void put_back(const DriverRawPtr driver, bool from_dispatcher) = 0;
    virtual void put_back(const std::vector<DriverRawPtr>& drivers, bool from_dispatcher) = 0;
    virtual StatusOr<DriverRawPtr> take() = 0;

    virtual void yield_driver(const DriverRawPtr driver) = 0;

    virtual size_t size() = 0;
    bool empty() { return size() == 0; }
};

class SubQuerySharedDriverQueue {
public:
    void update_accu_time(const DriverRawPtr driver) {
        _accu_consume_time.fetch_add(driver->driver_acct().get_last_time_spent());
    }

    double accu_time_after_divisor() { return _accu_consume_time.load() / factor_for_normal; }

    std::queue<DriverRawPtr> queue;
    // factor for normalization
    double factor_for_normal = 0;

private:
    std::atomic<int64_t> _accu_consume_time = 0;
};

class QuerySharedDriverQueue : public FactoryMethod<DriverQueue, QuerySharedDriverQueue> {
    friend class FactoryMethod<DriverQueue, QuerySharedDriverQueue>;

public:
    QuerySharedDriverQueue() {
        double factor = 1;
        for (int i = QUEUE_SIZE - 1; i >= 0; --i) {
            // initialize factor for every sub queue,
            // Higher priority queues have more execution time,
            // so they have a larger factor.
            _queues[i].factor_for_normal = factor;
            factor *= RATIO_OF_ADJACENT_QUEUE;
        }
    }
    ~QuerySharedDriverQueue() override = default;
    void close() override {}

    void put_back(const DriverRawPtr driver, bool from_dispatcher) override;
    void put_back(const std::vector<DriverRawPtr>& drivers, bool from_dispatcher) override;
    // return nullptr if queue is closed;
    StatusOr<DriverRawPtr> take() override;

    void yield_driver(const DriverRawPtr driver) override;

    size_t size() override { return _size; }

private:
    static constexpr size_t QUEUE_SIZE = 8;
    static constexpr double RATIO_OF_ADJACENT_QUEUE = 1.2;

    SubQuerySharedDriverQueue _queues[QUEUE_SIZE];

    size_t _size;
};

class DriverQueueWithWorkGroup : public FactoryMethod<DriverQueue, DriverQueueWithWorkGroup> {
    friend class FactoryMethod<DriverQueue, DriverQueueWithWorkGroup>;

public:
    ~DriverQueueWithWorkGroup() override = default;
    void close() override;

    void put_back(const DriverRawPtr driver, bool from_dispatcher) override;
    void put_back(const std::vector<DriverRawPtr>& drivers, bool from_dispatcher) override;
    StatusOr<DriverRawPtr> take() override;

    void yield_driver(const DriverRawPtr driver) override;

    size_t size() override;

private:
    static constexpr int64_t DISPATCH_LATENCY_NS = 200'1000'1000;

    void _put_back(const DriverRawPtr driver, bool from_dispatcher);
    workgroup::WorkGroup* _get_min_wg();
    int64_t _ideal_runtime_ns(workgroup::WorkGroup* wg);

    std::mutex _global_mutex;
    std::condition_variable _cv;
    std::unordered_set<workgroup::WorkGroup*> _wgs;
    size_t _sum_cpu_limit = 0;

    bool _is_closed = false;
};

} // namespace pipeline
} // namespace starrocks
