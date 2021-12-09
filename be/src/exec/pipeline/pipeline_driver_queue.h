// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <queue>

#include "exec/pipeline/pipeline_driver.h"
#include "util/factory_method.h"
namespace starrocks {
namespace pipeline {
class DriverQueue;
using DriverQueuePtr = std::unique_ptr<DriverQueue>;

class SubQuerySharedDriverQueue {
public:
    void update_accu_time(const DriverRawPtr driver) {
        _accu_consume_time.fetch_add(driver->driver_acct().get_last_time_spent());
    }

    double accu_time_after_divisor() { return _accu_consume_time.load() / factor_for_normal; }

    std::deque<DriverRawPtr> queue;
    // factor for normalization
    double factor_for_normal = 0;

private:
    std::atomic<int64_t> _accu_consume_time = 0;
};

class DriverQueue {
public:
    virtual ~DriverQueue() = default;

    virtual void put_back(const DriverRawPtr driver) = 0;
    virtual void put_back(const std::vector<DriverRawPtr>& drivers) = 0;
    virtual DriverRawPtr put_back_and_take(const std::vector<DriverRawPtr>& drivers, size_t* queue_index) = 0;

    virtual DriverRawPtr take(size_t* queue_index) = 0;
    virtual std::vector<DriverRawPtr> steal() = 0;

    virtual SubQuerySharedDriverQueue* get_sub_queue(size_t) = 0;
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

    static const size_t QUEUE_SIZE = 8;
    // maybe other value for ratio.
    static constexpr double RATIO_OF_ADJACENT_QUEUE = 1.7;

    void put_back(const DriverRawPtr driver) override;
    void put_back(const std::vector<DriverRawPtr>& drivers) override;
    DriverRawPtr put_back_and_take(const std::vector<DriverRawPtr>& drivers, size_t* queue_index) override;

    // return nullptr if queue is closed;
    DriverRawPtr take(size_t* queue_index) override;
    std::vector<DriverRawPtr> steal() override;

    SubQuerySharedDriverQueue* get_sub_queue(size_t) override;

private:
    // _do_take must hold _global_mutex.
    DriverRawPtr _do_take(size_t* queue_index);

    std::mutex _global_mutex;

    SubQuerySharedDriverQueue _queues[QUEUE_SIZE];
    int _size = 0;
};

} // namespace pipeline
} // namespace starrocks
