// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/pipeline_driver_queue_manager.h"

#include <random>

namespace starrocks::pipeline {

int gcd(int x, int y) {
    return y > 0 ? gcd(y, x % y) : x;
}

void DriverQueueManager::initialize(int num_dispatchers) {
    _num_dispatchers = num_dispatchers;

    _queue_per_dispatcher.reserve(_num_dispatchers);
    _remote_queue_per_dispatcher.reserve(_num_dispatchers);
    for (int i = 0; i < _num_dispatchers; i++) {
        _queue_per_dispatcher.emplace_back(std::make_unique<QuerySharedDriverQueue>());
        _remote_queue_per_dispatcher.emplace_back(std::make_unique<QuerySharedDriverQueue>());
    }

    // Every step size is coprime with _num_dispatchers.
    _rand_step_sizes.reserve(_num_dispatchers);
    for (int i = 1; _rand_step_sizes.size() <= _num_dispatchers; i++) {
        if (gcd(i, _num_dispatchers) == 1) {
            _rand_step_sizes.emplace_back(i);
        }
    }
}

void DriverQueueManager::close() {
    for (auto& pl : _pls) {
        pl.close();
    }
}

StatusOr<DriverRawPtr> DriverQueueManager::take(int dispatcher_id, size_t* queue_index, bool* is_from_remote) {
    *is_from_remote = false;

    // 1. take from own local queue.
    DriverRawPtr driver = _queue_per_dispatcher[dispatcher_id]->take_own(queue_index);
    if (driver != nullptr) {
        driver->set_dispatcher_id(dispatcher_id);
        return driver;
    }

    for (;;) {
        auto local_state = _pls[dispatcher_id % NUM_PL].get_state();
        if (local_state.closed()) {
            return Status::Cancelled("Shutdown");
        }

        // 2. take from own remote queue.
        auto driver = _remote_queue_per_dispatcher[dispatcher_id]->take(queue_index);
        if (driver != nullptr) {
            driver->set_dispatcher_id(dispatcher_id);
            *is_from_remote = true;
            return driver;
        }

        int pos = _random_dispatcher_id();
        const int offset = _rand_step_sizes[pos];
        for (int i = 0; i < _num_dispatchers; i++) {
            const int steal_id = pos % _num_dispatchers;
            pos += offset;

            // 3. steal from other remote queue.
            driver = _remote_queue_per_dispatcher[steal_id]->take(queue_index);
            if (driver != nullptr) {
                driver->set_dispatcher_id(dispatcher_id);
                *is_from_remote = true;
                return driver;
            }

            // 4. steal from other local queue.
            driver = _queue_per_dispatcher[steal_id]->take(queue_index);
            if (driver != nullptr) {
                driver->set_dispatcher_id(dispatcher_id);
                return driver;
            }
        }

        _pls[dispatcher_id % NUM_PL].wait(local_state);
    }
}

void DriverQueueManager::put_back(const DriverRawPtr driver) {
    if (driver->dispatcher_id() >= 0) {
        _queue_per_dispatcher[driver->dispatcher_id()]->put_back(driver);
    } else {
        int dispatcher_id = _random_dispatcher_id();
        _remote_queue_per_dispatcher[dispatcher_id]->put_back(driver);
        notify(dispatcher_id, 1);
    }
}

void DriverQueueManager::put_back(const std::vector<DriverRawPtr>& drivers) {
    int dispatcher_id = 0;
    std::vector<std::vector<DriverRawPtr>> ready_drivers_per_dispatcher(_num_dispatchers);
    for (auto driver : drivers) {
        dispatcher_id = driver->dispatcher_id();
        if (dispatcher_id < 0) {
            dispatcher_id = _random_dispatcher_id();
        }
        ready_drivers_per_dispatcher[dispatcher_id].emplace_back(driver);
    }

    for (int i = 0; i < _num_dispatchers; i++) {
        if (!ready_drivers_per_dispatcher[i].empty()) {
            _remote_queue_per_dispatcher[i]->put_back(ready_drivers_per_dispatcher[i]);
        }
    }

    notify(dispatcher_id, drivers.size());
}

void DriverQueueManager::notify(int dispatcher_id, int num_drivers) {
    int pl_i = dispatcher_id % NUM_PL;
    for (int i = 0; i < NUM_PL && num_drivers > 0; i++) {
        num_drivers -= _pls[pl_i % NUM_PL].notify(num_drivers);
        pl_i++;
    }
}

SubQuerySharedDriverQueue* DriverQueueManager::get_sub_queue(int dispatcher_id, size_t queue_index,
                                                             bool is_from_remote) {
    if (is_from_remote) {
        return _remote_queue_per_dispatcher[dispatcher_id]->get_sub_queue(queue_index);
    }

    return _queue_per_dispatcher[dispatcher_id]->get_sub_queue(queue_index);
}

int DriverQueueManager::_random_dispatcher_id() {
    std::random_device rand_dev;
    std::mt19937 generator(rand_dev());
    std::uniform_int_distribution<int> distribution(0, _num_dispatchers - 1);
    return distribution(generator);
}

} // namespace starrocks::pipeline
