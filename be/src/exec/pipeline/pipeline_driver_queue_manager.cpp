// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/pipeline_driver_queue_manager.h"

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
    _pl.close();
}

StatusOr<DriverRawPtr> DriverQueueManager::take(int dispatcher_id, size_t* queue_index, bool* is_from_remote) {
    *is_from_remote = false;

    // 1. take from own local queue.
    DriverRawPtr driver = _queue_per_dispatcher[dispatcher_id]->take(queue_index);
    if (driver != nullptr) {
        return driver;
    }

    for (;;) {
        auto local_state = _pl.get_state();
        if (local_state.closed()) {
            return Status::Cancelled("Shutdown");
        }

        // 2. take from own remote queue.
        auto driver = _remote_queue_per_dispatcher[dispatcher_id]->take(queue_index);
        if (driver != nullptr) {
            *is_from_remote = true;
            return driver;
        }

        int pos = _random_dispatcher_id();
        const int offset = _rand_step_sizes[pos];
        for (int i = 0; i < _num_dispatchers; i++) {
            const int steal_id = pos % _num_dispatchers;
            pos += offset;

            // 3. steal from other local queue.
            std::vector<DriverRawPtr> steal_drivers = _queue_per_dispatcher[steal_id]->steal();
            if (!steal_drivers.empty()) {
                return _queue_per_dispatcher[dispatcher_id]->put_back_and_take(steal_drivers, queue_index);
            }

            // 4. steal from other remote queue.
            steal_drivers = _remote_queue_per_dispatcher[steal_id]->steal();
            if (!steal_drivers.empty()) {
                return _queue_per_dispatcher[dispatcher_id]->put_back_and_take(steal_drivers, queue_index);
            }
        }

        _pl.wait(local_state);
    }
}

void DriverQueueManager::put_back(int dispatcher_id, const DriverRawPtr driver) {
    if (dispatcher_id >= 0) {
        _queue_per_dispatcher[dispatcher_id]->put_back(driver);
    } else {
        dispatcher_id = _random_dispatcher_id();
        _remote_queue_per_dispatcher[dispatcher_id]->put_back(driver);
        _pl.notify_one();
    }
}

void DriverQueueManager::put_back(int dispatcher_id, const std::vector<DriverRawPtr>& drivers) {
    if (dispatcher_id >= 0) {
        _queue_per_dispatcher[dispatcher_id]->put_back(drivers);
    } else {
        std::vector<std::vector<DriverRawPtr>> ready_drivers_per_dispatcher(_num_dispatchers);
        for (auto driver : drivers) {
            ready_drivers_per_dispatcher[_random_dispatcher_id()].emplace_back(driver);
        }

        for (int i = 0; i < _num_dispatchers; i++) {
            if (!ready_drivers_per_dispatcher[i].empty()) {
                _remote_queue_per_dispatcher[i]->put_back(ready_drivers_per_dispatcher[i]);
            }
        }

        _pl.notify(drivers.size());
    }
}

SubQuerySharedDriverQueue* DriverQueueManager::get_sub_queue(int dispatcher_id, size_t queue_index,
                                                             bool is_from_remote) {
    if (is_from_remote) {
        return _remote_queue_per_dispatcher[dispatcher_id]->get_sub_queue(queue_index);
    }

    return _queue_per_dispatcher[dispatcher_id]->get_sub_queue(queue_index);
}

} // namespace starrocks::pipeline
