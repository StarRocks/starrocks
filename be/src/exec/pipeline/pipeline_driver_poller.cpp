// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "pipeline_driver_poller.h"

#include <emmintrin.h>

#include <chrono>
namespace starrocks {
namespace pipeline {

void PipelineDriverPoller::start() {
    DCHECK(this->_polling_thread == nullptr);
    auto status = Thread::create(
            "pipeline", "PipelineDriverPoller", [this]() { run_internal(); }, nullptr);
    if (!status.ok()) {
        LOG(FATAL) << "Fail to create PipelineDriverPoller: error=" << status.to_string();
    }
    while (!this->_is_polling_thread_initialized.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}
void PipelineDriverPoller::shutdown() {
    this->_is_shutdown.store(true, std::memory_order_release);
    this->_polling_thread->join();
}

void PipelineDriverPoller::run_internal() {
    this->_polling_thread = Thread::current_thread();
    this->_is_polling_thread_initialized.store(true, std::memory_order_release);
    typeof(this->_blocked_drivers) local_blocked_drivers;
    int spin_count = 0;
    while (!_is_shutdown.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lock(this->_mutex);
            local_blocked_drivers.splice(local_blocked_drivers.end(), _blocked_drivers);
            if (local_blocked_drivers.empty() && _blocked_drivers.empty()) {
                _cond.wait(lock, [this]() {
                    return !this->_is_shutdown.load(std::memory_order_acquire) && !this->_blocked_drivers.empty();
                });
                if (_is_shutdown.load(std::memory_order_acquire)) {
                    break;
                }
                local_blocked_drivers.splice(local_blocked_drivers.end(), _blocked_drivers);
            }
        }
        size_t previous_num_blocked_drivers = local_blocked_drivers.size();
        auto driver_it = local_blocked_drivers.begin();
        while (driver_it != local_blocked_drivers.end()) {
            auto& driver = *driver_it;
            // driver->pending_finish() return true means that when a driver's sink operator is finished,
            // but its source operator still has pending io task that executed in io threads and has
            // reference to object outside(such as desc_tbl) owned by FragmentContext. So an driver in
            // PENDING_FINISH state should should wait for pending io task's completion, then turn into
            // FINISH state, otherwise, pending tasks shall reference to destructed objects in
            // FragmentContext since FragmentContext is unregistered prematurely.
            if (driver->pending_finish() && !driver->source_operator()->pending_finish()) {
                driver->set_driver_state(DriverState::FINISH);
                _dispatch_queue->put_back(*driver_it);
                local_blocked_drivers.erase(driver_it++);
            } else if (driver->is_finished()) {
                local_blocked_drivers.erase(driver_it++);
            } else if (driver->fragment_ctx()->is_canceled() || driver->is_not_blocked()) {
                _dispatch_queue->put_back(*driver_it);
                local_blocked_drivers.erase(driver_it++);
            } else {
                ++driver_it;
            }
        }
        size_t curr_num_blocked_drivers = local_blocked_drivers.size();
        if (curr_num_blocked_drivers == previous_num_blocked_drivers) {
            spin_count += 1;
        } else {
            spin_count = 0;
        }
        if (spin_count != 0 && spin_count % 64 == 0) {
            _mm_pause();
        }
        if (spin_count != 0 && spin_count == 640) {
            spin_count = 0;
            sched_yield();
        }
    }
}

void PipelineDriverPoller::add_blocked_driver(DriverPtr driver) {
    std::unique_lock<std::mutex> lock(this->_mutex);
    this->_blocked_drivers.push_back(driver);
    this->_cond.notify_one();
}

} // namespace pipeline
} // namespace starrocks
