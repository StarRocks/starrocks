// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "pipeline_driver_poller.h"

#include <chrono>
namespace starrocks::pipeline {

void PipelineDriverPoller::start() {
    DCHECK(this->_polling_thread.get() == nullptr);
    auto status = Thread::create(
            "pipeline", "pipeline_poller", [this]() { run_internal(); }, &this->_polling_thread);
    if (!status.ok()) {
        LOG(FATAL) << "Fail to create PipelineDriverPoller: error=" << status.to_string();
    }
    while (!this->_is_polling_thread_initialized.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void PipelineDriverPoller::shutdown() {
    if (this->_is_shutdown.load() == false && _polling_thread.get() != nullptr) {
        this->_is_shutdown.store(true, std::memory_order_release);
        _cond.notify_one();
        _polling_thread->join();
    }
}

void PipelineDriverPoller::run_internal() {
    this->_is_polling_thread_initialized.store(true, std::memory_order_release);
    DriverList local_blocked_drivers;
    int spin_count = 0;
    std::vector<DriverRawPtr> ready_drivers;
    while (!_is_shutdown.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lock(this->_mutex);
            local_blocked_drivers.splice(local_blocked_drivers.end(), _blocked_drivers);
            if (local_blocked_drivers.empty() && _blocked_drivers.empty()) {
                std::cv_status cv_status = std::cv_status::no_timeout;
                while (!_is_shutdown.load(std::memory_order_acquire) && this->_blocked_drivers.empty()) {
                    cv_status = _cond.wait_for(lock, std::chrono::milliseconds(10));
                }
                if (cv_status == std::cv_status::timeout) {
                    continue;
                }
                if (_is_shutdown.load(std::memory_order_acquire)) {
                    break;
                }
                local_blocked_drivers.splice(local_blocked_drivers.end(), _blocked_drivers);
            }
        }

        auto driver_it = local_blocked_drivers.begin();
        while (driver_it != local_blocked_drivers.end()) {
            auto* driver = *driver_it;

            if (driver->query_ctx()->is_expired()) {
                // there are not any drivers belonging to a query context can make progress for an expiration period
                // indicates that some fragments are missing because of failed exec_plan_fragment invocation. in
                // this situation, query is failed finally, so drivers are marked PENDING_FINISH/FINISH.
                //
                // If the fragment is expired when the source operator is already pending i/o task,
                // The state of driver shouldn't be changed.
                LOG(WARNING) << "[Driver] Timeout, query_id=" << print_id(driver->query_ctx()->query_id())
                             << ", instance_id=" << print_id(driver->fragment_ctx()->fragment_instance_id());
                driver->fragment_ctx()->cancel(Status::TimedOut(fmt::format(
                        "Query exceeded time limit of {} seconds", driver->query_ctx()->get_expire_seconds())));
                driver->cancel_operators(driver->fragment_ctx()->runtime_state());
                if (driver->is_still_pending_finish()) {
                    driver->set_driver_state(DriverState::PENDING_FINISH);
                    ++driver_it;
                } else {
                    driver->set_driver_state(DriverState::FINISH);
                    remove_blocked_driver(local_blocked_drivers, driver_it);
                    ready_drivers.emplace_back(driver);
                }
            } else if (driver->fragment_ctx()->is_canceled()) {
                // If the fragment is cancelled when the source operator is already pending i/o task,
                // The state of driver shouldn't be changed.
                driver->cancel_operators(driver->fragment_ctx()->runtime_state());
                if (driver->is_still_pending_finish()) {
                    driver->set_driver_state(DriverState::PENDING_FINISH);
                    ++driver_it;
                } else {
                    driver->set_driver_state(DriverState::CANCELED);
                    remove_blocked_driver(local_blocked_drivers, driver_it);
                    ready_drivers.emplace_back(driver);
                }
            } else if (driver->pending_finish()) {
                if (driver->is_still_pending_finish()) {
                    ++driver_it;
                } else {
                    // driver->pending_finish() return true means that when a driver's sink operator is finished,
                    // but its source operator still has pending io task that executed in io threads and has
                    // reference to object outside(such as desc_tbl) owned by FragmentContext. So a driver in
                    // PENDING_FINISH state should wait for pending io task's completion, then turn into FINISH state,
                    // otherwise, pending tasks shall reference to destructed objects in FragmentContext since
                    // FragmentContext is unregistered prematurely.
                    driver->set_driver_state(driver->fragment_ctx()->is_canceled() ? DriverState::CANCELED
                                                                                   : DriverState::FINISH);
                    remove_blocked_driver(local_blocked_drivers, driver_it);
                    ready_drivers.emplace_back(driver);
                }
            } else if (driver->is_finished()) {
                remove_blocked_driver(local_blocked_drivers, driver_it);
                ready_drivers.emplace_back(driver);
            } else if (driver->is_not_blocked()) {
                driver->set_driver_state(DriverState::READY);
                remove_blocked_driver(local_blocked_drivers, driver_it);
                ready_drivers.emplace_back(driver);
            } else {
                ++driver_it;
            }
        }

        if (ready_drivers.empty()) {
            spin_count += 1;
        } else {
            spin_count = 0;

            _driver_queue->put_back(ready_drivers);
            ready_drivers.clear();
        }

        if (spin_count != 0 && spin_count % 64 == 0) {
#ifdef __x86_64__
            _mm_pause();
#else
            // TODO: Maybe there's a better intrinsic like _mm_pause on non-x86_64 architecture.
            sched_yield();
#endif
        }
        if (spin_count == 640) {
            spin_count = 0;
            sched_yield();
        }
    }
}

void PipelineDriverPoller::add_blocked_driver(const DriverRawPtr driver) {
    std::unique_lock<std::mutex> lock(_mutex);
    _blocked_drivers.push_back(driver);
    driver->_pending_timer_sw->reset();
    _cond.notify_one();
}

void PipelineDriverPoller::remove_blocked_driver(DriverList& local_blocked_drivers, DriverList::iterator& driver_it) {
    auto& driver = *driver_it;
    driver->_pending_timer->update(driver->_pending_timer_sw->elapsed_time());
    local_blocked_drivers.erase(driver_it++);
}

} // namespace starrocks::pipeline
