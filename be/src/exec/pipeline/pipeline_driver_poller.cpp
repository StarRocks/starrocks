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
    if (!this->_is_shutdown.load() && _polling_thread.get() != nullptr) {
        this->_is_shutdown.store(true, std::memory_order_release);
        _cond.notify_one();
        _polling_thread->join();
    }
}

void PipelineDriverPoller::run_internal() {
    this->_is_polling_thread_initialized.store(true, std::memory_order_release);
    DriverList tmp_blocked_drivers;
    int spin_count = 0;
    std::vector<DriverRawPtr> ready_drivers;
    while (!_is_shutdown.load(std::memory_order_acquire)) {
        {
            std::unique_lock<std::mutex> lock(_global_mutex);
            tmp_blocked_drivers.splice(tmp_blocked_drivers.end(), _blocked_drivers);
            if (_local_blocked_drivers.empty() && tmp_blocked_drivers.empty() && _blocked_drivers.empty()) {
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
                tmp_blocked_drivers.splice(tmp_blocked_drivers.end(), _blocked_drivers);
            }
        }

        {
            std::unique_lock write_lock(_local_mutex);

            if (!tmp_blocked_drivers.empty()) {
                _local_blocked_drivers.splice(_local_blocked_drivers.end(), tmp_blocked_drivers);
            }

            auto driver_it = _local_blocked_drivers.begin();
            while (driver_it != _local_blocked_drivers.end()) {
                auto* driver = *driver_it;

                if (!driver->is_query_never_expired() && driver->query_ctx()->is_query_expired()) {
                    // there are not any drivers belonging to a query context can make progress for an expiration period
                    // indicates that some fragments are missing because of failed exec_plan_fragment invocation. in
                    // this situation, query is failed finally, so drivers are marked PENDING_FINISH/FINISH.
                    //
                    // If the fragment is expired when the source operator is already pending i/o task,
                    // The state of driver shouldn't be changed.
                    size_t expired_log_count = driver->fragment_ctx()->expired_log_count();
                    if (expired_log_count <= 10) {
                        LOG(WARNING) << "[Driver] Timeout, query_id=" << print_id(driver->query_ctx()->query_id())
                                     << ", instance_id=" << print_id(driver->fragment_ctx()->fragment_instance_id());
                        driver->fragment_ctx()->set_expired_log_count(++expired_log_count);
                    }
                    driver->fragment_ctx()->cancel(
                            Status::TimedOut(fmt::format("Query exceeded time limit of {} seconds",
                                                         driver->query_ctx()->get_query_expire_seconds())));
                    driver->cancel_operators(driver->fragment_ctx()->runtime_state());
                    if (driver->is_still_pending_finish()) {
                        driver->set_driver_state(DriverState::PENDING_FINISH);
                        ++driver_it;
                    } else {
                        driver->set_driver_state(DriverState::FINISH);
                        remove_blocked_driver(_local_blocked_drivers, driver_it);
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
                        remove_blocked_driver(_local_blocked_drivers, driver_it);
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
                        remove_blocked_driver(_local_blocked_drivers, driver_it);
                        ready_drivers.emplace_back(driver);
                    }
                } else if (driver->is_epoch_finishing()) {
                    if (driver->is_still_epoch_finishing()) {
                        ++driver_it;
                    } else {
                        driver->set_driver_state(driver->fragment_ctx()->is_canceled() ? DriverState::CANCELED
                                                                                       : DriverState::EPOCH_FINISH);
                        remove_blocked_driver(_local_blocked_drivers, driver_it);
                        ready_drivers.emplace_back(driver);
                    }
                } else if (driver->is_epoch_finished()) {
                    remove_blocked_driver(_local_blocked_drivers, driver_it);
                    ready_drivers.emplace_back(driver);
                } else if (driver->is_finished()) {
                    remove_blocked_driver(_local_blocked_drivers, driver_it);
                    ready_drivers.emplace_back(driver);
                } else if (driver->is_not_blocked()) {
                    driver->set_driver_state(DriverState::READY);
                    remove_blocked_driver(_local_blocked_drivers, driver_it);
                    ready_drivers.emplace_back(driver);
                } else {
                    ++driver_it;
                }
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
    std::unique_lock<std::mutex> lock(_global_mutex);
    _blocked_drivers.push_back(driver);
    _blocked_driver_queue_len++;
    driver->_pending_timer_sw->reset();
    _cond.notify_one();
}

void PipelineDriverPoller::park_driver(const DriverRawPtr driver) {
    std::unique_lock<std::mutex> lock(_global_parked_mutex);
    VLOG_ROW << "Add to parked driver:" << driver->to_readable_string();
    _parked_drivers.push_back(driver);
}

// activate the parked driver from poller
size_t PipelineDriverPoller::activate_parked_driver(const ImmutableDriverPredicateFunc& predicate_func) {
    std::vector<DriverRawPtr> ready_drivers;

    {
        std::unique_lock<std::mutex> lock(_global_parked_mutex);
        for (auto driver_it = _parked_drivers.begin(); driver_it != _parked_drivers.end();) {
            auto driver = *driver_it;
            if (predicate_func(driver)) {
                VLOG_ROW << "Active parked driver:" << driver->to_readable_string();
                driver->set_driver_state(DriverState::READY);
                ready_drivers.push_back(driver);
                driver_it = _parked_drivers.erase(driver_it);
            } else {
                driver_it++;
            }
        }
    }

    _driver_queue->put_back(ready_drivers);
    return ready_drivers.size();
}

size_t PipelineDriverPoller::calculate_parked_driver(const ImmutableDriverPredicateFunc& predicate_func) const {
    size_t parked_driver_num = 0;
    auto driver_it = _parked_drivers.begin();
    while (driver_it != _parked_drivers.end()) {
        auto driver = *driver_it;
        if (predicate_func(driver)) {
            parked_driver_num += 1;
        }
        driver_it++;
    }
    return parked_driver_num;
}

void PipelineDriverPoller::remove_blocked_driver(DriverList& local_blocked_drivers, DriverList::iterator& driver_it) {
    auto& driver = *driver_it;
    driver->_pending_timer->update(driver->_pending_timer_sw->elapsed_time());
    local_blocked_drivers.erase(driver_it++);
    _blocked_driver_queue_len--;
}

void PipelineDriverPoller::iterate_immutable_driver(const IterateImmutableDriverFunc& call) const {
    std::shared_lock guard(_local_mutex);
    for (auto* driver : _local_blocked_drivers) {
        call(driver);
    }
}

} // namespace starrocks::pipeline
