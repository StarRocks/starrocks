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

#include "common/config.h"
#include "exec/pipeline/pipeline_fwd.h"
namespace starrocks::pipeline {

void PipelineDriverPoller::start() {
    DCHECK(this->_polling_thread.get() == nullptr);
    auto status = Thread::create(
            "pipeline", "pipeline_poller", [this]() { run_internal(); }, &this->_polling_thread);
    if (!status.ok()) {
        LOG(FATAL) << "Fail to create PipelineDriverPoller: error=" << status.to_string();
    }

    status = Thread::create(
            "pipeline", "pipeline_backup_poller", [this]() { timepoint_run_internal(); }, &this->_backup_thread);
    if (!status.ok()) {
        LOG(FATAL) << "Fail to create PipelineDriverBackupPoller: error=" << status.to_string();
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
        _backup_thread->join();
    }
}

void PipelineDriverPoller::run_internal() {
    this->_is_polling_thread_initialized.store(true, std::memory_order_release);
    DriverList tmp_blocked_drivers;
    int spin_count = 0;
    std::vector<DriverRawPtr> ready_drivers;
    while (!_is_shutdown.load(std::memory_order_acquire)) {
        _poll_count++;
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
                        LOG(WARNING) << "[Driver] Timeout " << driver->to_readable_string();
                        driver->fragment_ctx()->set_expired_log_count(++expired_log_count);
                    }
                    driver->fragment_ctx()->cancel(
                            Status::TimedOut(fmt::format("Query exceeded time limit of {} seconds",
                                                         driver->query_ctx()->get_query_expire_seconds())));

                    LOG(INFO) << "Driver is exprired:" << driver->to_readable_string();
                    on_cancel(driver, ready_drivers, _local_blocked_drivers, driver_it);
                } else if (driver->fragment_ctx()->is_canceled()) {
                    // If the fragment is cancelled when the source operator is already pending i/o task,
                    // The state of driver shouldn't be changed.
                    LOG(INFO) << "Driver is cancel:" << driver->to_readable_string();
                    on_cancel(driver, ready_drivers, _local_blocked_drivers, driver_it);
                } else if (driver->need_report_exec_state()) {
                    // If the runtime profile is enabled, the driver should be rescheduled after the timeout for triggering
                    // the profile report prcessing.
                    LOG(INFO) << "Driver is report:" << driver->to_readable_string();
                    remove_blocked_driver(_local_blocked_drivers, driver_it);
                    ready_drivers.emplace_back(driver);
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
                        LOG(INFO) << "Driver is pending finish:" << driver->to_readable_string();
                        driver->set_driver_state(driver->fragment_ctx()->is_canceled() ? DriverState::CANCELED
                                                                                       : DriverState::FINISH);
                        remove_blocked_driver(_local_blocked_drivers, driver_it);
                        ready_drivers.emplace_back(driver);
                    }
                } else if (driver->is_epoch_finishing()) {
                    if (driver->is_still_epoch_finishing()) {
                        ++driver_it;
                    } else {
                        LOG(INFO) << "Driver is epoch_finishing:" << driver->to_readable_string();
                        driver->set_driver_state(driver->fragment_ctx()->is_canceled() ? DriverState::CANCELED
                                                                                       : DriverState::EPOCH_FINISH);
                        remove_blocked_driver(_local_blocked_drivers, driver_it);
                        ready_drivers.emplace_back(driver);
                    }
                } else if (driver->is_epoch_finished()) {
                    LOG(INFO) << "Driver is epoch_finish:" << driver->to_readable_string();
                    remove_blocked_driver(_local_blocked_drivers, driver_it);
                    ready_drivers.emplace_back(driver);
                } else if (driver->is_finished()) {
                    LOG(INFO) << "Driver is finish:" << driver->to_readable_string();
                    remove_blocked_driver(_local_blocked_drivers, driver_it);
                    ready_drivers.emplace_back(driver);
                } else {
                    auto status_or_is_not_blocked = driver->is_not_blocked();
                    if (!status_or_is_not_blocked.ok()) {
                        LOG(INFO) << "Driver is still cancel:" << driver->to_readable_string();
                        driver->fragment_ctx()->cancel(status_or_is_not_blocked.status());
                        on_cancel(driver, ready_drivers, _local_blocked_drivers, driver_it);
                    } else if (status_or_is_not_blocked.value()) {
                        LOG(INFO) << "Driver is still ready:" << driver->to_readable_string();
                        driver->set_driver_state(DriverState::READY);
                        remove_blocked_driver(_local_blocked_drivers, driver_it);
                        ready_drivers.emplace_back(driver);
                    } else {
                        ++driver_it;
                    }
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
#elif defined __aarch64__
            // A "yield" instruction in aarch64 is essentially a nop, and does
            // not cause enough delay to help backoff. "isb" is a barrier that,
            // especially inside a loop, creates a small delay without consuming
            // ALU resources.  Experiments shown that adding the isb instruction
            // improves stability and reduces result jitter. Adding more delay
            // to the UT_RELAX_CPU than a single isb reduces performance.
            asm volatile("isb" ::: "memory");
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

void PipelineDriverPoller::timepoint_run_internal() {
    DriverList tmp_drivers;
    std::vector<DriverRawPtr> ready_drivers;
    while (!_is_shutdown.load(std::memory_order_acquire)) {
        _backup_count++;
        {
            std::unique_lock<std::mutex> lock(_backup_mutex);
            tmp_drivers.assign(_backup_drivers.begin(), _backup_drivers.end());
            LOG(INFO) << "Driver backup queue size2:" << _backup_drivers.size();
        }
        auto driver_it = tmp_drivers.begin();
        ready_drivers.clear();
        while (driver_it != tmp_drivers.end()) {
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
                    LOG(WARNING) << "[Driver] Timeout " << driver->to_readable_string();
                    driver->fragment_ctx()->set_expired_log_count(++expired_log_count);
                }
                LOG(INFO) << "Driver is exprired:" << driver->to_readable_string();
                driver->fragment_ctx()->cancel(Status::TimedOut(fmt::format(
                        "Query exceeded time limit of {} seconds", driver->query_ctx()->get_query_expire_seconds())));
                on_cancel2(driver, ready_drivers);
            } else if (driver->fragment_ctx()->is_canceled()) {
                LOG(INFO) << "Driver is cancel:" << driver->to_readable_string();
                on_cancel2(driver, ready_drivers);
            } else if (driver->need_report_exec_state()) {
                LOG(INFO) << "Driver is report:" << driver->to_readable_string();
                ready_drivers.emplace_back(driver);
            } else if ((driver->pending_finish())) {
                LOG(INFO) << "Driver is pending finish:" << driver->is_still_pending_finish() << ", "
                          << driver->to_readable_string();
                if (!driver->is_still_pending_finish()) {
                    driver->set_driver_state(driver->fragment_ctx()->is_canceled() ? DriverState::CANCELED
                                                                                   : DriverState::FINISH);
                    ready_drivers.emplace_back(driver);
                }
            } else if ((driver->is_epoch_finishing())) {
                LOG(INFO) << "Driver is epoch_finishing:" << driver->is_still_epoch_finishing() << ", "
                          << driver->to_readable_string();
                if (!driver->is_still_epoch_finishing()) {
                    driver->set_driver_state(driver->fragment_ctx()->is_canceled() ? DriverState::CANCELED
                                                                                   : DriverState::EPOCH_FINISH);
                    ready_drivers.emplace_back(driver);
                }
            } else if (driver->is_epoch_finished() || driver->is_finished()) {
                LOG(INFO) << "Driver is finish:" << driver->to_readable_string();
                ready_drivers.emplace_back(driver);
            } else {
                auto status_or_is_not_blocked = driver->is_not_blocked();
                if (!status_or_is_not_blocked.ok()) {
                    LOG(INFO) << "Driver is still cancel:" << driver->to_readable_string();
                    on_cancel2(driver, ready_drivers);
                } else if (status_or_is_not_blocked.value()) {
                    LOG(INFO) << "Driver is still ready:" << driver->to_readable_string();
                    driver->set_driver_state(DriverState::READY);
                    ready_drivers.emplace_back(driver);
                } else {
                    LOG(INFO) << "Driver is not ready:" << driver->to_readable_string();
                }
            }
            driver_it++;
        }

        tmp_drivers.clear();
        if (!ready_drivers.empty()) {
            {
                std::unique_lock<std::mutex> lock(_backup_mutex);
                for (auto driver : ready_drivers) {
                    _backup_drivers.remove(driver);
                }
                LOG(INFO) << "Driver backup queue size3:" << _backup_drivers.size();
            }
            _driver_queue->put_back(ready_drivers);
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

// void PipelineDriverPoller::add_blocked_driver(const DriverRawPtr driver) {
//     std::unique_lock<std::mutex> lock(_global_mutex);
//     _blocked_drivers.push_back(driver);
//     _blocked_driver_queue_len++;
//     driver->_pending_timer_sw->reset();
//     driver->driver_acct().clean_local_queue_infos();
//     _cond.notify_one();
// }

bool PipelineDriverPoller::_driver_is_ready(DriverRawPtr driver, const std::string& msg) const {
    if (!driver->is_query_never_expired() && driver->query_ctx()->is_query_expired()) {
        // time driver
        return false;
    } else if (driver->fragment_ctx()->is_canceled() || driver->need_report_exec_state()) {
        // time driver
        return false;
    } else if (driver->pending_finish()) {
        LOG(INFO) << msg << " pending_finish:" << driver->is_still_pending_finish() << ", "
                  << driver->to_readable_string();
        if (!driver->is_still_pending_finish()) {
            driver->set_driver_state(driver->fragment_ctx()->is_canceled() ? DriverState::CANCELED
                                                                           : DriverState::FINISH);
            return true;
        }
    } else if (driver->is_epoch_finishing()) {
        LOG(INFO) << msg << " epoch finishing:" << driver->is_still_epoch_finishing() << ", "
                  << driver->to_readable_string();
        if (!driver->is_still_epoch_finishing()) {
            driver->set_driver_state(driver->fragment_ctx()->is_canceled() ? DriverState::CANCELED
                                                                           : DriverState::EPOCH_FINISH);
            return true;
        }
    } else if (driver->is_epoch_finished() || driver->is_finished()) {
        LOG(INFO) << msg << " finished:" << driver->to_readable_string();
        return true;
    } else {
        auto status_or_is_not_blocked = driver->is_not_blocked();
        if (status_or_is_not_blocked.ok() && status_or_is_not_blocked.value()) {
            LOG(INFO) << msg << " ready:" << driver->to_readable_string();
            driver->set_driver_state(DriverState::READY);
            return true;
        }
    }
    return false;
}

void PipelineDriverPoller::add_blocked_driver(const DriverRawPtr driver) {
    if (config::enable_pipeline_event_poller) {
        bool is_ready = _driver_is_ready(driver, "Driver put");
        if (is_ready) {
            _driver_queue->put_back(driver);
            return;
        }
        LOG(INFO) << "Driver put queue:" << driver->to_readable_string();
        std::unique_lock<std::mutex> lock(_backup_mutex);
        _backup_drivers.push_back(driver);
        return;
    }

    std::unique_lock<std::mutex> lock(_global_mutex);
    _blocked_drivers.push_back(driver);
    _blocked_driver_queue_len++;
    driver->_pending_timer_sw->reset();
    driver->driver_acct().clean_local_queue_infos();
    _cond.notify_one();
}

void PipelineDriverPoller::upgrade_to_blocked_driver(const DriverRawPtr driver) {
    {
        std::unique_lock<std::mutex> lock(_backup_mutex);
        if (_backup_drivers.remove(driver) < 1) {
            return;
        }
    }
    {
        std::unique_lock<std::mutex> lock(_global_mutex);
        _blocked_drivers.push_back(driver);
        _blocked_driver_queue_len++;
        driver->_pending_timer_sw->reset();
        driver->driver_acct().clean_local_queue_infos();
        _cond.notify_one();
    }
}

void PipelineDriverPoller::driver_event_action(const DriverRawPtr driver) {
    bool is_ready = _driver_is_ready(driver, "Driver event action ");
    bool in_backup = false;
    if (is_ready) {
        std::unique_lock<std::mutex> lock(_backup_mutex);
        in_backup = _backup_drivers.remove(driver) > 0;

        if (in_backup) {
            driver->_pending_timer_sw->reset();
            driver->driver_acct().clean_local_queue_infos();
            _driver_queue->put_back(driver);
        }
    }
    LOG(INFO) << "Driver event action: " << driver->to_readable_string() << ", is_ready: " << is_ready
              << ", in_backup: " << in_backup;
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
    _ready_count++;
    auto& driver = *driver_it;
    driver->_pending_timer->update(driver->_pending_timer_sw->elapsed_time());
    local_blocked_drivers.erase(driver_it++);
    _blocked_driver_queue_len--;
}

void PipelineDriverPoller::on_cancel(DriverRawPtr driver, std::vector<DriverRawPtr>& ready_drivers,
                                     DriverList& local_blocked_drivers, DriverList::iterator& driver_it) {
    driver->cancel_operators(driver->fragment_ctx()->runtime_state());
    if (driver->is_still_pending_finish()) {
        driver->set_driver_state(DriverState::PENDING_FINISH);
        ++driver_it;
    } else {
        driver->set_driver_state(DriverState::CANCELED);
        remove_blocked_driver(local_blocked_drivers, driver_it);
        ready_drivers.emplace_back(driver);
    }
}

void PipelineDriverPoller::on_cancel2(DriverRawPtr driver, std::vector<DriverRawPtr>& ready_drivers) {
    driver->cancel_operators(driver->fragment_ctx()->runtime_state());
    if (driver->is_still_pending_finish()) {
        driver->set_driver_state(DriverState::PENDING_FINISH);
    } else {
        driver->set_driver_state(DriverState::CANCELED);
        ready_drivers.emplace_back(driver);
    }
}

void PipelineDriverPoller::iterate_immutable_driver(const IterateImmutableDriverFunc& call) const {
    std::shared_lock guard(_local_mutex);
    for (auto* driver : _local_blocked_drivers) {
        call(driver);
    }
}

} // namespace starrocks::pipeline
