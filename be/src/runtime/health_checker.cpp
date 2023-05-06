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

#include "runtime/health_checker.h"
namespace starrocks {

HealthChecker::HealthChecker() : _thread( [this] { execute(); }), _stop(false) {
    Thread::set_thread_name(_thread, "HealthChecker");
}

Status HealthChecker::register_monitor(std::string monitor_name, BaseMonitor* monitor) {
    LOG(INFO) << "register_monitor " << monitor_name;
    std::lock_guard lg(_health_checker_mutex);
    if (_monitor_holder.find(monitor_name) != _monitor_holder.end()) {
        std::stringstream msg;
        msg << "monitor name " << monitor_name << " has been registered";
        LOG(WARNING) << msg.str();
        return Status::InternalError(msg.str());
    }
    _monitor_holder.emplace(monitor_name, monitor);
    return Status::OK();
}

void HealthChecker::register_monitors_to_http_action(MonitorAction* monitor_action) {
    std::lock_guard lg(_health_checker_mutex);
    for (const auto& iter : _monitor_holder) {
        LOG(INFO) << "register monitor " << iter.first << " to MonitorAction";
        monitor_action->register_module(iter.first, iter.second);
    }

}

std::unordered_map<std::string, BaseMonitor*> HealthChecker::get_all_monitors() {
    return _monitor_holder;
}

void HealthChecker::start_all_monitors() {
    std::lock_guard lg(_health_checker_mutex);
    for (const auto& iter : _monitor_holder) {
        iter.second->start();
    }
}

void HealthChecker::_collect_all_monitor_status() {
    std::lock_guard lg(_health_checker_mutex);
    for (const auto& iter : _monitor_holder) {
        if (!iter.second->getStatus().ok()) {
            LOG(WARNING) << "monitor " << iter.first << " not normal " << iter.second->getStatus().code_as_string();
            _monitor_unnormal.emplace(iter);
        }
    }
}

void HealthChecker::execute() {
    LOG(INFO) << "HealthChecker start working.";

    int32_t interval = config::health_check_interval;

    while (!_stop.load(std::memory_order_consume)) {
       _collect_all_monitor_status();

        if (interval <= 0) {
            LOG(WARNING) << "health_check_interval config is illegal: " << interval << ", force set to 1";
            interval = 1;
        }
        int32_t left_seconds = interval;
        while (!_stop.load(std::memory_order_consume) && left_seconds > 0) {
            sleep(1);
            --left_seconds;
        }
    }
    LOG(INFO) << "HealthChecker going to exit.";
}


void HealthChecker::stop() {
    _stop.store(true, std::memory_order_release);
    _thread.join();
}

HealthChecker::~HealthChecker() {
}

} // namespace starrocks
