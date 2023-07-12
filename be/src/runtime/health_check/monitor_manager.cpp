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

#include "runtime/health_check/monitor_manager.h"
namespace starrocks {

MonitorManager::MonitorManager() {}

Status MonitorManager::register_monitor(std::string monitor_name, BaseMonitor* monitor) {
    LOG(INFO) << "register monitor " << monitor_name;
    std::lock_guard lg(_monitor_manager_mutex);
    if (_monitor_holder.find(monitor_name) != _monitor_holder.end()) {
        std::stringstream msg;
        msg << "monitor name " << monitor_name << " has been registered";
        LOG(WARNING) << msg.str();
        return Status::InternalError(msg.str());
    }
    _monitor_holder.emplace(monitor_name, monitor);
    return Status::OK();
}

void MonitorManager::register_monitors_to_http_action(MonitorAction* monitor_action) {
    std::lock_guard lg(_monitor_manager_mutex);
    for (const auto& iter : _monitor_holder) {
        LOG(INFO) << "register monitor " << iter.first << " to MonitorAction";
        monitor_action->register_module(iter.first, iter.second);
    }
}

std::unordered_map<std::string, BaseMonitor*> MonitorManager::get_all_monitors() {
    return _monitor_holder;
}

void MonitorManager::start_all_monitors() {
    std::lock_guard lg(_monitor_manager_mutex);
    for (const auto& iter : _monitor_holder) {
        iter.second->start();
    }
}

void MonitorManager::stop_all_monitors() {
    std::lock_guard lg(_monitor_manager_mutex);
    for (const auto& iter : _monitor_holder) {
        iter.second->stop();
    }
}

MonitorManager::~MonitorManager() {}

} // namespace starrocks
