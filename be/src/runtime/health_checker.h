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

#pragma once

#include "http/monitor_action.h"
#include "runtime/base_monitor.h"
#include "util/thread.h"
#include "common/config.h"

namespace starrocks {

class HealthChecker {
public:
    HealthChecker();
    ~HealthChecker();
    void execute();
    Status register_monitor(std::string monitor_name, BaseMonitor* monitor);
    void register_monitors_to_http_action(MonitorAction* monitor_action);

    std::unordered_map<std::string, BaseMonitor*> get_all_monitors();

    void stop();
    void start_all_monitors();

private:
    void _collect_all_monitor_status();
    std::mutex _health_checker_mutex;
    std::unordered_map<std::string, BaseMonitor*> _monitor_holder;
    std::unordered_map<std::string, BaseMonitor*> _monitor_unnormal;
    std::thread _thread;
    std::atomic<bool> _stop;
};

} // namespace starrocks
