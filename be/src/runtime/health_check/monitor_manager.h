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

#include "base_monitor.h"
#include "common/config.h"
#include "http/monitor_action.h"
#include "util/thread.h"

namespace starrocks {

// A class managers all monitors
// TODO report the metrics to FE through heartBeat
class MonitorManager {
public:
    MonitorManager();
    ~MonitorManager();

    Status register_monitor(std::string monitor_name, BaseMonitor* monitor);

    // for metrics of all monitor_action can be get by /api/monitor?module=monitor_name
    void register_monitors_to_http_action(MonitorAction* monitor_action);

    std::unordered_map<std::string, BaseMonitor*> get_all_monitors();

    void start_all_monitors();
    void stop_all_monitors();

private:
    std::mutex _monitor_manager_mutex;
    std::unordered_map<std::string, BaseMonitor*> _monitor_holder;
};

} // namespace starrocks
