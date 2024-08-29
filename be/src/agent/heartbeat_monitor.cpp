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

#include "heartbeat_monitor.h"

#include <chrono>
#include <iostream>

#include "common/config.h"
#include "common/logging.h"
#include "util/stack_util.h"

namespace starrocks {

HeartbeatMonitor::HeartbeatMonitor() : _heartbeat_received(false), _stop_monitoring(false) {
    _monitor_thread = std::thread(&HeartbeatMonitor::_monitor_heartbeat, this);
}

HeartbeatMonitor::~HeartbeatMonitor() {
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _stop_monitoring = true;
    }
    _cv.notify_all();
    if (_monitor_thread.joinable()) {
        _monitor_thread.join();
    }
}

void HeartbeatMonitor::_monitor_heartbeat() {
    auto interval = config::heartbeat_monitor_check_interval_seconds;
    while (true) {
        if (!_heartbeat_server_started.load()) {
            std::cout << "wait heartbeat server start.." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(interval));
        } else {
            std::unique_lock<std::mutex> ul(_mutex);
            if (_cv.wait_for(ul, std::chrono::seconds(interval),
                             [this] { return _heartbeat_received || _stop_monitoring; })) {
                if (_stop_monitoring) {
                    break;
                }
                _heartbeat_received = false;
            } else {
                std::cout << "Heartbeat timeout detected!" << std::endl;
                if (config::enable_print_stacktrace_when_heartbeat_stuck) {
                    // for debug reason, print stacktrace
                    std::cout << "Heartbeat timeout detected! \n" << get_stack_trace_for_all_threads() << std::endl;
                }
            }
        }
    }
}

void HeartbeatMonitor::set_received_heartbeat() {
    std::lock_guard<std::mutex> lock(_mutex);
    _heartbeat_received = true;
    _cv.notify_all();
}

void HeartbeatMonitor::set_heartbeat_server_started() {
    _heartbeat_server_started.store(true);
    std::cout << "Heartbeat server started!" << std::endl;
}

} // namespace starrocks