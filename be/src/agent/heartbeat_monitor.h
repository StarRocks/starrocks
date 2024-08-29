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
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>

namespace starrocks {

// Debug heartbeat thread hang problem
class HeartbeatMonitor {
public:
    HeartbeatMonitor();
    ~HeartbeatMonitor();

    // set flag
    void set_received_heartbeat();
    void set_heartbeat_server_started();

private:
    void _monitor_heartbeat();

    std::thread _monitor_thread;
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _heartbeat_received;
    bool _stop_monitoring;
    std::atomic<bool> _heartbeat_server_started{false};
};

} // namespace starrocks