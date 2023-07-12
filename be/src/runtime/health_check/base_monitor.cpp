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
#include "runtime/health_check/base_monitor.h"

namespace starrocks {

BaseMonitor::BaseMonitor(std::string name) : _stop(false), _monitor_name(name) {}

std::string BaseMonitor::get_name() {
    return _monitor_name;
}

void BaseMonitor::stop() {
    if (_stop) {
        return;
    }
    _stop = true;
    if (_thread.joinable()) {
        _thread.join();
    }
    LOG(INFO) << _monitor_name << " stop";
}

void BaseMonitor::start() {
    if (_start) {
        return;
    }
    _start = true;
    if (_callback_function == nullptr) {
        LOG(INFO) << _monitor_name << " start without callback_function";
        return;
    }
    std::thread worker_thread(_callback_function, this);
    Thread::set_thread_name(worker_thread, _monitor_name);
    _thread = std::move(worker_thread);

    LOG(INFO) << _monitor_name << " start";
}

BaseMonitor::~BaseMonitor() {}

} // namespace starrocks
