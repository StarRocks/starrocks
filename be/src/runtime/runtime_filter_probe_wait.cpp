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

#include <chrono>
#include <thread>

#include "exec/runtime_filter/runtime_filter_probe.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// only used in non-pipeline mode
void RuntimeFilterProbeCollector::wait(bool on_scan_node) {
    if (_descriptors.empty()) return;

    std::list<RuntimeFilterProbeDescriptor*> wait_list;
    for (auto& it : _descriptors) {
        auto* rf = it.second;
        int filter_id = rf->filter_id();
        VLOG_FILE << "RuntimeFilterCollector::wait start. filter_id = " << filter_id
                  << ", plan_node_id = " << _plan_node_id << ", finst_id = " << _runtime_state->fragment_instance_id();
        wait_list.push_back(it.second);
    }

    int wait_time = _wait_timeout_ms;
    if (on_scan_node) {
        wait_time = _scan_wait_timeout_ms;
    }
    const int wait_interval = 5;
    auto wait_duration = std::chrono::milliseconds(wait_interval);
    while (wait_time >= 0 && !wait_list.empty()) {
        auto it = wait_list.begin();
        while (it != wait_list.end()) {
            auto* rf = (*it)->runtime_filter(-1);
            // find runtime filter in cache.
            if (rf == nullptr) {
                RuntimeFilterPtr t = _runtime_state->exec_env()->runtime_filter_cache()->get(_runtime_state->query_id(),
                                                                                             (*it)->filter_id());
                if (t != nullptr) {
                    VLOG_FILE << "RuntimeFilterCollector::wait: rf found in cache. filter_id = " << (*it)->filter_id()
                              << ", plan_node_id = " << _plan_node_id
                              << ", finst_id  = " << _runtime_state->fragment_instance_id();
                    (*it)->set_shared_runtime_filter(t);
                    rf = t.get();
                }
            }
            if (rf != nullptr) {
                it = wait_list.erase(it);
            } else {
                ++it;
            }
        }
        if (wait_list.empty()) break;
        std::this_thread::sleep_for(wait_duration);
        wait_time -= wait_interval;
    }

    if (_descriptors.size() != 0) {
        for (const auto& it : _descriptors) {
            auto* rf = it.second;
            int filter_id = rf->filter_id();
            bool ready = (rf->runtime_filter(-1) != nullptr);
            VLOG_FILE << "RuntimeFilterCollector::wait start. filter_id = " << filter_id
                      << ", plan_node_id = " << _plan_node_id
                      << ", finst_id = " << _runtime_state->fragment_instance_id()
                      << ", ready = " << std::to_string(ready);
        }
    }
}

} // namespace starrocks
