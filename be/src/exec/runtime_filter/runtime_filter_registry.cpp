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

#include "exec/runtime_filter/runtime_filter_registry.h"

#include <sstream>

#include "exec/runtime_filter/runtime_filter_probe.h"

namespace starrocks {

void RuntimeFilterRegistry::register_descriptor(RuntimeFilterProbeDescriptor* desc) {
    if (desc == nullptr) {
        return;
    }
    _descriptors[desc->filter_id()].push_back(desc);
}

void RuntimeFilterRegistry::install_local(int32_t filter_id, const RuntimeFilter* rf) {
    auto it = _descriptors.find(filter_id);
    if (it == _descriptors.end()) {
        return;
    }
    for (auto* desc : it->second) {
        desc->set_runtime_filter(rf);
    }
}

void RuntimeFilterRegistry::install_shared(int32_t filter_id, const std::shared_ptr<const RuntimeFilter>& rf) {
    auto it = _descriptors.find(filter_id);
    if (it == _descriptors.end()) {
        return;
    }
    for (auto* desc : it->second) {
        desc->set_shared_runtime_filter(rf);
    }
}

std::string RuntimeFilterRegistry::waiters(int32_t filter_id) const {
    std::stringstream ss;
    auto it = _descriptors.find(filter_id);
    if (it == _descriptors.end() || it->second.empty()) {
        return "[]";
    }

    auto waiter = it->second.begin();
    ss << "[" << (*waiter)->probe_plan_node_id();
    while (++waiter != it->second.end()) {
        ss << ", " << (*waiter)->probe_plan_node_id();
    }
    ss << "]";
    return ss.str();
}

} // namespace starrocks
