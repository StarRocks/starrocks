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
#include <utility>

#include "exec/runtime_filter/runtime_filter_probe.h"

namespace starrocks {

void RuntimeFilterRegistry::register_descriptor(RuntimeFilterProbeDescriptor* desc) {
    if (desc == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> l(_mutex);
    _descriptors[desc->filter_id()].push_back(desc);
}

void RuntimeFilterRegistry::install_local(int32_t filter_id, const RuntimeFilter* rf) {
    for (auto* desc : _snapshot_descriptors(filter_id)) {
        desc->set_runtime_filter(rf);
    }
}

void RuntimeFilterRegistry::install_shared(int32_t filter_id, const RuntimeFilterPtr& rf) {
    for (auto* desc : _snapshot_descriptors(filter_id)) {
        desc->set_shared_runtime_filter(rf);
    }
}

std::string RuntimeFilterRegistry::waiters(int32_t filter_id) const {
    std::stringstream ss;
    auto descriptors = _snapshot_descriptors(filter_id);
    if (descriptors.empty()) {
        return "[]";
    }

    auto it = descriptors.begin();
    ss << "[" << (*it)->probe_plan_node_id();
    while (++it != descriptors.end()) {
        ss << ", " << (*it)->probe_plan_node_id();
    }
    ss << "]";
    return ss.str();
}

RuntimeFilterRegistry::RuntimeFilterPtr RuntimeFilterRegistry::lookup_cached(int32_t filter_id) const {
    CachedLookup lookup;
    {
        std::lock_guard<std::mutex> l(_mutex);
        lookup = _cached_lookup;
    }
    return lookup ? lookup(filter_id) : nullptr;
}

void RuntimeFilterRegistry::trace_event(int32_t filter_id, std::string_view network, std::string_view msg) const {
    TraceSink sink;
    {
        std::lock_guard<std::mutex> l(_mutex);
        sink = _trace_sink;
    }
    if (sink) {
        sink(filter_id, network, msg);
    }
}

void RuntimeFilterRegistry::set_cached_lookup(CachedLookup lookup) {
    std::lock_guard<std::mutex> l(_mutex);
    _cached_lookup = std::move(lookup);
}

void RuntimeFilterRegistry::set_trace_sink(TraceSink sink) {
    std::lock_guard<std::mutex> l(_mutex);
    _trace_sink = std::move(sink);
}

std::vector<RuntimeFilterProbeDescriptor*> RuntimeFilterRegistry::_snapshot_descriptors(int32_t filter_id) const {
    std::lock_guard<std::mutex> l(_mutex);
    auto it = _descriptors.find(filter_id);
    if (it == _descriptors.end()) {
        return {};
    }
    return {it->second.begin(), it->second.end()};
}

} // namespace starrocks
