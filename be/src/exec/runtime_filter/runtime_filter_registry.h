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

#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace starrocks {

class RuntimeFilter;
class RuntimeFilterProbeDescriptor;

class RuntimeFilterRegistry {
public:
    using RuntimeFilterPtr = std::shared_ptr<const RuntimeFilter>;
    using CachedLookup = std::function<RuntimeFilterPtr(int32_t)>;
    using TraceSink = std::function<void(int32_t, std::string_view, std::string_view)>;

    void register_descriptor(RuntimeFilterProbeDescriptor* desc);
    void install_local(int32_t filter_id, const RuntimeFilter* rf);
    void install_shared(int32_t filter_id, const RuntimeFilterPtr& rf);
    std::string waiters(int32_t filter_id) const;

    RuntimeFilterPtr lookup_cached(int32_t filter_id) const;
    void trace_event(int32_t filter_id, std::string_view network, std::string_view msg) const;

    void set_cached_lookup(CachedLookup lookup);
    void set_trace_sink(TraceSink sink);

private:
    std::vector<RuntimeFilterProbeDescriptor*> _snapshot_descriptors(int32_t filter_id) const;

    mutable std::mutex _mutex;
    std::map<int32_t, std::list<RuntimeFilterProbeDescriptor*>> _descriptors;
    CachedLookup _cached_lookup;
    TraceSink _trace_sink;
};

} // namespace starrocks
