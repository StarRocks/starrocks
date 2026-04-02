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
#include <memory>
#include <vector>

#include "runtime/runtime_filter.h"

namespace starrocks {

// ExecCore-owned holder for either a singleton runtime-filter payload or one payload per driver
// for local colocate/group-execution build paths.
class RuntimeFilterInstanceSet {
public:
    RuntimeFilterInstanceSet() = default;
    explicit RuntimeFilterInstanceSet(size_t num_local_colocate_filters);

    void set_singleton_runtime_filter(const RuntimeFilter* rf);
    void set_shared_singleton_runtime_filter(const std::shared_ptr<const RuntimeFilter>& rf);
    void set_local_colocate_runtime_filter(MutableRuntimeFilterPtr rf, int32_t driver_sequence);

    const RuntimeFilter* runtime_filter(int32_t driver_sequence) const;
    bool has_local_colocate_filters() const { return !_local_colocate_filters.empty(); }

private:
    const RuntimeFilter* _singleton_runtime_filter = nullptr;
    std::shared_ptr<const RuntimeFilter> _shared_singleton_runtime_filter = nullptr;
    std::vector<MutableRuntimeFilterPtr> _local_colocate_filters;
};

} // namespace starrocks
