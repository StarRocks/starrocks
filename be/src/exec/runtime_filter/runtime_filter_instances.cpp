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

#include "exec/runtime_filter/runtime_filter_instances.h"

namespace starrocks {

RuntimeFilterInstanceSet::RuntimeFilterInstanceSet(size_t num_local_colocate_filters)
        : _local_colocate_filters(num_local_colocate_filters) {}

void RuntimeFilterInstanceSet::set_singleton_runtime_filter(const RuntimeFilter* rf) {
    _shared_singleton_runtime_filter = nullptr;
    _singleton_runtime_filter = rf;
}

void RuntimeFilterInstanceSet::set_shared_singleton_runtime_filter(const std::shared_ptr<const RuntimeFilter>& rf) {
    _shared_singleton_runtime_filter = rf;
    _singleton_runtime_filter = _shared_singleton_runtime_filter.get();
}

void RuntimeFilterInstanceSet::set_local_colocate_runtime_filter(MutableRuntimeFilterPtr rf, int32_t driver_sequence) {
    DCHECK_GE(driver_sequence, 0);
    DCHECK_LT(driver_sequence, _local_colocate_filters.size());
    _local_colocate_filters[driver_sequence] = std::move(rf);
}

const RuntimeFilter* RuntimeFilterInstanceSet::runtime_filter(int32_t driver_sequence) const {
    if (_local_colocate_filters.empty()) {
        return _singleton_runtime_filter;
    }

    if (driver_sequence < 0) {
        for (const auto& filter : _local_colocate_filters) {
            if (filter != nullptr) {
                return filter.get();
            }
        }
        return nullptr;
    }

    DCHECK_LT(driver_sequence, _local_colocate_filters.size());
    return _local_colocate_filters[driver_sequence].get();
}

} // namespace starrocks
