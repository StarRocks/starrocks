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
#include <unordered_map>

#include "gen_cpp/FrontendService.h"

namespace starrocks {

/// Record the resource usages of all the resource groups.
///
/// It is not thread-safe.
class ResourceGroupUsageRecorder {
public:
    ResourceGroupUsageRecorder();

    /// Get the resource usages of all the resource groups.
    ///
    /// The cpu usage of any group is recorded for the time interval between two invocations of this method.
    std::vector<TResourceGroupUsage> get_resource_group_usages();

private:
    int64_t _timestamp_ns = 0;
    std::unordered_map<int64_t, int64_t> _group_to_cpu_runtime_ns;
};

} // namespace starrocks
