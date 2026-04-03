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
#include <memory>

namespace starrocks {

class RuntimeFilter;

struct RuntimeFilterProbeListener {
    int32_t filter_id = -1;
    int32_t probe_plan_node_id = -1;
    std::function<void(const RuntimeFilter*)> on_local_ready;
    std::function<void(const std::shared_ptr<const RuntimeFilter>&)> on_shared_ready;

    void notify_local(const RuntimeFilter* rf) const {
        if (on_local_ready) {
            on_local_ready(rf);
        }
    }

    void notify_shared(const std::shared_ptr<const RuntimeFilter>& rf) const {
        if (on_shared_ready) {
            on_shared_ready(rf);
        }
    }
};

} // namespace starrocks
