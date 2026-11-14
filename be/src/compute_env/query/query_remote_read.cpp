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

#include "compute_env/query/query_remote_read.h"

#include <algorithm>
#include <limits>

#include "compute_env/query/fragment_runtime_state.h"
#include "compute_env/workgroup/work_group.h"
#include "runtime/runtime_state.h"

namespace starrocks {

RemoteReadContext query_remote_read_context(const RuntimeState* state) {
    int64_t group_id = 0;
    uint32_t group_weight = 1;
    uint64_t group_config_version = 0;
    const auto* fragment_state = state->fragment_runtime_state();
    if (fragment_state != nullptr && fragment_state->workgroup() != nullptr) {
        const auto& wg = fragment_state->workgroup();
        group_id = wg->id();
        group_weight =
                static_cast<uint32_t>(std::clamp<size_t>(wg->cpu_weight(), 1, std::numeric_limits<uint32_t>::max()));
        group_config_version = static_cast<uint64_t>(std::max<int64_t>(0, wg->version()));
    }
    const TUniqueId& query_id = state->query_id();
    return RemoteReadContext::for_query(group_id, group_weight, group_config_version,
                                        RemoteReadContext::flow_id_of(query_id.hi, query_id.lo));
}

} // namespace starrocks
