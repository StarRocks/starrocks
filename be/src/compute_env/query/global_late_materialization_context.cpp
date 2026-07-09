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

#include "compute_env/query/global_late_materialization_context.h"

#include "common/logging.h"

namespace starrocks {

GlobalLateMaterilizationContext* GlobalLateMaterilizationContextMgr::get_ctx(int64_t scan_node_id) const {
    // Read the value inside the map's read lock. Returning `_ctx_map.at()` would deref a slot
    // reference after the lock is released, racing with a concurrent lazy_emplace()/resize()
    // that frees the slot array (heap-use-after-free).
    GlobalLateMaterilizationContext* ctx = nullptr;
    [[maybe_unused]] bool found = _ctx_map.if_contains(scan_node_id, [&](const auto& value) { ctx = value; });
    DCHECK(found);
    return ctx;
}

GlobalLateMaterilizationContext* GlobalLateMaterilizationContextMgr::get_or_create_ctx(
        int64_t scan_node_id, const std::function<GlobalLateMaterilizationContext*()>& ctor_func) {
    // Get-or-create and read the value entirely inside the map's write lock via lazy_emplace_l.
    // Do not fall back to _ctx_map.at(): its returned reference outlives the lock and would be
    // dereferenced concurrently with another thread's resize().
    GlobalLateMaterilizationContext* ctx = nullptr;
    _ctx_map.lazy_emplace_l(
            scan_node_id, [&](GlobalLateMaterilizationContext*& value) { ctx = value; },
            [&](const auto& ctor) {
                auto* new_ctx = ctor_func();
                ctx = new_ctx;
                ctor(scan_node_id, new_ctx);
            });
    return ctx;
}

} // namespace starrocks
