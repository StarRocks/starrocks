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
    DCHECK(_ctx_map.contains(scan_node_id));
    return _ctx_map.at(scan_node_id);
}

GlobalLateMaterilizationContext* GlobalLateMaterilizationContextMgr::get_or_create_ctx(
        int64_t scan_node_id, const std::function<GlobalLateMaterilizationContext*()>& ctor_func) {
    _ctx_map.lazy_emplace(scan_node_id, [&](const auto& ctor) {
        auto ctx = ctor_func();
        ctor(scan_node_id, ctx);
    });
    return _ctx_map.at(scan_node_id);
}

} // namespace starrocks
