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

#include "exec/pipeline/scan/glm_manager.h"
#include "common/object_pool.h"

namespace starrocks::pipeline {

void GlobalLateMaterilizationContextMgr::add_ctx(int row_source_slot_id, GlobalLateMaterilizationContext* ctx) {
    _ctx_map.try_emplace(row_source_slot_id, ctx);
}

GlobalLateMaterilizationContext* GlobalLateMaterilizationContextMgr::get_ctx(int row_source_slot_id) const {
    DCHECK(_ctx_map.contains(row_source_slot_id));
    return _ctx_map.at(row_source_slot_id);
}

GlobalLateMaterilizationContext* GlobalLateMaterilizationContextMgr::get_or_create_ctx(int32_t row_source_slot_id,
        const std::function<GlobalLateMaterilizationContext*()>& ctor_func) {
    auto iter = _ctx_map.lazy_emplace(row_source_slot_id, [&](const auto& ctor) {
        auto ctx = ctor_func();
        ctor(row_source_slot_id, ctx);
    });
    return iter->second;
}
} // namespace starrocks::pipeline