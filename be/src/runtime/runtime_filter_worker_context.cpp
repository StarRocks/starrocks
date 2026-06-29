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

#include "runtime/runtime_filter_worker_context.h"

#include "exec/runtime/query_context.h"
#include "exec/runtime/query_context_manager.h"
#include "runtime/mem_tracker.h"
#include "runtime/service_contexts.h"

namespace starrocks {

// Using a query-level mem_tracker beyond QueryContext's lifetime may access already destructed parent mem_tracker.
// mem_trackers has a hierarchy: process->query_pool->resource_group->query, so when resource_group is dropped or
// altered, resource_group-level mem_tracker would be destructed, such a dangling query-level mem_tracker would cause
// BE's crash when it accesses its parent mem_tracker(i.e. resource_group-level mem_tracker). so we need capture
// query context to prevent it from being destructed, and when a dropping resource_group is used by outstanding query
// contexts, it would be delayed to be dropped until all its outstanding query contexts are destructed.
std::pair<pipeline::QueryContextPtr, std::shared_ptr<MemTracker>> get_runtime_filter_mem_tracker(
        const RuntimeServices* runtime_services, const PUniqueId& query_id, bool is_pipeline) {
    if (is_pipeline) {
        TUniqueId tquery_id;
        tquery_id.lo = query_id.lo();
        tquery_id.hi = query_id.hi();
        auto query_ctx = runtime_services->query_context_mgr->get(tquery_id);
        auto mem_tracker = query_ctx == nullptr ? nullptr : query_ctx->mem_tracker();
        return std::make_pair(query_ctx, mem_tracker);
    } else {
        return std::make_pair(nullptr, nullptr);
    }
}

} // namespace starrocks
