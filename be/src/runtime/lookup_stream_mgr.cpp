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

#include "runtime/lookup_stream_mgr.h"

#include <sstream>

#include "exec/pipeline/query_context.h"
#include "gen_cpp/internal_service.pb.h"
#include "util/time.h"

namespace starrocks {

void LookUpDispatcher::attach_query_ctx(pipeline::QueryContext* query_ctx) {
    if (_query_ctx.use_count() == 0) {
        _query_ctx = query_ctx->get_shared_ptr();
    }
}

Status LookUpDispatcher::add_request(const LookUpRequestVariant& request) {
    auto notify = this->defer_notify();
    std::visit(
            [&](auto& req) {
                req.receive_ts = MonotonicNanos();
                ;
            },
            request);
    _queue.enqueue(request);

    VLOG_ROW << "[GLM] add request to LookUpDispatcher, queue size: " << _queue.size_approx()
             << ", query id: " << print_id(_query_id) << ", target node id: " << _lookup_node_id
             << ", dispacher: " << (void*)this;
    return Status::OK();
}

bool LookUpDispatcher::try_get(int32_t driver_sequence, size_t max_num, LookUpContext* ctx) {
    ctx->requests.resize(max_num);
    if (size_t num = _queue.try_dequeue_bulk(ctx->requests.data(), max_num); num > 0) {
        ctx->requests.resize(num);
        return true;
    }
    return false;
}

bool LookUpDispatcher::has_data(int32_t driver_sequence) const {
    return _queue.size_approx() > 0;
}

std::shared_ptr<LookUpDispatcher> LookUpDispatcherMgr::create_dispatcher(RuntimeState* state, const TUniqueId& query_id,
                                                                         PlanNodeId target_node_id) {
    DispatcherKey key{query_id, target_node_id};
    auto [_, created] =
            _dispatcher_map.try_emplace(key, std::make_shared<LookUpDispatcher>(state, query_id, target_node_id));
    if (created) {
        VLOG_ROW << "[GLM] create LookUpDispatcher for query_id=" << print_id(query_id)
                 << ", target_node_id=" << target_node_id;
    }
    return _dispatcher_map.at(key);
}

StatusOr<LookUpDispatcherPtr> LookUpDispatcherMgr::get_dispatcher(const TUniqueId& query_id,
                                                                  PlanNodeId target_node_id) {
    DispatcherKey key{query_id, target_node_id};
    if (_dispatcher_map.contains(key)) {
        return _dispatcher_map.at(key);
    }

    LOG(WARNING) << "can't find LookUpDisPatcher for query_id=" << print_id(query_id)
                 << ", target_node_id=" << target_node_id;
    return Status::NotFound(
            fmt::format("can't find LookUpDispatcher for query {}, plan_node {}", print_id(query_id), target_node_id));
}

void LookUpDispatcherMgr::remove_dispatcher(const TUniqueId& query_id, PlanNodeId target_node_id) {
    DispatcherKey key{query_id, target_node_id};
    if (_dispatcher_map.contains(key)) {
        _dispatcher_map.erase(key);
        VLOG_ROW << "[GLM] remove LookUpDispatcher for query_id=" << print_id(query_id)
                 << ", target_node_id=" << target_node_id;
    }
}

Status LookUpDispatcherMgr::lookup(const RemoteLookUpRequest& ctx) {
    const auto& query_id = ctx.request->query_id();
    TUniqueId t_query_id;
    t_query_id.hi = query_id.hi();
    t_query_id.lo = query_id.lo();
    const auto lookup_node_id = ctx.request->lookup_node_id();
    ASSIGN_OR_RETURN(auto dispatcher, get_dispatcher(t_query_id, lookup_node_id));
    RETURN_IF_ERROR(dispatcher->add_request(ctx));
    return Status::OK();
}

} // namespace starrocks