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

#include <algorithm>
#include <sstream>
#include <vector>

#include "exec/pipeline/lookup_request.h"
#include "exec/pipeline/query_context.h"
#include "gen_cpp/internal_service.pb.h"
#include "util/time.h"

namespace starrocks {

LookUpDispatcher::LookUpDispatcher(RuntimeState* state, const TUniqueId& query_id, PlanNodeId lookup_node_id,
                                   const std::vector<TupleId>& request_tuple_ids)
        : _state(state), _query_id(query_id), _lookup_node_id(lookup_node_id) {
    for (const auto& tuple_id : request_tuple_ids) {
        DLOG(INFO) << "create request queue for tuple_id: " << tuple_id;
        _request_queues.emplace(tuple_id, std::make_shared<RequestsQueue>());
    }
}

void LookUpDispatcher::attach_query_ctx(pipeline::QueryContext* query_ctx) {
    if (_query_ctx.use_count() == 0) {
        _query_ctx = query_ctx->get_shared_ptr();
    }
}

Status LookUpDispatcher::add_request(const pipeline::LookUpRequestContextPtr& ctx) {
    auto notify = this->defer_notify();
    ctx->receive_ts = MonotonicNanos();
    auto request_tuple_id = ctx->request_tuple_id();
    DCHECK(_request_queues.contains(request_tuple_id)) << "missing tuple_id: " << request_tuple_id;
    _request_queues.at(request_tuple_id)->enqueue(std::move(ctx));
    DLOG(INFO) << "[GLM] add request to LookUpDispatcher, "
               << ", query id: " << print_id(_query_id) << ", target node id: " << _lookup_node_id
               << ", tuple id: " << request_tuple_id << ", dispacher: " << (void*)this;
    return Status::OK();
}

bool LookUpDispatcher::try_get(int32_t driver_sequence, size_t max_num, pipeline::LookUpTaskContext* ctx) {
    ctx->request_ctxs.resize(max_num);
    // find target_source_slot_id with the largest queue size
    size_t max_cnt = 0;
    SlotId target_tuple_id = 0;
    for (const auto& [tuple_id, queue] : _request_queues) {
        size_t cnt = queue->size_approx();
        if (cnt > max_cnt) {
            max_cnt = cnt;
            target_tuple_id = tuple_id;
        }
    }
    if (max_cnt > 0) {
        auto target_queue = _request_queues.at(target_tuple_id);
        if (size_t num = target_queue->try_dequeue_bulk(ctx->request_ctxs.data(), max_num); num > 0) {
            ctx->request_ctxs.resize(num);
            ctx->request_tuple_id = target_tuple_id;
            DLOG(INFO) << "[GLM] try_get LookUpDispatcher, queue size: " << max_cnt
                       << ", query id: " << print_id(_query_id) << ", target node id: " << _lookup_node_id
                       << ", tuple id: " << target_tuple_id << ", dispacher: " << (void*)this;
            return true;
        }
    }
    return false;
}

bool LookUpDispatcher::has_data(int32_t driver_sequence) const {
    for (const auto& [_, q] : _request_queues) {
        if (q->size_approx() > 0) {
            return true;
        }
    }
    return false;
}

std::shared_ptr<LookUpDispatcher> LookUpDispatcherMgr::create_dispatcher(RuntimeState* state, const TUniqueId& query_id,
                                                                         PlanNodeId target_node_id,
                                                                         const std::vector<SlotId>& source_id_slots) {
    DispatcherKey key{query_id, target_node_id};
    auto [_, created] = _dispatcher_map.try_emplace(
            key, std::make_shared<LookUpDispatcher>(state, query_id, target_node_id, source_id_slots));
    DLOG_IF(INFO, created) << "[GLM] create LookUpDispatcher for query_id=" << print_id(query_id)
                           << ", target_node_id=" << target_node_id;
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
        DLOG(INFO) << "[GLM] remove LookUpDispatcher for query_id=" << print_id(query_id)
                   << ", target_node_id=" << target_node_id;
    }
}

Status LookUpDispatcherMgr::lookup(const pipeline::RemoteLookUpRequestContextPtr& ctx) {
    const auto& query_id = ctx->request->query_id();
    TUniqueId t_query_id;
    t_query_id.hi = query_id.hi();
    t_query_id.lo = query_id.lo();
    const auto lookup_node_id = ctx->request->lookup_node_id();
    ASSIGN_OR_RETURN(auto dispatcher, get_dispatcher(t_query_id, lookup_node_id));
    RETURN_IF_ERROR(dispatcher->add_request(ctx));
    return Status::OK();
}

} // namespace starrocks