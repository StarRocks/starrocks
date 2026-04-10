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

#include <google/protobuf/service.h>

#include <atomic>
#include <shared_mutex>

#include "base/concurrency/moodycamel/concurrentqueue.h"
#include "base/phmap/phmap.h"
#include "base/phmap/phmap_base.h"
#include "base/phmap/phmap_utils.h"
#include "bthread/mutex.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "exec/pipeline/lookup_request.h"
#include "exec/pipeline/schedule/observer.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/runtime_fwd.h"

namespace starrocks {

class LookUpDispatcher {
public:
    LookUpDispatcher(const TUniqueId& query_id, PlanNodeId lookup_node_id,
                     const std::vector<TupleId>& request_tuple_ids, int64_t rpc_ref_cnt = 1);

    ~LookUpDispatcher() = default;

    TUniqueId query_id() const { return _query_id; }
    PlanNodeId lookup_node_id() const { return _lookup_node_id; }

    Status add_request(const pipeline::LookUpRequestContextPtr& ctx);

    bool try_get(int32_t driver_sequence, size_t max_num, pipeline::LookUpTaskContext* ctx);

    bool has_data(int32_t driver_sequence) const;

    void attach_query_ctx(pipeline::QueryContext* query_ctx);

    bool ref_dec() { return --_rpc_ref_cnt == 0; }
    int32_t ref_cnt() const { return _rpc_ref_cnt.load(); }

    void attach_observer(RuntimeState* state, pipeline::PipelineObserver* observer) {
        _observable.add_observer(state, observer);
    }
    auto defer_notify() {
        return DeferOp([query_ctx = _query_ctx, this]() {
            if (auto ctx = query_ctx.lock()) {
                this->_observable.notify_source_observers();
            }
        });
    }

    bool is_finished() const { return _is_finished; }

    void set_finished() { _is_finished = true; }

private:
    const TUniqueId _query_id;
    [[maybe_unused]] PlanNodeId _lookup_node_id;
    std::atomic<int64_t> _rpc_ref_cnt{0};

    using RequestsQueue = moodycamel::ConcurrentQueue<pipeline::LookUpRequestContextPtr>;
    using RequestsQueuePtr = std::shared_ptr<RequestsQueue>;
    using RequestQueueMap =
            phmap::parallel_flat_hash_map<TupleId, RequestsQueuePtr, phmap::Hash<TupleId>, phmap::EqualTo<TupleId>,
                                          phmap::Allocator<TupleId>, 4, bthread::Mutex>;

    RequestQueueMap _request_queues;

    bool _is_finished = false;
    std::weak_ptr<pipeline::QueryContext> _query_ctx;
    pipeline::Observable _observable;
};
using LookUpDispatcherPtr = std::shared_ptr<LookUpDispatcher>;

// used to manager lookup stream for all queries
class LookUpDispatcherMgr {
public:
    LookUpDispatcherMgr() = default;
    ~LookUpDispatcherMgr() = default;

    LookUpDispatcherPtr create_dispatcher(const TUniqueId& query_id, PlanNodeId target_node_id,
                                          const std::vector<TupleId>& request_tuple_ids, int64_t rpc_ref_cnt = 1);

    StatusOr<LookUpDispatcherPtr> get_dispatcher(const TUniqueId& query_id, PlanNodeId target_node_id);
    Status lookup(const pipeline::RemoteLookUpRequestContextPtr& ctx);
    Status lookup_close(const TUniqueId& query_id, PlanNodeId target_node_id);

    void close() {}

private:
    typedef std::pair<TUniqueId, PlanNodeId> DispatcherKey;
    typedef phmap::parallel_flat_hash_map<DispatcherKey, LookUpDispatcherPtr, phmap::Hash<DispatcherKey>,
                                          phmap::EqualTo<DispatcherKey>, phmap::Allocator<DispatcherKey>, 4,
                                          bthread::Mutex>
            QueryDispatcherMap;

    QueryDispatcherMap _dispatcher_map;
};
} // namespace starrocks
