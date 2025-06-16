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

#include <mutex>
#include <shared_mutex>

#include "bthread/mutex.h"
#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "exec/pipeline/schedule/observer.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/descriptors.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/runtime_state.h"
#include "util/moodycamel/concurrentqueue.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_base.h"
#include "util/phmap/phmap_utils.h"

namespace starrocks {
namespace pipeline {
class FetchContext;
using FetchContextPtr = std::shared_ptr<FetchContext>;
} // namespace pipeline

struct RemoteLookUpRequest {
    ::google::protobuf::RpcController* cntl = nullptr;
    const PLookUpRequest* request = nullptr;
    PLookUpResponse* response = nullptr;
    ::google::protobuf::Closure* done = nullptr;
    // row_id_slot => request row ids
    phmap::flat_hash_map<SlotId, ColumnPtr> request_columns;
    mutable int64_t receive_ts = 0;
};

struct LocalLookUpRequest {
    pipeline::FetchContextPtr fetch_ctx = nullptr;
    std::function<void(const Status&)> callback;
    mutable int64_t receive_ts = 0;
};

using LookUpRequestVariant = std::variant<RemoteLookUpRequest, LocalLookUpRequest>;

struct LookUpContext {
    std::vector<LookUpRequestVariant> requests;
};

class LookUpDispatcher {
public:
    LookUpDispatcher(RuntimeState* state, const TUniqueId& query_id, PlanNodeId lookup_node_id)
            : _state(state), _query_id(query_id), _lookup_node_id(lookup_node_id) {}

    ~LookUpDispatcher() = default;

    TUniqueId query_id() const { return _query_id; }
    PlanNodeId lookup_node_id() const { return _lookup_node_id; }

    Status add_request(const LookUpRequestVariant& ctx);

    bool try_get(int32_t driver_sequence, size_t max_num, LookUpContext* ctx);

    // bool try_get(int32_t driver_sequence, LookUpRequestCtx* ctx);
    bool has_data(int32_t driver_sequence) const;

    void attach_query_ctx(pipeline::QueryContext* query_ctx);

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

private:
    [[maybe_unused]] RuntimeState* _state;
    const TUniqueId _query_id;
    [[maybe_unused]] PlanNodeId _lookup_node_id;

    typedef moodycamel::ConcurrentQueue<LookUpRequestVariant> RequestQueue;
    RequestQueue _queue;

    std::weak_ptr<pipeline::QueryContext> _query_ctx;
    pipeline::Observable _observable;
};
using LookUpDispatcherPtr = std::shared_ptr<LookUpDispatcher>;

// used to manager lookup stream for all queries
class LookUpDispatcherMgr {
public:
    LookUpDispatcherMgr() = default;
    ~LookUpDispatcherMgr() = default;

    LookUpDispatcherPtr create_dispatcher(RuntimeState* state, const TUniqueId& query_id, PlanNodeId target_node_id);

    StatusOr<LookUpDispatcherPtr> get_dispatcher(const TUniqueId& query_id, PlanNodeId target_node_id);
    void remove_dispatcher(const TUniqueId& query_id, PlanNodeId target_node_id);
    Status lookup(const RemoteLookUpRequest& ctx);

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
