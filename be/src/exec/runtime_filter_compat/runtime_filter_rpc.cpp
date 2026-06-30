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

#include "exec/runtime_filter_compat/runtime_filter_rpc.h"

#include "common/brpc/brpc_stub_cache.h"
#include "common/brpc/internal_service_recoverable_stub.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "runtime/service_contexts.h"

namespace starrocks {

#define WARN_IF_RF_RPC_ERROR(closure)                                                                                 \
    if (closure->cntl.Failed()) {                                                                                     \
        LOG(WARNING) << "runtime filter brpc failed, error_code=" << berror(closure->cntl.ErrorCode())                \
                     << ", error_text=" << closure->cntl.ErrorText() << ", filter_id=" << closure->result.filter_id() \
                     << ", debug_info=" << closure->debug_info;                                                       \
    }

BatchClosuresJoinAndClean::~BatchClosuresJoinAndClean() {
    for (auto& closure : _closures) {
        closure->join();
        WARN_IF_RF_RPC_ERROR(closure);
        if (closure->unref()) {
            delete closure;
        }
    }
}

void send_rpc_runtime_filter(const RpcServices* rpc_services, const TNetworkAddress& dest,
                             RuntimeFilterRpcClosure* rpc_closure, int timeout_ms, int64_t http_min_size,
                             const PTransmitRuntimeFilterParams& request) {
    std::shared_ptr<PInternalService_RecoverableStub> stub = nullptr;
    bool via_http = request.data().size() >= http_min_size;
    if (via_http) {
        if (auto res = HttpBrpcStubCache::getInstance()->get_http_stub(dest); res.ok()) {
            stub = res.value();
        }
    } else {
        stub = rpc_services->brpc_stub_cache->get_stub(dest);
    }
    if (stub == nullptr) {
        LOG(WARNING) << strings::Substitute("The brpc stub of {}: {} is null.", dest.hostname, dest.port);
        return;
    }

    rpc_closure->ref();
    rpc_closure->cntl.Reset();
    rpc_closure->cntl.set_timeout_ms(timeout_ms);
    stub->transmit_runtime_filter(&rpc_closure->cntl, &request, &rpc_closure->result, rpc_closure);
}

// Fire-and-forget closure dispatched from RuntimeFilterWorker's drain thread.
// brpc owns the only reference; on RPC completion the closure logs any error
// and self-deletes, so the worker thread is no longer blocked by bthread_id_join
// while forwarding broadcast/total runtime filters.
struct AsyncRuntimeFilterRpcClosure : public RuntimeFilterRpcClosure {
    void Run() override {
        WARN_IF_RF_RPC_ERROR(this);
        if (unref()) {
            delete this;
        }
    }
};

void submit_async_runtime_filter_rpc(const RpcServices* rpc_services, const TNetworkAddress& dest, int timeout_ms,
                                     int64_t http_min_size, const PTransmitRuntimeFilterParams& request,
                                     const char* debug_info) {
    // The closure and brpc's internal serialization buffers are released by a
    // brpc bthread after this function returns, where no thread-local mem
    // tracker is set. Callers usually run under a query mem tracker
    // (RuntimeFilterMerger / _receive_total_runtime_filter set one at the top
    // of the function), so allocating here would charge the query tracker but
    // free untracked. Detach from the query tracker to keep alloc/free
    // accounting on the same (process default) tracker.
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);

    auto* closure = new AsyncRuntimeFilterRpcClosure();
    closure->result.set_filter_id(request.filter_id());
    closure->debug_info = debug_info;
    // Hold one ref while dispatching. send_rpc_runtime_filter adds another ref
    // when it successfully hands the closure to brpc; if the stub is null it
    // returns without ref'ing, so dropping our ref deletes the closure here.
    closure->ref();
    send_rpc_runtime_filter(rpc_services, dest, closure, timeout_ms, http_min_size, request);
    if (closure->unref()) {
        delete closure;
    }
}

#undef WARN_IF_RF_RPC_ERROR

} // namespace starrocks
