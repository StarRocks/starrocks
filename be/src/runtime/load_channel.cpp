// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/load_channel.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/load_channel.h"

#include <memory>

#include "common/closure_guard.h"
#include "runtime/lake_tablets_channel.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/local_tablets_channel.h"
#include "runtime/mem_tracker.h"
#include "util/lru_cache.h"

namespace starrocks {

LoadChannel::LoadChannel(LoadChannelMgr* mgr, const UniqueId& load_id, const std::string& txn_trace_parent,
                         int64_t timeout_s, std::unique_ptr<MemTracker> mem_tracker)
        : _load_mgr(mgr),
          _load_id(load_id),
          _timeout_s(timeout_s),
          _mem_tracker(std::move(mem_tracker)),
          _last_updated_time(time(nullptr)) {
    _span = Tracer::Instance().start_trace_or_add_span("load_channel", txn_trace_parent);
    _span->SetAttribute("load_id", load_id.to_string());
}

LoadChannel::~LoadChannel() {
    _span->SetAttribute("num_chunk", _num_chunk);
    _span->End();
}

void LoadChannel::open(brpc::Controller* cntl, const PTabletWriterOpenRequest& request,
                       PTabletWriterOpenResult* response, google::protobuf::Closure* done) {
    _span->AddEvent("open_index", {{"index_id", request.index_id()}});
    auto scoped = trace::Scope(_span);
    ClosureGuard done_guard(done);

    _last_updated_time.store(time(nullptr), std::memory_order_relaxed);
    int64_t index_id = request.index_id();
    bool is_lake_tablet = request.has_is_lake_tablet() && request.is_lake_tablet();

    Status st;
    {
        // We will `bthread::execution_queue_join()` in the destructor of AsnycDeltaWriter,
        // it will block the bthread, so we put its destructor outside the lock.
        std::shared_ptr<TabletsChannel> channel;
        std::lock_guard l(_lock);
        if (_tablets_channels.find(index_id) == _tablets_channels.end()) {
            TabletsChannelKey key(request.id(), index_id);
            channel = is_lake_tablet ? new_lake_tablets_channel(this, key, _mem_tracker.get())
                                     : new_local_tablets_channel(this, key, _mem_tracker.get());
            if (st = channel->open(request); st.ok()) {
                _tablets_channels.insert({index_id, std::move(channel)});
            }
        }
    }
    LOG_IF(WARNING, !st.ok()) << "Fail to open index " << index_id << " of load " << _load_id << ": " << st.to_string();
    response->mutable_status()->set_status_code(st.code());
    response->mutable_status()->add_error_msgs(st.get_error_msg());
}

void LoadChannel::add_chunk(brpc::Controller* cntl, const PTabletWriterAddChunkRequest& request,
                            PTabletWriterAddBatchResult* response, google::protobuf::Closure* done) {
    _num_chunk++;
    ClosureGuard done_guard(done);
    _last_updated_time.store(time(nullptr), std::memory_order_relaxed);
    auto channel = get_tablets_channel(request.index_id());
    if (channel == nullptr) {
        response->mutable_status()->set_status_code(TStatusCode::INTERNAL_ERROR);
        response->mutable_status()->add_error_msgs("cannot find the tablets channel associated with the index id");
        return;
    }
    channel->add_chunk(cntl, request, response, done_guard.release());
}

void LoadChannel::cancel() {
    _span->AddEvent("cancel");
    auto scoped = trace::Scope(_span);
    std::lock_guard l(_lock);
    for (auto& it : _tablets_channels) {
        it.second->cancel();
    }
}

void LoadChannel::remove_tablets_channel(int64_t index_id) {
    std::unique_lock l(_lock);
    _tablets_channels.erase(index_id);
    if (_tablets_channels.empty()) {
        l.unlock();
        _load_mgr->remove_load_channel(_load_id);
        // Do NOT touch |this| since here, it could have been deleted.
    }
}

std::shared_ptr<TabletsChannel> LoadChannel::get_tablets_channel(int64_t index_id) {
    std::lock_guard l(_lock);
    auto it = _tablets_channels.find(index_id);
    return (it != _tablets_channels.end()) ? it->second : nullptr;
}

} // namespace starrocks
