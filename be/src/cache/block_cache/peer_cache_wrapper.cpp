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

#include "cache/block_cache/peer_cache_wrapper.h"

#include "common/closure_guard.h"
#include "common/logging.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/exec_env.h"
#include "util/brpc_stub_cache.h"
#include "util/internal_service_recoverable_stub.h"
#include "util/ref_count_closure.h"
#include "util/hash_util.hpp"

namespace starrocks {

Status PeerCacheWrapper::init(const CacheOptions& options) {
    _cache_adaptor.reset(starcache::create_default_adaptor(options.skip_read_factor));
    return Status::OK();
}

Status PeerCacheWrapper::read_buffer(const std::string& key, size_t off, size_t size, IOBuffer* buffer,
                                     ReadCacheOptions* options) {
    if (options->use_adaptor && !_cache_adaptor->check_read_cache()) {
        return Status::ResourceBusy("resource is busy");
    }

    std::shared_ptr<PInternalService_RecoverableStub> stub =
        ExecEnv::GetInstance()->brpc_stub_cache()->get_stub(options->remote_host, options->remote_port);
    PFetchDataCacheRequest request;
    PFetchDataCacheResponse response;
    request.set_request_id(butil::monotonic_time_ns());
    request.set_cache_key(key);
    request.set_offset(off);
    request.set_size(size);

    brpc::Controller cntl;
    cntl.set_timeout_ms(2000);

    auto begin_us = GetCurrentTimeMicros();

    VLOG_CACHE << "[Gavin] start fetch datacache, request_id: " << request.request_id()
               << ", cache_id: " << HashUtil::hash64(key.data(), key.size(), 0)
               << ", offset: " << off << ", size: " << size;
    stub->fetch_datacache(&cntl, &request, &response, nullptr);
    Status st;
    if (cntl.Failed()) {
        st = Status::InternalError(cntl.ErrorText());
        LOG(WARNING) << "failed to send fetch_datacache rpc, err: " << st;
        return st;
    }
    st = response.status();
    if (!st.ok()) {
        LOG(WARNING) << "fetch datacache rpc failed, err: " << st;
        return st;
    }
    cntl.response_attachment().swap(buffer->raw_buf());
    VLOG_CACHE << "[Gavin] finish read buffer from peer node: " << options->remote_host
               << ", cache_id: " << HashUtil::hash64(key.data(), key.size(), 0)
               << ", offset: " << off << ", size: " << buffer->size()
               << ", request_id: " << request.request_id() << ", st: " << st
               << ", buf: " << buffer << ", latency_us: " << GetCurrentTimeMicros() - begin_us;

    return st;
}

void PeerCacheWrapper::record_read_remote(size_t size, int64_t lateny_us) {
    if (_cache_adaptor) {
        return _cache_adaptor->record_read_remote(size, lateny_us);
    }
}

void PeerCacheWrapper::record_read_cache(size_t size, int64_t lateny_us) {
    if (_cache_adaptor) {
        return _cache_adaptor->record_read_cache(size, lateny_us);
    }
}

Status PeerCacheWrapper::shutdown() {
    // TODO: starcache implement shutdown to release memory
    return Status::OK();
}

} // namespace starrocks
