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

#include <cstdint>
#include <vector>

#include "base/brpc/ref_count_closure.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/macros.h"

namespace starrocks {

struct RpcServices;

using RuntimeFilterRpcClosure = RefCountClosure<PTransmitRuntimeFilterResult>;
using RuntimeFilterRpcClosures = std::vector<RuntimeFilterRpcClosure*>;

struct BatchClosuresJoinAndClean {
public:
    BatchClosuresJoinAndClean(RuntimeFilterRpcClosures& closures) : _closures(closures) {}
    ~BatchClosuresJoinAndClean();

private:
    RuntimeFilterRpcClosures& _closures;
    DISALLOW_COPY_AND_MOVE(BatchClosuresJoinAndClean);
};

void send_rpc_runtime_filter(const RpcServices* rpc_services, const TNetworkAddress& dest,
                             RuntimeFilterRpcClosure* rpc_closure, int timeout_ms, int64_t http_min_size,
                             const PTransmitRuntimeFilterParams& request);

void submit_async_runtime_filter_rpc(const RpcServices* rpc_services, const TNetworkAddress& dest, int timeout_ms,
                                     int64_t http_min_size, const PTransmitRuntimeFilterParams& request,
                                     const char* debug_info);

} // namespace starrocks
