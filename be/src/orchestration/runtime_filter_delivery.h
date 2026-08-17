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
#include <string>
#include <vector>

#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"

namespace starrocks {

struct RuntimeServices;
struct RpcServices;

namespace orchestration {

class FragmentMgr;

class RuntimeFilterDelivery {
public:
    RuntimeFilterDelivery(const RuntimeServices* runtime_services, const RpcServices* rpc_services,
                          FragmentMgr* fragment_mgr)
            : _runtime_services(runtime_services), _rpc_services(rpc_services), _fragment_mgr(fragment_mgr) {}

    void receive_total_runtime_filter(PTransmitRuntimeFilterParams& params, int timeout_ms, int64_t rpc_http_min_size);
    void process_send_broadcast_runtime_filter_event(PTransmitRuntimeFilterParams&& params,
                                                     std::vector<TRuntimeFilterDestination>&& destinations,
                                                     int timeout_ms, int64_t rpc_http_min_size);
    void deliver_part_runtime_filter(std::vector<TNetworkAddress>&& transmit_addrs,
                                     PTransmitRuntimeFilterParams&& params, int transmit_timeout_ms,
                                     int64_t rpc_http_min_size, const std::string& msg);

private:
    void _deliver_broadcast_runtime_filter_passthrough(PTransmitRuntimeFilterParams&& params,
                                                       std::vector<TRuntimeFilterDestination>&& destinations,
                                                       int timeout_ms, int64_t rpc_http_min_size);
    void _deliver_broadcast_runtime_filter_relay(PTransmitRuntimeFilterParams&& params,
                                                 std::vector<TRuntimeFilterDestination>&& destinations, int timeout_ms,
                                                 int64_t rpc_http_min_size);
    void _deliver_broadcast_runtime_filter_local(PTransmitRuntimeFilterParams& params,
                                                 const TRuntimeFilterDestination& destinations, int timeout_ms,
                                                 int64_t rpc_http_min_size);

    const RuntimeServices* _runtime_services;
    const RpcServices* _rpc_services;
    FragmentMgr* _fragment_mgr;
};

} // namespace orchestration
} // namespace starrocks
