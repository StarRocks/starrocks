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

#include "gen_cpp/RuntimeFilter_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/runtime_filter_event_type.h"

namespace starrocks {

class RuntimeFilterSender {
public:
    virtual ~RuntimeFilterSender() = default;

    virtual void send_part_runtime_filter(PTransmitRuntimeFilterParams&& params,
                                          const std::vector<TNetworkAddress>& addrs, int timeout_ms,
                                          int64_t rpc_http_min_size, EventType type) = 0;
    virtual void send_broadcast_runtime_filter(PTransmitRuntimeFilterParams&& params,
                                               const std::vector<TRuntimeFilterDestination>& destinations,
                                               int timeout_ms, int64_t rpc_http_min_size) = 0;
};

} // namespace starrocks
