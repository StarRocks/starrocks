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

#include <array>
#include <atomic>
#include <cstdint>
#include <type_traits>
#include <vector>

#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/runtime_filter_event_type.h"

namespace starrocks {

// RuntimeFilterWorker works in a separated thread, and does following jobs:
// 1. deserialize runtime filters.
// 2. merge runtime filters.

// it works in a event-driven way, and possible events are:
// - create a runtime filter merger for a query
// - receive partitioned RF, deserialize it and merge it, and sent total RF(for merge node)
// - receive total RF and send it to RuntimeFilterPort
// - send partitioned RF(for hash join node)
// - close a query(delete runtime filter merger)
struct RuntimeFilterWorkerEvent {
    RuntimeFilterWorkerEvent() = default;
    EventType type;
    TUniqueId query_id;
    // For OPEN_QUERY.
    TQueryOptions query_options;
    TRuntimeFilterParams create_rf_merger_request;
    bool is_opened_by_pipeline;
    // For SEND_PART_RF.
    std::vector<TNetworkAddress> transmit_addrs;
    std::vector<TRuntimeFilterDestination> destinations;
    int transmit_timeout_ms;
    int64_t transmit_via_http_min_size;
    // For SEND_PART_RF, RECEIVE_PART_RF, and RECEIVE_TOTAL_RF.
    PTransmitRuntimeFilterParams transmit_rf_request;
};

static_assert(std::is_move_assignable<RuntimeFilterWorkerEvent>::value);

struct RuntimeFilterWorkerMetrics {
    void update_event_nums(EventType event_type, int64_t delta) { event_nums[event_type] += delta; }

    void update_rf_bytes(EventType event_type, int64_t delta) { runtime_filter_bytes[event_type] += delta; }

    int64_t total_rf_bytes() {
        int64_t total = 0;
        for (int i = 0; i < EventType::MAX_COUNT; i++) {
            total += runtime_filter_bytes[i];
        }
        return total;
    }

    std::array<std::atomic_int64_t, EventType::MAX_COUNT> event_nums{};
    std::array<std::atomic_int64_t, EventType::MAX_COUNT> runtime_filter_bytes{};
};

} // namespace starrocks
