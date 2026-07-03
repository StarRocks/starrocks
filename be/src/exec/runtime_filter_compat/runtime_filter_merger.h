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
#include <map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/uid_util.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/runtime_filter_compat/runtime_filter_serde.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"

namespace starrocks {

struct RuntimeServices;
struct RpcServices;
class RuntimeFilter;

using SkewBroadcastRfMaterial = RuntimeFilterSkewMaterial;

class RuntimeFilterMergerStatus {
public:
    RuntimeFilterMergerStatus() = default;
    RuntimeFilterMergerStatus(RuntimeFilterMergerStatus&& other) noexcept
            : arrives(std::move(other.arrives)),
              expect_number(other.expect_number),
              pool(std::move(other.pool)),
              filters(std::move(other.filters)),
              current_size(other.current_size),
              max_size(other.max_size),
              stop(other.stop),
              is_skew_join(other.is_skew_join),
              recv_first_filter_ts(other.recv_first_filter_ts),
              recv_last_filter_ts(other.recv_last_filter_ts),
              broadcast_filter_ts(other.broadcast_filter_ts) {}
    // merge skew_broadcast_rf_material into out's _hash_partition_bf
    Status _merge_skew_broadcast_runtime_filter(RuntimeFilter* out);
    // which be number send this rf.
    std::unordered_set<int32_t> arrives;
    // how many partitioned rf we expect
    int32_t expect_number;
    ObjectPool pool;
    // each partitioned rf.
    std::map<int32_t, RuntimeFilter*> filters;
    size_t current_size = 0;
    size_t max_size = 0;
    bool stop = false;
    bool exceeded = true;
    bool is_skew_join;

    // only set when skew join optimization is used
    SkewBroadcastRfMaterial* skew_broadcast_rf_material = nullptr;
    // statistics.
    // timestamp in ms since unix epoch;
    // we care about diff not abs value.
    int64_t recv_first_filter_ts = 0;
    int64_t recv_last_filter_ts = 0;
    int64_t broadcast_filter_ts = 0;

    // only send once
    bool isSent = false;
};

// RuntimeFilterMerger is to merge partitioned RF
// and sent merged RF to consumer nodes.
class RuntimeFilterMerger {
public:
    RuntimeFilterMerger(const RuntimeServices* runtime_services, const RpcServices* rpc_services,
                        const UniqueId& query_id, const TQueryOptions& query_options, bool is_pipeline);
    Status init(const TRuntimeFilterParams& params);
    void merge_runtime_filter(PTransmitRuntimeFilterParams& params);
    void store_skew_broadcast_join_runtime_filter(PTransmitRuntimeFilterParams& params);

private:
    void _send_total_runtime_filter(int rf_version, int32_t filter_id);
    // filter_id -> where this filter should send to
    std::map<int32_t, std::vector<TRuntimeFilterProberParams>> _targets;
    std::map<int32_t, RuntimeFilterMergerStatus> _statuses;
    const RuntimeServices* _runtime_services;
    const RpcServices* _rpc_services;
    UniqueId _query_id;
    TQueryOptions _query_options;
    const bool _is_pipeline;
};

} // namespace starrocks
