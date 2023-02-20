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

#include <memory>
#include <unordered_map>

#include "exec/pipeline/operator.h"
#include "exec/query_cache/cache_manager.h"
#include "exec/query_cache/cache_param.h"
#include "exec/query_cache/lane_arbiter.h"
#include "exec/query_cache/multilane_operator.h"
#include "storage/rowset/rowset.h"
namespace starrocks {
namespace pipeline {
class PipelineDriver;
using DriverRawPtr = PipelineDriver*;
} // namespace pipeline

namespace query_cache {
class PerLaneBuffer;
using PerLaneBufferRawPtr = PerLaneBuffer*;
using PerLaneBufferPtr = std::unique_ptr<PerLaneBuffer>;
using PerLaneBuffers = std::vector<PerLaneBufferPtr>;

class CacheOperator;
using CacheOperatorRawPtr = CacheOperator*;
using CacheOperatorPtr = std::shared_ptr<CacheOperator>;

class CacheOperator final : public pipeline::Operator {
public:
    CacheOperator(pipeline::OperatorFactory* factory, int32_t driver_sequence, CacheManagerRawPtr cache_mgr,
                  const CacheParam& cache_param);

    ~CacheOperator() override = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    bool probe_cache(int64_t tablet_id, int64_t version);
    Status reset_lane(RuntimeState* state, LaneOwnerType lane_owner);
    void populate_cache(int64_t tablet_id);
    int64_t cached_version(int64_t tablet_id);
    std::tuple<int64_t, std::vector<RowsetSharedPtr>> delta_version_and_rowsets(int64_t tablet_id);
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    bool has_output() const override;
    bool need_input() const override;
    bool is_finished() const override;
    Status set_finished(RuntimeState* state) override;
    Status set_finishing(RuntimeState* state) override;
    LaneArbiterPtr lane_arbiter() { return _lane_arbiter; }
    void set_multilane_operators(MultilaneOperators&& multilane_operators) {
        _multilane_operators = std::move(multilane_operators);
    }
    void set_scan_operator(pipeline::OperatorRawPtr scan_operator) { _scan_operator = scan_operator; }

private:
    void _update_probe_metrics(int64_t, const std::vector<ChunkPtr>& chunks);
    void _handle_stale_cache_value(int64_t tablet_id, CacheValue& cache_value, PerLaneBufferPtr& buffer,
                                   int64_t version);
    void _handle_stale_cache_value_for_non_pk(int64_t tablet_id, CacheValue& cache_value, PerLaneBufferPtr& buffer,
                                              int64_t version);
    void _handle_stale_cache_value_for_pk(int64_t tablet_id, CacheValue& cache_value, PerLaneBufferPtr& buffer,
                                          int64_t version);
    bool _should_passthrough(size_t num_rows, size_t num_bytes);
    ChunkPtr _pull_chunk_from_per_lane_buffer(PerLaneBufferPtr& buffer);
    CacheManagerRawPtr _cache_mgr;
    const CacheParam& _cache_param;
    LaneArbiterPtr _lane_arbiter;
    std::unordered_map<int64_t, size_t> _owner_to_lanes;
    PerLaneBuffers _per_lane_buffers;
    ChunkPtr _passthrough_chunk;
    MultilaneOperators _multilane_operators;
    pipeline::OperatorRawPtr _scan_operator;
    bool _is_input_finished{false};

    std::unordered_set<int64_t> _populate_tablets;
    std::unordered_set<int64_t> _probe_tablets;
    std::unordered_set<int64_t> _all_tablets;

    RuntimeProfile::Counter* _cache_probe_timer = nullptr;
    RuntimeProfile::Counter* _cache_probe_chunks_counter = nullptr;
    RuntimeProfile::Counter* _cache_probe_tablets_counter = nullptr;
    RuntimeProfile::Counter* _cache_probe_rows_counter = nullptr;
    RuntimeProfile::Counter* _cache_probe_bytes_counter = nullptr;

    RuntimeProfile::Counter* _cache_populate_timer = nullptr;
    RuntimeProfile::Counter* _cache_populate_chunks_counter = nullptr;
    RuntimeProfile::Counter* _cache_populate_tablets_counter = nullptr;
    RuntimeProfile::Counter* _cache_populate_rows_counter = nullptr;
    RuntimeProfile::Counter* _cache_populate_bytes_counter = nullptr;

    RuntimeProfile::Counter* _cache_passthrough_timer = nullptr;
    RuntimeProfile::Counter* _cache_passthrough_chunks_counter = nullptr;
    RuntimeProfile::Counter* _cache_passthrough_tablets_counter = nullptr;
    RuntimeProfile::Counter* _cache_passthrough_rows_counter = nullptr;
    RuntimeProfile::Counter* _cache_passthrough_bytes_counter = nullptr;
};

class CacheOperatorFactory;
using CacheOperatorFactoryRawPtr = CacheOperatorFactory*;
using CacheOperatorFactoryPtr = std::shared_ptr<CacheOperatorFactory>;

class CacheOperatorFactory : public pipeline::OperatorFactory {
public:
    CacheOperatorFactory(int32_t id, int32_t plan_node_id, CacheManagerRawPtr cache_mgr, const CacheParam& cache_param);
    ~CacheOperatorFactory() override = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    pipeline::OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    CacheManagerRawPtr _cache_mgr;
    const CacheParam& _cache_param;
};
} // namespace query_cache
} // namespace starrocks
