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
#include <vector>

#include "exec/partition/chunks_partitioner.h"
#include "exec/partition/partition_hash_topn.h"
#include "exec/pipeline/schedule/observer.h"
#include "exec/sort_exec_exprs.h"
#include "exprs/runtime_filter.h"
#include "runtime/runtime_state.h"

namespace starrocks {
class RuntimeFilterBuildDescriptor;
} // namespace starrocks

namespace starrocks::pipeline {

// A lightweight hash+sort local partition topn that keeps only topN rows across partitions.
// Unlike LocalPartitionTopnContext, this version does not support pre-aggregation.
class LocalPartitionHashTopnContext {
public:
    LocalPartitionHashTopnContext(const std::vector<TExpr>& t_partition_exprs, bool has_nullable_key,
                                  const std::vector<ExprContext*>& sort_exprs, std::vector<bool> is_asc_order,
                                  std::vector<bool> is_null_first, std::string sort_keys, int64_t offset,
                                  int64_t partition_limit);

    Status prepare(RuntimeState* state, RuntimeProfile* runtime_profile);

    Status push_one_chunk(RuntimeState* state, const ChunkPtr& chunk);

    void sink_complete();

    Status finalize(RuntimeState* state);

    bool has_output() const;

    bool is_finished() const;

    StatusOr<ChunkPtr> pull_one_chunk();

    size_t num_partitions() const { return _partition_num; }

    PipeObservable& observable() { return _observable; }

private:
    Status _consume_partition_chunks(RuntimeState* state);

    const std::vector<TExpr>& _t_partition_exprs;
    std::vector<ExprContext*> _partition_exprs;
    std::vector<PartitionColumnType> _partition_types;
    bool _has_nullable_key = false;

    std::unique_ptr<MemPool> _mem_pool = nullptr;
    ChunksPartitionerPtr _chunks_partitioner;
    size_t _partition_num = 0;

    const std::vector<ExprContext*>& _sort_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _is_null_first;
    const std::string _sort_keys;
    int64_t _offset;
    int64_t _partition_limit;

    std::unique_ptr<PartitionHashTopn> _hash_topn;
    bool _is_sink_complete = false;
    bool _is_finalized = false;

    RuntimeState* _runtime_state = nullptr;

    PipeObservable _observable;
};

using LocalPartitionHashTopnContextPtr = std::shared_ptr<LocalPartitionHashTopnContext>;

class LocalPartitionHashTopnContextFactory {
public:
    LocalPartitionHashTopnContextFactory(RuntimeState* state, const TTopNType::type topn_type, bool is_merging,
                                         const std::vector<ExprContext*>& sort_exprs, std::vector<bool> is_asc_order,
                                         std::vector<bool> is_null_first, const std::vector<TExpr>& t_partition_exprs,
                                         bool enable_pre_agg, const std::vector<TExpr>& t_pre_agg_exprs,
                                         const std::vector<TSlotId>& t_pre_agg_output_slot_id, int64_t offset,
                                         int64_t limit, std::string sort_keys,
                                         const std::vector<OrderByType>& order_by_types, bool has_outer_join_child,
                                         const std::vector<RuntimeFilterBuildDescriptor*>& rfs);

    Status prepare(RuntimeState* state);

    LocalPartitionHashTopnContext* create(int32_t driver_sequence);

private:
    std::unordered_map<int32_t, LocalPartitionHashTopnContextPtr> _ctxs;
    const std::vector<ExprContext*>& _sort_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _is_null_first;
    const std::vector<TExpr>& _t_partition_exprs;
    int64_t _offset;
    int64_t _limit;
    const std::string _sort_keys;
};

using LocalPartitionHashTopnContextFactoryPtr = std::shared_ptr<LocalPartitionHashTopnContextFactory>;

} // namespace starrocks::pipeline
