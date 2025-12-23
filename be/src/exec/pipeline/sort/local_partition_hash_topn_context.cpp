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

#include "exec/pipeline/sort/local_partition_hash_topn_context.h"

#include <utility>

#include "column/chunk.h"
#include "common/constexpr.h"
#include "exprs/expr.h"
#include "fmt/format.h"

namespace starrocks::pipeline {

LocalPartitionHashTopnContext::LocalPartitionHashTopnContext(const std::vector<TExpr>& t_partition_exprs,
                                                             bool has_nullable_key,
                                                             const std::vector<ExprContext*>& sort_exprs,
                                                             std::vector<bool> is_asc_order,
                                                             std::vector<bool> is_null_first, std::string sort_keys,
                                                             int64_t offset, int64_t partition_limit)
        : _t_partition_exprs(t_partition_exprs),
          _has_nullable_key(has_nullable_key),
          _sort_exprs(sort_exprs),
          _is_asc_order(std::move(is_asc_order)),
          _is_null_first(std::move(is_null_first)),
          _sort_keys(std::move(sort_keys)),
          _offset(offset),
          _partition_limit(partition_limit) {}

Status LocalPartitionHashTopnContext::prepare(RuntimeState* state, RuntimeProfile* runtime_profile) {
    _runtime_state = state;
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_partition_exprs, &_partition_exprs, state));
    RETURN_IF_ERROR(Expr::prepare(_partition_exprs, state));
    RETURN_IF_ERROR(Expr::open(_partition_exprs, state));
    for (auto& expr : _partition_exprs) {
        auto& type_desc = expr->root()->type();
        if (!type_desc.support_groupby()) {
            return Status::NotSupported(fmt::format("partition by type {} is not supported", type_desc.debug_string()));
        }
    }
    auto partition_size = _t_partition_exprs.size();
    _partition_types.resize(partition_size);
    for (auto i = 0; i < partition_size; ++i) {
        TExprNode expr = _t_partition_exprs[i].nodes[0];
        _partition_types[i].result_type = TypeDescriptor::from_thrift(expr.type);
        _partition_types[i].is_nullable = expr.is_nullable;
        _has_nullable_key = _has_nullable_key || _partition_types[i].is_nullable;
    }

    _mem_pool = std::make_unique<MemPool>();
    _chunks_partitioner =
            std::make_shared<ChunksPartitioner>(_has_nullable_key, _partition_exprs, _partition_types, _mem_pool.get());
    RETURN_IF_ERROR(_chunks_partitioner->prepare(state, runtime_profile, false));

    _hash_topn =
            std::make_unique<PartitionHashTopn>(&_sort_exprs, _is_asc_order, _is_null_first, _offset, _partition_limit);
    return Status::OK();
}

Status LocalPartitionHashTopnContext::push_one_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    auto new_partition_cb = [this](size_t partition_idx) { (void)_hash_topn->new_partition(partition_idx); };
    auto partition_consumer = [this](size_t partition_idx, const ChunkPtr& partition_chunk) {
        (void)_hash_topn->offer(partition_idx, partition_chunk);
    };
    auto partition_validator = [this](size_t partition_idx) { return _hash_topn->is_valid(partition_idx); };
    return _chunks_partitioner->offer<true>(chunk, std::move(new_partition_cb), std::move(partition_consumer),
                                            std::move(partition_validator));
}

void LocalPartitionHashTopnContext::sink_complete() {
    _is_sink_complete = true;
}

Status LocalPartitionHashTopnContext::finalize(RuntimeState* state) {
    if (_is_finalized) {
        return Status::OK();
    }
    _partition_num = _chunks_partitioner->num_partitions();
    RETURN_IF_ERROR(
            _chunks_partitioner->consume_from_hash_map([this, state](int32_t partition_idx, const ChunkPtr& chunk) {
                return _hash_topn->offer(static_cast<size_t>(partition_idx), chunk).ok();
            }));
    RETURN_IF_ERROR(_hash_topn->done());
    _is_finalized = true;
    _mem_pool.reset();
    return Status::OK();
}

bool LocalPartitionHashTopnContext::has_output() const {
    return _is_finalized && !_hash_topn->exhausted();
}

bool LocalPartitionHashTopnContext::is_finished() const {
    if (!_is_sink_complete) {
        return false;
    }
    return _is_finalized && _hash_topn->exhausted();
}

StatusOr<ChunkPtr> LocalPartitionHashTopnContext::pull_one_chunk() {
    if (!_is_finalized) {
        RETURN_IF_ERROR(finalize(_runtime_state));
    }
    auto chunk_size = _runtime_state != nullptr ? _runtime_state->chunk_size() : DEFAULT_CHUNK_SIZE;
    return _hash_topn->get_next_chunk(chunk_size);
}

LocalPartitionHashTopnContextFactory::LocalPartitionHashTopnContextFactory(
        RuntimeState* state, const TTopNType::type topn_type, bool is_merging,
        const std::vector<ExprContext*>& sort_exprs, std::vector<bool> is_asc_order, std::vector<bool> is_null_first,
        const std::vector<TExpr>& t_partition_exprs, bool enable_pre_agg, const std::vector<TExpr>& t_pre_agg_exprs,
        const std::vector<TSlotId>& t_pre_agg_output_slot_id, int64_t offset, int64_t limit, std::string sort_keys,
        const std::vector<OrderByType>& order_by_types, bool has_outer_join_child,
        const std::vector<RuntimeFilterBuildDescriptor*>& rfs)
        : _sort_exprs(sort_exprs),
          _is_asc_order(std::move(is_asc_order)),
          _is_null_first(std::move(is_null_first)),
          _t_partition_exprs(t_partition_exprs),
          _offset(offset),
          _limit(limit),
          _sort_keys(std::move(sort_keys)) {
    (void)state;
    VLOG_ROW << "LocalPartitionHashTopnContextFactory constructor, sort_exprs size: " << _sort_exprs.size()
             << ", t_partition_exprs size: " << _t_partition_exprs.size() << ", offset: " << _offset
             << ", limit: " << _limit << ", sort_keys: " << _sort_keys << ", enable_pre_agg: " << enable_pre_agg
             << ", t_pre_agg_exprs size: " << t_pre_agg_exprs.size()
             << ", t_pre_agg_output_slot_id size: " << t_pre_agg_output_slot_id.size() << ", topn_type: " << topn_type
             << ", is_merging: " << is_merging << ", order_by_types size: " << order_by_types.size();
}

Status LocalPartitionHashTopnContextFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::prepare(_sort_exprs, state));
    RETURN_IF_ERROR(Expr::open(_sort_exprs, state));
    return Status::OK();
}

LocalPartitionHashTopnContext* LocalPartitionHashTopnContextFactory::create(int32_t driver_sequence) {
    if (auto it = _ctxs.find(driver_sequence); it != _ctxs.end()) {
        return it->second.get();
    }

    auto ctx = std::make_shared<LocalPartitionHashTopnContext>(_t_partition_exprs, false, _sort_exprs, _is_asc_order,
                                                               _is_null_first, _sort_keys, _offset, _limit);
    auto* raw = ctx.get();
    _ctxs.emplace(driver_sequence, std::move(ctx));
    return raw;
}

} // namespace starrocks::pipeline
