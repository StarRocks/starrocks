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

#include "exec/pipeline/sort/local_partition_topn_context.h"

#include <exec/partition/chunks_partitioner.h>

#include <utility>

#include "exec/chunks_sorter_topn.h"

namespace starrocks::pipeline {

LocalPartitionTopnContext::LocalPartitionTopnContext(const std::vector<TExpr>& t_partition_exprs,
                                                     const std::vector<ExprContext*>& sort_exprs,
                                                     std::vector<bool> is_asc_order, std::vector<bool> is_null_first,
                                                     std::string sort_keys, int64_t offset, int64_t partition_limit,
                                                     const TTopNType::type topn_type)
        : _t_partition_exprs(t_partition_exprs),
          _sort_exprs(sort_exprs),
          _is_asc_order(std::move(is_asc_order)),
          _is_null_first(std::move(is_null_first)),
          _sort_keys(std::move(sort_keys)),
          _offset(offset),
          _partition_limit(partition_limit),
          _topn_type(topn_type) {}

Status LocalPartitionTopnContext::prepare(RuntimeState* state) {
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

    _chunks_partitioner = std::make_unique<ChunksPartitioner>(_has_nullable_key, _partition_exprs, _partition_types);
    return _chunks_partitioner->prepare(state);
}

Status LocalPartitionTopnContext::push_one_chunk_to_partitioner(RuntimeState* state, const ChunkPtr& chunk) {
    auto st = _chunks_partitioner->offer<true>(
            chunk,
            [this, state](size_t partition_idx) {
                _chunks_sorters.emplace_back(std::make_shared<ChunksSorterTopn>(
                        state, &_sort_exprs, &_is_asc_order, &_is_null_first, _sort_keys, _offset, _partition_limit,
                        _topn_type, ChunksSorterTopn::tunning_buffered_chunks(_partition_limit)));
            },
            [this, state](size_t partition_idx, const ChunkPtr& chunk) {
                (void)_chunks_sorters[partition_idx]->update(state, chunk);
            });
    if (_chunks_partitioner->is_passthrough()) {
        RETURN_IF_ERROR(transfer_all_chunks_from_partitioner_to_sorters(state));
    }
    return st;
}

void LocalPartitionTopnContext::sink_complete() {
    _is_sink_complete = true;
}

Status LocalPartitionTopnContext::transfer_all_chunks_from_partitioner_to_sorters(RuntimeState* state) {
    if (_is_transfered) {
        return Status::OK();
    }

    _partition_num = _chunks_partitioner->num_partitions();
    RETURN_IF_ERROR(
            _chunks_partitioner->consume_from_hash_map([this, state](int32_t partition_idx, const ChunkPtr& chunk) {
                _chunks_sorters[partition_idx]->update(state, chunk);
                return true;
            }));

    for (auto& chunks_sorter : _chunks_sorters) {
        RETURN_IF_ERROR(chunks_sorter->done(state));
    }

    _is_transfered = true;
    return Status::OK();
}

bool LocalPartitionTopnContext::has_output() {
    if (_chunks_partitioner->is_passthrough() && _is_transfered) {
        return _sorter_index < _chunks_sorters.size() || !_chunks_partitioner->is_passthrough_buffer_empty();
    }
    return _is_sink_complete && _sorter_index < _chunks_sorters.size();
}

bool LocalPartitionTopnContext::is_finished() {
    if (!_is_sink_complete) {
        return false;
    }
    return !has_output();
}

StatusOr<ChunkPtr> LocalPartitionTopnContext::pull_one_chunk() {
    ChunkPtr chunk = nullptr;
    if (_sorter_index < _chunks_sorters.size()) {
        ASSIGN_OR_RETURN(chunk, pull_one_chunk_from_sorters());
        if (chunk != nullptr) {
            return chunk;
        }
    }
    chunk = _chunks_partitioner->consume_from_passthrough_buffer();
    return chunk;
}

StatusOr<ChunkPtr> LocalPartitionTopnContext::pull_one_chunk_from_sorters() {
    auto& chunks_sorter = _chunks_sorters[_sorter_index];
    ChunkPtr chunk = nullptr;
    bool eos = false;
    RETURN_IF_ERROR(chunks_sorter->get_next(&chunk, &eos));
    if (eos) {
        // Current sorter has no output, try to get chunk from next sorter
        _sorter_index++;
    }
    return chunk;
}

LocalPartitionTopnContextFactory::LocalPartitionTopnContextFactory(
        RuntimeState*, const TTopNType::type topn_type, bool is_merging, const std::vector<ExprContext*>& sort_exprs,
        std::vector<bool> is_asc_order, std::vector<bool> is_null_first, const std::vector<TExpr>& t_partition_exprs,
        int64_t offset, int64_t limit, std::string sort_keys, const std::vector<OrderByType>& order_by_types,
        const std::vector<RuntimeFilterBuildDescriptor*>&)
        : _topn_type(topn_type),
          _sort_exprs(sort_exprs),
          _is_asc_order(std::move(is_asc_order)),
          _is_null_first(std::move(is_null_first)),
          _t_partition_exprs(t_partition_exprs),
          _offset(offset),
          _partition_limit(limit),
          _sort_keys(std::move(sort_keys)) {}

LocalPartitionTopnContext* LocalPartitionTopnContextFactory::create(int32_t driver_sequence) {
    if (auto it = _ctxs.find(driver_sequence); it != _ctxs.end()) {
        return it->second.get();
    }

    auto ctx =
            std::make_shared<LocalPartitionTopnContext>(_t_partition_exprs, _sort_exprs, _is_asc_order, _is_null_first,
                                                        _sort_keys, _offset, _partition_limit, _topn_type);
    auto* ctx_raw_ptr = ctx.get();
    _ctxs.emplace(driver_sequence, std::move(ctx));
    return ctx_raw_ptr;
}

Status LocalPartitionTopnContextFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::prepare(_sort_exprs, state));
    RETURN_IF_ERROR(Expr::open(_sort_exprs, state));
    return Status::OK();
}

} // namespace starrocks::pipeline
