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

#include "exec/pipeline/hash_partition_context.h"

#include "exprs/expr.h"

namespace starrocks::pipeline {

Status HashPartitionContext::prepare(RuntimeState* state) {
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

Status HashPartitionContext::push_one_chunk_to_partitioner(RuntimeState* state, const ChunkPtr& chunk) {
    return _chunks_partitioner->offer<false>(chunk, nullptr, nullptr);
}

void HashPartitionContext::sink_complete() {
    _is_sink_complete = true;
}

bool HashPartitionContext::has_output() {
    return _is_sink_complete && (!_chunks_partitioner->is_hash_map_eos() || _acc.has_output());
}

bool HashPartitionContext::is_finished() {
    if (!_is_sink_complete) {
        return false;
    }
    return !has_output();
}

StatusOr<ChunkPtr> HashPartitionContext::pull_one_chunk(RuntimeState* state) {
    if (!_chunks_partitioner->is_hash_map_eos()) {
        _chunks_partitioner->consume_from_hash_map([&](int32_t partition_idx, ChunkPtr& chunk) {
            _acc.push(std::move(chunk));
            return _acc.need_input();
        });
        if (_chunks_partitioner->is_hash_map_eos()) {
            _acc.finalize();
        }
    }
    if (_acc.has_output()) {
        return std::move(_acc.pull());
    } else {
        return nullptr;
    }
}

HashPartitionContext* HashPartitionContextFactory::create(int32_t driver_sequence) {
    if (auto it = _ctxs.find(driver_sequence); it != _ctxs.end()) {
        return it->second.get();
    }

    auto ctx = std::make_shared<HashPartitionContext>(_t_partition_exprs);
    auto* ctx_raw_ptr = ctx.get();
    _ctxs.emplace(driver_sequence, std::move(ctx));
    return ctx_raw_ptr;
}
}; // namespace starrocks::pipeline
