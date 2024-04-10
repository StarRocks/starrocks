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

#include "exec/pipeline/exchange/exchange_parallel_merge_source_operator.h"

#include "exec/sort_exec_exprs.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/exec_env.h"

namespace starrocks::pipeline {

Status ExchangeParallelMergeSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    auto* factory = down_cast<ExchangeParallelMergeSourceOperatorFactory*>(_factory);
    _stream_recvr = factory->get_stream_recvr(state);
    _stream_recvr->bind_profile(_driver_sequence, _unique_metrics);
    _merger = factory->get_merge_path_merger(state);
    _merger->bind_profile(_driver_sequence, _unique_metrics.get());
    return Status::OK();
}

bool ExchangeParallelMergeSourceOperator::has_output() const {
    if (_is_finished) {
        return false;
    }
    if (_merger->is_current_stage_finished(_driver_sequence, false)) {
        return false;
    }
    if (_merger->is_pending(_driver_sequence)) {
        return false;
    }
    return true;
}

bool ExchangeParallelMergeSourceOperator::is_finished() const {
    return _is_finished;
}

Status ExchangeParallelMergeSourceOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    auto* factory = down_cast<ExchangeParallelMergeSourceOperatorFactory*>(_factory);
    factory->close_stream_recvr();
    return Status::OK();
}

StatusOr<ChunkPtr> ExchangeParallelMergeSourceOperator::pull_chunk(RuntimeState* state) {
    ChunkPtr chunk = _merger->try_get_next(_driver_sequence);

    if (_merger->is_finished()) {
        _is_finished = true;
    }

    eval_runtime_bloom_filters(chunk.get());
    return std::move(chunk);
}

Status ExchangeParallelMergeSourceOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(_sort_exec_exprs->prepare(state, _row_desc, _row_desc));
    RETURN_IF_ERROR(_sort_exec_exprs->open(state));
    return Status::OK();
}

OperatorPtr ExchangeParallelMergeSourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    ++_stream_recvr_cnt;
    return std::make_shared<ExchangeParallelMergeSourceOperator>(this, _id, _plan_node_id, driver_sequence);
}

void ExchangeParallelMergeSourceOperatorFactory::close(RuntimeState* state) {
    _sort_exec_exprs->close(state);
    OperatorFactory::close(state);
}

DataStreamRecvr* ExchangeParallelMergeSourceOperatorFactory::get_stream_recvr(RuntimeState* state) {
    if (_stream_recvr == nullptr) {
        auto query_statistic_recv = state->query_recv();
        _stream_recvr = state->exec_env()->stream_mgr()->create_recvr(
                state, _row_desc, state->fragment_instance_id(), _plan_node_id, _num_sender,
                config::exchg_node_buffer_size_bytes, true, query_statistic_recv, true, _degree_of_parallelism, true);
    }
    return _stream_recvr.get();
}

merge_path::MergePathCascadeMerger* ExchangeParallelMergeSourceOperatorFactory::get_merge_path_merger(
        RuntimeState* state) {
    if (_merger == nullptr) {
        auto chunk_providers = _stream_recvr->create_merge_path_chunk_providers();
        SortDescs sort_descs(_is_asc_order, _nulls_first);
        _merger = std::make_unique<merge_path::MergePathCascadeMerger>(
                state->chunk_size(), degree_of_parallelism(), _sort_exec_exprs->lhs_ordering_expr_ctxs(), sort_descs,
                _row_desc.tuple_descriptors()[0], TTopNType::ROW_NUMBER, _offset, _limit, chunk_providers);
    }
    return _merger.get();
}

void ExchangeParallelMergeSourceOperatorFactory::close_stream_recvr() {
    if (--_stream_recvr_cnt == 0) {
        _stream_recvr->close();
    }
}

} // namespace starrocks::pipeline
