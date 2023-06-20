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

#include "exec/pipeline/sort/partition_sort_sink_operator.h"

#include <memory>

#include "exec/chunks_sorter.h"
#include "exec/chunks_sorter_full_sort.h"
#include "exec/chunks_sorter_heap_sort.h"
#include "exec/chunks_sorter_topn.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exprs/expr.h"
#include "exprs/runtime_filter_bank.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "gutil/casts.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_filter_worker.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "types/logical_type.h"

using namespace starrocks;

namespace starrocks::pipeline {
Status PartitionSortSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));

    _sort_context->ref();
    _sort_context->incr_sinker();

    _chunks_sorter->setup_runtime(state, _unique_metrics.get(), this->mem_tracker());

    return Status::OK();
}

void PartitionSortSinkOperator::close(RuntimeState* state) {
    _sort_context->unref(state);
    _chunks_sorter.reset();

    Operator::close(state);
}

StatusOr<ChunkPtr> PartitionSortSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from partition sort sink operator");
}

Status PartitionSortSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    auto materialize_chunk = ChunksSorter::materialize_chunk_before_sort(chunk.get(), _materialized_tuple_desc,
                                                                         _sort_exec_exprs, _order_by_types);
    RETURN_IF_ERROR(materialize_chunk);
    TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_chunks_sorter->update(state, materialize_chunk.value())));

    const auto& build_runtime_filters = _sort_context->build_runtime_filters();
    if (!build_runtime_filters.empty()) {
        auto runtime_filter = _chunks_sorter->runtime_filters(state->obj_pool());
        if (runtime_filter == nullptr) {
            return Status::OK();
        }
        DCHECK_EQ(runtime_filter->size(), build_runtime_filters.size());
        std::list<RuntimeFilterBuildDescriptor*> build_descs(build_runtime_filters.begin(),
                                                             build_runtime_filters.end());
        for (size_t i = 0; i < build_runtime_filters.size(); ++i) {
            build_runtime_filters[i]->set_or_intersect_filter((*runtime_filter)[i]);
            auto rf = build_runtime_filters[i]->runtime_filter();
            VLOG(1) << "runtime filter version:" << rf->rf_version() << "," << rf->debug_string() << rf;
            RuntimeBloomFilterList lst = {build_runtime_filters[i]};
            _sort_context->set_runtime_filter_collector(
                    _hub, _plan_node_id,
                    std::make_unique<RuntimeFilterCollector>(RuntimeInFilterList{}, std::move(lst)));
        }
        state->runtime_filter_port()->publish_runtime_filters(build_descs);
    }

    return Status::OK();
}

Status PartitionSortSinkOperator::set_finishing(RuntimeState* state) {
    // skip sorting if cancelled
    if (state->is_cancelled()) {
        _is_finished = true;
        return Status::Cancelled("runtime state is cancelled");
    }
    RETURN_IF_ERROR(_chunks_sorter->done(state));

    // Current partition sort is ended, and
    // the last call will drive LocalMergeSortSourceOperator to work.
    _sort_context->finish_partition(_chunks_sorter->get_output_rows());
    _is_finished = true;
    return Status::OK();
}

Status PartitionSortSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(_sort_exec_exprs.prepare(state, _parent_node_row_desc, _parent_node_child_row_desc));
    RETURN_IF_ERROR(_sort_exec_exprs.open(state));
    RETURN_IF_ERROR(Expr::prepare(_analytic_partition_exprs, state));
    RETURN_IF_ERROR(Expr::open(_analytic_partition_exprs, state));
    return Status::OK();
}

OperatorPtr PartitionSortSinkOperatorFactory::create(int32_t dop, int32_t driver_sequence) {
    std::shared_ptr<ChunksSorter> chunks_sorter;
    if (_limit >= 0) {
        if (_topn_type == TTopNType::ROW_NUMBER && _limit <= ChunksSorter::USE_HEAP_SORTER_LIMIT_SZ) {
            chunks_sorter = std::make_unique<ChunksSorterHeapSort>(
                    runtime_state(), &(_sort_exec_exprs.lhs_ordering_expr_ctxs()), &_is_asc_order, &_is_null_first,
                    _sort_keys, 0, _limit + _offset);
        } else {
            size_t max_buffered_chunks = ChunksSorterTopn::tunning_buffered_chunks(_limit);
            chunks_sorter = std::make_unique<ChunksSorterTopn>(
                    runtime_state(), &(_sort_exec_exprs.lhs_ordering_expr_ctxs()), &_is_asc_order, &_is_null_first,
                    _sort_keys, 0, _limit + _offset, _topn_type, max_buffered_chunks);
        }
    } else {
        chunks_sorter = std::make_unique<ChunksSorterFullSort>(
                runtime_state(), &(_sort_exec_exprs.lhs_ordering_expr_ctxs()), &_is_asc_order, &_is_null_first,
                _sort_keys, _max_buffered_rows, _max_buffered_bytes, _early_materialized_slots);
    }

    auto sort_context = _sort_context_factory->create(driver_sequence);
    sort_context->add_partition_chunks_sorter(chunks_sorter);
    auto ope = std::make_shared<PartitionSortSinkOperator>(this, _id, _plan_node_id, driver_sequence, chunks_sorter,
                                                           _sort_exec_exprs, _order_by_types, _materialized_tuple_desc,
                                                           sort_context.get(), _runtime_filter_hub);
    return ope;
}

void PartitionSortSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_analytic_partition_exprs, state);
    _sort_exec_exprs.close(state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
