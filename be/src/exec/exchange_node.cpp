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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/exchange_node.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/exchange_node.h"

#include "column/chunk.h"
#include "common/config_exec_flow_fwd.h"
#include "common/runtime_profile.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/exchange/exchange_merge_sort_source_operator.h"
#include "exec/pipeline/exchange/exchange_parallel_merge_source_operator.h"
#include "exec/pipeline/exchange/exchange_source_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/offset_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks {

ExchangeNode::ExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _texchange_node(tnode.exchange_node),
          _num_senders(0),
          _stream_recvr(nullptr),
          _input_row_desc(descs, tnode.exchange_node.input_row_tuples),
          _is_merging(tnode.exchange_node.__isset.sort_info),
          _is_parallel_merge(tnode.exchange_node.__isset.enable_parallel_merge &&
                             tnode.exchange_node.enable_parallel_merge),
          _offset(tnode.exchange_node.__isset.offset ? tnode.exchange_node.offset : 0),
          _num_rows_skipped(0) {
    DCHECK_GE(_offset, 0);
}

Status ExchangeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    if (!_is_merging) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_sort_exec_exprs.init(tnode.exchange_node.sort_info, _pool, state));
    _is_asc_order = tnode.exchange_node.sort_info.is_asc_order;
    _nulls_first = tnode.exchange_node.sort_info.nulls_first;
    return Status::OK();
}

Status ExchangeNode::prepare(RuntimeState* state) {
    return Status::NotSupported("non-pipeline execution is not supported");
}

Status ExchangeNode::open(RuntimeState* state) {
    return Status::NotSupported("non-pipeline execution is not supported");
}

Status ExchangeNode::collect_query_statistics(QueryStatistics* statistics) {
    RETURN_IF_ERROR(ExecNode::collect_query_statistics(statistics));
    _sub_plan_query_statistics_recvr->aggregate(statistics);
    return Status::OK();
}

void ExchangeNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    if (_is_merging) {
        _sort_exec_exprs.close(state);
    }
    if (_stream_recvr != nullptr) {
        _stream_recvr->close();
    }
    // _stream_recvr.reset();
    ExecNode::close(state);
}

Status ExchangeNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    return Status::NotSupported("non-pipeline execution is not supported");
}

Status ExchangeNode::get_next_merging(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    return Status::NotSupported("non-pipeline execution is not supported");
}

void ExchangeNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "ExchangeNode(#senders=" << _num_senders;
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

pipeline::OpFactories ExchangeNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    auto exec_group = context->find_exec_group_by_plan_node_id(_id);
    context->set_current_execution_group(exec_group);
    OpFactories operators;
    if (!_is_merging) {
        auto* query_ctx = context->runtime_state()->query_ctx();
        auto exchange_source_op = std::make_shared<ExchangeSourceOperatorFactory>(
                context->next_operator_id(), id(), _texchange_node, _num_senders, _input_row_desc,
                query_ctx->enable_pipeline_level_shuffle());
        exchange_source_op->set_degree_of_parallelism(context->degree_of_parallelism());
        operators.emplace_back(exchange_source_op);

        if (_offset > 0) {
            operators.emplace_back(std::make_shared<OffsetOperatorFactory>(context->next_operator_id(), id(), _offset));
        }

    } else {
        if ((_is_parallel_merge || _sort_exec_exprs.is_constant_lhs_ordering()) &&
            !_sort_exec_exprs.lhs_ordering_expr_ctxs().empty()) {
            auto exchange_merge_sort_source_operator = std::make_shared<ExchangeParallelMergeSourceOperatorFactory>(
                    context->next_operator_id(), id(), _num_senders, _input_row_desc, &_sort_exec_exprs, _is_asc_order,
                    _nulls_first, _offset, _limit);
            if (_texchange_node.__isset.parallel_merge_late_materialize_mode) {
                exchange_merge_sort_source_operator->set_materialized_mode(
                        _texchange_node.parallel_merge_late_materialize_mode);
            }
            exchange_merge_sort_source_operator->set_degree_of_parallelism(context->degree_of_parallelism());
            operators.emplace_back(std::move(exchange_merge_sort_source_operator));
            // This particular exchange source will be executed in a concurrent way, and finally we need to gather them into one
            // stream to satisfied the ordering property
            operators = context->maybe_interpolate_local_passthrough_exchange(runtime_state(), id(), operators);
        } else {
            auto exchange_merge_sort_source_operator = std::make_shared<ExchangeMergeSortSourceOperatorFactory>(
                    context->next_operator_id(), id(), _num_senders, _input_row_desc, &_sort_exec_exprs, _is_asc_order,
                    _nulls_first, _offset, _limit);
            exchange_merge_sort_source_operator->set_degree_of_parallelism(1);
            operators.emplace_back(std::move(exchange_merge_sort_source_operator));
        }
    }

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(operators.back().get(), context, rc_rf_probe_collector);

    if (limit() != -1) {
        operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    if (operators.back()->has_runtime_filters()) {
        may_add_chunk_accumulate_operator(operators, context, id());
    }

    operators = context->maybe_interpolate_debug_ops(runtime_state(), _id, operators);
    operators = context->maybe_interpolate_collect_stats(runtime_state(), id(), operators);

    return operators;
}

} // namespace starrocks
