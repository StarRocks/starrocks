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
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/exchange/exchange_merge_sort_source_operator.h"
#include "exec/pipeline/exchange/exchange_source_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

ExchangeNode::ExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _texchange_node(tnode.exchange_node),
          _num_senders(0),
          _stream_recvr(nullptr),
          _input_row_desc(
                  descs, tnode.exchange_node.input_row_tuples,
                  std::vector<bool>(tnode.nullable_tuples.begin(),
                                    tnode.nullable_tuples.begin() + tnode.exchange_node.input_row_tuples.size())),
          _is_merging(tnode.exchange_node.__isset.sort_info),
          _offset(tnode.exchange_node.__isset.offset ? tnode.exchange_node.offset : 0),
          _num_rows_skipped(0) {
    DCHECK_GE(_offset, 0);
    DCHECK(_is_merging || (_offset == 0));
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
    RETURN_IF_ERROR(ExecNode::prepare(state));
    // TODO: figure out appropriate buffer size
    DCHECK_GT(_num_senders, 0);
    _sub_plan_query_statistics_recvr.reset(new QueryStatisticsRecvr());
    _stream_recvr = state->exec_env()->stream_mgr()->create_recvr(
            state, _input_row_desc, state->fragment_instance_id(), _id, _num_senders,
            config::exchg_node_buffer_size_bytes, _runtime_profile, _is_merging, _sub_plan_query_statistics_recvr,
            false, DataStreamRecvr::INVALID_DOP_FOR_NON_PIPELINE_LEVEL_SHUFFLE, false);
    if (_is_merging) {
        RETURN_IF_ERROR(_sort_exec_exprs.prepare(state, _row_descriptor, _row_descriptor));
    }
    return Status::OK();
}

Status ExchangeNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    if (_is_merging) {
        RETURN_IF_ERROR(_sort_exec_exprs.open(state));
        RETURN_IF_ERROR(_stream_recvr->create_merger(state, &_sort_exec_exprs, &_is_asc_order, &_nulls_first));
    }
    return Status::OK();
}

Status ExchangeNode::collect_query_statistics(QueryStatistics* statistics) {
    RETURN_IF_ERROR(ExecNode::collect_query_statistics(statistics));
    _sub_plan_query_statistics_recvr->aggregate(statistics);
    return Status::OK();
}

Status ExchangeNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    if (_is_merging) {
        _sort_exec_exprs.close(state);
    }
    if (_stream_recvr != nullptr) {
        _stream_recvr->close();
    }
    // _stream_recvr.reset();
    return ExecNode::close(state);
}

Status ExchangeNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if (_is_finished || reached_limit()) {
        *eos = true;
        *chunk = nullptr;
        return Status::OK();
    }

    if (_is_merging) {
        RETURN_IF_ERROR(get_next_merging(state, chunk, eos));
        eval_join_runtime_filters(chunk);
        return Status::OK();
    }

    do {
        RETURN_IF_ERROR(_stream_recvr->get_chunk(&_input_chunk));
        *eos = (_input_chunk == nullptr);
        if (*eos) {
            *chunk = nullptr;
            return Status::OK();
        }
        eval_join_runtime_filters(_input_chunk.get());
    } while (_input_chunk->num_rows() <= 0);

    _num_rows_returned += _input_chunk->num_rows();
    if (reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        _input_chunk->set_num_rows(_input_chunk->num_rows() - num_rows_over);
        COUNTER_SET(_rows_returned_counter, _limit);
        *chunk = std::move(_input_chunk);
        _is_finished = true;

        DCHECK_CHUNK(*chunk);
        return Status::OK();
    }

    *chunk = std::move(_input_chunk);

    COUNTER_SET(_rows_returned_counter, _num_rows_returned);

    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

Status ExchangeNode::get_next_merging(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("Exchange, while merging next."));

    *chunk = nullptr;
    if (_is_finished || reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    if (_num_rows_skipped < _offset) {
        ChunkPtr tmp_chunk;
        do {
            RETURN_IF_ERROR(_stream_recvr->get_next(&tmp_chunk, &_is_finished));
            if (tmp_chunk != nullptr) {
                _num_rows_skipped += tmp_chunk->num_rows();
            }
        } while (_num_rows_skipped < _offset && !_is_finished);
        *eos = _is_finished;

        if (_num_rows_skipped > _offset) {
            int64_t size = _num_rows_skipped - _offset;
            int64_t offset_in_chunk = tmp_chunk->num_rows() - size;
            if (_limit > 0 && size > _limit) {
                size = _limit;
            }
            *chunk = tmp_chunk->clone_empty_with_slot(size);
            for (size_t c = 0; c < tmp_chunk->num_columns(); ++c) {
                const ColumnPtr& src = tmp_chunk->get_column_by_index(c);
                ColumnPtr& dest = (*chunk)->get_column_by_index(c);
                dest->append(*src, offset_in_chunk, size);
                // resize constant column as same as other non-constant columns, so Chunk::num_rows()
                // can return a right number if this ConstColumn is the first column of the chunk.
                if (dest->is_constant()) {
                    dest->resize(size);
                }
            }
            _num_rows_skipped = _offset;
            _num_rows_returned += size;
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);
            // the first Chunk will have a size less than chunk_size.
            return Status::OK();
        }

        if (_is_finished) {
            // check EOS after (_num_rows_skipped < _offset), so the only one chunk can be returned.
            return Status::OK();
        }
    }

    do {
        RETURN_IF_ERROR(_stream_recvr->get_next(chunk, &_is_finished));
        if ((*chunk) != nullptr) {
            size_t size_in_chunk = (*chunk)->num_rows();
            if (_limit > 0 && size_in_chunk + _num_rows_returned > _limit) {
                size_in_chunk -= (size_in_chunk + _num_rows_returned - _limit);
                (*chunk)->set_num_rows(size_in_chunk);
            }
            _num_rows_returned += size_in_chunk;
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);
            break;
        }
    } while (!_is_finished);
    *eos = _is_finished;

    return Status::OK();
}

void ExchangeNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "ExchangeNode(#senders=" << _num_senders;
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

pipeline::OpFactories ExchangeNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    OpFactories operators;
    if (!_is_merging) {
        auto exchange_source_op = std::make_shared<ExchangeSourceOperatorFactory>(
                context->next_operator_id(), id(), _texchange_node, _num_senders, _input_row_desc);
        exchange_source_op->set_degree_of_parallelism(context->degree_of_parallelism());
        operators.emplace_back(exchange_source_op);
    } else {
        auto exchange_merge_sort_source_operator = std::make_shared<ExchangeMergeSortSourceOperatorFactory>(
                context->next_operator_id(), id(), _num_senders, _input_row_desc, &_sort_exec_exprs, _is_asc_order,
                _nulls_first, _offset, _limit);
        exchange_merge_sort_source_operator->set_degree_of_parallelism(1);
        operators.emplace_back(std::move(exchange_merge_sort_source_operator));
    }

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(operators.back().get(), context, rc_rf_probe_collector);

    if (operators.back()->has_runtime_filters()) {
        may_add_chunk_accumulate_operator(operators, context, id());
    }

    if (limit() != -1) {
        operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    operators = context->maybe_interpolate_collect_stats(runtime_state(), operators);

    return operators;
}

} // namespace starrocks
