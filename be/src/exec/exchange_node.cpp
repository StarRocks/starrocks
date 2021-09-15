// This file is made available under Elastic License 2.0.
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
#include "exec/pipeline/exchange/exchange_source_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

namespace starrocks {

ExchangeNode::ExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _num_senders(0),
          _stream_recvr(NULL),
          _input_row_desc(
                  descs, tnode.exchange_node.input_row_tuples,
                  std::vector<bool>(tnode.nullable_tuples.begin(),
                                    tnode.nullable_tuples.begin() + tnode.exchange_node.input_row_tuples.size())),
          _next_row_idx(0),
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

    RETURN_IF_ERROR(_sort_exec_exprs.init(tnode.exchange_node.sort_info, _pool));
    _is_asc_order = tnode.exchange_node.sort_info.is_asc_order;
    _nulls_first = tnode.exchange_node.sort_info.nulls_first;
    return Status::OK();
}

Status ExchangeNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _convert_row_batch_timer = ADD_TIMER(runtime_profile(), "ConvertRowBatchTime");
    // TODO: figure out appropriate buffer size
    DCHECK_GT(_num_senders, 0);
    _sub_plan_query_statistics_recvr.reset(new QueryStatisticsRecvr());
    _stream_recvr = state->exec_env()->stream_mgr()->create_recvr(
            state, _input_row_desc, state->fragment_instance_id(), _id, _num_senders,
            config::exchg_node_buffer_size_bytes, _runtime_profile, _is_merging, _sub_plan_query_statistics_recvr);
    if (_is_merging) {
        RETURN_IF_ERROR(_sort_exec_exprs.prepare(state, _row_descriptor, _row_descriptor, expr_mem_tracker()));
        // AddExprCtxsToFree(_sort_exec_exprs);
    }
    return Status::OK();
}

Status ExchangeNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    if (_use_vectorized) {
        if (_is_merging) {
            RETURN_IF_ERROR(_sort_exec_exprs.open(state));
            RETURN_IF_ERROR(_stream_recvr->create_merger(&_sort_exec_exprs, &_is_asc_order, &_nulls_first));
        }
        return Status::OK();
    }

    if (_is_merging) {
        RETURN_IF_ERROR(_sort_exec_exprs.open(state));
        TupleRowComparator less_than(_sort_exec_exprs, _is_asc_order, _nulls_first);
        // create_merger() will populate its merging heap with batches from the _stream_recvr,
        // so it is not necessary to call fill_input_row_batch().
        RETURN_IF_ERROR(_stream_recvr->create_merger(less_than));
    } else {
        RETURN_IF_ERROR(fill_input_row_batch(state));
    }
    return Status::OK();
}

Status ExchangeNode::collect_query_statistics(QueryStatistics* statistics) {
    RETURN_IF_ERROR(ExecNode::collect_query_statistics(statistics));
    statistics->merge(_sub_plan_query_statistics_recvr.get());
    return Status::OK();
}

Status ExchangeNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    if (_is_merging) {
        _sort_exec_exprs.close(state);
    }
    if (_stream_recvr != NULL) {
        _stream_recvr->close();
    }
    // _stream_recvr.reset();
    return ExecNode::close(state);
}

Status ExchangeNode::fill_input_row_batch(RuntimeState* state) {
    DCHECK(!_is_merging);
    Status ret_status;
    {
        // SCOPED_TIMER(state->total_network_receive_timer());
        ret_status = _stream_recvr->get_batch(&_input_batch);
    }
    VLOG_FILE << "exch: has batch=" << (_input_batch == NULL ? "false" : "true")
              << " #rows=" << (_input_batch != NULL ? _input_batch->num_rows() : 0)
              << " is_cancelled=" << (ret_status.is_cancelled() ? "true" : "false")
              << " instance_id=" << state->fragment_instance_id();
    return ret_status;
}

Status ExchangeNode::get_next(RuntimeState* state, RowBatch* output_batch, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (reached_limit()) {
        _stream_recvr->transfer_all_resources(output_batch);
        *eos = true;
        return Status::OK();
    } else {
        *eos = false;
    }

    if (_is_merging) {
        return get_next_merging(state, output_batch, eos);
    }

    ExprContext* const* ctxs = &_conjunct_ctxs[0];
    int num_ctxs = _conjunct_ctxs.size();

    while (true) {
        {
            SCOPED_TIMER(_convert_row_batch_timer);
            RETURN_IF_CANCELLED(state);
            // copy rows until we hit the limit/capacity or until we exhaust _input_batch
            while (!reached_limit() && !output_batch->at_capacity() && _input_batch != NULL &&
                   _next_row_idx < _input_batch->capacity()) {
                TupleRow* src = _input_batch->get_row(_next_row_idx);

                if (ExecNode::eval_conjuncts(ctxs, num_ctxs, src)) {
                    int j = output_batch->add_row();
                    TupleRow* dest = output_batch->get_row(j);
                    // if the input row is shorter than the output row, make sure not to leave
                    // uninitialized Tuple* around
                    output_batch->clear_row(dest);
                    // this works as expected if rows from input_batch form a prefix of
                    // rows in output_batch
                    _input_batch->copy_row(src, dest);
                    output_batch->commit_last_row();
                    ++_num_rows_returned;
                }

                ++_next_row_idx;
            }

            if (VLOG_ROW_IS_ON) {
                VLOG_ROW << "ExchangeNode output batch: " << output_batch->to_string();
            }

            COUNTER_SET(_rows_returned_counter, _num_rows_returned);

            if (reached_limit()) {
                _stream_recvr->transfer_all_resources(output_batch);
                *eos = true;
                return Status::OK();
            }

            if (output_batch->at_capacity()) {
                *eos = false;
                return Status::OK();
            }
        }

        // we need more rows
        if (_input_batch != NULL) {
            _input_batch->transfer_resource_ownership(output_batch);
        }

        RETURN_IF_ERROR(fill_input_row_batch(state));
        *eos = (_input_batch == NULL);
        if (*eos) {
            return Status::OK();
        }

        _next_row_idx = 0;
        DCHECK(_input_batch->row_desc().layout_is_prefix_of(output_batch->row_desc()));
    }
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
        return get_next_merging(state, chunk, eos);
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

Status ExchangeNode::get_next_merging(RuntimeState* state, RowBatch* output_batch, bool* eos) {
    DCHECK_EQ(output_batch->num_rows(), 0);
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("Exchange, while merging next."));

    RETURN_IF_ERROR(_stream_recvr->get_next(output_batch, eos));
    while ((_num_rows_skipped < _offset)) {
        _num_rows_skipped += output_batch->num_rows();
        // Throw away rows in the output batch until the offset is skipped.
        int rows_to_keep = _num_rows_skipped - _offset;
        if (rows_to_keep > 0) {
            output_batch->copy_rows(0, output_batch->num_rows() - rows_to_keep, rows_to_keep);
            output_batch->set_num_rows(rows_to_keep);
        } else {
            output_batch->set_num_rows(0);
        }
        if (rows_to_keep > 0 || *eos || output_batch->at_capacity()) {
            break;
        }
        RETURN_IF_ERROR(_stream_recvr->get_next(output_batch, eos));
    }

    _num_rows_returned += output_batch->num_rows();
    if (reached_limit()) {
        output_batch->set_num_rows(output_batch->num_rows() - (_num_rows_returned - _limit));
        *eos = true;
    }

    // On eos, transfer all remaining resources from the input batches maintained
    // by the merger to the output batch.
    if (*eos) {
        _stream_recvr->transfer_all_resources(output_batch);
    }

    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
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
            // the first Chunk will have a size less than config::vector_chunk_size.
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
    auto exchange_operator = std::make_shared<ExchangeSourceOperatorFactory>(context->next_operator_id(), id(),
                                                                             _num_senders, _input_row_desc);
    // A merging ExchangeSourceOperator should not be parallelized.
    exchange_operator->set_degree_of_parallelism(_is_merging ? 1 : context->degree_of_parallelism());
    operators.emplace_back(std::move(exchange_operator));
    if (limit() != -1) {
        operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return operators;
}

} // namespace starrocks
