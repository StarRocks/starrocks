// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/select_node.cpp

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

#include "exec/select_node.h"

#include "exprs/expr.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// Our new vectorized query executor is more powerful and stable than old query executor,
// The executor query executor related codes could be deleted safely.
// TODO: Remove old query executor related codes before 2021-09-30

SelectNode::SelectNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _child_row_batch(nullptr), _child_row_idx(0), _child_eos(false) {}

Status SelectNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    if (use_vectorized()) {
        _conjunct_evaluate_timer = ADD_TIMER(_runtime_profile, "ConjunctEvaluateTimer");
    }
    _child_row_batch.reset(new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker()));
    return Status::OK();
}

Status SelectNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(child(0)->open(state));
    return Status::OK();
}

Status SelectNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (reached_limit() || (_child_row_idx == _child_row_batch->num_rows() && _child_eos)) {
        // we're already done or we exhausted the last child batch and there won't be any
        // new ones
        _child_row_batch->transfer_resource_ownership(row_batch);
        *eos = true;
        return Status::OK();
    }
    *eos = false;

    // start (or continue) consuming row batches from child
    while (true) {
        RETURN_IF_CANCELLED(state);
        if (_child_row_idx == _child_row_batch->num_rows()) {
            // fetch next batch
            _child_row_idx = 0;
            _child_row_batch->transfer_resource_ownership(row_batch);
            _child_row_batch->reset();
            if (row_batch->at_capacity()) {
                return Status::OK();
            }
            RETURN_IF_ERROR(child(0)->get_next(state, _child_row_batch.get(), &_child_eos));
        }

        if (copy_rows(row_batch)) {
            *eos = reached_limit() || (_child_row_idx == _child_row_batch->num_rows() && _child_eos);
            if (*eos) {
                _child_row_batch->transfer_resource_ownership(row_batch);
            }
            return Status::OK();
        }

        if (_child_eos) {
            // finished w/ last child row batch, and child eos is true
            _child_row_batch->transfer_resource_ownership(row_batch);
            *eos = true;
            return Status::OK();
        }
    }

    return Status::OK();
}

Status SelectNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    *eos = false;
    RETURN_IF_ERROR(_children[0]->get_next(state, chunk, eos));
    if (*eos) {
        return Status::OK();
    }
    {
        SCOPED_TIMER(_conjunct_evaluate_timer);
        ExecNode::eval_conjuncts(_conjunct_ctxs, (*chunk).get());
    }
    _num_rows_returned += (*chunk)->num_rows();

    if (reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        (*chunk)->set_num_rows((*chunk)->num_rows() - num_rows_over);
        COUNTER_SET(_rows_returned_counter, _limit);
        return Status::OK();
    }

    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    return Status::OK();
}

bool SelectNode::copy_rows(RowBatch* output_batch) {
    ExprContext** ctxs = &_conjunct_ctxs[0];
    int num_ctxs = _conjunct_ctxs.size();

    for (; _child_row_idx < _child_row_batch->num_rows(); ++_child_row_idx) {
        // Add a new row to output_batch
        int dst_row_idx = output_batch->add_row();

        if (dst_row_idx == RowBatch::INVALID_ROW_INDEX) {
            return true;
        }

        TupleRow* dst_row = output_batch->get_row(dst_row_idx);
        TupleRow* src_row = _child_row_batch->get_row(_child_row_idx);

        if (ExecNode::eval_conjuncts(ctxs, num_ctxs, src_row)) {
            output_batch->copy_row(src_row, dst_row);
            output_batch->commit_last_row();
            ++_num_rows_returned;
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);

            if (reached_limit()) {
                return true;
            }
        }
    }

    if (VLOG_ROW_IS_ON) {
        for (int i = 0; i < output_batch->num_rows(); ++i) {
            TupleRow* row = output_batch->get_row(i);
            VLOG_ROW << "SelectNode input row: " << row->to_string(row_desc());
        }
    }

    return output_batch->is_full() || output_batch->at_resource_limit();
}

Status SelectNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    _child_row_batch.reset();
    return ExecNode::close(state);
}

} // namespace starrocks
