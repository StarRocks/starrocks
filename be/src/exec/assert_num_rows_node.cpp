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

#include "exec/assert_num_rows_node.h"

#include "exec/pipeline/assert_num_rows_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

AssertNumRowsNode::AssertNumRowsNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _desired_num_rows(tnode.assert_num_rows_node.desired_num_rows),
          _subquery_string(tnode.assert_num_rows_node.subquery_string),

          _has_assert(false) {
    if (tnode.assert_num_rows_node.__isset.assertion) {
        _assertion = tnode.assert_num_rows_node.assertion;
    } else {
        _assertion = TAssertion::LE; // just comptiable for the previous code
    }
}

Status AssertNumRowsNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return Status::OK();
}

Status AssertNumRowsNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    return Status::OK();
}

Status AssertNumRowsNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));

    assert(_children.size() == 1);
    ChunkPtr chunk = nullptr;
    bool eos = false;
    RETURN_IF_ERROR(child(0)->open(state));
    while (true) {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(child(0)->get_next(state, &chunk, &eos));
        if (eos || chunk == nullptr) {
            break;
        } else if (chunk->num_rows() == 0) {
            continue;
        } else {
            _num_rows_returned += chunk->num_rows();
            _input_chunks.emplace_back(std::move(chunk));
        }
    }

    // assert num rows node only use for un-correlate scalar subquery, return empty chunk is error, least fill one rows
    if (_assertion == TAssertion::LE && _num_rows_returned == 0) {
        _input_chunks.clear();

        chunk = std::make_shared<Chunk>();
        for (const auto& desc : row_desc().tuple_descriptors()) {
            for (const auto& slot : desc->slots()) {
                auto column = ColumnHelper::create_column(slot->type(), true);
                column->append_nulls(_desired_num_rows);
                chunk->append_column(column, slot->id());
            }
        }

        _input_chunks.emplace_back(std::move(chunk));
    }

    int64_t usage = 0;
    for (auto& item : _input_chunks) {
        usage += item->memory_usage();
    }
    _mem_tracker->set(usage);

    return Status::OK();
}

Status AssertNumRowsNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (!_has_assert) {
        _has_assert = true;
        bool assert_res = false;
        switch (_assertion) {
        case TAssertion::EQ:
            assert_res = _num_rows_returned == _desired_num_rows;
            break;
        case TAssertion::NE:
            assert_res = _num_rows_returned != _desired_num_rows;
            break;
        case TAssertion::LT:
            assert_res = _num_rows_returned < _desired_num_rows;
            break;
        case TAssertion::LE:
            assert_res = _num_rows_returned <= _desired_num_rows;
            break;
        case TAssertion::GT:
            assert_res = _num_rows_returned > _desired_num_rows;
            break;
        case TAssertion::GE:
            assert_res = _num_rows_returned >= _desired_num_rows;
            break;
        default:
            break;
        }

        if (!assert_res) {
            auto to_string_lamba = [](TAssertion::type assertion) {
                auto it = _TAssertion_VALUES_TO_NAMES.find(assertion);

                if (it == _TAggregationOp_VALUES_TO_NAMES.end()) {
                    return "NULL";
                } else {
                    return it->second;
                }
            };
            LOG(INFO) << "Expected " << to_string_lamba(_assertion) << " " << _desired_num_rows
                      << " to be returned by expression " << _subquery_string;
            return Status::Cancelled(strings::Substitute("Expected $0 $1 to be returned by expression $2",
                                                         to_string_lamba(_assertion), _desired_num_rows,
                                                         _subquery_string));
        }
    }
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);

    if (_input_chunks.size() > 0) {
        *chunk = _input_chunks.front();
        _input_chunks.pop_front();
        DCHECK_CHUNK(*chunk);
    } else {
        *eos = true;
    }

    return Status::OK();
}

Status AssertNumRowsNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

pipeline::OpFactories AssertNumRowsNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;

    OpFactories operator_before_assert_num_rows_source = _children[0]->decompose_to_pipeline(context);
    operator_before_assert_num_rows_source = context->maybe_interpolate_local_passthrough_exchange(
            runtime_state(), operator_before_assert_num_rows_source);

    auto source_factory = std::make_shared<AssertNumRowsOperatorFactory>(
            context->next_operator_id(), id(), _desired_num_rows, _subquery_string, _assertion);
    operator_before_assert_num_rows_source.emplace_back(std::move(source_factory));

    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(operator_before_assert_num_rows_source.back().get(), context,
                                           rc_rf_probe_collector);
    if (limit() != -1) {
        operator_before_assert_num_rows_source.emplace_back(
                std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return operator_before_assert_num_rows_source;
}

} // namespace starrocks
