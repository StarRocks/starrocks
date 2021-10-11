// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/assert_num_rows_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

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

    return Status::OK();
}

Status AssertNumRowsNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("get_next for row_batch is not supported");
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
                std::map<int, const char*>::const_iterator it = _TAssertion_VALUES_TO_NAMES.find(assertion);

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

} // namespace starrocks::vectorized
