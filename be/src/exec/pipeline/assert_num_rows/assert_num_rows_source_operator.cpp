// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/assert_num_rows/assert_num_rows_source_operator.h"

#include "column/chunk.h"
#include "gutil/strings/substitute.h"

using namespace starrocks::vectorized;

namespace starrocks::pipeline {
Status AssertNumRowsSourceOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    return Status::OK();
}

Status AssertNumRowsSourceOperator::close(RuntimeState* state) {
    return Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> AssertNumRowsSourceOperator::pull_chunk(RuntimeState* state) {
    if (!_has_assert) {
        _has_assert = true;
        bool assert_res = false;
        switch (_assertion) {
        case TAssertion::EQ:
            assert_res = _assert_num_rows_context->get_rows() == _desired_num_rows;
            break;
        case TAssertion::NE:
            assert_res = _assert_num_rows_context->get_rows() != _desired_num_rows;
            break;
        case TAssertion::LT:
            assert_res = _assert_num_rows_context->get_rows() < _desired_num_rows;
            break;
        case TAssertion::LE:
            assert_res = _assert_num_rows_context->get_rows() <= _desired_num_rows;
            break;
        case TAssertion::GT:
            assert_res = _assert_num_rows_context->get_rows() > _desired_num_rows;
            break;
        case TAssertion::GE:
            assert_res = _assert_num_rows_context->get_rows() >= _desired_num_rows;
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

    return _assert_num_rows_context->get_chunk();
}

bool AssertNumRowsSourceOperator::has_output() const {
    auto flag = _assert_num_rows_context->is_data_complete();
    return flag;
}

void AssertNumRowsSourceOperator::finish(RuntimeState* state) {
    _is_finished = true;
}

bool AssertNumRowsSourceOperator::is_finished() const {
    if (!_assert_num_rows_context->is_data_complete()) {
        return false;
    }

    return !_assert_num_rows_context->has_chunk();
}

Status AssertNumRowsSourceOperatorFactory::prepare(RuntimeState* state, MemTracker* mem_tracker) {
    return Status::OK();
}

void AssertNumRowsSourceOperatorFactory::close(RuntimeState* state) {}

} // namespace starrocks::pipeline
