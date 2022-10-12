// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/assert_num_rows_operator.h"

#include "column/chunk.h"
#include "gutil/strings/substitute.h"

using namespace starrocks::vectorized;

namespace starrocks::pipeline {
Status AssertNumRowsOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);

    // AssertNumRows should return exactly one row, report error if more than one row, return null if empty input
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();

    for (const auto& desc : _factory->row_desc()->tuple_descriptors()) {
        for (const auto& slot : desc->slots()) {
            vectorized::ColumnPtr column = ColumnHelper::create_column(slot->type(), true);
            column->append_nulls(1);
            chunk->append_column(column, slot->id());
        }
    }

    _cur_chunk = std::move(chunk);
    return Status::OK();
}

void AssertNumRowsOperator::close(RuntimeState* state) {
    _cur_chunk.reset();
    Operator::close(state);
}

StatusOr<vectorized::ChunkPtr> AssertNumRowsOperator::pull_chunk(RuntimeState* state) {
    return std::move(_cur_chunk);
}

bool AssertNumRowsOperator::has_output() const {
    return _input_finished && _cur_chunk;
}

bool AssertNumRowsOperator::need_input() const {
    return !_input_finished;
}

Status AssertNumRowsOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    _actual_num_rows += chunk->num_rows();
    if (_actual_num_rows > 1) {
        auto iter = _TAssertion_VALUES_TO_NAMES.find(_assertion);
        std::string message;
        if (iter == _TAggregationOp_VALUES_TO_NAMES.end()) {
            message = "NULL";
        } else {
            message = iter->second;
        }
        LOG(INFO) << "Expected " << message << " " << _desired_num_rows << " to be returned by expression "
                  << _subquery_string;
        return Status::Cancelled(strings::Substitute("Expected $0 $1 to be returned by expression $2", message,
                                                     _desired_num_rows, _subquery_string));
    }

    _cur_chunk = chunk;
    return Status::OK();
}

Status AssertNumRowsOperator::set_finishing(RuntimeState* state) {
    _input_finished = true;
    return Status::OK();
}

bool AssertNumRowsOperator::is_finished() const {
    return !_cur_chunk;
}

Status AssertNumRowsOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    return Status::OK();
}

void AssertNumRowsOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
