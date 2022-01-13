// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/assert_num_rows_operator.h"

#include "column/chunk.h"
#include "gutil/strings/substitute.h"

using namespace starrocks::vectorized;

namespace starrocks::pipeline {
Status AssertNumRowsOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);

    // assert num rows node only use for un-correlate scalar subquery, return empty chunk is error, least fill one rows
    vectorized::ChunkPtr chunk = std::make_shared<vectorized::Chunk>();

    for (const auto& desc : _factory->row_desc()->tuple_descriptors()) {
        for (const auto& slot : desc->slots()) {
            chunk->append_column(ColumnHelper::create_const_null_column(1), slot->id());
        }
    }

    _cur_chunk = std::move(chunk);
    return Status::OK();
}

Status AssertNumRowsOperator::close(RuntimeState* state) {
    return Operator::close(state);
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

void AssertNumRowsOperator::set_finishing(RuntimeState* state) {
    _input_finished = true;
}

bool AssertNumRowsOperator::is_finished() const {
    return !_cur_chunk;
}

Status AssertNumRowsOperatorFactory::prepare(RuntimeState* state) {
    return Status::OK();
}

void AssertNumRowsOperatorFactory::close(RuntimeState* state) {}

} // namespace starrocks::pipeline
