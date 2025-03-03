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

#include "exec/pipeline/assert_num_rows_operator.h"

#include "column/chunk.h"
#include "gutil/strings/substitute.h"

using namespace starrocks;

namespace starrocks::pipeline {
Status AssertNumRowsOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));

    // AssertNumRows should return exactly one row, report error if more than one row, return null if empty input
    ChunkPtr chunk = std::make_shared<Chunk>();

    for (const auto& desc : _factory->row_desc()->tuple_descriptors()) {
        for (const auto& slot : desc->slots()) {
            MutableColumnPtr column = ColumnHelper::create_column(slot->type(), true);
            column->append_nulls(1);
            chunk->append_column(std::move(column), slot->id());
        }
    }

    _cur_chunk = std::move(chunk);
    return Status::OK();
}

void AssertNumRowsOperator::close(RuntimeState* state) {
    _cur_chunk.reset();
    Operator::close(state);
}

StatusOr<ChunkPtr> AssertNumRowsOperator::pull_chunk(RuntimeState* state) {
    return std::move(_cur_chunk);
}

bool AssertNumRowsOperator::has_output() const {
    return _input_finished && _cur_chunk;
}

bool AssertNumRowsOperator::need_input() const {
    return !_input_finished;
}

Status AssertNumRowsOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
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
