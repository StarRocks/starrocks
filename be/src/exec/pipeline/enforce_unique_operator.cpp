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

#include "exec/pipeline/enforce_unique_operator.h"

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"

namespace starrocks::pipeline {

Status EnforceUniqueOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void EnforceUniqueOperator::close(RuntimeState* state) {
    _output_chunk.reset();
    // Force deallocation (clear() retains backing storage)
    decltype(_seen){}.swap(_seen);
    decltype(_path_ids){}.swap(_path_ids);
    Operator::close(state);
}

bool EnforceUniqueOperator::has_output() const {
    return _output_chunk != nullptr;
}

bool EnforceUniqueOperator::need_input() const {
    return !_input_finished && _output_chunk == nullptr;
}

bool EnforceUniqueOperator::is_finished() const {
    return _input_finished && _output_chunk == nullptr;
}

Status EnforceUniqueOperator::set_finishing(RuntimeState* state) {
    _input_finished = true;
    return Status::OK();
}

StatusOr<ChunkPtr> EnforceUniqueOperator::pull_chunk(RuntimeState* state) {
    return std::move(_output_chunk);
}

Status EnforceUniqueOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    size_t num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    if (_unique_key_col_indices.size() != 2) {
        return Status::RuntimeError("EnforceUniqueOperator expects exactly 2 key columns, got " +
                                    std::to_string(_unique_key_col_indices.size()));
    }

    auto file_path_col = chunk->get_column_by_index(_unique_key_col_indices[0]);
    auto row_pos_col = chunk->get_column_by_index(_unique_key_col_indices[1]);

    // Determine if columns are nullable
    const NullableColumn* file_path_nullable = nullptr;
    const NullableColumn* row_pos_nullable = nullptr;
    if (file_path_col->is_nullable()) {
        file_path_nullable = down_cast<const NullableColumn*>(file_path_col.get());
    }
    if (row_pos_col->is_nullable()) {
        row_pos_nullable = down_cast<const NullableColumn*>(row_pos_col.get());
    }

    // Get the underlying data columns (unwrap nullable/const).
    //
    // Defense-in-depth type check: the FE resolves unique_key_col_indices by
    // slot ID now (MergeIntoPlanner::insertEnforceUniqueNode), so this shouldn't
    // fire in practice. But down_cast is unchecked in RELEASE builds, so without
    // these guards a bad index — i.e. the historical Bug #1 where indices [0,1]
    // pointed at non-binary/non-int64 data columns — would dereference garbage
    // and SIGSEGV instead of producing a diagnosable error. Verify the unwrapped
    // types first and fail with a clear message if they mismatch.
    const Column* file_path_inner = ColumnHelper::get_data_column(file_path_col.get());
    if (!file_path_inner->is_binary()) {
        return Status::InternalError("EnforceUniqueOperator: expected _file key column (index " +
                                     std::to_string(_unique_key_col_indices[0]) + ") to be a binary column, got " +
                                     file_path_inner->get_name());
    }
    const Column* row_pos_data_col = ColumnHelper::get_data_column(row_pos_col.get());
    const auto* row_pos_typed = dynamic_cast<const FixedLengthColumn<int64_t>*>(row_pos_data_col);
    if (row_pos_typed == nullptr) {
        return Status::InternalError("EnforceUniqueOperator: expected _pos key column (index " +
                                     std::to_string(_unique_key_col_indices[1]) +
                                     ") to be FixedLengthColumn<int64_t>, got " + row_pos_data_col->get_name());
    }
    const auto* file_path_data = down_cast<const BinaryColumn*>(file_path_inner);
    const auto* row_pos_data = row_pos_typed->get_data().data();

    for (size_t i = 0; i < num_rows; ++i) {
        // Skip rows where any key column is null
        if (file_path_nullable != nullptr && file_path_nullable->is_null(i)) {
            continue;
        }
        if (row_pos_nullable != nullptr && row_pos_nullable->is_null(i)) {
            continue;
        }

        Slice path_slice = file_path_data->get_slice(i);
        int64_t pos = row_pos_data[i];

        // Intern file path: assign a compact ID to each unique path string.
        // Most chunks share the same few file paths, so this avoids millions
        // of identical string copies in the seen-set.
        auto [it, inserted_path] = _path_ids.try_emplace(path_slice.to_string(), _next_path_id);
        if (inserted_path) {
            _next_path_id++;
        }
        int32_t path_id = it->second;

        auto key = std::make_pair(path_id, pos);
        if (!_seen.insert(key).second) {
            return Status::RuntimeError(
                    "Each target row should be matched by at most one source row, "
                    "but a target row in file '" +
                    path_slice.to_string() + "' at position " + std::to_string(pos) +
                    " was matched by more than one source row");
        }
    }

    _output_chunk = chunk;
    return Status::OK();
}

Status EnforceUniqueOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    return Status::OK();
}

void EnforceUniqueOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
