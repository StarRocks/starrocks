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

#include "exec/pipeline/enforce_unique_row_locator_operator.h"

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"

namespace starrocks::pipeline {

Status EnforceUniqueRowLocatorOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _seen_rows_counter = ADD_COUNTER(_unique_metrics, "SeenSetRows", TUnit::UNIT);
    _seen_memory_counter = ADD_COUNTER(_unique_metrics, "SeenSetMemoryUsage", TUnit::BYTES);
    return Status::OK();
}

int64_t EnforceUniqueRowLocatorOperator::_seen_memory_usage() const {
    // flat_hash_set/map store entries inline in the bucket array; capacity()
    // reflects allocated buckets. Path strings are heap-allocated separately.
    int64_t bytes = static_cast<int64_t>(_seen.capacity()) * sizeof(std::pair<int32_t, int64_t>);
    bytes += static_cast<int64_t>(_path_ids.capacity()) * sizeof(std::pair<const std::string, int32_t>);
    for (const auto& [path, id] : _path_ids) {
        bytes += static_cast<int64_t>(path.capacity());
    }
    return bytes;
}

void EnforceUniqueRowLocatorOperator::close(RuntimeState* state) {
    if (_seen_rows_counter != nullptr) {
        COUNTER_SET(_seen_rows_counter, static_cast<int64_t>(_seen.size()));
        COUNTER_SET(_seen_memory_counter, _seen_memory_usage());
    }
    _output_chunk.reset();
    // Force deallocation (clear() retains backing storage)
    decltype(_seen){}.swap(_seen);
    decltype(_path_ids){}.swap(_path_ids);
    Operator::close(state);
}

bool EnforceUniqueRowLocatorOperator::has_output() const {
    return _output_chunk != nullptr;
}

bool EnforceUniqueRowLocatorOperator::need_input() const {
    return !_input_finished && _output_chunk == nullptr;
}

bool EnforceUniqueRowLocatorOperator::is_finished() const {
    return _input_finished && _output_chunk == nullptr;
}

Status EnforceUniqueRowLocatorOperator::set_finishing(RuntimeState* state) {
    _input_finished = true;
    return Status::OK();
}

StatusOr<ChunkPtr> EnforceUniqueRowLocatorOperator::pull_chunk(RuntimeState* state) {
    return std::move(_output_chunk);
}

Status EnforceUniqueRowLocatorOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    size_t num_rows = chunk->num_rows();
    if (num_rows == 0) {
        return Status::OK();
    }

    // Key-count validation runs once in EnforceUniqueRowLocatorOperatorFactory::prepare.
    DCHECK_EQ(_unique_key_slot_ids.size(), 2);

    // Resolve the key columns by slot id. Chunk::get_column_by_slot_id is only
    // DCHECK-guarded, so verify existence explicitly: in RELEASE builds a missing
    // slot would silently default-construct a map entry pointing at column 0.
    for (SlotId slot_id : _unique_key_slot_ids) {
        if (!chunk->is_slot_exist(slot_id)) {
            return Status::InternalError("EnforceUniqueRowLocatorOperator: key slot " + std::to_string(slot_id) +
                                         " does not exist in the input chunk");
        }
    }
    auto file_path_col = chunk->get_column_by_slot_id(_unique_key_slot_ids[0]);
    auto row_pos_col = chunk->get_column_by_slot_id(_unique_key_slot_ids[1]);

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
    // Defense-in-depth type check: the keys are resolved by slot id, so a
    // mismatch means the FE bound the wrong slots. down_cast is unchecked in
    // RELEASE builds — without these guards a wrong slot pointing at a
    // non-binary/non-int64 column would dereference garbage and SIGSEGV
    // instead of producing a diagnosable error. Verify the unwrapped types
    // first and fail with a clear message if they mismatch.
    const Column* file_path_inner = ColumnHelper::get_data_column(file_path_col.get());
    if (!file_path_inner->is_binary()) {
        return Status::InternalError("EnforceUniqueRowLocatorOperator: expected _file key column (slot " +
                                     std::to_string(_unique_key_slot_ids[0]) + ") to be a binary column, got " +
                                     file_path_inner->get_name());
    }
    const Column* row_pos_data_col = ColumnHelper::get_data_column(row_pos_col.get());
    const auto* row_pos_typed = dynamic_cast<const FixedLengthColumn<int64_t>*>(row_pos_data_col);
    if (row_pos_typed == nullptr) {
        return Status::InternalError("EnforceUniqueRowLocatorOperator: expected _pos key column (slot " +
                                     std::to_string(_unique_key_slot_ids[1]) +
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

        // Materialize the path string once per row and reuse it for both the intern
        // lookup and the (rare) duplicate-error message.
        std::string path_str = path_slice.to_string();

        // Intern file path: assign a compact ID to each unique path string.
        // Most chunks share the same few file paths, so this avoids millions
        // of identical string copies in the seen-set.
        auto [it, inserted_path] = _path_ids.try_emplace(path_str, _next_path_id);
        if (inserted_path) {
            _next_path_id++;
        }
        int32_t path_id = it->second;

        auto key = std::make_pair(path_id, pos);
        if (!_seen.insert(key).second) {
            return Status::RuntimeError(
                    "Each target row should be matched by at most one source row, "
                    "but a target row in file '" +
                    path_str + "' at position " + std::to_string(pos) + " was matched by more than one source row");
        }
    }

    _output_chunk = chunk;
    return Status::OK();
}

Status EnforceUniqueRowLocatorOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    if (_unique_key_slot_ids.size() != 2) {
        return Status::InternalError("EnforceUniqueRowLocatorOperator expects exactly 2 key columns, got " +
                                     std::to_string(_unique_key_slot_ids.size()));
    }
    return Status::OK();
}

void EnforceUniqueRowLocatorOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
