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

#include "exec/pipeline/set/raw_values_source_operator.h"

#include "column/column_helper.h"
#include "glog/logging.h"

namespace starrocks::pipeline {

StatusOr<ChunkPtr> RawValuesSourceOperator::pull_chunk(RuntimeState* state) {
    DCHECK(_next_processed_row_index < _rows_total);
    auto chunk = std::make_shared<Chunk>();

    size_t rows_count = std::min(static_cast<size_t>(state->chunk_size()), _rows_total - _next_processed_row_index);

    DCHECK(_dst_slots.size() == 1);
    const auto* dst_slot = _dst_slots[0];

    MutableColumnPtr dst_column = ColumnHelper::create_column(dst_slot->type(), dst_slot->is_nullable());
    dst_column->reserve(rows_count);

    size_t global_start_index = _start_index + _next_processed_row_index;

    if (_long_values != nullptr) {
        DCHECK(_string_values == nullptr);
        for (size_t i = 0; i < rows_count; i++) {
            int64_t value = (*_long_values)[global_start_index + i];
            dst_column->append_datum(Datum(value));
        }
    } else if (_string_values != nullptr) {
        DCHECK(_long_values == nullptr);
        for (size_t i = 0; i < rows_count; i++) {
            const std::string& value = (*_string_values)[global_start_index + i];
            dst_column->append_datum(Datum(Slice(value)));
        }
    } else {
        LOG(ERROR) << "ERROR: Both long_values and string_values are null!";
        return Status::InternalError("Both long_values and string_values are null");
    }

    chunk->append_column(std::move(dst_column), dst_slot->id());
    _next_processed_row_index += rows_count;

    DCHECK_CHUNK(chunk);
    return std::move(chunk);
}

} // namespace starrocks::pipeline
