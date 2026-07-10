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

#include "runtime/chunk_accumulator.h"

#include <algorithm>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/json_column.h"
#include "gutil/casts.h"
#include "runtime/current_thread.h"

namespace starrocks {

ChunkAccumulator::ChunkAccumulator(size_t desired_size) : _desired_size(desired_size) {}

void ChunkAccumulator::set_desired_size(size_t desired_size) {
    _desired_size = desired_size;
}

void ChunkAccumulator::reset() {
    _output.clear();
    _tmp_chunk.reset();
    _accumulate_count = 0;
}

bool check_json_schema_compatibility(const Chunk* one, const Chunk* two) {
    if (one->num_columns() != two->num_columns()) {
        return false;
    }

    for (size_t i = 0; i < one->num_columns(); i++) {
        auto& c1 = one->get_column_by_index(i);
        auto& c2 = two->get_column_by_index(i);
        const auto* a1 = ColumnHelper::get_data_column(c1.get());
        const auto* a2 = ColumnHelper::get_data_column(c2.get());

        if (a1->is_json() && a2->is_json()) {
            auto json1 = down_cast<const JsonColumn*>(a1);
            if (!json1->is_equallity_schema(a2)) {
                return false;
            }
        } else if (a1->is_json() || a2->is_json()) {
            // never hit
            DCHECK_EQ(a1->is_json(), a2->is_json());
            return false;
        }
    }

    return true;
}

Status ChunkAccumulator::push(ChunkPtr&& chunk) {
    size_t input_rows = chunk->num_rows();
    // TODO: optimize for zero-copy scenario
    // Cut the input chunk into pieces if larger than desired
    for (size_t start = 0; start < input_rows;) {
        size_t remain_rows = input_rows - start;
        size_t need_rows = 0;
        if (_tmp_chunk) {
            need_rows = std::min(_desired_size - _tmp_chunk->num_rows(), remain_rows);
            // Check JSON schema compatibility before appending
            if (!check_json_schema_compatibility(_tmp_chunk.get(), chunk.get())) {
                // Schema mismatch, output current chunk and create a new one
                _output.emplace_back(std::move(_tmp_chunk));
                _tmp_chunk = chunk->clone_empty(_desired_size);
                TRY_CATCH_BAD_ALLOC(_tmp_chunk->append(*chunk, start, need_rows));
            } else {
                TRY_CATCH_BAD_ALLOC(_tmp_chunk->append(*chunk, start, need_rows));
            }
            RETURN_IF_ERROR(_tmp_chunk->capacity_limit_reached());
        } else {
            need_rows = std::min(_desired_size, remain_rows);
            _tmp_chunk = chunk->clone_empty(_desired_size);
            TRY_CATCH_BAD_ALLOC(_tmp_chunk->append(*chunk, start, need_rows));
        }

        if (_tmp_chunk->num_rows() >= _desired_size) {
            _output.emplace_back(std::move(_tmp_chunk));
        }
        start += need_rows;
    }
    _accumulate_count++;
    return Status::OK();
}

bool ChunkAccumulator::empty() const {
    return _output.empty();
}

bool ChunkAccumulator::reach_limit() const {
    return _accumulate_count >= kAccumulateLimit;
}

ChunkPtr ChunkAccumulator::pull() {
    if (!_output.empty()) {
        auto res = std::move(_output.front());
        _output.pop_front();
        _accumulate_count = 0;
        return res;
    }
    return nullptr;
}

void ChunkAccumulator::finalize() {
    if (_tmp_chunk) {
        _output.emplace_back(std::move(_tmp_chunk));
    }
    _accumulate_count = 0;
}

} // namespace starrocks
