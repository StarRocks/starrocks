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

#include "exec/stream/join/join_state_table.h"

#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"

namespace starrocks::stream {

Status JoinStateTable::prepare(RuntimeState* state) {
    _runtime_state = state;
    return Status::OK();
}

Status JoinStateTable::open(RuntimeState* state) {
    return Status::OK();
}

Status JoinStateTable::seek(const Columns& keys, StateTableResult& values) const {
    size_t num_rows = keys.empty() ? 0 : keys[0]->size();
    values.found.resize(num_rows);

    // Create result chunk with match_count column
    auto match_count_col = Int64Column::create();

    for (size_t i = 0; i < num_rows; ++i) {
        std::string key = serialize_key(keys, i);
        auto it = _state_map.find(key);
        if (it != _state_map.end()) {
            values.found[i] = true;
            match_count_col->append(it->second.match_count);
        } else {
            values.found[i] = false;
            match_count_col->append(0);
        }
    }

    values.result_chunk = std::make_shared<Chunk>();
    values.result_chunk->append_column(match_count_col, 0);

    return Status::OK();
}

Status JoinStateTable::seek(const Columns& keys, const Filter& selection, StateTableResult& values) const {
    size_t num_rows = keys.empty() ? 0 : keys[0]->size();
    values.found.resize(num_rows);

    auto match_count_col = Int64Column::create();

    for (size_t i = 0; i < num_rows; ++i) {
        if (!selection[i]) {
            values.found[i] = false;
            match_count_col->append(0);
            continue;
        }

        std::string key = serialize_key(keys, i);
        auto it = _state_map.find(key);
        if (it != _state_map.end()) {
            values.found[i] = true;
            match_count_col->append(it->second.match_count);
        } else {
            values.found[i] = false;
            match_count_col->append(0);
        }
    }

    values.result_chunk = std::make_shared<Chunk>();
    values.result_chunk->append_column(match_count_col, 0);

    return Status::OK();
}

Status JoinStateTable::seek(const Columns& keys, const std::vector<std::string>& projection_columns,
                            StateTableResult& values) const {
    // For JoinStateTable, we only have match_count column, so projection is simple
    return seek(keys, values);
}

ChunkIteratorPtrOr JoinStateTable::prefix_scan(const Columns& keys, size_t row_idx) const {
    // Not typically used for JoinStateTable - return empty iterator
    return Status::NotSupported("prefix_scan not supported for JoinStateTable");
}

ChunkIteratorPtrOr JoinStateTable::prefix_scan(const std::vector<std::string>& projection_columns, const Columns& keys,
                                               size_t row_idx) const {
    return Status::NotSupported("prefix_scan not supported for JoinStateTable");
}

Status JoinStateTable::write(RuntimeState* state, const StreamChunkPtr& chunk) {
    if (!chunk || chunk->is_empty()) {
        return Status::OK();
    }

    const StreamRowOp* ops = StreamChunkConverter::ops(chunk);
    if (ops == nullptr) {
        return Status::InternalError("StreamChunk must have ops column for JoinStateTable");
    }

    // Assume the chunk contains join key columns
    // Extract keys and update match counts based on ops
    Columns key_columns;
    for (size_t i = 0; i < chunk->num_columns(); ++i) {
        key_columns.push_back(chunk->get_column_by_index(i));
    }

    size_t num_rows = chunk->num_rows();
    for (size_t i = 0; i < num_rows; ++i) {
        std::string key = serialize_key(key_columns, i);
        StreamRowOp op = ops[i];

        int64_t delta = 0;
        if (op == StreamRowOp::OP_INSERT || op == StreamRowOp::OP_UPDATE_AFTER) {
            delta = 1;
        } else if (op == StreamRowOp::OP_DELETE || op == StreamRowOp::OP_UPDATE_BEFORE) {
            delta = -1;
        }

        if (delta != 0) {
            int64_t old_count, new_count;
            bool has_transition = update_match_count(key, delta, &old_count, &new_count);

            // Track NULL row transitions
            if (has_transition) {
                if (old_count == 0 && new_count > 0) {
                    _newly_matched_keys.push_back(key);
                } else if (old_count > 0 && new_count == 0) {
                    _newly_unmatched_keys.push_back(key);
                }
            }
        }
    }

    return Status::OK();
}

Status JoinStateTable::commit(RuntimeState* state) {
    // In a full implementation, this would persist state to durable storage
    // For now, state is kept in memory
    return Status::OK();
}

Status JoinStateTable::reset_epoch(RuntimeState* state) {
    clear_transitions();
    return Status::OK();
}

int64_t JoinStateTable::get_match_count(const std::string& key) const {
    auto it = _state_map.find(key);
    return it != _state_map.end() ? it->second.match_count : 0;
}

bool JoinStateTable::update_match_count(const std::string& key, int64_t delta, int64_t* old_count, int64_t* new_count) {
    auto it = _state_map.find(key);

    if (it != _state_map.end()) {
        *old_count = it->second.match_count;
        it->second.match_count += delta;
        *new_count = it->second.match_count;

        // Remove entry if count goes to 0 or below
        if (it->second.match_count <= 0) {
            _state_map.erase(it);
            *new_count = 0;
        }
    } else {
        *old_count = 0;
        if (delta > 0) {
            JoinStateEntry entry;
            entry.key = key;
            entry.match_count = delta;
            _state_map[key] = entry;
            *new_count = delta;
        } else {
            *new_count = 0;
        }
    }

    // Return true if there's a NULL row change
    // (transition between matched and unmatched state)
    return (*old_count == 0 && *new_count > 0) || (*old_count > 0 && *new_count == 0);
}

std::string JoinStateTable::serialize_key(const Columns& key_columns, size_t row_idx) {
    std::string result;
    for (const auto& col : key_columns) {
        // Get the datum at this row
        auto datum = col->get(row_idx);
        // Simple serialization - append string representation with separator
        result += datum.to_string();
        result += '\0'; // Use null byte as separator
    }
    return result;
}

bool JoinStateTable::has_matches(const std::string& key) const {
    auto it = _state_map.find(key);
    return it != _state_map.end() && it->second.match_count > 0;
}

std::vector<std::string> JoinStateTable::get_newly_unmatched_keys() const {
    return _newly_unmatched_keys;
}

std::vector<std::string> JoinStateTable::get_newly_matched_keys() const {
    return _newly_matched_keys;
}

void JoinStateTable::clear_transitions() {
    _newly_matched_keys.clear();
    _newly_unmatched_keys.clear();
}

} // namespace starrocks::stream
