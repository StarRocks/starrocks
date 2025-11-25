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

#pragma once

#include <memory>
#include <unordered_map>

#include "column/stream_chunk.h"
#include "common/statusor.h"
#include "exec/stream/state/state_table.h"

namespace starrocks::stream {

/**
 * JoinStateEntry stores the match state for a single join key.
 *
 * For LEFT OUTER JOIN:
 * - match_count: number of matching rows on the right side for this left key
 * - When match_count goes 0 -> 1: need to retract NULL-padded row
 * - When match_count goes 1 -> 0: need to emit NULL-padded row
 *
 * For FULL OUTER JOIN:
 * - left_match_count: matches from left side perspective
 * - right_match_count: matches from right side perspective
 */
struct JoinStateEntry {
    // Serialized join key
    std::string key;

    // Number of matching rows (for tracking NULL row changes)
    int64_t match_count = 0;

    // Timestamp of last update (for cleanup)
    int64_t last_update_ts = 0;

    bool has_matches() const { return match_count > 0; }
};

/**
 * JoinStateTable is a StateTable implementation for tracking join match counts.
 *
 * Schema:
 * - Primary key: serialized join key columns
 * - Value columns: match_count (INT64), last_update_ts (INT64)
 *
 * This table is used by StreamJoinOperator to:
 * 1. Track match counts for OUTER JOIN NULL row change detection
 * 2. Persist state across epochs for incremental MV maintenance
 */
class JoinStateTable : public StateTable {
public:
    JoinStateTable() = default;
    ~JoinStateTable() override = default;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    /**
     * Look up match state for given keys.
     * Keys should be the join key columns.
     */
    Status seek(const Columns& keys, StateTableResult& values) const override;

    Status seek(const Columns& keys, const Filter& selection, StateTableResult& values) const override;

    Status seek(const Columns& keys, const std::vector<std::string>& projection_columns,
                StateTableResult& values) const override;

    ChunkIteratorPtrOr prefix_scan(const Columns& keys, size_t row_idx) const override;

    ChunkIteratorPtrOr prefix_scan(const std::vector<std::string>& projection_columns, const Columns& keys,
                                   size_t row_idx) const override;

    /**
     * Update join state based on StreamChunk with ops.
     * For each row:
     * - INSERT: increment match_count
     * - DELETE: decrement match_count
     */
    Status write(RuntimeState* state, const StreamChunkPtr& chunk) override;

    Status commit(RuntimeState* state) override;

    Status reset_epoch(RuntimeState* state) override;

    // JoinStateTable specific methods

    /**
     * Get the match count for a specific key.
     */
    int64_t get_match_count(const std::string& key) const;

    /**
     * Update match count and return the old and new counts.
     * This is the key method for detecting NULL row changes.
     *
     * @param key Serialized join key
     * @param delta Change in match count (+1 for INSERT, -1 for DELETE)
     * @param old_count Output: match count before update
     * @param new_count Output: match count after update
     * @return true if this causes a NULL row change (0->positive or positive->0)
     */
    bool update_match_count(const std::string& key, int64_t delta, int64_t* old_count, int64_t* new_count);

    /**
     * Serialize key columns to a string for hash map lookup.
     */
    static std::string serialize_key(const Columns& key_columns, size_t row_idx);

    /**
     * Check if a key has any matches.
     */
    bool has_matches(const std::string& key) const;

    /**
     * Get all keys that transitioned from matched to unmatched
     * (i.e., match_count went from >0 to 0).
     */
    std::vector<std::string> get_newly_unmatched_keys() const;

    /**
     * Get all keys that transitioned from unmatched to matched
     * (i.e., match_count went from 0 to >0).
     */
    std::vector<std::string> get_newly_matched_keys() const;

    /**
     * Clear tracked transitions for a new epoch.
     */
    void clear_transitions();

private:
    // In-memory state storage
    // Key: serialized join key
    // Value: JoinStateEntry
    std::unordered_map<std::string, JoinStateEntry> _state_map;

    // Track keys that had NULL row changes in this epoch
    std::vector<std::string> _newly_matched_keys;
    std::vector<std::string> _newly_unmatched_keys;

    RuntimeState* _runtime_state = nullptr;
};

using JoinStateTablePtr = std::unique_ptr<JoinStateTable>;

} // namespace starrocks::stream
