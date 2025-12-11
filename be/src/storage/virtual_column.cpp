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

#include "storage/virtual_column.h"

#include <algorithm>
#include <cstring>
#include <string>
#include <unordered_set>
#include <vector>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "runtime/descriptors.h"

namespace starrocks {

bool is_virtual_column(const std::string& column_name) {
    return strcasecmp(column_name.c_str(), "_tablet_id_") == 0;
}

void process_virtual_columns(Chunk* chunk, const std::vector<SlotDescriptor*>& query_slots, int64_t tablet_id) {
    // First, set slot_id_to_index for regular columns
    for (auto slot : query_slots) {
        if (!slot->is_virtual_column()) {
            size_t column_index = chunk->schema()->get_field_index_by_name(slot->col_name());
            chunk->set_slot_id_to_index(slot->id(), column_index);
        }
    }

    // Then, handle virtual columns
    // Virtual columns are not in the chunk's schema, so we append them directly using slot_id
    for (auto slot : query_slots) {
        if (slot->is_virtual_column()) {
            // Create a BIGINT column filled with tablet_id for all rows
            size_t num_rows = chunk->num_rows();
            auto tablet_id_column = Int64Column::create();
            if (num_rows > 0) {
                tablet_id_column->resize(num_rows);
                auto& data = tablet_id_column->get_data();
                std::fill(data.begin(), data.end(), tablet_id);
            }

            // Append the virtual column to the chunk using slot_id
            // This doesn't require the column to be in the schema
            chunk->append_column(tablet_id_column, slot->id());
            chunk->set_slot_id_to_index(slot->id(), chunk->num_columns() - 1);
        }
    }

    // Remove columns that don't have slot_ids (e.g., helper columns added to read row count
    // when only virtual columns are selected). This ensures _columns.size() == _slot_id_to_index.size()
    // which is required for clone_empty_with_slot().
    if (chunk->schema() != nullptr && chunk->num_columns() > chunk->get_slot_id_to_index_map().size()) {
        std::vector<size_t> columns_to_remove;
        const auto& slot_id_to_index = chunk->get_slot_id_to_index_map();
        std::unordered_set<size_t> slot_index_set;
        for (const auto& [slot_id, index] : slot_id_to_index) {
            slot_index_set.insert(index);
        }

        // Find columns without slot_ids
        for (size_t i = 0; i < chunk->num_columns(); i++) {
            if (slot_index_set.find(i) == slot_index_set.end()) {
                columns_to_remove.push_back(i);
            }
        }

        // Remove columns in reverse order to avoid index shifting
        if (!columns_to_remove.empty()) {
            std::sort(columns_to_remove.begin(), columns_to_remove.end());

            // Update slot_id_to_index: for each slot_id, count how many removed columns
            // have indices less than its index, then subtract that count
            for (const auto& [slot_id, old_index] : slot_id_to_index) {
                size_t decrement = 0;
                for (size_t removed_idx : columns_to_remove) {
                    if (removed_idx < old_index) {
                        decrement++;
                    } else {
                        break; // columns_to_remove is sorted, so we can break early
                    }
                }
                if (decrement > 0) {
                    chunk->set_slot_id_to_index(slot_id, old_index - decrement);
                }
            }

            // Sort in descending order for removal (remove from end to avoid index shifting)
            std::sort(columns_to_remove.begin(), columns_to_remove.end(), std::greater<size_t>());
            chunk->remove_columns_by_index(columns_to_remove);
        }
    }
}

} // namespace starrocks