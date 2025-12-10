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

#include <string>
#include <vector>

#include "column/vectorized_fwd.h"

namespace starrocks {

class SlotDescriptor;

// Check if a column name represents a virtual column.
// Virtual columns are read-only metadata columns that are not persisted.
// Currently supported: _tablet_id_
bool is_virtual_column(const std::string& column_name);

// Process virtual columns in a chunk by:
// 1. Setting slot_id_to_index for regular columns
// 2. Appending virtual columns (e.g., _tablet_id_) to the chunk
// 3. Removing columns that don't have slot_ids (e.g., helper columns added to read row count
//    when only virtual columns are selected)
//
// This ensures _columns.size() == _slot_id_to_index.size() which is required for clone_empty_with_slot().
//
// @param chunk The chunk to process
// @param query_slots The slot descriptors for query columns
// @param tablet_id The tablet ID to use for _tablet_id_ virtual column
void process_virtual_columns(Chunk* chunk, const std::vector<SlotDescriptor*>& query_slots, int64_t tablet_id);

} // namespace starrocks