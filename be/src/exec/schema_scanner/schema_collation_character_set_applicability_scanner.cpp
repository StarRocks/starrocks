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

#include "exec/schema_scanner/schema_collation_character_set_applicability_scanner.h"

#include "exec/schema_scanner/schema_collations_scanner.h"
#include "exec/schema_scanner/schema_column_filler.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaCollationCharacterSetApplicabilityScanner::_s_cols[] = {
        //   name,       type,          size,                     is_null
        {"COLLATION_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"CHARACTER_SET_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
};

SchemaCollationCharacterSetApplicabilityScanner::SchemaCollationCharacterSetApplicabilityScanner()
        : SchemaScanner(_s_cols, sizeof(_s_cols) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaCollationCharacterSetApplicabilityScanner::~SchemaCollationCharacterSetApplicabilityScanner() = default;

Status SchemaCollationCharacterSetApplicabilityScanner::fill_chunk(ChunkPtr* chunk) {
    // In MySQL this table is a view derived from COLLATIONS: one row per
    // collation mapping it to its character set. We iterate the same static
    // collation list that SchemaCollationsScanner exposes, so adding a
    // collation there automatically reflects here and client joins by
    // COLLATION_NAME stay consistent.
    const auto* row = &SchemaCollationsScanner::collations()[_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // COLLATION_NAME
            auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(1);
            Slice value(row->name, strlen(row->name));
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            break;
        }
        case 2: {
            // CHARACTER_SET_NAME
            auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(2);
            Slice value(row->charset, strlen(row->charset));
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            break;
        }
        default:
            break;
        }
    }

    _index++;
    return Status::OK();
}

Status SchemaCollationCharacterSetApplicabilityScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (SchemaCollationsScanner::collations()[_index].name == nullptr) {
        *eos = true;
        return Status::OK();
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks
