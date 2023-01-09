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

#include "exec/schema_scanner/schema_collations_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaCollationsScanner::_s_cols_columns[] = {
        //   name,       type,          size
        {"COLLATION_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"IS_DEFAULT", TYPE_VARCHAR, sizeof(StringValue), false},
        {"IS_COMPILED", TYPE_VARCHAR, sizeof(StringValue), false},
        {"SORTLEN", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaCollationsScanner::CollationStruct SchemaCollationsScanner::_s_collations[] = {
        {"utf8_general_ci", "utf8", 33, "Yes", "Yes", 1},
        {nullptr, nullptr, 0, nullptr, nullptr, 0},
};

SchemaCollationsScanner::SchemaCollationsScanner()
        : SchemaScanner(_s_cols_columns, sizeof(_s_cols_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaCollationsScanner::~SchemaCollationsScanner() = default;

Status SchemaCollationsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // COLLATION_NAME
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                Slice value(_s_collations[_index].name, strlen(_s_collations[_index].name));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // charset
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                Slice value(_s_collations[_index].charset, strlen(_s_collations[_index].charset));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 3: {
            // id
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&_s_collations[_index].id);
            }
            break;
        }
        case 4: {
            // is_default
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                Slice value(_s_collations[_index].is_default, strlen(_s_collations[_index].is_default));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 5: {
            // IS_COMPILED
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(5);
                Slice value(_s_collations[_index].is_compile, strlen(_s_collations[_index].is_compile));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 6: {
            // sortlen
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(6);
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&_s_collations[_index].sortlen);
            }
            break;
        }
        default:
            break;
        }
    }

    _index++;
    return Status::OK();
}

Status SchemaCollationsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (nullptr == _s_collations[_index].name) {
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
