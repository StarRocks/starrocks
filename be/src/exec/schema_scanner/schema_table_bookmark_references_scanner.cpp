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

#include "exec/schema_scanner/schema_table_bookmark_references_scanner.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaTableBookmarkReferencesScanner::_s_columns_desc[] = {
        //   name,                                  type,                                                   size,                   is_null
        {"DB_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"TABLE_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"BOOKMARK_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"HOLDER_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"CREATE_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), false},
};

SchemaTableBookmarkReferencesScanner::SchemaTableBookmarkReferencesScanner()
        : SchemaScanner(_s_columns_desc, sizeof(_s_columns_desc) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaTableBookmarkReferencesScanner::~SchemaTableBookmarkReferencesScanner() = default;

Status SchemaTableBookmarkReferencesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    _ctz = state->timezone_obj();
    return _fetch_all();
}

Status SchemaTableBookmarkReferencesScanner::_fetch_all() {
    TGetTableBookmarkReferencesRequest req;
    req.__set_auth_info(build_auth_info());

    // Predicate pushdown: forward db_id / table_id / bookmark_id from SchemaScannerParam
    // into the FE request so FE skips rows the query would discard.
    if (_param->db_id >= 0) {
        req.__set_db_id(_param->db_id);
    }
    if (_param->table_id >= 0) {
        req.__set_table_id(_param->table_id);
    }
    if (_param->bookmark_id >= 0) {
        req.__set_bookmark_id(_param->bookmark_id);
    }

    // Projection: build selected_columns from dest_slot_descs.
    if (_param->dest_slot_descs != nullptr) {
        std::vector<std::string> selected;
        selected.reserve(_param->dest_slot_descs->size());
        for (SlotDescriptor* slot : *_param->dest_slot_descs) {
            selected.emplace_back(slot->col_name());
        }
        req.__set_selected_columns(selected);
    }

    TGetTableBookmarkReferencesResponse resp;
    RETURN_IF_ERROR(SchemaHelper::get_table_bookmark_references(_ss_state, req, &resp));

    _rows = resp.table_bookmark_reference_infos;
    return Status::OK();
}

Status SchemaTableBookmarkReferencesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_row_idx >= _rows.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return _fill_chunk(chunk);
}

Status SchemaTableBookmarkReferencesScanner::_fill_chunk(ChunkPtr* chunk) {
    const TTableBookmarkReferenceInfo& info = _rows[_row_idx];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        if (slot_id < 1 || slot_id > _column_num) {
            return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
        }
        auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(slot_id);

        switch (slot_id) {
        case 1: {
            // DB_ID
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.db_id);
            break;
        }
        case 2: {
            // TABLE_ID
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.table_id);
            break;
        }
        case 3: {
            // BOOKMARK_ID
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.bookmark_id);
            break;
        }
        case 4: {
            // HOLDER_ID
            Slice holder_id = Slice(info.holder_id);
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&holder_id);
            break;
        }
        case 5: {
            // CREATE_TIME
            // FE sends epoch-millis (Reference.getAcquiredAtMs); DATETIME stores
            // only seconds, so divide by 1000 before from_unixtime.
            if (info.create_time > 0) {
                DateTimeValue ts;
                ts.from_unixtime(info.create_time / 1000, _ctz);
                fill_column_with_slot<TYPE_DATETIME>(column, (void*)&ts);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        default:
            break;
        }
    }
    _row_idx++;
    return Status::OK();
}

} // namespace starrocks
