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

#include "exec/schema_scanner/schema_table_bookmark_partitions_scanner.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaTableBookmarkPartitionsScanner::_s_columns_desc[] = {
        //   name,                                  type,                                                   size,                   is_null
        {"DB_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"TABLE_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"BOOKMARK_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"LOGICAL_PARTITION_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"PHYSICAL_PARTITION_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"VISIBLE_VERSION", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"VISIBLE_VERSION_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), false},
        {"BASE_MATERIALIZED_INDEX_META_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"BASE_MATERIALIZED_INDEX_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
};

SchemaTableBookmarkPartitionsScanner::SchemaTableBookmarkPartitionsScanner()
        : SchemaScanner(_s_columns_desc, sizeof(_s_columns_desc) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaTableBookmarkPartitionsScanner::~SchemaTableBookmarkPartitionsScanner() = default;

Status SchemaTableBookmarkPartitionsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    _ctz = state->timezone_obj();
    return _fetch_all();
}

Status SchemaTableBookmarkPartitionsScanner::_fetch_all() {
    TGetTableBookmarkPartitionsRequest req;
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

    TGetTableBookmarkPartitionsResponse resp;
    RETURN_IF_ERROR(SchemaHelper::get_table_bookmark_partitions(_ss_state, req, &resp));

    _rows = resp.table_bookmark_partition_infos;
    return Status::OK();
}

Status SchemaTableBookmarkPartitionsScanner::get_next(ChunkPtr* chunk, bool* eos) {
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

Status SchemaTableBookmarkPartitionsScanner::_fill_chunk(ChunkPtr* chunk) {
    const TTableBookmarkPartitionInfo& info = _rows[_row_idx];
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
            // LOGICAL_PARTITION_ID
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.logical_partition_id);
            break;
        }
        case 5: {
            // PHYSICAL_PARTITION_ID
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.physical_partition_id);
            break;
        }
        case 6: {
            // VISIBLE_VERSION
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.visible_version);
            break;
        }
        case 7: {
            // VISIBLE_VERSION_TIME
            // FE sends epoch-millis (PhysicalPartitionMeta.getVisibleVersionTimeMs);
            // DATETIME stores only seconds, so divide by 1000 before from_unixtime.
            if (info.visible_version_time > 0) {
                DateTimeValue ts;
                ts.from_unixtime(info.visible_version_time / 1000, _ctz);
                fill_column_with_slot<TYPE_DATETIME>(column, (void*)&ts);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 8: {
            // BASE_MATERIALIZED_INDEX_META_ID
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.base_materialized_index_meta_id);
            break;
        }
        case 9: {
            // BASE_MATERIALIZED_INDEX_ID
            fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.base_materialized_index_id);
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
