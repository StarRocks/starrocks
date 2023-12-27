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

#include "exec/schema_scanner/schema_materialized_views_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

// Keep tracks with `information_schema.materialized_views` table's schema.
SchemaScanner::ColumnDesc SchemaMaterializedViewsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"MATERIALIZED_VIEW_ID", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"REFRESH_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"IS_ACTIVE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"INACTIVE_REASON", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PARTITION_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TASK_ID", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TASK_NAME", TYPE_VARCHAR, sizeof(StringValue), false},

        {"LAST_REFRESH_START_TIME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"LAST_REFRESH_FINISHED_TIME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"LAST_REFRESH_DURATION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"LAST_REFRESH_STATE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"LAST_REFRESH_FORCE_REFRESH", TYPE_VARCHAR, sizeof(StringValue), false},
        {"LAST_REFRESH_START_PARTITION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"LAST_REFRESH_END_PARTITION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"LAST_REFRESH_BASE_REFRESH_PARTITIONS", TYPE_VARCHAR, sizeof(StringValue), false},
        {"LAST_REFRESH_MV_REFRESH_PARTITIONS", TYPE_VARCHAR, sizeof(StringValue), false},

        {"LAST_REFRESH_ERROR_CODE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"LAST_REFRESH_ERROR_MESSAGE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_ROWS", TYPE_VARCHAR, sizeof(StringValue), false},
        {"MATERIALIZED_VIEW_DEFINITION", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaMaterializedViewsScanner::SchemaMaterializedViewsScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaMaterializedViewsScanner::~SchemaMaterializedViewsScanner() = default;

Status SchemaMaterializedViewsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TGetDbsParams db_params;
    if (nullptr != _param->db) {
        db_params.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->current_user_ident) {
        db_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            db_params.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            db_params.__set_user_ip(*(_param->user_ip));
        }
    }

    if (nullptr != _param->ip && 0 != _param->port) {
        int timeout_ms = state->query_options().query_timeout * 1000;
        RETURN_IF_ERROR(SchemaHelper::get_db_names(*(_param->ip), _param->port, db_params, &_db_result, timeout_ms));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaMaterializedViewsScanner::fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    const TMaterializedViewStatus& info = _mv_results.materialized_views[_table_index];
    std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
    DatumArray datum_array{
            Slice(info.id),
            Slice(db_name),
            Slice(info.name),
            Slice(info.refresh_type),
            Slice(info.is_active),
            Slice(info.inactive_reason),
            Slice(info.partition_type),
            Slice(info.task_id),
            Slice(info.task_name),
            Slice(info.last_refresh_start_time),
            Slice(info.last_refresh_finished_time),
            Slice(info.last_refresh_duration),
            Slice(info.last_refresh_state),
            Slice(info.last_refresh_force_refresh),
            Slice(info.last_refresh_start_partition),
            Slice(info.last_refresh_end_partition),
            Slice(info.last_refresh_base_refresh_partitions),
            Slice(info.last_refresh_mv_refresh_partitions),
            Slice(info.last_refresh_error_code),
            Slice(info.last_refresh_error_message),
            Slice(info.rows),
            Slice(info.text),
    };

    for (const auto& [slot_id, index] : slot_id_map) {
        Column* column = (*chunk)->get_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    _table_index++;
    return {};
}

Status SchemaMaterializedViewsScanner::get_materialized_views() {
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index++]);
    if (nullptr != _param->wild) {
        table_params.__set_pattern(*(_param->wild));
    }
    if (nullptr != _param->current_user_ident) {
        table_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            table_params.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            table_params.__set_user_ip(*(_param->user_ip));
        }
    }
    table_params.__set_type(TTableType::MATERIALIZED_VIEW);

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(
                SchemaHelper::list_materialized_view_status(*(_param->ip), _param->port, table_params, &_mv_results));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _table_index = 0;
    return Status::OK();
}

Status SchemaMaterializedViewsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    while (_table_index >= _mv_results.materialized_views.size()) {
        if (_db_index < _db_result.dbs.size()) {
            RETURN_IF_ERROR(get_materialized_views());
        } else {
            *eos = true;
            return Status::OK();
        }
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks
