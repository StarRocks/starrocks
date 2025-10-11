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
#include "types/logical_type.h"

namespace starrocks {

// Keep tracks with `information_schema.materialized_views` table's schema.
SchemaScanner::ColumnDesc SchemaMaterializedViewsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"MATERIALIZED_VIEW_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"TABLE_SCHEMA", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"REFRESH_TYPE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"IS_ACTIVE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"INACTIVE_REASON", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"PARTITION_TYPE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TASK_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"TASK_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},

        {"LAST_REFRESH_START_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"LAST_REFRESH_FINISHED_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"LAST_REFRESH_DURATION", TypeDescriptor::from_logical_type(TYPE_DOUBLE), sizeof(double), true},
        {"LAST_REFRESH_STATE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"LAST_REFRESH_FORCE_REFRESH", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"LAST_REFRESH_START_PARTITION", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"LAST_REFRESH_END_PARTITION", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"LAST_REFRESH_BASE_REFRESH_PARTITIONS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice),
         true},
        {"LAST_REFRESH_MV_REFRESH_PARTITIONS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},

        {"LAST_REFRESH_ERROR_CODE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"LAST_REFRESH_ERROR_MESSAGE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TABLE_ROWS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"MATERIALIZED_VIEW_DEFINITION", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"EXTRA_MESSAGE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"QUERY_REWRITE_STATUS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"CREATOR", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"LAST_REFRESH_PROCESS_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"LAST_REFRESH_JOB_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
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

    RETURN_IF_ERROR(init_schema_scanner_state(state));
    RETURN_IF_ERROR(SchemaHelper::get_db_names(_ss_state, db_params, &_db_result));
    return Status::OK();
}

Status SchemaMaterializedViewsScanner::fill_chunk(ChunkPtr* chunk) {
    if (_table_index >= _mv_results.materialized_views.size() || _table_index < 0) {
        return Status::OK();
    }
    if (_db_index > _db_result.dbs.size() || _db_index <= 0) {
        return Status::OK();
    }
    const TMaterializedViewStatus& info = _mv_results.materialized_views[_table_index];
    VLOG(2) << "info: " << apache::thrift::ThriftDebugString(info);

    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
    for (const auto& [slot_id, index] : slot_id_map) {
        if (slot_id < 1 || slot_id > std::size(SchemaMaterializedViewsScanner::_s_tbls_columns)) {
            return Status::InternalError("Invalid slot id: " + std::to_string(slot_id));
        }
        auto column = (*chunk)->get_mutable_column_by_slot_id(slot_id);

        switch (slot_id) {
        case 1: {
            // id
            int64_t value = 0;
            try {
                value = std::stoll(info.id);
            } catch (const std::exception& e) {
                // ingore exception
            }
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&value);
            break;
        }
        case 2: {
            // database_name
            if (info.__isset.database_name) {
                const std::string* str = &info.database_name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 3: {
            // name
            if (info.__isset.name) {
                const std::string* str = &info.name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 4: {
            // refresh_type
            if (info.__isset.refresh_type) {
                const std::string* str = &info.refresh_type;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 5: {
            // is_active
            if (info.__isset.is_active) {
                const std::string* str = &info.is_active;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 6: {
            // INACTIVE_REASON
            if (info.__isset.inactive_reason) {
                const std::string* str = &info.inactive_reason;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 7: {
            // PARTITION_TYPE
            if (info.__isset.partition_type) {
                const std::string* db_name = &info.partition_type;
                Slice value(db_name->c_str(), db_name->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 8: {
            // TASK_ID
            if (info.__isset.task_id) {
                try {
                    int64_t value = std::stoll(info.task_id);
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&value);
                } catch (const std::exception& e) {
                    fill_data_column_with_null(column.get());
                }
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 9: {
            // TASK_NAME
            if (info.__isset.task_name) {
                const std::string* str = &info.task_name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 10: {
            // LAST_REFRESH_START_TIME
            if (info.__isset.last_refresh_start_time) {
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                DateTimeValue t;
                if (!t.from_date_str(info.last_refresh_start_time.data(), info.last_refresh_start_time.size())) {
                    nullable_column->append_nulls(1);
                } else {
                    fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                }
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 11: {
            // LAST_REFRESH_FINISHED_TIME
            if (info.__isset.last_refresh_finished_time) {
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                DateTimeValue t;
                if (!t.from_date_str(info.last_refresh_finished_time.data(), info.last_refresh_finished_time.size())) {
                    nullable_column->append_nulls(1);
                } else {
                    fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                }
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 12: {
            // LAST_REFRESH_DURATION
            if (info.__isset.last_refresh_duration) {
                try {
                    double value = std::stod(info.last_refresh_duration);
                    fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&value);
                } catch (const std::exception& e) {
                    fill_data_column_with_null(column.get());
                }
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 13: {
            // LAST_REFRESH_STATE
            if (info.__isset.last_refresh_state) {
                const std::string* str = &info.last_refresh_state;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 14: {
            // LAST_REFRESH_FORCE_REFRESH
            if (info.__isset.last_refresh_force_refresh) {
                const std::string* str = &info.last_refresh_force_refresh;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 15: {
            // LAST_REFRESH_START_PARTITION
            if (info.__isset.last_refresh_start_partition) {
                const std::string* str = &info.last_refresh_start_partition;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 16: {
            // LAST_REFRESH_END_PARTITION
            if (info.__isset.last_refresh_end_partition) {
                const std::string* str = &info.last_refresh_end_partition;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 17: {
            // LAST_REFRESH_BASE_REFRESH_PARTITIONS
            if (info.__isset.last_refresh_base_refresh_partitions) {
                const std::string* str = &info.last_refresh_base_refresh_partitions;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 18: {
            // LAST_REFRESH_MV_REFRESH_PARTITIONS
            if (info.__isset.last_refresh_mv_refresh_partitions) {
                const std::string* str = &info.last_refresh_mv_refresh_partitions;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 19: {
            // LAST_REFRESH_ERROR_CODE
            if (info.__isset.last_refresh_error_code) {
                const std::string* str = &info.last_refresh_error_code;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 20: {
            // LAST_REFRESH_ERROR_MESSAGE
            if (info.__isset.last_refresh_error_message) {
                const std::string* str = &info.last_refresh_error_message;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 21: {
            // TABLE_ROWS
            if (info.__isset.rows) {
                if (info.__isset.rows) {
                    try {
                        int64_t value = std::stoll(info.rows);
                        fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&value);
                    } catch (const std::exception& e) {
                        fill_data_column_with_null(column.get());
                    }
                } else {
                    fill_data_column_with_null(column.get());
                }
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 22: {
            // MATERIALIZED_VIEW_DEFINITION
            if (info.__isset.text) {
                const std::string* db_name = &info.text;
                Slice value(db_name->c_str(), db_name->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 23: {
            // EXTRA_MESSAGE
            if (info.__isset.extra_message) {
                const std::string* str = &info.extra_message;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 24: {
            // QUERY_REWRITE_STATUS
            if (info.__isset.query_rewrite_status) {
                const std::string* str = &info.query_rewrite_status;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 25: {
            // CREATOR
            if (info.__isset.creator) {
                const std::string* str = &info.creator;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 26: {
            // LAST_REFRESH_PROCESS_TIME
            if (info.__isset.last_refresh_process_time) {
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                DateTimeValue t;
                if (!t.from_date_str(info.last_refresh_process_time.data(), info.last_refresh_process_time.size())) {
                    nullable_column->append_nulls(1);
                } else {
                    fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                }
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 27: {
            // LAST_REFRESH_JOB_ID
            if (info.__isset.last_refresh_job_id) {
                const std::string* str = &info.last_refresh_job_id;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        default:
            break;
        }
    }
    _table_index++;
    return Status::OK();
}

Status SchemaMaterializedViewsScanner::get_materialized_views() {
    if (_db_index >= _db_result.dbs.size()) {
        return Status::OK();
    }
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index++]);
    // table_name
    std::string table_name;
    if (_parse_expr_predicate("TABLE_NAME", table_name)) {
        table_params.__set_table_name(table_name);
    }
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

    RETURN_IF_ERROR(SchemaHelper::list_materialized_view_status(_ss_state, table_params, &_mv_results));
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
