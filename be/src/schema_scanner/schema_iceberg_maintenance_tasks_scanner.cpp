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

#include "schema_scanner/schema_iceberg_maintenance_tasks_scanner.h"

#include "exec/schema_scanner.h"
#include "runtime/runtime_state.h"
#include "schema_scanner/schema_helper.h"
#include "types/constexpr.h"
#include "types/datetime_value.h"
#include "types/json_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaIcebergMaintenanceTasksScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"TASK_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"CATALOG_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"DATABASE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"TABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"ACTION", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TRIGGER_REASON", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"STMT", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"START_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"END_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"DURATION_MS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"STATUS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"FAILURE_REASON", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"DETAILS", TypeDescriptor::from_logical_type(TYPE_JSON), kJsonDefaultSize, true}};

SchemaIcebergMaintenanceTasksScanner::SchemaIcebergMaintenanceTasksScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaIcebergMaintenanceTasksScanner::~SchemaIcebergMaintenanceTasksScanner() = default;

Status SchemaIcebergMaintenanceTasksScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    TGetIcebergMaintenanceTasksParams params;
    std::string catalog_name;
    std::string database_name;
    std::string table_name;
    if (_parse_expr_predicate("CATALOG_NAME", catalog_name)) {
        params.__set_catalog_name(catalog_name);
    }
    if (_parse_expr_predicate("DATABASE_NAME", database_name)) {
        params.__set_database_name(database_name);
    }
    if (_parse_expr_predicate("TABLE_NAME", table_name)) {
        params.__set_table_name(table_name);
    }
    if (nullptr != _param->current_user_ident) {
        params.__set_current_user_ident(*(_param->current_user_ident));
    }
    if (_param->limit > 0) {
        params.__isset.pagination = true;
        params.pagination.__set_limit(_param->limit);
    }
    RETURN_IF_ERROR(SchemaHelper::get_iceberg_maintenance_tasks(_ss_state, params, &_task_result));
    _task_index = 0;
    return Status::OK();
}

Status SchemaIcebergMaintenanceTasksScanner::fill_chunk(ChunkPtr* chunk) {
    const TIcebergMaintenanceTaskInfo& info = _task_result.tasks[_task_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    auto fill_varchar = [&](SlotId slot_id, const std::string& str, bool isset) {
        auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(slot_id);
        if (isset) {
            Slice value(str.c_str(), str.length());
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
        } else {
            fill_data_column_with_null(column);
        }
    };
    auto fill_datetime = [&](SlotId slot_id, int64_t unix_seconds, bool isset) {
        auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(slot_id);
        auto* nullable_column = down_cast<NullableColumn*>(column);
        if (!isset || unix_seconds <= 0) {
            nullable_column->append_nulls(1);
        } else {
            DateTimeValue t;
            t.from_unixtime(unix_seconds, _runtime_state->timezone_obj());
            fill_column_with_slot<TYPE_DATETIME>(column, (void*)&t);
        }
    };
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // TASK_ID
            fill_varchar(1, info.task_id, true);
            break;
        }
        case 2: {
            // CATALOG_NAME
            fill_varchar(2, info.catalog_name, true);
            break;
        }
        case 3: {
            // DATABASE_NAME
            fill_varchar(3, info.database_name, true);
            break;
        }
        case 4: {
            // TABLE_NAME
            fill_varchar(4, info.table_name, true);
            break;
        }
        case 5: {
            // ACTION
            fill_varchar(5, info.action, info.__isset.action);
            break;
        }
        case 6: {
            // TRIGGER_REASON
            fill_varchar(6, info.trigger_reason, info.__isset.trigger_reason);
            break;
        }
        case 7: {
            // STMT
            fill_varchar(7, info.stmt, info.__isset.stmt);
            break;
        }
        case 8: {
            // START_TIME
            fill_datetime(8, info.start_time, info.__isset.start_time);
            break;
        }
        case 9: {
            // END_TIME
            fill_datetime(9, info.end_time, info.__isset.end_time);
            break;
        }
        case 10: {
            // DURATION_MS
            auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(10);
            if (info.__isset.duration_ms) {
                int64_t value = info.duration_ms;
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 11: {
            // STATUS
            fill_varchar(11, info.status, info.__isset.status);
            break;
        }
        case 12: {
            // FAILURE_REASON
            fill_varchar(12, info.failure_reason, info.__isset.failure_reason);
            break;
        }
        case 13: {
            // DETAILS
            auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(13);
            if (info.__isset.details) {
                Slice details = Slice(info.details);
                JsonValue json_value;
                JsonValue* json_value_ptr = &json_value;
                Status s = JsonValue::parse(details, &json_value);
                if (!s.ok()) {
                    LOG(WARNING) << "parse iceberg maintenance task details failed. details:" << details.to_string()
                                 << " error:" << s;
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                } else {
                    fill_column_with_slot<TYPE_JSON>(column, (void*)&json_value_ptr);
                }
            } else {
                down_cast<NullableColumn*>(column)->append_nulls(1);
            }
            break;
        }
        default:
            break;
        }
    }
    _task_index++;
    return Status::OK();
}

Status SchemaIcebergMaintenanceTasksScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init || chunk == nullptr || eos == nullptr) {
        return Status::InternalError("Used before initialized.");
    }
    if (_task_index >= _task_result.tasks.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks
