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

#include "exec/schema_scanner/schema_materialized_view_refresh_jobs_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "types/datetime_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaMaterializedViewRefreshJobsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"JOB_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"MATERIALIZED_VIEW_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"TABLE_SCHEMA", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TASK_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"WAREHOUSE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"RESOURCE_GROUP", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"CREATOR", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"SUBMIT_USER", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"RUN_AS_USER", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"SUBMIT_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"REFRESH_STATE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"FINISH_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"DURATION_TIME", TypeDescriptor::from_logical_type(TYPE_DOUBLE), sizeof(double), true},
        {"REFRESH_TRIGGER", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"REFRESH_MODE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"IMV_SOURCE_VERSION_RANGE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"IMV_SOURCE_TIMESTAMP_RANGE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"FAILED_TASK_RUN_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"FAILED_QUERY_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"ERROR_CODE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"ERROR_MESSAGE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
};

SchemaMaterializedViewRefreshJobsScanner::SchemaMaterializedViewRefreshJobsScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaMaterializedViewRefreshJobsScanner::~SchemaMaterializedViewRefreshJobsScanner() = default;

Status SchemaMaterializedViewRefreshJobsScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    TGetTasksParams params;
    // Push TABLE_SCHEMA down as the FE `db` filter so a single-database query doesn't make the leader aggregate
    // every MV's task-run history cluster-wide. Only TABLE_SCHEMA is job-invariant and thus safe to push; a
    // per-run filter such as REFRESH_STATE would drop runs the job roll-up needs, so it stays a BE-side filter.
    std::string db;
    if (_parse_expr_predicate("TABLE_SCHEMA", db)) {
        params.__set_db(db);
    }
    if (nullptr != _param->current_user_ident) {
        params.__set_current_user_ident(*(_param->current_user_ident));
    }
    // Do not push LIMIT down to the task-run query: jobs are aggregated from task runs, so a row limit must
    // cap final job rows (applied by the executor), not the task runs each job is rolled up from.
    RETURN_IF_ERROR(SchemaHelper::list_materialized_view_refresh_jobs(_ss_state, params, &_jobs_result));
    _jobs_index = 0;
    return Status::OK();
}

Status SchemaMaterializedViewRefreshJobsScanner::fill_chunk(ChunkPtr* chunk) {
    const TMaterializedViewRefreshJobInfo& info = _jobs_result.jobs[_jobs_index];
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_map) {
        if (slot_id < 1 || slot_id > std::size(SchemaMaterializedViewRefreshJobsScanner::_s_tbls_columns)) {
            return Status::InternalError("Invalid slot id: " + std::to_string(slot_id));
        }
        auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(slot_id);

        switch (slot_id) {
        case 1: {
            // JOB_ID (non-nullable: the FE always sets job_id, so fill unconditionally)
            const std::string* str = &info.job_id;
            Slice value(str->c_str(), str->length());
            fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            break;
        }
        case 2: {
            // MATERIALIZED_VIEW_ID
            if (info.__isset.materialized_view_id) {
                try {
                    int64_t value = std::stoll(info.materialized_view_id);
                    fill_column_with_slot<TYPE_BIGINT>(column, (void*)&value);
                } catch (const std::exception& e) {
                    fill_data_column_with_null(column);
                }
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 3: {
            // TABLE_SCHEMA
            if (info.__isset.table_schema) {
                const std::string* str = &info.table_schema;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 4: {
            // TABLE_NAME
            if (info.__isset.table_name) {
                const std::string* str = &info.table_name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 5: {
            // TASK_ID
            if (info.__isset.task_id) {
                try {
                    int64_t value = std::stoll(info.task_id);
                    fill_column_with_slot<TYPE_BIGINT>(column, (void*)&value);
                } catch (const std::exception& e) {
                    fill_data_column_with_null(column);
                }
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 6: {
            // WAREHOUSE
            if (info.__isset.warehouse) {
                const std::string* str = &info.warehouse;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 7: {
            // RESOURCE_GROUP
            if (info.__isset.resource_group) {
                const std::string* str = &info.resource_group;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 8: {
            // CREATOR
            if (info.__isset.creator) {
                const std::string* str = &info.creator;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 9: {
            // SUBMIT_USER
            if (info.__isset.submit_user) {
                const std::string* str = &info.submit_user;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 10: {
            // RUN_AS_USER
            if (info.__isset.run_as_user) {
                const std::string* str = &info.run_as_user;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 11: {
            // SUBMIT_TIME
            if (info.__isset.submit_time) {
                auto* nullable_column = down_cast<NullableColumn*>(column);
                DateTimeValue t;
                if (!t.from_date_str(info.submit_time.data(), info.submit_time.size())) {
                    nullable_column->append_nulls(1);
                } else {
                    fill_column_with_slot<TYPE_DATETIME>(column, (void*)&t);
                }
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 12: {
            // REFRESH_STATE
            if (info.__isset.refresh_state) {
                const std::string* str = &info.refresh_state;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 13: {
            // FINISH_TIME
            if (info.__isset.finish_time) {
                auto* nullable_column = down_cast<NullableColumn*>(column);
                DateTimeValue t;
                if (!t.from_date_str(info.finish_time.data(), info.finish_time.size())) {
                    nullable_column->append_nulls(1);
                } else {
                    fill_column_with_slot<TYPE_DATETIME>(column, (void*)&t);
                }
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 14: {
            // DURATION_TIME
            if (info.__isset.duration_time) {
                try {
                    double value = std::stod(info.duration_time);
                    fill_column_with_slot<TYPE_DOUBLE>(column, (void*)&value);
                } catch (const std::exception& e) {
                    fill_data_column_with_null(column);
                }
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 15: {
            // REFRESH_TRIGGER
            if (info.__isset.refresh_trigger) {
                const std::string* str = &info.refresh_trigger;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 16: {
            // REFRESH_MODE
            if (info.__isset.refresh_mode) {
                const std::string* str = &info.refresh_mode;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 17: {
            // IMV_SOURCE_VERSION_RANGE
            if (info.__isset.imv_source_version_range) {
                const std::string* str = &info.imv_source_version_range;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 18: {
            // IMV_SOURCE_TIMESTAMP_RANGE
            if (info.__isset.imv_source_timestamp_range) {
                const std::string* str = &info.imv_source_timestamp_range;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 19: {
            // IMV_SOURCE_PINNED_SNAPSHOT_ID_MAP
            if (info.__isset.imv_source_pinned_snapshot_id_map) {
                const std::string* str = &info.imv_source_pinned_snapshot_id_map;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 20: {
            // FAILED_TASK_RUN_ID
            if (info.__isset.failed_task_run_id) {
                const std::string* str = &info.failed_task_run_id;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 21: {
            // FAILED_QUERY_ID
            if (info.__isset.failed_query_id) {
                const std::string* str = &info.failed_query_id;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 22: {
            // ERROR_CODE
            if (info.__isset.error_code) {
                const std::string* str = &info.error_code;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        case 23: {
            // ERROR_MESSAGE
            if (info.__isset.error_message) {
                const std::string* str = &info.error_message;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&value);
            } else {
                fill_data_column_with_null(column);
            }
            break;
        }
        default:
            break;
        }
    }
    _jobs_index++;
    return Status::OK();
}

Status SchemaMaterializedViewRefreshJobsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init || chunk == nullptr || eos == nullptr) {
        return Status::InternalError("Used before initialized.");
    }
    if (_jobs_index >= _jobs_result.jobs.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks
