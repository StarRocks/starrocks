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

#include "exec/schema_scanner/schema_task_runs_scanner.h"

#include "exec/schema_scanner.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/datetime_value.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaTaskRunsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"QUERY_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"TASK_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"CREATE_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"FINISH_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"STATE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"CATALOG", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"DATABASE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"DEFINITION", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"EXPIRE_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(Slice), true},
        {"ERROR_CODE", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(Slice), true},
        {"ERROR_MESSAGE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"PROGRESS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"EXTRA_MESSAGE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"PROPERTIES", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"JOB_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"PROCESS_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true}};

SchemaTaskRunsScanner::SchemaTaskRunsScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaTaskRunsScanner::~SchemaTaskRunsScanner() = default;

Status SchemaTaskRunsScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    // init schema scanner state
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    std::string task_name;
    std::string query_id;
    std::string task_run_state;
    TGetTasksParams task_params;
    // task_name
    if (_parse_expr_predicate("TASK_NAME", task_name)) {
        task_params.__set_task_name(task_name);
    }
    // query_id
    if (_parse_expr_predicate("QUERY_ID", query_id)) {
        task_params.__set_query_id(query_id);
    }
    // task_run_state
    if (_parse_expr_predicate("STATE", task_run_state)) {
        task_params.__set_state(task_run_state);
    }
    if (nullptr != _param->current_user_ident) {
        task_params.__set_current_user_ident(*(_param->current_user_ident));
    }
    if (_param->limit > 0) {
        task_params.__isset.pagination = true;
        task_params.pagination.__set_limit(_param->limit);
    }
    RETURN_IF_ERROR(SchemaHelper::get_task_runs(_ss_state, task_params, &_task_run_result));
    _task_run_index = 0;
    return Status::OK();
}

Status SchemaTaskRunsScanner::fill_chunk(ChunkPtr* chunk) {
    const TTaskRunInfo& task_run_info = _task_run_result.task_runs[_task_run_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // QUERY_ID
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(1);
                const std::string* str = &task_run_info.query_id;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // TASK_NAME
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(2);
                const std::string* str = &task_run_info.task_name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 3: {
            // CREATE_TIME
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(3);
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                if (task_run_info.__isset.create_time) {
                    int64_t create_time = task_run_info.create_time;
                    if (create_time <= 0) {
                        nullable_column->append_nulls(1);
                    } else {
                        DateTimeValue t;
                        t.from_unixtime(create_time, _runtime_state->timezone_obj());
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                    }
                } else {
                    nullable_column->append_nulls(1);
                }
            }
            break;
        }
        case 4: {
            // FINISH_TIME
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(4);
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                if (task_run_info.__isset.finish_time) {
                    int64_t complete_time = task_run_info.finish_time;
                    if (complete_time <= 0) {
                        nullable_column->append_nulls(1);
                    } else {
                        DateTimeValue t;
                        t.from_unixtime(complete_time, _runtime_state->timezone_obj());
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                    }
                } else {
                    nullable_column->append_nulls(1);
                }
            }
            break;
        }
        case 5: {
            // STATE
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(5);
                if (task_run_info.__isset.state) {
                    const std::string* str = &task_run_info.state;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 6: {
            // CATALOG
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(6);
                if (task_run_info.__isset.catalog) {
                    const std::string* str = &task_run_info.catalog;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 7: {
            // DATABASE
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(7);
                if (task_run_info.__isset.database) {
                    const std::string* str = &task_run_info.database;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 8: {
            // DEFINITION
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(8);
                if (task_run_info.__isset.definition) {
                    const std::string* str = &task_run_info.definition;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 9: {
            // EXPIRE_TIME
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(9);
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                if (task_run_info.__isset.expire_time) {
                    int64_t expire_time = task_run_info.expire_time;
                    if (expire_time <= 0) {
                        nullable_column->append_nulls(1);
                    } else {
                        DateTimeValue t;
                        t.from_unixtime(expire_time, _runtime_state->timezone_obj());
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                    }
                } else {
                    nullable_column->append_nulls(1);
                }
            }
            break;
        }
        case 10: {
            // ERROR_CODE
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(10);
                if (task_run_info.__isset.error_code) {
                    int64_t value = task_run_info.error_code;
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 11: {
            // ERROR_MESSAGE
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(11);
                if (task_run_info.__isset.error_message) {
                    const std::string* str = &task_run_info.error_message;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 12: {
            // progress
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(12);
                if (task_run_info.__isset.progress) {
                    const std::string* str = &task_run_info.progress;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 13: {
            // extra_message
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(13);
                if (task_run_info.__isset.extra_message) {
                    const std::string* str = &task_run_info.extra_message;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 14: {
            // properties
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(14);
                if (task_run_info.__isset.properties) {
                    const std::string* str = &task_run_info.properties;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 15: {
            // job_id
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(15);
                if (task_run_info.__isset.job_id) {
                    const std::string* str = &task_run_info.job_id;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 16: {
            // process_time
            {
                auto column = (*chunk)->get_mutable_column_by_slot_id(16);
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                if (task_run_info.__isset.process_time) {
                    int64_t complete_time = task_run_info.process_time;
                    if (complete_time <= 0) {
                        nullable_column->append_nulls(1);
                    } else {
                        DateTimeValue t;
                        t.from_unixtime(complete_time, _runtime_state->timezone_obj());
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                    }
                } else {
                    nullable_column->append_nulls(1);
                }
            }
            break;
        }
        default:
            break;
        }
    }
    _task_run_index++;
    return Status::OK();
}

Status SchemaTaskRunsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init || chunk == nullptr || eos == nullptr) {
        return Status::InternalError("Used before initialized.");
    }
    if (_task_run_index >= _task_run_result.task_runs.size()) {
        *eos = true;
        return Status::OK();
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks
