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

#include "exec/schema_scanner/schema_tasks_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaTasksScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"TASK_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CREATE_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"SCHEDULE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DATABASE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DEFINITION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"EXPIRE_TIME", TYPE_DATETIME, sizeof(StringValue), true}};

SchemaTasksScanner::SchemaTasksScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaTasksScanner::~SchemaTasksScanner() = default;

Status SchemaTasksScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    TGetTasksParams task_params;
    if (nullptr != _param->current_user_ident) {
        task_params.__set_current_user_ident(*(_param->current_user_ident));
    }
    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_tasks(*(_param->ip), _param->port, task_params, &_task_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _task_index = 0;
    return Status::OK();
}

Status SchemaTasksScanner::fill_chunk(ChunkPtr* chunk) {
    const TTaskInfo& task_info = _task_result.tasks[_task_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // TASK_NAME
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                const std::string* str = &task_info.task_name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // CREATE_TIME
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                if (task_info.__isset.create_time) {
                    int64_t create_time = task_info.create_time;
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
        case 3: {
            // SCHEDULE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                const std::string* str = &task_info.schedule;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 4: {
            // DATABASE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                const std::string* db_name = &task_info.database;
                Slice value(db_name->c_str(), db_name->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 5: {
            // DEFINITION
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(5);
                const std::string* str = &task_info.definition;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 6: {
            // EXPIRE_TIME
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(6);
                auto* nullable_column = down_cast<NullableColumn*>(column.get());
                if (task_info.__isset.expire_time) {
                    int64_t expire_time = task_info.expire_time;
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
        default:
            break;
        }
    }
    _task_index++;
    return Status::OK();
}

Status SchemaTasksScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (_task_index >= _task_result.tasks.size()) {
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
