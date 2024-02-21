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
        {"EXPIRE_TIME", TYPE_DATETIME, sizeof(StringValue), true},
        {"PROPERTIES", TYPE_VARCHAR, sizeof(StringValue), false},
};

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

DatumArray SchemaTasksScanner::_build_row() {
    auto& task = _task_result.tasks.at(_task_index++);
    return {
            Slice(task.task_name),
            TimestampValue::create_from_unixtime(task.create_time, _runtime_state->timezone_obj()),
            Slice(task.schedule),
            Slice(task.database),
            Slice(task.definition),
            TimestampValue::create_from_unixtime(task.expire_time, _runtime_state->timezone_obj()),
            Slice(task.properties),
    };
}

Status SchemaTasksScanner::fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    auto datum_array = _build_row();
    for (const auto& [slot_id, index] : slot_id_map) {
        Column* column = (*chunk)->get_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    return {};
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
