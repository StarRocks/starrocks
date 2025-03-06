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

#include "exec/schema_scanner/schema_routine_load_jobs_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "gutil/strings/substitute.h"
#include "http/http_client.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaRoutineLoadJobsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"CREATE_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"PAUSE_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"END_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"DB_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"TABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"STATE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"DATA_SOURCE_TYPE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"CURRENT_TASK_NUM", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"JOB_PROPERTIES", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"DATA_SOURCE_PROPERTIES", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue),
         false},
        {"CUSTOM_PROPERTIES", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"STATISTICS", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"PROGRESS", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"REASONS_OF_STATE_CHANGED", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue),
         true},
        {"ERROR_LOG_URLS", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"TRACKING_SQL", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"OTHER_MSG", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"LATEST_SOURCE_POSITION", TypeDescriptor::from_logical_type(TYPE_JSON), kJsonDefaultSize, true},
        {"OFFSET_LAG", TypeDescriptor::from_logical_type(TYPE_JSON), kJsonDefaultSize, true}};

SchemaRoutineLoadJobsScanner::SchemaRoutineLoadJobsScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaRoutineLoadJobsScanner::~SchemaRoutineLoadJobsScanner() = default;

Status SchemaRoutineLoadJobsScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    TGetLoadsParams load_params;
    if (nullptr != _param->db) {
        load_params.__set_db(*(_param->db));
    }
    if (nullptr != _param->label) {
        load_params.__set_label(*(_param->label));
    }
    if (_param->job_id != -1) {
        load_params.__set_job_id(_param->job_id);
    }

    // init schema scanner state
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    RETURN_IF_ERROR(SchemaHelper::get_routine_load_jobs(_ss_state, load_params, &_result));

    _cur_idx = 0;
    return Status::OK();
}

Status SchemaRoutineLoadJobsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (; _cur_idx < _result.loads.size(); _cur_idx++) {
        auto& info = _result.loads[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 21) {
                return Status::InternalError(strings::Substitute("invalid slot id: $0", slot_id));
            }
            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.id);
                break;
            }
            case 2: {
                // name
                Slice name = Slice(info.name);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&name);
                break;
            }
            case 3: {
                // create time
                DateTimeValue t;
                if (info.__isset.create_time) {
                    if (t.from_date_str(info.create_time.data(), info.create_time.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 4: {
                // pause time
                DateTimeValue t;
                if (info.__isset.pause_time) {
                    if (t.from_date_str(info.pause_time.data(), info.pause_time.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 5: {
                // end time
                DateTimeValue t;
                if (info.__isset.end_time) {
                    if (t.from_date_str(info.end_time.data(), info.end_time.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 6: {
                // db_name
                Slice db_name = Slice(info.db_name);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&db_name);
                break;
            }
            case 7: {
                // table_name
                Slice table_name = Slice(info.table_name);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&table_name);
                break;
            }
            case 8: {
                // state
                Slice state = Slice(info.state);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&state);
                break;
            }
            case 9: {
                // data_source_type
                Slice data_source_type = Slice(info.data_source_type);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&data_source_type);
                break;
            }

            case 10: {
                // current_task_num
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.current_task_num);
                break;
            }
            case 11: {
                // job_properties
                Slice job_properties = Slice(info.job_properties);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&job_properties);
                break;
            }
            case 12: {
                // data_source_properties
                Slice data_source_properties = Slice(info.data_source_properties);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&data_source_properties);
                break;
            }
            case 13: {
                // custom_properties
                Slice custom_properties = Slice(info.custom_properties);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&custom_properties);
                break;
            }

            case 14: {
                // statistic
                Slice statistic = Slice(info.statistic);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&statistic);
                break;
            }
            case 15: {
                // progress
                Slice progress = Slice(info.progress);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&progress);
                break;
            }
            case 16: {
                // reasons_of_state_changed
                if (info.__isset.reasons_of_state_changed) {
                    Slice reasons_of_state_changed = Slice(info.reasons_of_state_changed);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&reasons_of_state_changed);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 17: {
                // error_log_urls
                if (info.__isset.error_log_urls) {
                    Slice error_log_urls = Slice(info.error_log_urls);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&error_log_urls);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 18: {
                // tracking sql
                if (info.__isset.tracking_sql) {
                    Slice sql = Slice(info.tracking_sql);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&sql);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 19: {
                // other_msg
                if (info.__isset.other_msg) {
                    Slice other_msg = Slice(info.other_msg);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&other_msg);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 20: {
                // latest_source_position
                Slice latest_source_position = Slice(info.latest_source_position);
                JsonValue json_value;
                JsonValue* json_value_ptr = &json_value;
                Status s = JsonValue::parse(latest_source_position, &json_value);
                if (!s.ok()) {
                    LOG(WARNING) << "parse latest_source_position failed. latest_source_position:"
                                 << latest_source_position.to_string() << " error:" << s;
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                } else {
                    fill_column_with_slot<TYPE_JSON>(column.get(), (void*)&json_value_ptr);
                }
                break;
            }
            case 21: {
                // offset_lag
                Slice offset_lag = Slice(info.offset_lag);
                JsonValue json_value;
                JsonValue* json_value_ptr = &json_value;
                Status s = JsonValue::parse(offset_lag, &json_value);
                if (!s.ok()) {
                    LOG(WARNING) << "parse offset_lag failed. offset_lag:" << offset_lag.to_string() << " error:" << s;
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                } else {
                    fill_column_with_slot<TYPE_JSON>(column.get(), (void*)&json_value_ptr);
                }
                break;
            }
            default:
                break;
            }
        }
    }
    return Status::OK();
}

Status SchemaRoutineLoadJobsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_cur_idx >= _result.loads.size()) {
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
