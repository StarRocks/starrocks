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

#include "exec/schema_scanner/schema_loads_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "http/http_client.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaLoadsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"LABEL", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"PROFILE_ID", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"DB_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"TABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"USER", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"WAREHOUSE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"STATE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"PROGRESS", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"TYPE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"PRIORITY", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"SCAN_ROWS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"SCAN_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"FILTERED_ROWS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"UNSELECTED_ROWS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"SINK_ROWS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"RUNTIME_DETAILS", TypeDescriptor::from_logical_type(TYPE_JSON), kJsonDefaultSize, true},
        {"CREATE_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"LOAD_START_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"LOAD_COMMIT_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"LOAD_FINISH_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"PROPERTIES", TypeDescriptor::from_logical_type(TYPE_JSON), kJsonDefaultSize, true},
        {"ERROR_MSG", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"TRACKING_SQL", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"REJECTED_RECORD_PATH", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"JOB_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false}};

SchemaLoadsScanner::SchemaLoadsScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaLoadsScanner::~SchemaLoadsScanner() = default;

Status SchemaLoadsScanner::start(RuntimeState* state) {
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
    RETURN_IF_ERROR(SchemaHelper::get_loads(_ss_state, load_params, &_result));
    _cur_idx = 0;
    return Status::OK();
}

Status SchemaLoadsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (; _cur_idx < _result.loads.size(); _cur_idx++) {
        auto& info = _result.loads[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 26) {
                return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
            }
            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.job_id);
                break;
            }
            case 2: {
                // label
                Slice label = Slice(info.label);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&label);
                break;
            }
            case 3: {
                // profile_id
                if (info.__isset.profile_id) {
                    Slice profile_id = Slice(info.profile_id);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&profile_id);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 4: {
                // database
                Slice db = Slice(info.db);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&db);
                break;
            }
            case 5: {
                // table
                Slice table = Slice(info.table);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&table);
                break;
            }
            case 6: {
                // user
                Slice user = Slice(info.user);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&user);
                break;
            }
            case 7: {
                // warehouse
                if (info.__isset.warehouse) {
                    Slice warehouse = Slice(info.warehouse);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&warehouse);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 8: {
                // state
                Slice state = Slice(info.state);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&state);
                break;
            }
            case 9: {
                // progress
                Slice progress = Slice(info.progress);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&progress);
                break;
            }
            case 10: {
                // type
                Slice type = Slice(info.type);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&type);
                break;
            }
            case 11: {
                // priority
                Slice priority = Slice(info.priority);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&priority);
                break;
            }
            case 12: {
                // scan_rows
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_scan_rows);
                break;
            }
            case 13: {
                // scan_bytes
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_scan_bytes);
                break;
            }
            case 14: {
                // filtered_rows
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_filtered_rows);
                break;
            }
            case 15: {
                // unselected_rows
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_unselected_rows);
                break;
            }
            case 16: {
                // sink_rows
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_sink_rows);
                break;
            }
            case 17: {
                // runtime details
                Slice runtime_details = Slice(info.runtime_details);
                JsonValue json_value;
                JsonValue* json_value_ptr = &json_value;
                Status s = JsonValue::parse(runtime_details, &json_value);
                if (!s.ok()) {
                    LOG(WARNING) << "parse runtime details failed. runtime details:" << runtime_details.to_string()
                                 << " error:" << s;
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                } else {
                    fill_column_with_slot<TYPE_JSON>(column.get(), (void*)&json_value_ptr);
                }
                break;
            }
            case 18: {
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
            case 19: {
                // load start time
                DateTimeValue t;
                if (info.__isset.load_start_time) {
                    if (t.from_date_str(info.load_start_time.data(), info.load_start_time.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 20: {
                // load commit time
                DateTimeValue t;
                if (info.__isset.load_commit_time) {
                    if (t.from_date_str(info.load_commit_time.data(), info.load_commit_time.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 21: {
                // load finish time
                DateTimeValue t;
                if (info.__isset.load_finish_time) {
                    if (t.from_date_str(info.load_finish_time.data(), info.load_finish_time.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 22: {
                // properties
                Slice properties = Slice(info.properties);
                JsonValue json_value;
                JsonValue* json_value_ptr = &json_value;
                Status s = JsonValue::parse(properties, &json_value);
                if (!s.ok()) {
                    LOG(WARNING) << "parse properties failed. properties:" << properties.to_string() << " error:" << s;
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                } else {
                    fill_column_with_slot<TYPE_JSON>(column.get(), (void*)&json_value_ptr);
                }
                break;
            }
            case 23: {
                // error_msg
                if (info.__isset.error_msg) {
                    Slice error_msg = Slice(info.error_msg);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&error_msg);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 24: {
                // tracking sql
                if (info.__isset.tracking_sql) {
                    Slice sql = Slice(info.tracking_sql);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&sql);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 25: {
                // rejected record path
                if (info.__isset.rejected_record_path) {
                    Slice path = Slice(info.rejected_record_path);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&path);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 26: {
                // job id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.job_id);
                break;
            }
            default:
                break;
            }
        }
    }
    return Status::OK();
}

Status SchemaLoadsScanner::get_next(ChunkPtr* chunk, bool* eos) {
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
