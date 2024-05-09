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
        {"JOB_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"LABEL", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DATABASE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"STATE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PROGRESS", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PRIORITY", TYPE_VARCHAR, sizeof(StringValue), false},
        {"SCAN_ROWS", TYPE_BIGINT, sizeof(int64_t), false},
        {"FILTERED_ROWS", TYPE_BIGINT, sizeof(int64_t), false},
        {"UNSELECTED_ROWS", TYPE_BIGINT, sizeof(int64_t), false},
        {"SINK_ROWS", TYPE_BIGINT, sizeof(int64_t), false},
        {"ETL_INFO", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TASK_INFO", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CREATE_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"ETL_START_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"ETL_FINISH_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"LOAD_START_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"LOAD_FINISH_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"JOB_DETAILS", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ERROR_MSG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TRACKING_URL", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TRACKING_SQL", TYPE_VARCHAR, sizeof(StringValue), true},
        {"REJECTED_RECORD_PATH", TYPE_VARCHAR, sizeof(StringValue), true}};

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

    int32_t timeout = static_cast<int32_t>(std::min(state->query_options().query_timeout * 1000 / 2, INT_MAX));
    RETURN_IF_ERROR(SchemaHelper::get_loads(load_params, &_result, timeout));

    _cur_idx = 0;
    return Status::OK();
}

Status SchemaLoadsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (; _cur_idx < _result.loads.size(); _cur_idx++) {
        auto& info = _result.loads[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 23) {
                return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
            }
            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // job id
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
                // database
                Slice db = Slice(info.db);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&db);
                break;
            }
            case 4: {
                // state
                Slice state = Slice(info.state);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&state);
                break;
            }
            case 5: {
                // progress
                Slice progress = Slice(info.progress);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&progress);
                break;
            }
            case 6: {
                // type
                Slice type = Slice(info.type);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&type);
                break;
            }
            case 7: {
                // priority
                Slice priority = Slice(info.priority);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&priority);
                break;
            }
            case 8: {
                // scan_rows
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_scan_rows);
                break;
            }
            case 9: {
                // filtered_rows
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_filtered_rows);
                break;
            }
            case 10: {
                // unselected_rows
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_unselected_rows);
                break;
            }
            case 11: {
                // sink_rows
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_sink_rows);
                break;
            }
            case 12: {
                // etl_info
                if (info.__isset.etl_info) {
                    Slice etl_info = Slice(info.etl_info);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&etl_info);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 13: {
                // task info
                Slice task_info = Slice(info.task_info);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&task_info);
                break;
            }
            case 14: {
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
            case 15: {
                // etl start time
                DateTimeValue t;
                if (info.__isset.etl_start_time) {
                    if (t.from_date_str(info.etl_start_time.data(), info.etl_start_time.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 16: {
                // etl finish time
                DateTimeValue t;
                if (info.__isset.etl_finish_time) {
                    if (t.from_date_str(info.etl_finish_time.data(), info.etl_finish_time.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 17: {
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
            case 18: {
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
            case 19: {
                // job details
                Slice job_details = Slice(info.job_details);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&job_details);
                break;
            }
            case 20: {
                // error_msg
                if (info.__isset.error_msg) {
                    Slice error_msg = Slice(info.error_msg);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&error_msg);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 21: {
                // url
                if (info.__isset.url) {
                    Slice url = Slice(info.url);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&url);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 22: {
                // tracking sql
                if (info.__isset.tracking_sql) {
                    Slice sql = Slice(info.tracking_sql);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&sql);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 23: {
                // rejected record path
                if (info.__isset.rejected_record_path) {
                    Slice path = Slice(info.rejected_record_path);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&path);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
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
