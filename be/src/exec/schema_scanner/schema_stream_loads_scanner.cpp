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

#include "exec/schema_scanner/schema_stream_loads_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "http/http_client.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaStreamLoadsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"LABEL", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"LOAD_ID", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TXN_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"DB_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"STATE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ERROR_MSG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TRACKING_URL", TYPE_VARCHAR, sizeof(StringValue), true},
        {"CHANNEL_NUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"PREPARED_CHANNEL_NUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"NUM_ROWS_NORMAL", TYPE_BIGINT, sizeof(int64_t), true},
        {"NUM_ROWS_AB_NORMAL", TYPE_BIGINT, sizeof(int64_t), true},
        {"NUM_ROWS_UNSELECTED", TYPE_BIGINT, sizeof(int64_t), true},
        {"NUM_LOAD_BYTES", TYPE_BIGINT, sizeof(int64_t), true},
        {"TIMEOUT_SECOND", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_TIME_MS", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"BEFORE_LOAD_TIME_MS", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"START_LOADING_TIME_MS", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"START_PREPARING_TIME_MS", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"FINISH_PREPARING_TIME_MS", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"END_TIME_MS", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"CHANNEL_STATE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TRACKING_SQL", TYPE_VARCHAR, sizeof(StringValue), true}};

SchemaStreamLoadsScanner::SchemaStreamLoadsScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaStreamLoadsScanner::~SchemaStreamLoadsScanner() = default;

Status SchemaStreamLoadsScanner::start(RuntimeState* state) {
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
    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_stream_loads(*(_param->ip), _param->port, load_params, &_result, timeout));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }

    _cur_idx = 0;
    return Status::OK();
}

Status SchemaStreamLoadsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (; _cur_idx < _result.loads.size(); _cur_idx++) {
        auto& info = _result.loads[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 25) {
                return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
            }
            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // LABEL
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&info.label);
                break;
            }
            case 2: {
                // ID
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.id);
                break;
            }
            case 3: {
                // LOAD_ID
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&info.load_id);
                break;
            }
            case 4: {
                // TXN_ID
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.txn_id);
                break;
            }
            case 5: {
                // DB_NAME
                Slice db_name = Slice(info.db_name);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&db_name);
                break;
            }
            case 6: {
                // TABLE_NAME
                Slice table_name = Slice(info.table_name);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&table_name);
                break;
            }
            case 7: {
                // STATE
                Slice state = Slice(info.state);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&state);
                break;
            }
            case 8: {
                // ERROR_MSG
                if (info.__isset.error_msg) {
                    Slice error_msg = Slice(info.error_msg);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&error_msg);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 9: {
                // TRACKING_SQL
                if (info.__isset.tracking_url) {
                    Slice tracking_url = Slice(info.tracking_url);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&tracking_url);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 10: {
                // CHANNEL_NUM
                if (info.__isset.channel_num) {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.channel_num);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 11: {
                // PREPARED_CHANNEL_NUM
                if (info.__isset.prepared_channel_num) {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.prepared_channel_num);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 12: {
                // NUM_ROWS_NORMAL
                if (info.__isset.num_rows_normal) {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_rows_normal);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 13: {
                // NUM_ROWS_AB_NORMAL
                if (info.__isset.num_rows_ab_normal) {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_rows_ab_normal);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 14: {
                // NUM_ROWS_UNSELECTED
                if (info.__isset.num_rows_unselected) {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_rows_unselected);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 15: {
                // NUM_LOAD_BYTES
                if (info.__isset.num_load_bytes) {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.num_load_bytes);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 16: {
                // TIMEOUT_SECOND
                if (info.__isset.timeout_second) {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.timeout_second);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 17: {
                // CREATE_TIME_MS
                DateTimeValue t;
                if (info.__isset.create_time_ms) {
                    if (t.from_date_str(info.create_time_ms.data(), info.create_time_ms.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 18: {
                // BEFORE_LOAD_TIME_MS
                DateTimeValue t;
                if (info.__isset.before_load_time_ms) {
                    if (t.from_date_str(info.before_load_time_ms.data(), info.before_load_time_ms.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 19: {
                // START_LOADING_TIME_MS
                DateTimeValue t;
                if (info.__isset.start_loading_time_ms) {
                    if (t.from_date_str(info.start_loading_time_ms.data(), info.start_loading_time_ms.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 20: {
                // START_PREPARING_TIME_MS
                DateTimeValue t;
                if (info.__isset.start_preparing_time_ms) {
                    if (t.from_date_str(info.start_preparing_time_ms.data(), info.start_preparing_time_ms.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 21: {
                // FINISH_PREPARING_TIME_MS
                DateTimeValue t;
                if (info.__isset.finish_preparing_time_ms) {
                    if (t.from_date_str(info.finish_preparing_time_ms.data(), info.finish_preparing_time_ms.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 22: {
                // END_TIME_MS
                DateTimeValue t;
                if (info.__isset.end_time_ms) {
                    if (t.from_date_str(info.end_time_ms.data(), info.end_time_ms.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column.get())->append_nulls(1);
                break;
            }
            case 23: {
                // CHANNEL_STATE
                if (info.__isset.channel_state) {
                    Slice channel_state = Slice(info.channel_state);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&channel_state);
                } else {
                    down_cast<NullableColumn*>(column.get())->append_nulls(1);
                }
                break;
            }
            case 24: {
                // TYPE
                Slice type = Slice(info.type);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&type);
                break;
            }
            case 25: {
                // TRACKING_SQL
                if (info.__isset.tracking_sql) {
                    Slice tracking_sql = Slice(info.tracking_sql);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&tracking_sql);
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

Status SchemaStreamLoadsScanner::get_next(ChunkPtr* chunk, bool* eos) {
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
