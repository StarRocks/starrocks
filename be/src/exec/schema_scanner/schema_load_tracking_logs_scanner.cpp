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

#include "exec/schema_scanner/schema_load_tracking_logs_scanner.h"

#include <climits>

#include "exec/schema_scanner/schema_helper.h"
#include "gutil/strings/substitute.h"
#include "http/http_client.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaLoadTrackingLogsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"JOB_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"LABEL", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DATABASE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TRACKING_LOG", TYPE_VARCHAR, sizeof(StringValue), true}};

SchemaLoadTrackingLogsScanner::SchemaLoadTrackingLogsScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaLoadTrackingLogsScanner::~SchemaLoadTrackingLogsScanner() = default;

Status SchemaLoadTrackingLogsScanner::start(RuntimeState* state) {
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
        RETURN_IF_ERROR(SchemaHelper::get_loads(*(_param->ip), _param->port, load_params, &_result, timeout));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _start_ts = UnixSeconds();
    _state = state;

    _cur_idx = 0;
    return Status::OK();
}

Status SchemaLoadTrackingLogsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (; _cur_idx < _result.loads.size(); _cur_idx++) {
        if ((UnixSeconds() - _start_ts) > _state->query_options().query_timeout) {
            return Status::InternalError(fmt::format("fill_chunk timeout $0s", _state->query_options().query_timeout));
        }
        auto& info = _result.loads[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 4) {
                return Status::InternalError(strings::Substitute("invalid slot id: $0", slot_id));
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
                // tracking message
                if (info.__isset.url) {
                    auto url = info.url;
                    std::string tracking_msg;
                    auto timeout = _state->query_options().query_timeout * 1000 / 2;

                    auto tracking_msg_cb = [&url, &tracking_msg, &timeout](HttpClient* client) {
                        RETURN_IF_ERROR(client->init(url));
                        client->set_timeout_ms(timeout);
                        RETURN_IF_ERROR(client->execute(&tracking_msg));
                        return Status::OK();
                    };
                    auto st = HttpClient::execute_with_retry(2 /* retry times */, 1 /* sleep interval */,
                                                             tracking_msg_cb);
                    if (!st.ok()) {
                        tracking_msg = "Failed to access " + info.url + " err: " + st.to_string();
                    }
                    Slice msg = Slice(tracking_msg);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&msg);
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

Status SchemaLoadTrackingLogsScanner::get_next(ChunkPtr* chunk, bool* eos) {
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
