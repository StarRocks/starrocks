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

#include "exec/schema_scanner/schema_fe_threads_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaFeThreadsScanner::_s_columns[] = {
        {"FE_ADDRESS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"THREAD_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"THREAD_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"THREAD_STATE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"IS_DAEMON", TypeDescriptor::from_logical_type(TYPE_BOOLEAN), 1, false},
        {"PRIORITY", TypeDescriptor::from_logical_type(TYPE_INT), sizeof(int32_t), false},
        {"CPU_TIME_MS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"USER_TIME_MS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
};

SchemaFeThreadsScanner::SchemaFeThreadsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaFeThreadsScanner::~SchemaFeThreadsScanner() = default;

Status SchemaFeThreadsScanner::_get_fe_threads(RuntimeState* state) {
    TGetFeThreadsRequest request;
    if (nullptr != _param->current_user_ident) {
        request.__set_auth_info(TAuthInfo());
        request.auth_info.__set_current_user_ident(*(_param->current_user_ident));
    }

    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    TGetFeThreadsResponse response;
    RETURN_IF_ERROR(SchemaHelper::get_fe_threads(_ss_state, request, &response));

    if (response.status.status_code != TStatusCode::OK) {
        if (response.status.error_msgs.empty()) {
            return Status::InternalError("Failed to get FE threads");
        } else {
            return Status::InternalError(response.status.error_msgs[0]);
        }
    }

    for (const auto& thread_info : response.threads) {
        auto& info = _infos.emplace_back();
        info.fe_address = thread_info.fe_address;
        info.thread_id = thread_info.thread_id;
        info.thread_name = thread_info.thread_name;
        info.thread_state = thread_info.thread_state;
        info.is_daemon = thread_info.is_daemon;
        info.priority = thread_info.priority;
        info.cpu_time_ms = thread_info.cpu_time_ms;
        info.user_time_ms = thread_info.user_time_ms;
        VLOG(2) << "fe_address: " << info.fe_address << ", thread_id: " << info.thread_id
                << ", thread_name: " << info.thread_name << ", thread_state: " << info.thread_state;
    }

    return Status::OK();
}

Status SchemaFeThreadsScanner::start(RuntimeState* state) {
    _infos.clear();
    _cur_idx = 0;
    RETURN_IF_ERROR(_get_fe_threads(state));
    return SchemaScanner::start(state);
}

Status SchemaFeThreadsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    auto end = _cur_idx + 1;
    for (; _cur_idx < end; _cur_idx++) {
        auto& info = _infos[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 8) {
                return Status::InternalError(strings::Substitute("invalid slot id:$0", slot_id));
            }
            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // fe_address
                Slice v(info.fe_address);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 2: {
                // thread_id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.thread_id);
                break;
            }
            case 3: {
                // thread_name
                Slice v(info.thread_name);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 4: {
                // thread_state
                Slice v(info.thread_state);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 5: {
                // is_daemon
                fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&info.is_daemon);
                break;
            }
            case 6: {
                // priority
                fill_column_with_slot<TYPE_INT>(column.get(), (void*)&info.priority);
                break;
            }
            case 7: {
                // cpu_time_ms
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.cpu_time_ms);
                break;
            }
            case 8: {
                // user_time_ms
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.user_time_ms);
                break;
            }
            default:
                break;
            }
        }
    }
    return Status::OK();
}

Status SchemaFeThreadsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_cur_idx >= _infos.size()) {
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
