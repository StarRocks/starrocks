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

#include "exec/schema_scanner/schema_be_threads_scanner.h"

#include "agent/master_info.h"
#include "exec/schema_scanner/schema_helper.h"
#include "gutil/strings/substitute.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"
#include "util/thread.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaBeThreadsScanner::_s_columns[] = {
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"GROUP", TYPE_VARCHAR, sizeof(StringValue), false},
        {"NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PTHREAD_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"TID", TYPE_BIGINT, sizeof(int64_t), false},
        {"IDLE", TYPE_BOOLEAN, 1, false},
        {"FINISHED_TASKS", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaBeThreadsScanner::SchemaBeThreadsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaBeThreadsScanner::~SchemaBeThreadsScanner() = default;

Status SchemaBeThreadsScanner::start(RuntimeState* state) {
    auto o_id = get_backend_id();
    _be_id = o_id.has_value() ? o_id.value() : -1;
    _infos.clear();
    Thread::get_thread_infos(_infos);
    _cur_idx = 0;
    return Status::OK();
}

Status SchemaBeThreadsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    auto end = _cur_idx + 1;
    for (; _cur_idx < end; _cur_idx++) {
        auto& info = _infos[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 7) {
                return Status::InternalError(strings::Substitute("invalid slot id:$0", slot_id));
            }
            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // be id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&_be_id);
                break;
            }
            case 2: {
                // group
                Slice v(info.group);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 3: {
                // name
                Slice v(info.name);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 4: {
                // pthread_id
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.pthread_id);
                break;
            }
            case 5: {
                // os tid
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.tid);
                break;
            }
            case 6: {
                // idle
                fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&info.idle);
                break;
            }
            case 7: {
                // finished_tasks
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.finished_tasks);
                break;
            }
            default:
                break;
            }
        }
    }
    return Status::OK();
}

Status SchemaBeThreadsScanner::get_next(ChunkPtr* chunk, bool* eos) {
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
