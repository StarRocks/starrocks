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

#include "exec/schema_scanner/schema_be_logs_scanner.h"

#include "agent/master_info.h"
#include "exec/schema_scanner/schema_helper.h"
#include "gutil/strings/substitute.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"
#include "util/thread.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaBeLogsScanner::_s_columns[] = {
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},     {"LEVEL", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TIMESTAMP", TYPE_BIGINT, sizeof(int64_t), false}, {"TID", TYPE_BIGINT, sizeof(int64_t), false},
        {"LOG", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaBeLogsScanner::SchemaBeLogsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaBeLogsScanner::~SchemaBeLogsScanner() = default;

Status SchemaBeLogsScanner::start(RuntimeState* state) {
    auto o_id = get_backend_id();
    _be_id = o_id.has_value() ? o_id.value() : -1;
    _infos.clear();
    int64_t start_ts = 0;
    if (_param->log_start_ts > 0) {
        start_ts = _param->log_start_ts;
    }
    int64_t end_ts = 0;
    if (_param->log_end_ts > 0) {
        end_ts = _param->log_end_ts;
    }
    string level;
    string pattern;
    if (_param->log_level != nullptr) {
        level = *_param->log_level;
    }
    if (_param->log_pattern != nullptr) {
        pattern = *_param->log_pattern;
    }
    size_t limit = 0;
    if (_param->log_limit > 0) {
        limit = _param->log_limit;
    }
    int64_t ts0 = MonotonicMillis();
    Status st = grep_log(start_ts, end_ts, level[0], pattern, limit, _infos);
    int64_t ts1 = MonotonicMillis();
    string msg =
            strings::Substitute("grep_log pattern:$0 level:$1 start_ts:$2 end_ts:$3 limit:$4 #result:$5 duration:$6ms",
                                pattern, level, start_ts, end_ts, limit, _infos.size(), (ts1 - ts0));
    if (st.ok()) {
        VLOG(3) << msg;
    } else {
        LOG(WARNING) << msg << " error:" << st.message();
        // send err info to client as log
        auto& err_log = _infos.emplace_back();
        err_log.log = strings::Substitute("grep_log failed pattern:$0 level:$1 limit:$2 error:$3", pattern, level,
                                          _param->limit, st.message());
    }
    _cur_idx = 0;
    return Status::OK();
}

Status SchemaBeLogsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    auto end = _cur_idx + 1;
    for (; _cur_idx < end; _cur_idx++) {
        auto& info = _infos[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 5) {
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
                // level
                Slice v(&info.level, 1);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 3: {
                // timestamp
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.timestamp);
                break;
            }
            case 4: {
                // tid
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.thread_id);
                break;
            }
            case 5: {
                // log
                Slice v(info.log);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            default:
                break;
            }
        }
    }
    return Status::OK();
}

Status SchemaBeLogsScanner::get_next(ChunkPtr* chunk, bool* eos) {
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
