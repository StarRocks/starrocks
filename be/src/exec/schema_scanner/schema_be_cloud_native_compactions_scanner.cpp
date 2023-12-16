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

#include "exec/schema_scanner/schema_be_cloud_native_compactions_scanner.h"

#include "agent/master_info.h"
#include "exec/schema_scanner/schema_helper.h"
#include "gutil/strings/substitute.h"
#include "runtime/datetime_value.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "storage/compaction_manager.h"
#include "storage/lake/tablet_manager.h"
#include "storage/storage_engine.h"
#include "types/logical_type.h"
#include "util/metrics.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaBeCloudNativeCompactionsScanner::_s_columns[] = {
        {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"TXN_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"TABLET_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"VERSION", TYPE_BIGINT, sizeof(int64_t), false},
        {"SKIPPED", TYPE_BOOLEAN, sizeof(bool), false},
        {"RUNS", TYPE_INT, sizeof(int), false},
        {"START_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"FINISH_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"PROGRESS", TYPE_INT, sizeof(int32_t), false},
        {"STATUS", TYPE_VARCHAR, sizeof(StringValue), false}};

SchemaBeCloudNativeCompactionsScanner::SchemaBeCloudNativeCompactionsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaBeCloudNativeCompactionsScanner::~SchemaBeCloudNativeCompactionsScanner() = default;

Status SchemaBeCloudNativeCompactionsScanner::start(RuntimeState* state) {
    auto o_id = get_backend_id();
    _be_id = o_id.has_value() ? o_id.value() : -1;
    _infos.clear();
    _cur_idx = 0;
    _ctz = state->timezone_obj();
    _chunk_size = state->chunk_size();
    if (UNLIKELY(_chunk_size <= 0)) {
        return Status::InternalError("RuntimeState::chunk_size() cannot be zero or negative");
    }
    auto tablet_manager = ExecEnv::GetInstance()->lake_tablet_manager();
    if (tablet_manager != nullptr) {
        tablet_manager->compaction_scheduler()->list_tasks(&_infos);
    }
    return Status::OK();
}

Status SchemaBeCloudNativeCompactionsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    auto end = _cur_idx + 1;
    for (; _cur_idx < end; _cur_idx++) {
        auto& info = _infos[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 10) {
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
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.txn_id);
                break;
            }
            case 3: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.tablet_id);
                break;
            }
            case 4: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.version);
                break;
            }
            case 5: {
                fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&info.skipped);
                break;
            }
            case 6: {
                fill_column_with_slot<TYPE_INT>(column.get(), (void*)&info.runs);
                break;
            }
            case 7: {
                if (info.start_time > 0) {
                    DateTimeValue ts;
                    ts.from_unixtime(info.start_time, _ctz);
                    fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&ts);
                } else {
                    fill_data_column_with_null(column.get());
                }
                break;
            }
            case 8: {
                if (info.finish_time > 0) {
                    DateTimeValue ts;
                    ts.from_unixtime(info.finish_time, _ctz);
                    fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&ts);
                } else {
                    fill_data_column_with_null(column.get());
                }
                break;
            }
            case 9: {
                fill_column_with_slot<TYPE_INT>(column.get(), (void*)&info.progress);
                break;
            }
            case 10: {
                auto s = info.status.message();
                Slice v(s.data(), s.size());
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

Status SchemaBeCloudNativeCompactionsScanner::get_next(ChunkPtr* chunk, bool* eos) {
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
