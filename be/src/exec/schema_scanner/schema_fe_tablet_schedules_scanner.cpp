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

#include "exec/schema_scanner/schema_fe_tablet_schedules_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "gutil/strings/substitute.h"
#include "runtime/string_value.h"
#include "storage/tablet.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaFeTabletSchedulesScanner::_s_columns[] = {
        {"TABLE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"PARTITION_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"TABLET_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PRIORITY", TYPE_VARCHAR, sizeof(StringValue), false},
        {"STATE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLET_STATUS", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CREATE_TIME", TYPE_DOUBLE, sizeof(double), false},
        {"SCHEDULE_TIME", TYPE_DOUBLE, sizeof(double), false},
        {"FINISH_TIME", TYPE_DOUBLE, sizeof(double), false},
        {"CLONE_SRC", TYPE_BIGINT, sizeof(int64_t), false},
        {"CLONE_DEST", TYPE_BIGINT, sizeof(int64_t), false},
        {"CLONE_BYTES", TYPE_BIGINT, sizeof(int64_t), false},
        {"CLONE_DURATION", TYPE_DOUBLE, sizeof(double), false},
        {"MSG", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaFeTabletSchedulesScanner::SchemaFeTabletSchedulesScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaFeTabletSchedulesScanner::~SchemaFeTabletSchedulesScanner() = default;

Status SchemaFeTabletSchedulesScanner::start(RuntimeState* state) {
    _cur_idx = 0;
    TGetTabletScheduleRequest request;
    TGetTabletScheduleResponse response;
    if (_param->table_id != -1) {
        request.__set_table_id(_param->table_id);
    }
    if (_param->partition_id != -1) {
        request.__set_partition_id(_param->partition_id);
    }
    if (_param->tablet_id != -1) {
        request.__set_tablet_id(_param->tablet_id);
    }
    if (_param->type != nullptr) {
        request.__set_type(*_param->type);
    }
    if (_param->state != nullptr) {
        request.__set_state(*_param->state);
    }
    if (_param->limit > 0) {
        request.__set_limit(_param->limit);
    }
    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_tablet_schedules(*(_param->ip), _param->port, request, &response));
        _infos.swap(response.tablet_schedules);
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaFeTabletSchedulesScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    auto end = _cur_idx + 1;
    for (; _cur_idx < end; _cur_idx++) {
        auto& info = _infos[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 15) {
                return Status::InternalError(strings::Substitute("invalid slot id:$0", slot_id));
            }
            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.table_id);
                break;
            }
            case 2: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.partition_id);
                break;
            }
            case 3: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.tablet_id);
                break;
            }
            case 4: {
                Slice v = Slice(info.type);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 5: {
                Slice v = Slice(info.priority);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 6: {
                Slice v = Slice(info.state);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 7: {
                Slice v = Slice(info.tablet_status);
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&v);
                break;
            }
            case 8: {
                fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&info.create_time);
                break;
            }
            case 9: {
                fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&info.schedule_time);
                break;
            }
            case 10: {
                fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&info.finish_time);
                break;
            }
            case 11: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.clone_src);
                break;
            }
            case 12: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.clone_dest);
                break;
            }
            case 13: {
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.clone_bytes);
                break;
            }
            case 14: {
                fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&info.clone_duration);
                break;
            }
            case 15: {
                Slice v = Slice(info.error_msg);
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

Status SchemaFeTabletSchedulesScanner::get_next(ChunkPtr* chunk, bool* eos) {
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
