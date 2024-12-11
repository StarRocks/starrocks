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

#include "exec/schema_scanner/sys_fe_memory_usage.h"

#include "exec/schema_scanner/schema_helper.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

<<<<<<< HEAD
SchemaScanner::ColumnDesc SysFeMemoryUsage::_s_columns[] = {{"module_name", TYPE_VARCHAR, sizeof(StringValue), true},
                                                            {"class_name", TYPE_VARCHAR, sizeof(StringValue), true},
                                                            {"current_consumption", TYPE_BIGINT, sizeof(long), true},
                                                            {"peak_consumption", TYPE_BIGINT, sizeof(long), true},
                                                            {"counter_info", TYPE_VARCHAR, sizeof(StringValue), true}};
=======
SchemaScanner::ColumnDesc SysFeMemoryUsage::_s_columns[] = {
        {"module_name", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"class_name", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"current_consumption", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(long), true},
        {"peak_consumption", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(long), true},
        {"counter_info", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true}};
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

SysFeMemoryUsage::SysFeMemoryUsage()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SysFeMemoryUsage::~SysFeMemoryUsage() = default;

Status SysFeMemoryUsage::start(RuntimeState* state) {
    RETURN_IF(!_is_init, Status::InternalError("used before initialized."));
    RETURN_IF(!_param->ip || !_param->port, Status::InternalError("IP or port not exists"));

    RETURN_IF_ERROR(SchemaScanner::start(state));
<<<<<<< HEAD
=======
    // init schema scanner state
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    TAuthInfo auth = build_auth_info();
    TFeMemoryReq request;
    request.__set_auth_info(auth);

<<<<<<< HEAD
    return (SchemaHelper::list_fe_memory_usage(*(_param->ip), _param->port, request, &_result));
=======
    return SchemaHelper::list_fe_memory_usage(_ss_state, request, &_result);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
}

Status SysFeMemoryUsage::_fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    const TFeMemoryItem& info = _result.items[_index];
    DatumArray datum_array{Slice(info.module_name), Slice(info.class_name), info.current_consumption,
                           info.peak_consumption, Slice(info.counter_info)};
    for (const auto& [slot_id, index] : slot_id_map) {
        Column* column = (*chunk)->get_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    _index++;
    return {};
}

Status SysFeMemoryUsage::get_next(ChunkPtr* chunk, bool* eos) {
    RETURN_IF(!_is_init, Status::InternalError("Used before initialized."));
    RETURN_IF((nullptr == chunk || nullptr == eos), Status::InternalError("input pointer is nullptr."));

    if (_index >= _result.items.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return _fill_chunk(chunk);
}

} // namespace starrocks