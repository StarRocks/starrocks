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

SchemaScanner::ColumnDesc SysFeMemoryUsage::_s_columns[] = {
        {"module_name", TYPE_VARCHAR, sizeof(StringValue), true},
        {"class_name", TYPE_VARCHAR, sizeof(StringValue), true},
        {"current_consumption", TYPE_VARCHAR, sizeof(StringValue), true},
        {"peak_consumption", TYPE_VARCHAR, sizeof(StringValue), true}};

SysFeMemoryUsage::SysFeMemoryUsage()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SysFeMemoryUsage::~SysFeMemoryUsage() = default;

Status SysFeMemoryUsage::start(RuntimeState* state) {
    RETURN_IF(!_is_init, Status::InternalError("used before initialized."));
    RETURN_IF(!_param->ip || !_param->port, Status::InternalError("IP or port not exists"));

    RETURN_IF_ERROR(SchemaScanner::start(state));

    TAuthInfo auth = build_auth_info();
    TFeMemoryReq request;
    request.__set_auth_info(auth);

    return (SchemaHelper::list_fe_memory_usage(*(_param->ip), _param->port, request, &_result));
}

Status SysFeMemoryUsage::_fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    const TFeMemoryItem& info = _result.items[_index];
    DatumArray datum_array{Slice(info.module_name), Slice(info.class_name), info.current_consumption,
                           info.peak_consumption};
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