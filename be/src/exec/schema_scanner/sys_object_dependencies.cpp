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

#include "exec/schema_scanner/sys_object_dependencies.h"

#include "exec/schema_scanner/schema_helper.h"
#include "exec/schema_scanner/starrocks_grants_to_scanner.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SysObjectDependencies::_s_columns[] = {
        {"OBJECT_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"OBJECT_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"OBJECT_DATABASE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"OBJECT_CATALOG", TYPE_VARCHAR, sizeof(StringValue), false},
        {"OBJECT_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},

        {"REF_OBJECT_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"REF_OBJECT_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"REF_OBJECT_DATABASE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"REF_OBJECT_CATALOG", TYPE_VARCHAR, sizeof(StringValue), false},
        {"REF_OBJECT_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
};

SysObjectDependencies::SysObjectDependencies()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SysObjectDependencies::~SysObjectDependencies() = default;

Status SysObjectDependencies::start(RuntimeState* state) {
    RETURN_IF(!_is_init, Status::InternalError("used before initialized."));
    RETURN_IF(!_param->ip || !_param->port, Status::InternalError("IP or port not exists"));

    TAuthInfo auth = build_auth_info();
    TObjectDependencyReq request;
    request.__set_auth_info(auth);

    return (SchemaHelper::list_object_dependencies(*(_param->ip), _param->port, request, &_result));
}

Status SysObjectDependencies::_fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    const TObjectDependencyItem& info = _result.items[_index];
    DatumArray datum_array{
            (info.object_id),        Slice(info.object_name),     Slice(info.database),
            Slice(info.catalog),     Slice(info.object_type),

            (info.ref_object_id),    Slice(info.ref_object_name), Slice(info.ref_database),
            Slice(info.ref_catalog), Slice(info.ref_object_type),
    };
    for (const auto& [slot_id, index] : slot_id_map) {
        Column* column = (*chunk)->get_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    _index++;
    return {};
}

Status SysObjectDependencies::get_next(ChunkPtr* chunk, bool* eos) {
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
