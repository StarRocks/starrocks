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

#include "exec/schema_scanner/starrocks_role_edges_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc StarrocksRoleEdgesScanner::_s_role_edges_columns[] = {
        //   name,       type,          size
        {"FROM_ROLE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TO_ROLE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TO_USER", TYPE_VARCHAR, sizeof(StringValue), true},
};

StarrocksRoleEdgesScanner::StarrocksRoleEdgesScanner()
        : SchemaScanner(_s_role_edges_columns, sizeof(_s_role_edges_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

StarrocksRoleEdgesScanner::~StarrocksRoleEdgesScanner() = default;

Status StarrocksRoleEdgesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TGetRoleEdgesRequest role_edges_params;
    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(
                SchemaHelper::get_role_edges(*(_param->ip), _param->port, role_edges_params, &_role_edges_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status StarrocksRoleEdgesScanner::fill_chunk(ChunkPtr* chunk) {
    const TGetRoleEdgesItem& role_edges_item = _role_edges_result.role_edges[_role_edges_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // FROM_ROLE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                const std::string* str = &role_edges_item.from_role;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // TO_ROLE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                if (role_edges_item.__isset.to_role) {
                    const std::string* str = &role_edges_item.to_role;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 3: {
            // TO_USER
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                if (role_edges_item.__isset.to_user) {
                    const std::string* str = &role_edges_item.to_user;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        default:
            break;
        }
    }
    _role_edges_index++;
    return Status::OK();
}

Status StarrocksRoleEdgesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_role_edges_index >= _role_edges_result.role_edges.size()) {
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