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

#include "exec/schema_scanner/schema_table_privileges_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaTablePrivilegesScanner::_s_table_privs_columns[] = {
        //   name,       type,          size
        {"GRANTEE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PRIVILEGE_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"IS_GRANTABLE", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaTablePrivilegesScanner::SchemaTablePrivilegesScanner()
        : SchemaScanner(_s_table_privs_columns, sizeof(_s_table_privs_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaTablePrivilegesScanner::~SchemaTablePrivilegesScanner() = default;

Status SchemaTablePrivilegesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    // construct request params for `FrontendService.getTablePrivs()`
    TGetTablePrivsParams table_privs_params;
    table_privs_params.__set_current_user_ident(*(_param->current_user_ident));

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(
                SchemaHelper::get_table_privs(*(_param->ip), _param->port, table_privs_params, &_table_privs_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaTablePrivilegesScanner::fill_chunk(ChunkPtr* chunk) {
    const TTablePrivDesc& table_priv_desc = _table_privs_result.table_privs[_table_priv_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // GRANTEE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                const std::string* str = &table_priv_desc.user_ident_str;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // TABLE_CATALOG
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 3: {
            // TABLE_SCHEMA
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                const std::string* str = &table_priv_desc.db_name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 4: {
            // TABLE_NAME
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                const std::string* str = &table_priv_desc.table_name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 5: {
            // PRIVILEGE_TYPE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(5);
                const std::string* str = &table_priv_desc.priv;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 6: {
            // IS_GRANTABLE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(6);
                const char* str = table_priv_desc.is_grantable ? "YES" : "NO";
                Slice value(str, strlen(str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        default:
            break;
        }
    }

    _table_priv_index++;
    return Status::OK();
}

Status SchemaTablePrivilegesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_table_priv_index >= _table_privs_result.table_privs.size()) {
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
