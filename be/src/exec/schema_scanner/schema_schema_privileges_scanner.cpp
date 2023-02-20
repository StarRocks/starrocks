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

#include "exec/schema_scanner/schema_schema_privileges_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaSchemaPrivilegesScanner::_s_db_privs_columns[] = {
        //   name,       type,          size
        {"GRANTEE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PRIVILEGE_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"IS_GRANTABLE", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaSchemaPrivilegesScanner::SchemaSchemaPrivilegesScanner()
        : SchemaScanner(_s_db_privs_columns, sizeof(_s_db_privs_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaSchemaPrivilegesScanner::~SchemaSchemaPrivilegesScanner() = default;

Status SchemaSchemaPrivilegesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    // construct request params for `FrontendService.getDBPrivs()`
    TGetDBPrivsParams db_privs_params;
    db_privs_params.__set_current_user_ident(*(_param->current_user_ident));

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_db_privs(*(_param->ip), _param->port, db_privs_params, &_db_privs_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaSchemaPrivilegesScanner::fill_chunk(ChunkPtr* chunk) {
    const TDBPrivDesc& db_priv_desc = _db_privs_result.db_privs[_db_priv_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // GRANTEE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                const std::string* str = &db_priv_desc.user_ident_str;
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
                const std::string* str = &db_priv_desc.db_name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 4: {
            // PRIVILEGE_TYPE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                const std::string* str = &db_priv_desc.priv;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 5: {
            // IS_GRANTABLE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(5);
                const char* str = db_priv_desc.is_grantable ? "YES" : "NO";
                Slice value(str, strlen(str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        default:
            break;
        }
    }
    _db_priv_index++;
    return Status::OK();
}

Status SchemaSchemaPrivilegesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_db_priv_index >= _db_privs_result.db_privs.size()) {
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
