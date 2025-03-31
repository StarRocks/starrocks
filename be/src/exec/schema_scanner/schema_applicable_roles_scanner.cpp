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

#include "exec/schema_scanner/schema_applicable_roles_scanner.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaApplicableRolesScanner::_s_columns[] = {
        //   name,       type,          size,     is_null
        {"USER", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"HOST", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"GRANTEE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"GRANTEE_HOST", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"ROLE_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"ROLE_HOST", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"IS_GRANTABLE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"IS_DEFAULT", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"IS_MANDATORY", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
};

SchemaApplicableRolesScanner::SchemaApplicableRolesScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

Status SchemaApplicableRolesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TAuthInfo auth_info;
    if (nullptr != _param->db) {
        auth_info.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->current_user_ident) {
        auth_info.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            auth_info.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            auth_info.__set_user_ip(*(_param->user_ip));
        }
    }

    // init schema scanner state
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    int64_t table_id_offset = 0;
    while (true) {
        TGetApplicableRolesRequest applicable_roles_req;
        applicable_roles_req.__set_auth_info(auth_info);
        applicable_roles_req.__set_start_table_id_offset(table_id_offset);
        TGetApplicableRolesResponse applicable_roles_response;
        RETURN_IF_ERROR(
                SchemaHelper::get_applicable_roles(_ss_state, applicable_roles_req, &applicable_roles_response));
        _applicable_roles_vec.insert(_applicable_roles_vec.end(), applicable_roles_response.roles.begin(),
                                     applicable_roles_response.roles.end());
        table_id_offset = applicable_roles_response.next_table_id_offset;
        if (!table_id_offset) {
            break;
        }
    }
    _ctz = state->timezone_obj();
    _applicable_roles_index = 0;
    return Status::OK();
}

Status SchemaApplicableRolesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_applicable_roles_index >= _applicable_roles_vec.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return fill_chunk(chunk);
}

Status SchemaApplicableRolesScanner::fill_chunk(ChunkPtr* chunk) {
    const TApplicableRolesInfo& info = _applicable_roles_vec[_applicable_roles_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        if (slot_id < 1 || slot_id > _column_num) {
            return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
        }
        ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);

        switch (slot_id) {
        case 1: {
            // USER
            Slice user = Slice(info.user);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&user);
            break;
        }
        case 2: {
            // HOST
            Slice host = Slice(info.host);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&host);
            break;
        }
        case 3: {
            // GRANTEE
            Slice grantee = Slice(info.grantee);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&grantee);
            break;
        }
        case 4: {
            // GRANTEE_HOST
            Slice grantee_host = Slice(info.grantee_host);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&grantee_host);
            break;
        }
        case 5: {
            // ROLE_NAME
            Slice role_name = Slice(info.role_name);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&role_name);
            break;
        }
        case 6: {
            // ROLE_HOST
            Slice role_host = Slice(info.role_host);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&role_host);
            break;
        }
        case 7: {
            // IS_GRANTABLE
            Slice is_grantable = Slice(info.is_grantable);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&is_grantable);
            break;
        }
        case 8: {
            // IS_DEFAULT
            Slice is_default = Slice(info.is_default);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&is_default);
            break;
        }
        case 9: {
            // IS_MANDATORY
            Slice is_mandatory = Slice(info.is_mandatory);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&is_mandatory);
            break;
        }
        default:
            break;
        }
    }
    _applicable_roles_index++;
    return Status::OK();
}
} // namespace starrocks