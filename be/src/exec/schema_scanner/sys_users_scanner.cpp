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

#include "exec/schema_scanner/sys_users_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SysUsersScanner::_s_columns[] = {
        //   name,       type,          size
        {"HOST", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"USER", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"PASSWORD_EXPIRED", TypeDescriptor::from_logical_type(TYPE_BOOLEAN), 1, false},
        {"PASSWORD_POLICY", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"PASSWORD_LAST_CHANGED", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"IS_LOCKED", TypeDescriptor::from_logical_type(TYPE_BOOLEAN), 1, false}};

SysUsersScanner::SysUsersScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SysUsersScanner::~SysUsersScanner() = default;

Status SysUsersScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    // init schema scanner state
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    TGetUsersRequest users_request;
    RETURN_IF_ERROR(SchemaHelper::get_users(_ss_state, users_request, &_result));
    return Status::OK();
}

Status SysUsersScanner::fill_chunk(ChunkPtr* chunk) {
    const TGetUsersResponseItem& user_item = _result.users[_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // HOST
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                const std::string* str = &user_item.host;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // USER
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                const std::string* str = &user_item.user;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 3: {
            // PASSWORD_EXPIRED
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&user_item.password_expired);
            }
            break;
        }
        case 4: {
            // PASSWORD_POLICY
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                const std::string* str = &user_item.password_policy;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 5: {
            // PASSWORD_LAST_CHANGED
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(5);
                const std::string* str = &user_item.password_last_change;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 6: {
            // IS_LOCKED
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(6);
                fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&user_item.is_locked);
            }
            break;
        }
        default:
            break;
        }
    }
    _index++;
    return Status::OK();
}

Status SysUsersScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_index >= _result.users.size()) {
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