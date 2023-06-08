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

#include "exec/schema_scanner/starrocks_grants_to_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc StarrocksGrantsToScanner::_s_grants_to_columns[] = {
        //   name,       type,          size
        {"GRANTEE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"OBJECT_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"OBJECT_DATABASE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"OBJECT_NAME", TYPE_VARCHAR, sizeof(StringValue), true},
        {"OBJECT_TYPE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"PRIVILEGE_TYPE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"IS_GRANTABLE", TYPE_VARCHAR, sizeof(StringValue), true},
};

StarrocksGrantsToScanner::StarrocksGrantsToScanner(TGrantsToType::type type)
        : SchemaScanner(_s_grants_to_columns, sizeof(_s_grants_to_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _type(type) {}

StarrocksGrantsToScanner::~StarrocksGrantsToScanner() = default;

Status StarrocksGrantsToScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TGetGrantsToRolesOrUserRequest grants_to_params;
    grants_to_params.__set_type(_type);
    if (nullptr != _param->ip && 0 != _param->port) {
        int32_t timeout = static_cast<int32_t>(std::min(state->query_options().query_timeout * 1000, INT_MAX));
        RETURN_IF_ERROR(SchemaHelper::get_grants_to(*(_param->ip), _param->port, grants_to_params, &_grants_to_result,
                                                    timeout));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status StarrocksGrantsToScanner::fill_chunk(ChunkPtr* chunk) {
    const TGetGrantsToRolesOrUserItem& grants_to_item = _grants_to_result.grants_to[_grants_to_index];

    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // GRANTEE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                const std::string* str = &grants_to_item.grantee;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // OBJECT_CATALOG
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                if (grants_to_item.__isset.object_catalog) {
                    const std::string* str = &grants_to_item.object_catalog;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 3: {
            // OBJECT_DATABASE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                if (grants_to_item.__isset.object_database) {
                    const std::string* str = &grants_to_item.object_database;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 4: {
            // OBJECT_NAME
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                if (grants_to_item.__isset.object_name) {
                    const std::string* str = &grants_to_item.object_name;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 5: {
            // OBJECT_TYPE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(5);
                if (grants_to_item.__isset.object_type) {
                    const std::string* str = &grants_to_item.object_type;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 6: {
            // PRIVILEGE_TYPE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(6);
                if (grants_to_item.__isset.privilege_type) {
                    const std::string* str = &grants_to_item.privilege_type;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    fill_data_column_with_null(column.get());
                }
            }
            break;
        }
        case 7: {
            // IS_GRANTABLEA
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(7);
                const std::string str = grants_to_item.is_grantable ? "YES" : "NO";
                Slice value(str.c_str(), str.length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        default:
            break;
        }
    }
    _grants_to_index++;
    return Status::OK();
}

Status StarrocksGrantsToScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_grants_to_index >= _grants_to_result.grants_to.size()) {
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