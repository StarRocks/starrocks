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

#include "exec/schema_scanner/schema_views_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaViewsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"VIEW_DEFINITION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CHECK_OPTION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"IS_UPDATABLE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DEFINER", TYPE_VARCHAR, sizeof(StringValue), false},
        {"SECURITY_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CHARACTER_SET_CLIENT", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLLATION_CONNECTION", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaViewsScanner::SchemaViewsScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaViewsScanner::~SchemaViewsScanner() = default;

Status SchemaViewsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TGetDbsParams db_params;
    if (nullptr != _param->db) {
        db_params.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->current_user_ident) {
        db_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            db_params.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            db_params.__set_user_ip(*(_param->user_ip));
        }
    }

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_db_names(*(_param->ip), _param->port, db_params, &_db_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaViewsScanner::fill_chunk(ChunkPtr* chunk) {
    const TTableStatus& tbl_status = _table_result.tables[_table_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // TABLE_CATALOG
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                const char* str = "def";
                Slice value(str, strlen(str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // TABLE_SCHEMA
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
                Slice value(db_name.c_str(), db_name.length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 3: {
            // TABLE_NAME
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                const std::string* str = &tbl_status.name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 4: {
            // VIEW_DEFINITION
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                const std::string* str = &tbl_status.ddl_sql;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 5: {
            // CHECK_OPTION
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(5);
                const char* str = "NONE";
                Slice value(str, strlen(str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 6: {
            // IS_UPDATABLE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(6);
                const char* str = "NO";
                Slice value(str, strlen(str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 7: {
            // DEFINER
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(7);
                // since we did not record the creater of a certain `view` or `table` , just leave this column empty at this stage.
                const char* str = "";
                Slice value(str, strlen(str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 8: {
            // SECURITY_TYPE
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(8);
                // since we did not record the creater of a certain `view` or `table` , just leave this column empty at this stage.
                const char* str = "";
                Slice value(str, strlen(str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 9: {
            // CHARACTER_SET_CLIENT
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(9);
                const char* str = "utf8";
                Slice value(str, strlen(str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 10: {
            // COLLATION_CONNECTION
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(10);
                const char* str = "utf8_general_ci";
                Slice value(str, strlen(str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        default:
            break;
        }
    }
    _table_index++;
    return Status::OK();
}

Status SchemaViewsScanner::get_new_table() {
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index++]);
    if (nullptr != _param->wild) {
        table_params.__set_pattern(*(_param->wild));
    }
    if (nullptr != _param->current_user_ident) {
        table_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            table_params.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            table_params.__set_user_ip(*(_param->user_ip));
        }
    }
    table_params.__set_type(TTableType::VIEW);

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::list_table_status(*(_param->ip), _param->port, table_params, &_table_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _table_index = 0;
    return Status::OK();
}

Status SchemaViewsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    while (_table_index >= _table_result.tables.size()) {
        if (_db_index < _db_result.dbs.size()) {
            RETURN_IF_ERROR(get_new_table());
        } else {
            *eos = true;
            return Status::OK();
        }
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks
