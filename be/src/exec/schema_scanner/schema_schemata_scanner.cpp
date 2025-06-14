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

#include "exec/schema_scanner/schema_schemata_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaSchemataScanner::_s_columns[] = {
        //   name,       type,          size
        {"CATALOG_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"SCHEMA_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"DEFAULT_CHARACTER_SET_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue),
         false},
        {"DEFAULT_COLLATION_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue),
         false},
        {"SQL_PATH", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
};

SchemaSchemataScanner::SchemaSchemataScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaSchemataScanner::~SchemaSchemataScanner() = default;

Status SchemaSchemataScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initial.");
    }
    TGetDbsParams db_params;
    if (nullptr != _param->catalog) {
        db_params.__set_catalog_name(*(_param->catalog));
    }
    if (nullptr != _param->wild) {
        db_params.__set_pattern(*(_param->wild));
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
    // init schema scanner state
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    RETURN_IF_ERROR(SchemaHelper::get_db_names(_ss_state, db_params, &_db_result));
    return SchemaScanner::start(state);
}

Status SchemaSchemataScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // catalog
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                const char* str = "def";
                Slice value(str, strlen(str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // schema
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index]);
                Slice value(db_name.c_str(), db_name.length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 3: {
            // DEFAULT_CHARACTER_SET_NAME
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                const char* str = "utf8";
                Slice value(str, strlen(str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 4: {
            // DEFAULT_COLLATION_NAME
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                const char* str = "utf8_general_ci";
                Slice value(str, strlen(str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 5: {
            // SQL_PATH
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(5);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        default:
            break;
        }
    }
    _db_index++;
    return Status::OK();
}

Status SchemaSchemataScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before Initialized.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_db_index >= _db_result.dbs.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks
