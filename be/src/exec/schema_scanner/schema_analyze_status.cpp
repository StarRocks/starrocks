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

#include "exec/schema_scanner/schema_analyze_status.h"

#include "common/status.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaAnalyzeStatus::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"Id", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"Catalog", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"Database", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"Table", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"Columns", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"Type", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"Schedule", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"Status", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"StartTime", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"EndTime", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"Properties", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"Reason", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
};

SchemaAnalyzeStatus::SchemaAnalyzeStatus()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaAnalyzeStatus::~SchemaAnalyzeStatus() = default;

Status SchemaAnalyzeStatus::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));

    std::string table_catalog;
    std::string table_database;
    std::string table_name;
    TAnalyzeStatusReq req;
    if (_parse_expr_predicate("CATALOG", table_catalog)) {
        req.__set_table_catalog(table_catalog);
    }
    if (_parse_expr_predicate("DATABASE", table_database)) {
        req.__set_table_database(table_database);
    }
    if (_parse_expr_predicate("TABLE", table_name)) {
        req.__set_table_name(table_name);
    }
    if (nullptr != _param->current_user_ident) {
        TAuthInfo auth;
        auth.__set_current_user_ident(*_param->current_user_ident);
        req.__set_auth_info(auth);
    }

    RETURN_IF_ERROR(SchemaHelper::get_analyze_status(_ss_state, req, &_res));
    return Status::OK();
}

DatumArray SchemaAnalyzeStatus::_build_row() {
    auto& item = _res.items.at(_index++);
    return {Slice(item.id),         Slice(item.catalog_name), Slice(item.database_name), Slice(item.table_name),
            Slice(item.columns),    Slice(item.type),         Slice(item.schedule),      Slice(item.status),
            Slice(item.start_time), Slice(item.end_time),     Slice(item.properties),    Slice(item.reason)};
}

Status SchemaAnalyzeStatus::_fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    auto datum_array = _build_row();
    for (const auto& [slot_id, index] : slot_id_map) {
        Column* column = (*chunk)->get_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    return {};
}

Status SchemaAnalyzeStatus::get_next(ChunkPtr* chunk, bool* eos) {
    RETURN_IF(!_is_init, Status::InternalError("Used before initialized."));
    if (_index >= _res.items.size()) {
        *eos = true;
        return Status::OK();
    }
    RETURN_IF(nullptr == chunk || nullptr == eos, Status::InternalError("input pointer is nullptr."));
    *eos = false;
    return _fill_chunk(chunk);
}

} // namespace starrocks