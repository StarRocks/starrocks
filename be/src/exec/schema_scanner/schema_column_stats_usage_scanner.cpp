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

#include "exec/schema_scanner/schema_column_stats_usage_scanner.h"

#include "common/status.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaColumnStatsUsageScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"TABLE_CATALOG", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"TABLE_DATABASE", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"TABLE_NAME", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"COLUMN_NAME", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"USAGE", TypeDescriptor::create_varchar_type(1024), sizeof(StringValue), false},
        {"LAST_USED", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(StringValue), false},
        {"CREATED", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(StringValue), false},
};

SchemaColumnStatsUsageScanner::SchemaColumnStatsUsageScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaColumnStatsUsageScanner::~SchemaColumnStatsUsageScanner() = default;

Status SchemaColumnStatsUsageScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));

    std::string table_catalog;
    std::string table_database;
    std::string table_name;
    TColumnStatsUsageReq req;
    if (_parse_expr_predicate("TABLE_CATALOG", table_catalog)) {
        req.__set_table_catalog(table_catalog);
    }
    if (_parse_expr_predicate("TABLE_DATABASE", table_database)) {
        req.__set_table_database(table_database);
    }
    if (_parse_expr_predicate("TABLE_NAME", table_name)) {
        req.__set_table_name(table_name);
    }
    if (nullptr != _param->current_user_ident) {
        TAuthInfo auth;
        auth.__set_current_user_ident(*_param->current_user_ident);
        req.__set_auth_info(auth);
    }

    RETURN_IF_ERROR(SchemaHelper::get_column_stats_usage(_ss_state, req, &_column_stats));
    return Status::OK();
}

DatumArray SchemaColumnStatsUsageScanner::_build_row() {
    auto& usage = _column_stats.items.at(_index++);
    Datum last_used = usage.__isset.last_used && usage.last_used > 0
                              ? TimestampValue::create_from_unixtime(usage.last_used, _runtime_state->timezone_obj())
                              : kNullDatum;
    Datum created = usage.__isset.created && usage.created > 0
                            ? TimestampValue::create_from_unixtime(usage.created, _runtime_state->timezone_obj())
                            : kNullDatum;

    return {Slice(usage.table_catalog),
            Slice(usage.table_database),
            Slice(usage.table_name),
            Slice(usage.column_name),
            Slice(usage.usage),
            last_used,
            created};
}

Status SchemaColumnStatsUsageScanner::_fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    auto datum_array = _build_row();
    for (const auto& [slot_id, index] : slot_id_map) {
        Column* column = (*chunk)->get_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    return {};
}

Status SchemaColumnStatsUsageScanner::get_next(ChunkPtr* chunk, bool* eos) {
    RETURN_IF(!_is_init, Status::InternalError("Used before initialized."));
    if (_index >= _column_stats.items.size()) {
        *eos = true;
        return Status::OK();
    }
    RETURN_IF(nullptr == chunk || nullptr == eos, Status::InternalError("input pointer is nullptr."));
    *eos = false;
    return _fill_chunk(chunk);
}

} // namespace starrocks