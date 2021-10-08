// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/schema_scanner/schema_schemata_scanner.h"

#include "column/chunk.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaSchemataScanner::_s_columns[] = {
        //   name,       type,          size
        {"CATALOG_NAME", TYPE_VARCHAR, sizeof(StringValue), true},
        {"SCHEMA_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DEFAULT_CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DEFAULT_COLLATION_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"SQL_PATH", TYPE_VARCHAR, sizeof(StringValue), true},
};

SchemaSchemataScanner::SchemaSchemataScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaSchemataScanner::~SchemaSchemataScanner() = default;

Status SchemaSchemataScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initial.");
    }
    TGetDbsParams db_params;
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

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_db_names(*(_param->ip), _param->port, db_params, &_db_result));
    } else {
        return Status::InternalError("IP or port dosn't exists");
    }

    return Status::OK();
}

Status SchemaSchemataScanner::fill_chunk(ChunkPtr* chunk) {
    // catalog
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[0]->id());
        fill_data_column_with_null(column.get());
    }
    // schema
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[1]->id());
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index]);
        Slice value(db_name.c_str(), db_name.length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // DEFAULT_CHARACTER_SET_NAME
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[2]->id());
        const char* str = "utf8";
        Slice value(str, strlen(str));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // DEFAULT_COLLATION_NAME
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[3]->id());
        const char* str = "utf8_general_ci";
        Slice value(str, strlen(str));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // SQL_PATH
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[4]->id());
        fill_data_column_with_null(column.get());
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

} // namespace starrocks::vectorized
