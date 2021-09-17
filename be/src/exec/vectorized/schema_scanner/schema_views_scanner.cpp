// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/schema_scanner/schema_views_scanner.h"

#include "column/chunk.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"
//#include "runtime/datetime_value.h"

namespace starrocks::vectorized {

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
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _db_index(0),
          _table_index(0) {}

SchemaViewsScanner::~SchemaViewsScanner() {}

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
    // TABLE_CATALOG
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[0]->id());
        fill_data_column_with_null(column.get());
    }
    // TABLE_SCHEMA
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[1]->id());
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        Slice value(db_name.c_str(), db_name.length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // TABLE_NAME
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[2]->id());
        const std::string* str = &tbl_status.name;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // VIEW_DEFINITION
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[3]->id());
        const std::string* str = &tbl_status.ddl_sql;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // CHECK_OPTION
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[4]->id());
        const char* str = "NONE";
        Slice value(str, strlen(str));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // IS_UPDATABLE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[5]->id());
        const char* str = "NO";
        Slice value(str, strlen(str));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // DEFINER
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[6]->id());
        // since we did not record the creater of a certain `view` or `table` , just leave this column empty at this stage.
        const char* str = "";
        Slice value(str, strlen(str));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // SECURITY_TYPE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[7]->id());
        // since we did not record the creater of a certain `view` or `table` , just leave this column empty at this stage.
        const char* str = "";
        Slice value(str, strlen(str));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // CHARACTER_SET_CLIENT
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[8]->id());
        const char* str = "utf8";
        Slice value(str, strlen(str));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // COLLATION_CONNECTION
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[9]->id());
        const char* str = "utf8_general_ci";
        Slice value(str, strlen(str));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
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

} // namespace starrocks::vectorized
