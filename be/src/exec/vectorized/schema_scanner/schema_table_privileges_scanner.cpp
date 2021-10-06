// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/schema_scanner/schema_table_privileges_scanner.h"

#include "column/chunk.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaTablePrivilegesScanner::_s_table_privs_columns[] = {
        //   name,       type,          size
        {"GRANTEE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PRIVILEGE_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"IS_GRANTABLE", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaTablePrivilegesScanner::SchemaTablePrivilegesScanner()
        : SchemaScanner(_s_table_privs_columns, sizeof(_s_table_privs_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaTablePrivilegesScanner::~SchemaTablePrivilegesScanner() = default;

Status SchemaTablePrivilegesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    // construct request params for `FrontendService.getTablePrivs()`
    TGetTablePrivsParams table_privs_params;
    table_privs_params.__set_current_user_ident(*(_param->current_user_ident));

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(
                SchemaHelper::get_table_privs(*(_param->ip), _param->port, table_privs_params, &_table_privs_result));
    } else {
        return Status::InternalError("IP or port dosn't exists");
    }
    return Status::OK();
}

Status SchemaTablePrivilegesScanner::fill_chunk(ChunkPtr* chunk) {
    const TTablePrivDesc& table_priv_desc = _table_privs_result.table_privs[_table_priv_index];
    // GRANTEE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[0]->id());
        const std::string* str = &table_priv_desc.user_ident_str;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // TABLE_CATALOG
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[1]->id());
        fill_data_column_with_null(column.get());
    }
    // TABLE_SCHEMA
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[2]->id());
        const std::string* str = &table_priv_desc.db_name;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // TABLE_NAME
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[3]->id());
        const std::string* str = &table_priv_desc.table_name;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // PRIVILEGE_TYPE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[4]->id());
        const std::string* str = &table_priv_desc.priv;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // IS_GRANTABLE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[5]->id());
        const char* str = table_priv_desc.is_grantable ? "YES" : "NO";
        Slice value(str, strlen(str));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    _table_priv_index++;
    return Status::OK();
}

Status SchemaTablePrivilegesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_table_priv_index >= _table_privs_result.table_privs.size()) {
        *eos = true;
        return Status::OK();
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks::vectorized
