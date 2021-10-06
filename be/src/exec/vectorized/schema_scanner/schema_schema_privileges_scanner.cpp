// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/schema_scanner/schema_schema_privileges_scanner.h"

#include "column/chunk.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaSchemaPrivilegesScanner::_s_db_privs_columns[] = {
        //   name,       type,          size
        {"GRANTEE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PRIVILEGE_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"IS_GRANTABLE", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaSchemaPrivilegesScanner::SchemaSchemaPrivilegesScanner()
        : SchemaScanner(_s_db_privs_columns, sizeof(_s_db_privs_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaSchemaPrivilegesScanner::~SchemaSchemaPrivilegesScanner() = default;

Status SchemaSchemaPrivilegesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    // construct request params for `FrontendService.getDBPrivs()`
    TGetDBPrivsParams db_privs_params;
    db_privs_params.__set_current_user_ident(*(_param->current_user_ident));

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_db_privs(*(_param->ip), _param->port, db_privs_params, &_db_privs_result));
    } else {
        return Status::InternalError("IP or port dosn't exists");
    }
    return Status::OK();
}

Status SchemaSchemaPrivilegesScanner::fill_chunk(ChunkPtr* chunk) {
    const TDBPrivDesc& db_priv_desc = _db_privs_result.db_privs[_db_priv_index];
    // GRANTEE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[0]->id());
        const std::string* str = &db_priv_desc.user_ident_str;
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
        const std::string* str = &db_priv_desc.db_name;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // PRIVILEGE_TYPE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[3]->id());
        const std::string* str = &db_priv_desc.priv;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // IS_GRANTABLE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[4]->id());
        const char* str = db_priv_desc.is_grantable ? "YES" : "NO";
        Slice value(str, strlen(str));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    _db_priv_index++;
    return Status::OK();
}

Status SchemaSchemaPrivilegesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_db_priv_index >= _db_privs_result.db_privs.size()) {
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
