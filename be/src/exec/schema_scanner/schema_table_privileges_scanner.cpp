// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/schema_scanner/schema_table_privileges_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace starrocks {

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
        : SchemaScanner(_s_table_privs_columns, sizeof(_s_table_privs_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _table_priv_index(0) {}

SchemaTablePrivilegesScanner::~SchemaTablePrivilegesScanner() {}

Status SchemaTablePrivilegesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    // construct request params for `FrontendService.getTablePrivs()`
    TGetTablePrivsParams table_privs_params;
    table_privs_params.__set_current_user_ident(*(_param->current_user_ident));

    if (NULL != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(
                SchemaHelper::get_table_privs(*(_param->ip), _param->port, table_privs_params, &_table_privs_result));
    } else {
        return Status::InternalError("IP or port dosn't exists");
    }
    return Status::OK();
}

Status SchemaTablePrivilegesScanner::fill_one_row(Tuple* tuple, MemPool* pool) {
    // set all bit to not null
    memset((void*)tuple, 0, _tuple_desc->num_null_bytes());
    const TTablePrivDesc& table_priv_desc = _table_privs_result.table_privs[_table_priv_index];
    // GRANTEE
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[0]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string* src = &table_priv_desc.user_ident_str;
        str_slot->len = src->length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, src->c_str(), str_slot->len);
    }
    // TABLE_CATALOG
    { tuple->set_null(_tuple_desc->slots()[1]->null_indicator_offset()); }
    // TABLE_SCHEMA
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[2]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string* src = &table_priv_desc.db_name;
        str_slot->len = src->length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, src->c_str(), str_slot->len);
    }
    // TABLE_NAME
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[3]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string* src = &table_priv_desc.table_name;
        str_slot->len = src->length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, src->c_str(), str_slot->len);
    }
    // PRIVILEGE_TYPE
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[4]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string* src = &table_priv_desc.priv;
        str_slot->len = src->length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, src->c_str(), str_slot->len);
    }
    // IS_GRANTABLE
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[5]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const char* is_grantable_str = table_priv_desc.is_grantable ? "YES" : "NO";
        int len = strlen(is_grantable_str);
        str_slot->ptr = (char*)pool->allocate(len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, is_grantable_str, len);
        str_slot->len = len;
    }
    _table_priv_index++;
    return Status::OK();
}

Status SchemaTablePrivilegesScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_table_priv_index >= _table_privs_result.table_privs.size()) {
        *eos = true;
        return Status::OK();
    }
    if (NULL == tuple || NULL == pool || NULL == eos) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = false;
    return fill_one_row(tuple, pool);
}

} // namespace starrocks
