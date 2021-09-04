// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/schema_scanner/schema_views_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"
//#include "runtime/datetime_value.h"

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
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _db_index(0),
          _table_index(0) {}

SchemaViewsScanner::~SchemaViewsScanner() {}

Status SchemaViewsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TGetDbsParams db_params;
    if (NULL != _param->db) {
        db_params.__set_pattern(*(_param->db));
    }
    if (NULL != _param->current_user_ident) {
        db_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (NULL != _param->user) {
            db_params.__set_user(*(_param->user));
        }
        if (NULL != _param->user_ip) {
            db_params.__set_user_ip(*(_param->user_ip));
        }
    }

    if (NULL != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_db_names(*(_param->ip), _param->port, db_params, &_db_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    return Status::OK();
}

Status SchemaViewsScanner::fill_one_row(Tuple* tuple, MemPool* pool) {
    // set all bit to not null
    memset((void*)tuple, 0, _tuple_desc->num_null_bytes());
    const TTableStatus& tbl_status = _table_result.tables[_table_index];
    // TABLE_CATALOG
    { tuple->set_null(_tuple_desc->slots()[0]->null_indicator_offset()); }
    // TABLE_SCHEMA
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[1]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        str_slot->ptr = (char*)pool->allocate(db_name.size());
        if (UNLIKELY(str_slot->ptr == nullptr)) {
            return Status::InternalError("Mem usage has exceed the limit of BE");
        }
        str_slot->len = db_name.size();
        memcpy(str_slot->ptr, db_name.c_str(), str_slot->len);
    }
    // TABLE_NAME
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[2]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string* src = &tbl_status.name;
        str_slot->len = src->length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, src->c_str(), str_slot->len);
    }
    // VIEW_DEFINITION
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[3]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string* ddl_sql = &tbl_status.ddl_sql;
        str_slot->len = ddl_sql->length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, ddl_sql->c_str(), str_slot->len);
    }
    // CHECK_OPTION
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[4]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string check_option = "NONE";
        str_slot->len = check_option.length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, check_option.c_str(), str_slot->len);
    }
    // IS_UPDATABLE
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[5]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string is_updatable = "NO";
        str_slot->len = is_updatable.length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, is_updatable.c_str(), str_slot->len);
    }
    // DEFINER
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[6]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        // since we did not record the creater of a certain `view` or `table` , just leave this column empty at this stage.
        const std::string definer = "";
        str_slot->len = definer.length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, definer.c_str(), str_slot->len);
    }
    // SECURITY_TYPE
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[7]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        // since we did not record the creater of a certain `view` or `table` , just leave this column empty at this stage.
        const std::string security_type = "";
        str_slot->len = security_type.length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, security_type.c_str(), str_slot->len);
    }
    // CHARACTER_SET_CLIENT
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[8]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string encoding = "utf8";
        str_slot->len = encoding.length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, encoding.c_str(), str_slot->len);
    }
    // COLLATION_CONNECTION
    {
        void* slot = tuple->get_slot(_tuple_desc->slots()[9]->tuple_offset());
        StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
        const std::string collation = "utf8_general_ci";
        str_slot->len = collation.length();
        str_slot->ptr = (char*)pool->allocate(str_slot->len);
        if (NULL == str_slot->ptr) {
            return Status::InternalError("Allocate memcpy failed.");
        }
        memcpy(str_slot->ptr, collation.c_str(), str_slot->len);
    }
    _table_index++;
    return Status::OK();
}

Status SchemaViewsScanner::get_new_table() {
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index++]);
    if (NULL != _param->wild) {
        table_params.__set_pattern(*(_param->wild));
    }
    if (NULL != _param->current_user_ident) {
        table_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (NULL != _param->user) {
            table_params.__set_user(*(_param->user));
        }
        if (NULL != _param->user_ip) {
            table_params.__set_user_ip(*(_param->user_ip));
        }
    }
    table_params.__set_type(TTableType::VIEW);

    if (NULL != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::list_table_status(*(_param->ip), _param->port, table_params, &_table_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _table_index = 0;
    return Status::OK();
}

Status SchemaViewsScanner::get_next_row(Tuple* tuple, MemPool* pool, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (NULL == tuple || NULL == pool || NULL == eos) {
        return Status::InternalError("input pointer is NULL.");
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
    return fill_one_row(tuple, pool);
}

} // namespace starrocks
