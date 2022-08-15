// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/vectorized/schema_scanner/schema_tables_scanner.h"

#include "column/nullable_column.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaTablesScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ENGINE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"VERSION", TYPE_BIGINT, sizeof(int64_t), true},
        {"ROW_FORMAT", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"AVG_ROW_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"MAX_DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"INDEX_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_FREE", TYPE_BIGINT, sizeof(int64_t), true},
        {"AUTO_INCREMENT", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"UPDATE_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"CHECK_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"TABLE_COLLATION", TYPE_VARCHAR, sizeof(StringValue), true},
        {"CHECKSUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_OPTIONS", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_COMMENT", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaTablesScanner::SchemaTablesScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaTablesScanner::~SchemaTablesScanner() = default;

Status SchemaTablesScanner::start(RuntimeState* state) {
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

Status SchemaTablesScanner::fill_chunk(ChunkPtr* chunk) {
    const TTableStatus& tbl_status = _table_result.tables[_table_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // catalog
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 2: {
            // schema
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
                Slice value(db_name.c_str(), db_name.length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 3: {
            // name
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                const std::string* str = &tbl_status.name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 4: {
            // type
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                const std::string* str = &tbl_status.type;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 5: {
            // engine
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(5);
                if (tbl_status.__isset.engine) {
                    const std::string* str = &tbl_status.engine;
                    Slice value(str->c_str(), str->length());
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
                } else {
                    NullableColumn* nullable_column = down_cast<NullableColumn*>(column.get());
                    nullable_column->append_nulls(1);
                }
            }
            break;
        }
        case 6: {
            // version
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(6);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 7: {
            // row_format
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(7);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 8: {
            // rows
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(8);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 9: {
            // avg_row_length
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(9);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 10: {
            // data_length
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(10);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 11: {
            // max_data_length
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(11);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 12: {
            // index_length
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(12);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 13: {
            // data_free
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(13);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 14: {
            // auto_increment
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(14);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 15: {
            // creation_time
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(15);
                NullableColumn* nullable_column = down_cast<NullableColumn*>(column.get());
                if (tbl_status.__isset.create_time) {
                    int64_t create_time = tbl_status.create_time;
                    if (create_time <= 0) {
                        nullable_column->append_nulls(1);
                    } else {
                        DateTimeValue t;
                        t.from_unixtime(create_time, TimezoneUtils::default_time_zone);
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                    }
                } else {
                    nullable_column->append_nulls(1);
                }
            }
            break;
        }
        case 16: {
            // update_time
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(16);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 17: {
            // check_time
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(17);
                NullableColumn* nullable_column = down_cast<NullableColumn*>(column.get());
                if (tbl_status.__isset.last_check_time) {
                    int64_t check_time = tbl_status.last_check_time;
                    if (check_time <= 0) {
                        nullable_column->append_nulls(1);
                    } else {
                        DateTimeValue t;
                        t.from_unixtime(check_time, TimezoneUtils::default_time_zone);
                        fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
                    }
                } else {
                    nullable_column->append_nulls(1);
                }
            }
            break;
        }
        case 18: {
            // collation
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(18);
                const char* collation_str = "utf8_general_ci";
                Slice value(collation_str, strlen(collation_str));
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 19: {
            // checksum
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(19);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 20: {
            // create_options
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(20);
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 21: {
            // create_comment
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(21);
                const std::string* str = &tbl_status.comment;
                Slice value(str->c_str(), str->length());
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

Status SchemaTablesScanner::get_new_table() {
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index++]);
    if (nullptr != _param->wild) {
        table_params.__set_pattern(*(_param->wild));
    }
    if (nullptr != _param->table) {
        table_params.__set_pattern(*(_param->table));
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
    if (_param->limit > 0) {
        table_params.__set_limit(_param->limit);
    }

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::list_table_status(*(_param->ip), _param->port, table_params, &_table_result));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _table_index = 0;
    return Status::OK();
}

Status SchemaTablesScanner::get_next(ChunkPtr* chunk, bool* eos) {
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
