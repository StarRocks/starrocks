// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/schema_scanner/schema_columns_scanner.h"

#include <sstream>

#include "column/chunk.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "gutil/strings/substitute.h"
#include "runtime/datetime_value.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaColumnsScanner::_s_col_columns[] = {
        //   name,       type,          size,                     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLUMN_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ORDINAL_POSITION", TYPE_BIGINT, sizeof(int64_t), false},
        {"COLUMN_DEFAULT", TYPE_VARCHAR, sizeof(StringValue), true},
        {"IS_NULLABLE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DATA_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CHARACTER_MAXIMUM_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"CHARACTER_OCTET_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"NUMERIC_PRECISION", TYPE_BIGINT, sizeof(int64_t), true},
        {"NUMERIC_SCALE", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATETIME_PRECISION", TYPE_BIGINT, sizeof(int64_t), true},
        {"CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringValue), true},
        {"COLLATION_NAME", TYPE_VARCHAR, sizeof(StringValue), true},
        {"COLUMN_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLUMN_KEY", TYPE_VARCHAR, sizeof(StringValue), false},
        {"EXTRA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PRIVILEGES", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLUMN_COMMENT", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COLUMN_SIZE", TYPE_BIGINT, sizeof(int64_t), true},
        {"DECIMAL_DIGITS", TYPE_BIGINT, sizeof(int64_t), true},
        {"GENERATION_EXPRESSION", TYPE_VARCHAR, sizeof(StringValue), true},
        {"SRS_ID", TYPE_BIGINT, sizeof(int64_t), true},
};

SchemaColumnsScanner::SchemaColumnsScanner()
        : SchemaScanner(_s_col_columns, sizeof(_s_col_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _db_index(0),
          _table_index(0),
          _column_index(0) {}

SchemaColumnsScanner::~SchemaColumnsScanner() {}

Status SchemaColumnsScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("schema columns scanner not inited.");
    }
    // get all database
    TGetDbsParams db_params;
    if (nullptr != _param->db) {
        db_params.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->current_user_ident) {
        db_params.__set_current_user_ident(*_param->current_user_ident);
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

//For compatibility with mysql the result of DATA_TYPE in information_schema.columns
std::string SchemaColumnsScanner::to_mysql_data_type_string(TColumnDesc& desc) {
    switch (desc.columnType) {
    case TPrimitiveType::BOOLEAN:
        return "tinyint";
    case TPrimitiveType::TINYINT:
        return "tinyint";
    case TPrimitiveType::SMALLINT:
        return "smallint";
    case TPrimitiveType::INT:
        return "int";
    case TPrimitiveType::BIGINT:
        return "bigint";
    case TPrimitiveType::LARGEINT:
        return "bigint unsigned";
    case TPrimitiveType::FLOAT:
        return "float";
    case TPrimitiveType::DOUBLE:
        return "double";
    case TPrimitiveType::VARCHAR:
        return "varchar";
    case TPrimitiveType::CHAR:
        return "char";
    case TPrimitiveType::DATE:
        return "date";
    case TPrimitiveType::DATETIME:
        return "datetime";
    case TPrimitiveType::DECIMAL32:
    case TPrimitiveType::DECIMAL64:
    case TPrimitiveType::DECIMAL128:
    case TPrimitiveType::DECIMALV2:
    case TPrimitiveType::DECIMAL: {
        return "decimal";
    }
    default:
        return "unknown";
    }
}

std::string SchemaColumnsScanner::type_to_string(TColumnDesc& desc) {
    switch (desc.columnType) {
    case TPrimitiveType::BOOLEAN:
        return "tinyint(1)";
    case TPrimitiveType::TINYINT:
        return "tinyint(4)";
    case TPrimitiveType::SMALLINT:
        return "smallint(6)";
    case TPrimitiveType::INT:
        return "int(11)";
    case TPrimitiveType::BIGINT:
        return "bigint(20)";
    case TPrimitiveType::LARGEINT:
        return "bigint(20) unsigned";
    case TPrimitiveType::FLOAT:
        return "float";
    case TPrimitiveType::DOUBLE:
        return "double";
    case TPrimitiveType::VARCHAR:
        if (desc.__isset.columnLength) {
            return "varchar(" + std::to_string(desc.columnLength) + ")";
        } else {
            return "varchar(20)";
        }
    case TPrimitiveType::CHAR:
        if (desc.__isset.columnLength) {
            return "char(" + std::to_string(desc.columnLength) + ")";
        } else {
            return "char(20)";
        }
    case TPrimitiveType::DATE:
        return "date";
    case TPrimitiveType::DATETIME:
        return "datetime";
    case TPrimitiveType::DECIMALV2:
    case TPrimitiveType::DECIMAL: {
        std::stringstream stream;
        stream << "decimal(";
        if (desc.__isset.columnPrecision) {
            stream << desc.columnPrecision;
        } else {
            stream << 27;
        }
        stream << ",";
        if (desc.__isset.columnScale) {
            stream << desc.columnScale;
        } else {
            stream << 9;
        }
        stream << ")";
        return stream.str();
    }
    case TPrimitiveType::DECIMAL32:
    case TPrimitiveType::DECIMAL64:
    case TPrimitiveType::DECIMAL128: {
        auto precision = desc.__isset.columnPrecision ? desc.columnPrecision : -1;
        auto scale = desc.__isset.columnScale ? desc.columnScale : -1;
        return strings::Substitute("decimal($0,$1)", precision, scale);
    }
    default:
        return "unknown";
    }
}

Status SchemaColumnsScanner::fill_chunk(ChunkPtr* chunk) {
    // https://dev.mysql.com/doc/refman/5.7/en/information-schema-columns-table.html
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
        std::string* str = &_table_result.tables[_table_index - 1];
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // COLUMN_NAME
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[3]->id());
        std::string* str = &_desc_result.columns[_column_index].columnDesc.columnName;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // ORDINAL_POSITION
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[4]->id());
        int64_t value = _column_index + 1;
        fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&value);
    }
    // COLUMN_DEFAULT
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[5]->id());
        fill_data_column_with_null(column.get());
    }
    // IS_NULLABLE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[6]->id());
        const char* str = "NO";
        Slice value(str, strlen(str));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // DATA_TYPE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[7]->id());
        std::string str = to_mysql_data_type_string(_desc_result.columns[_column_index].columnDesc);
        Slice value(str.c_str(), str.length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // CHARACTER_MAXIMUM_LENGTH
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[8]->id());
        int data_type = _desc_result.columns[_column_index].columnDesc.columnType;
        if (data_type == TPrimitiveType::VARCHAR || data_type == TPrimitiveType::CHAR) {
            if (_desc_result.columns[_column_index].columnDesc.__isset.columnLength) {
                int64_t value = _desc_result.columns[_column_index].columnDesc.columnLength;
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
        } else {
            fill_data_column_with_null(column.get());
        }
    }
    // CHARACTER_OCTET_LENGTH
    // For string columns, the maximum length in bytes.
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[9]->id());
        int data_type = _desc_result.columns[_column_index].columnDesc.columnType;
        if (data_type == TPrimitiveType::VARCHAR || data_type == TPrimitiveType::CHAR) {
            if (_desc_result.columns[_column_index].columnDesc.__isset.columnLength) {
                // currently we save string use UTF-8 so * 3
                int64_t value = _desc_result.columns[_column_index].columnDesc.columnLength * 3;
                fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&value);
            } else {
                fill_data_column_with_null(column.get());
            }
        } else {
            fill_data_column_with_null(column.get());
        }
    }
    // NUMERIC_PRECISION
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[10]->id());
        if (_desc_result.columns[_column_index].columnDesc.__isset.columnPrecision) {
            int64_t value = _desc_result.columns[_column_index].columnDesc.columnPrecision;
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&value);
        } else {
            fill_data_column_with_null(column.get());
        }
    }
    // NUMERIC_SCALE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[11]->id());
        if (_desc_result.columns[_column_index].columnDesc.__isset.columnScale) {
            int64_t value = _desc_result.columns[_column_index].columnDesc.columnScale;
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&value);
        } else {
            fill_data_column_with_null(column.get());
        }
    }
    // DATETIME_PRECISION
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[12]->id());
        fill_data_column_with_null(column.get());
    }
    // CHARACTER_SET_NAME
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[13]->id());
        fill_data_column_with_null(column.get());
    }
    // COLLATION_NAME
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[14]->id());
        fill_data_column_with_null(column.get());
    }
    // COLUMN_TYPE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[15]->id());
        std::string value = type_to_string(_desc_result.columns[_column_index].columnDesc);
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // COLUMN_KEY (UNI, AGG, DUP, PRI)
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[16]->id());
        std::string* str = &_desc_result.columns[_column_index].columnDesc.columnKey;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // EXTRA
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[17]->id());
        Slice value;
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // PRIVILEGES
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[18]->id());
        Slice value;
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // COLUMN_COMMENT
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[19]->id());
        std::string* str = &_desc_result.columns[_column_index].comment;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // COLUMN_SIZE
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[20]->id());
        if (_desc_result.columns[_column_index].columnDesc.__isset.columnLength) {
            int64_t value = _desc_result.columns[_column_index].columnDesc.columnLength;
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&value);
        } else {
            fill_data_column_with_null(column.get());
        }
    }
    // DECIMAL_DIGITS
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[21]->id());
        if (_desc_result.columns[_column_index].columnDesc.__isset.columnScale) {
            int64_t value = _desc_result.columns[_column_index].columnDesc.columnScale;
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&value);
        } else {
            fill_data_column_with_null(column.get());
        }
    }
    // GENERATION_EXPRESSION
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[22]->id());
        fill_data_column_with_null(column.get());
    }
    // SRS_ID
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[23]->id());
        fill_data_column_with_null(column.get());
    }
    _column_index++;
    return Status::OK();
}

Status SchemaColumnsScanner::get_new_desc() {
    TDescribeTableParams desc_params;
    desc_params.__set_db(_db_result.dbs[_db_index - 1]);
    desc_params.__set_table_name(_table_result.tables[_table_index++]);
    if (nullptr != _param->current_user_ident) {
        desc_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            desc_params.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            desc_params.__set_user_ip(*(_param->user_ip));
        }
    }

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::describe_table(*(_param->ip), _param->port, desc_params, &_desc_result));
    } else {
        return Status::InternalError("IP or port dosn't exists");
    }
    _column_index = 0;

    return Status::OK();
}

Status SchemaColumnsScanner::get_new_table() {
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index++]);
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

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_table_names(*(_param->ip), _param->port, table_params, &_table_result));
    } else {
        return Status::InternalError("IP or port dosn't exists");
    }
    _table_index = 0;
    return Status::OK();
}

Status SchemaColumnsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("use this class before inited.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input parameter is nullptr.");
    }
    while (_column_index >= _desc_result.columns.size()) {
        if (_table_index >= _table_result.tables.size()) {
            if (_db_index < _db_result.dbs.size()) {
                RETURN_IF_ERROR(get_new_table());
            } else {
                *eos = true;
                return Status::OK();
            }
        } else {
            RETURN_IF_ERROR(get_new_desc());
        }
    }

    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks::vectorized
