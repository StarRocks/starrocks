// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/schema_scanner.h"

#include "column/type_traits.h"
#include "exec/vectorized/schema_scanner/schema_charsets_scanner.h"
#include "exec/vectorized/schema_scanner/schema_collations_scanner.h"
#include "exec/vectorized/schema_scanner/schema_columns_scanner.h"
#include "exec/vectorized/schema_scanner/schema_dummy_scanner.h"
#include "exec/vectorized/schema_scanner/schema_events_scanner.h"
#include "exec/vectorized/schema_scanner/schema_schema_privileges_scanner.h"
#include "exec/vectorized/schema_scanner/schema_schemata_scanner.h"
#include "exec/vectorized/schema_scanner/schema_statistics_scanner.h"
#include "exec/vectorized/schema_scanner/schema_table_privileges_scanner.h"
#include "exec/vectorized/schema_scanner/schema_tables_scanner.h"
#include "exec/vectorized/schema_scanner/schema_triggers_scanner.h"
#include "exec/vectorized/schema_scanner/schema_user_privileges_scanner.h"
#include "exec/vectorized/schema_scanner/schema_variables_scanner.h"
#include "exec/vectorized/schema_scanner/schema_views_scanner.h"

namespace starrocks::vectorized {

StarRocksServer* SchemaScanner::_s_starrocks_server;

SchemaScanner::SchemaScanner(ColumnDesc* columns, int column_num)
        : _is_init(false), _param(nullptr), _columns(columns), _column_num(column_num) {}

SchemaScanner::~SchemaScanner() {}

Status SchemaScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("call Start before Init.");
    }

    return Status::OK();
}

Status SchemaScanner::get_next(vectorized::ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }

    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    *eos = true;
    return Status::OK();
}

Status SchemaScanner::init(SchemaScannerParam* param, ObjectPool* pool) {
    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == param || nullptr == pool || nullptr == _columns) {
        return Status::InternalError("invalid parameter");
    }

    _param = param;
    _is_init = true;

    return Status::OK();
}

SchemaScanner* SchemaScanner::create(TSchemaTableType::type type) {
    switch (type) {
    case TSchemaTableType::SCH_TABLES:
        return new (std::nothrow) vectorized::SchemaTablesScanner();
    case TSchemaTableType::SCH_SCHEMATA:
        return new (std::nothrow) vectorized::SchemaSchemataScanner();
    case TSchemaTableType::SCH_COLUMNS:
        return new (std::nothrow) vectorized::SchemaColumnsScanner();
    case TSchemaTableType::SCH_CHARSETS:
        return new (std::nothrow) vectorized::SchemaCharsetsScanner();
    case TSchemaTableType::SCH_COLLATIONS:
        return new (std::nothrow) vectorized::SchemaCollationsScanner();
    case TSchemaTableType::SCH_GLOBAL_VARIABLES:
        return new (std::nothrow) vectorized::SchemaVariablesScanner(TVarType::GLOBAL);
    case TSchemaTableType::SCH_SESSION_VARIABLES:
    case TSchemaTableType::SCH_VARIABLES:
        return new (std::nothrow) vectorized::SchemaVariablesScanner(TVarType::SESSION);
    case TSchemaTableType::SCH_USER_PRIVILEGES:
        return new (std::nothrow) vectorized::SchemaUserPrivilegesScanner();
    case TSchemaTableType::SCH_SCHEMA_PRIVILEGES:
        return new (std::nothrow) vectorized::SchemaSchemaPrivilegesScanner();
    case TSchemaTableType::SCH_TABLE_PRIVILEGES:
        return new (std::nothrow) vectorized::SchemaTablePrivilegesScanner();
    case TSchemaTableType::SCH_VIEWS:
        return new (std::nothrow) vectorized::SchemaViewsScanner();
    default:
        return new (std::nothrow) vectorized::SchemaDummyScanner();
        break;
    }
}

} // namespace starrocks::vectorized
