// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/schema_scanner.h"

#include "column/type_traits.h"
#include "exec/vectorized/schema_scanner/schema_charsets_scanner.h"
#include "exec/vectorized/schema_scanner/schema_collations_scanner.h"
#include "exec/vectorized/schema_scanner/schema_columns_scanner.h"
#include "exec/vectorized/schema_scanner/schema_dummy_scanner.h"
#include "exec/vectorized/schema_scanner/schema_materialized_views_scanner.h"
#include "exec/vectorized/schema_scanner/schema_schema_privileges_scanner.h"
#include "exec/vectorized/schema_scanner/schema_schemata_scanner.h"
#include "exec/vectorized/schema_scanner/schema_table_privileges_scanner.h"
#include "exec/vectorized/schema_scanner/schema_tables_scanner.h"
#include "exec/vectorized/schema_scanner/schema_task_runs_scanner.h"
#include "exec/vectorized/schema_scanner/schema_tasks_scanner.h"
#include "exec/vectorized/schema_scanner/schema_user_privileges_scanner.h"
#include "exec/vectorized/schema_scanner/schema_variables_scanner.h"
#include "exec/vectorized/schema_scanner/schema_views_scanner.h"

namespace starrocks::vectorized {

StarRocksServer* SchemaScanner::_s_starrocks_server;

SchemaScanner::SchemaScanner(ColumnDesc* columns, int column_num)
        : _is_init(false), _param(nullptr), _columns(columns), _column_num(column_num) {}

SchemaScanner::~SchemaScanner() = default;

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

    RETURN_IF_ERROR(_create_slot_descs(pool));

    _param = param;
    _is_init = true;

    return Status::OK();
}

std::unique_ptr<SchemaScanner> SchemaScanner::create(TSchemaTableType::type type) {
    switch (type) {
    case TSchemaTableType::SCH_TABLES:
        return std::make_unique<vectorized::SchemaTablesScanner>();
    case TSchemaTableType::SCH_SCHEMATA:
        return std::make_unique<vectorized::SchemaSchemataScanner>();
    case TSchemaTableType::SCH_COLUMNS:
        return std::make_unique<vectorized::SchemaColumnsScanner>();
    case TSchemaTableType::SCH_CHARSETS:
        return std::make_unique<vectorized::SchemaCharsetsScanner>();
    case TSchemaTableType::SCH_COLLATIONS:
        return std::make_unique<vectorized::SchemaCollationsScanner>();
    case TSchemaTableType::SCH_GLOBAL_VARIABLES:
        return std::make_unique<vectorized::SchemaVariablesScanner>(TVarType::GLOBAL);
    case TSchemaTableType::SCH_SESSION_VARIABLES:
    case TSchemaTableType::SCH_VARIABLES:
        return std::make_unique<vectorized::SchemaVariablesScanner>(TVarType::SESSION);
    case TSchemaTableType::SCH_USER_PRIVILEGES:
        return std::make_unique<vectorized::SchemaUserPrivilegesScanner>();
    case TSchemaTableType::SCH_SCHEMA_PRIVILEGES:
        return std::make_unique<vectorized::SchemaSchemaPrivilegesScanner>();
    case TSchemaTableType::SCH_TABLE_PRIVILEGES:
        return std::make_unique<vectorized::SchemaTablePrivilegesScanner>();
    case TSchemaTableType::SCH_VIEWS:
        return std::make_unique<vectorized::SchemaViewsScanner>();
    case TSchemaTableType::SCH_TASKS:
        return std::make_unique<vectorized::SchemaTasksScanner>();
    case TSchemaTableType::SCH_TASK_RUNS:
        return std::make_unique<vectorized::SchemaTaskRunsScanner>();
    case TSchemaTableType::SCH_MATERIALIZED_VIEWS:
        return std::make_unique<vectorized::SchemaMaterializedViewsScanner>();
    default:
        return std::make_unique<vectorized::SchemaDummyScanner>();
    }
}

Status SchemaScanner::_create_slot_descs(ObjectPool* pool) {
    int null_column = 0;

    for (int i = 0; i < _column_num; ++i) {
        if (_columns[i].is_null) {
            null_column++;
        }
    }

    int offset = (null_column + 7) / 8;
    int null_byte = 0;
    int null_bit = 0;

    for (int i = 0; i < _column_num; ++i) {
        TSlotDescriptor t_slot_desc;
        auto type_desc = TypeDescriptor(_columns[i].type);
        if (_columns[i].type == PrimitiveType::TYPE_VARCHAR || _columns[i].type == PrimitiveType::TYPE_CHAR) {
            type_desc.len = _columns[i].size;
        }

        t_slot_desc.__set_id(i + 1);
        t_slot_desc.__set_slotType(type_desc.to_thrift());
        t_slot_desc.__set_colName(_columns[i].name);
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);

        if (_columns[i].is_null) {
            t_slot_desc.__set_nullIndicatorByte(null_byte);
            t_slot_desc.__set_nullIndicatorBit(null_bit);
            null_bit = (null_bit + 1) % 8;

            if (0 == null_bit) {
                null_byte++;
            }
        } else {
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
        }

        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);

        SlotDescriptor* slot = pool->add(new (std::nothrow) SlotDescriptor(t_slot_desc));

        if (nullptr == slot) {
            return Status::InternalError("no memory for _slot_descs.");
        }

        _slot_descs.push_back(slot);
        offset += _columns[i].size;
    }

    return Status::OK();
}

} // namespace starrocks::vectorized
