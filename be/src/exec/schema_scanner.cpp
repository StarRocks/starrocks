// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/schema_scanner.h"

#include "column/type_traits.h"
#include "exec/schema_scanner/schema_be_bvars_scanner.h"
#include "exec/schema_scanner/schema_be_cloud_native_compactions_scanner.h"
#include "exec/schema_scanner/schema_be_compactions_scanner.h"
#include "exec/schema_scanner/schema_be_configs_scanner.h"
#include "exec/schema_scanner/schema_be_logs_scanner.h"
#include "exec/schema_scanner/schema_be_metrics_scanner.h"
#include "exec/schema_scanner/schema_be_tablets_scanner.h"
#include "exec/schema_scanner/schema_be_threads_scanner.h"
#include "exec/schema_scanner/schema_be_txns_scanner.h"
#include "exec/schema_scanner/schema_charsets_scanner.h"
#include "exec/schema_scanner/schema_collations_scanner.h"
#include "exec/schema_scanner/schema_columns_scanner.h"
#include "exec/schema_scanner/schema_dummy_scanner.h"
#include "exec/schema_scanner/schema_fe_tablet_schedules_scanner.h"
#include "exec/schema_scanner/schema_load_tracking_logs_scanner.h"
#include "exec/schema_scanner/schema_loads_scanner.h"
#include "exec/schema_scanner/schema_materialized_views_scanner.h"
#include "exec/schema_scanner/schema_pipe_files.h"
#include "exec/schema_scanner/schema_pipes.h"
#include "exec/schema_scanner/schema_routine_load_jobs_scanner.h"
#include "exec/schema_scanner/schema_schema_privileges_scanner.h"
#include "exec/schema_scanner/schema_schemata_scanner.h"
#include "exec/schema_scanner/schema_stream_loads_scanner.h"
#include "exec/schema_scanner/schema_table_privileges_scanner.h"
#include "exec/schema_scanner/schema_tables_config_scanner.h"
#include "exec/schema_scanner/schema_tables_scanner.h"
#include "exec/schema_scanner/schema_task_runs_scanner.h"
#include "exec/schema_scanner/schema_tasks_scanner.h"
#include "exec/schema_scanner/schema_user_privileges_scanner.h"
#include "exec/schema_scanner/schema_variables_scanner.h"
#include "exec/schema_scanner/schema_views_scanner.h"
#include "exec/schema_scanner/starrocks_grants_to_scanner.h"
#include "exec/schema_scanner/starrocks_role_edges_scanner.h"
namespace starrocks {

StarRocksServer* SchemaScanner::_s_starrocks_server;

SchemaScanner::SchemaScanner(ColumnDesc* columns, int column_num)
        : _is_init(false), _param(nullptr), _columns(columns), _column_num(column_num) {}

SchemaScanner::~SchemaScanner() = default;

Status SchemaScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("call Start before Init.");
    }
    _runtime_state = state;

    return Status::OK();
}

Status SchemaScanner::get_next(ChunkPtr* chunk, bool* eos) {
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
        return std::make_unique<SchemaTablesScanner>();
    case TSchemaTableType::SCH_SCHEMATA:
        return std::make_unique<SchemaSchemataScanner>();
    case TSchemaTableType::SCH_COLUMNS:
        return std::make_unique<SchemaColumnsScanner>();
    case TSchemaTableType::SCH_CHARSETS:
        return std::make_unique<SchemaCharsetsScanner>();
    case TSchemaTableType::SCH_COLLATIONS:
        return std::make_unique<SchemaCollationsScanner>();
    case TSchemaTableType::SCH_GLOBAL_VARIABLES:
        return std::make_unique<SchemaVariablesScanner>(TVarType::GLOBAL);
    case TSchemaTableType::SCH_SESSION_VARIABLES:
    case TSchemaTableType::SCH_VARIABLES:
        return std::make_unique<SchemaVariablesScanner>(TVarType::SESSION);
    case TSchemaTableType::SCH_USER_PRIVILEGES:
        return std::make_unique<SchemaUserPrivilegesScanner>();
    case TSchemaTableType::SCH_SCHEMA_PRIVILEGES:
        return std::make_unique<SchemaSchemaPrivilegesScanner>();
    case TSchemaTableType::SCH_TABLE_PRIVILEGES:
        return std::make_unique<SchemaTablePrivilegesScanner>();
    case TSchemaTableType::SCH_VIEWS:
        return std::make_unique<SchemaViewsScanner>();
    case TSchemaTableType::SCH_TASKS:
        return std::make_unique<SchemaTasksScanner>();
    case TSchemaTableType::SCH_TASK_RUNS:
        return std::make_unique<SchemaTaskRunsScanner>();
    case TSchemaTableType::SCH_MATERIALIZED_VIEWS:
        return std::make_unique<SchemaMaterializedViewsScanner>();
    case TSchemaTableType::SCH_LOADS:
        return std::make_unique<SchemaLoadsScanner>();
    case TSchemaTableType::SCH_LOAD_TRACKING_LOGS:
        return std::make_unique<SchemaLoadTrackingLogsScanner>();
    case TSchemaTableType::SCH_TABLES_CONFIG:
        return std::make_unique<SchemaTablesConfigScanner>();
    case TSchemaTableType::SCH_VERBOSE_SESSION_VARIABLES:
        return std::make_unique<SchemaVariablesScanner>(TVarType::VERBOSE);
    case TSchemaTableType::SCH_BE_TABLETS:
        return std::make_unique<SchemaBeTabletsScanner>();
    case TSchemaTableType::SCH_BE_METRICS:
        return std::make_unique<SchemaBeMetricsScanner>();
    case TSchemaTableType::SCH_BE_TXNS:
        return std::make_unique<SchemaBeTxnsScanner>();
    case TSchemaTableType::SCH_BE_CONFIGS:
        return std::make_unique<SchemaBeConfigsScanner>();
    case TSchemaTableType::SCH_BE_THREADS:
        return std::make_unique<SchemaBeThreadsScanner>();
    case TSchemaTableType::SCH_BE_LOGS:
        return std::make_unique<SchemaBeLogsScanner>();
    case TSchemaTableType::SCH_FE_TABLET_SCHEDULES:
        return std::make_unique<SchemaFeTabletSchedulesScanner>();
    case TSchemaTableType::SCH_BE_COMPACTIONS:
        return std::make_unique<SchemaBeCompactionsScanner>();
    case TSchemaTableType::SCH_BE_BVARS:
        return std::make_unique<SchemaBeBvarsScanner>();
    case TSchemaTableType::SCH_BE_CLOUD_NATIVE_COMPACTIONS:
        return std::make_unique<SchemaBeCloudNativeCompactionsScanner>();
    case TSchemaTableType::STARROCKS_ROLE_EDGES:
        return std::make_unique<StarrocksRoleEdgesScanner>();
    case TSchemaTableType::STARROCKS_GRANT_TO_ROLES:
        return std::make_unique<StarrocksGrantsToScanner>(TGrantsToType::ROLE);
    case TSchemaTableType::STARROCKS_GRANT_TO_USERS:
        return std::make_unique<StarrocksGrantsToScanner>(TGrantsToType::USER);
    case TSchemaTableType::SCH_ROUTINE_LOAD_JOBS:
        return std::make_unique<SchemaRoutineLoadJobsScanner>();
    case TSchemaTableType::SCH_STREAM_LOADS:
        return std::make_unique<SchemaStreamLoadsScanner>();
    case TSchemaTableType::SCH_PIPE_FILES:
        return std::make_unique<SchemaTablePipeFiles>();
    case TSchemaTableType::SCH_PIPES:
        return std::make_unique<SchemaTablePipes>();
    default:
        return std::make_unique<SchemaDummyScanner>();
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
        if (_columns[i].type == LogicalType::TYPE_VARCHAR || _columns[i].type == LogicalType::TYPE_CHAR) {
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

} // namespace starrocks
