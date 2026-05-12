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

#include <memory>

#include "exec/schema_scanner.h"
#include "exec/schema_scanner/schema_analyze_status.h"
#include "exec/schema_scanner/schema_applicable_roles_scanner.h"
#include "exec/schema_scanner/schema_be_bvars_scanner.h"
#include "exec/schema_scanner/schema_be_cloud_native_compactions_scanner.h"
#include "exec/schema_scanner/schema_be_compactions_scanner.h"
#include "exec/schema_scanner/schema_be_configs_scanner.h"
#include "exec/schema_scanner/schema_be_datacache_metrics_scanner.h"
#include "exec/schema_scanner/schema_be_logs_scanner.h"
#include "exec/schema_scanner/schema_be_metrics_scanner.h"
#include "exec/schema_scanner/schema_be_tablet_write_log_scanner.h"
#include "exec/schema_scanner/schema_be_tablets_scanner.h"
#include "exec/schema_scanner/schema_be_threads_scanner.h"
#include "exec/schema_scanner/schema_be_txns_scanner.h"
#include "exec/schema_scanner/schema_charsets_scanner.h"
#include "exec/schema_scanner/schema_cluster_snapshot_jobs_scanner.h"
#include "exec/schema_scanner/schema_cluster_snapshots_scanner.h"
#include "exec/schema_scanner/schema_collations_scanner.h"
#include "exec/schema_scanner/schema_column_stats_usage_scanner.h"
#include "exec/schema_scanner/schema_columns_scanner.h"
#include "exec/schema_scanner/schema_dummy_scanner.h"
#include "exec/schema_scanner/schema_fe_metrics_scanner.h"
#include "exec/schema_scanner/schema_fe_tablet_schedules_scanner.h"
#include "exec/schema_scanner/schema_fe_threads_scanner.h"
#include "exec/schema_scanner/schema_keywords_scanner.h"
#include "exec/schema_scanner/schema_load_tracking_logs_scanner.h"
#include "exec/schema_scanner/schema_loads_scanner.h"
#include "exec/schema_scanner/schema_materialized_views_scanner.h"
#include "exec/schema_scanner/schema_partitions_meta_scanner.h"
#include "exec/schema_scanner/schema_pipe_files.h"
#include "exec/schema_scanner/schema_pipes.h"
#include "exec/schema_scanner/schema_recyclebin_catalogs.h"
#include "exec/schema_scanner/schema_routine_load_jobs_scanner.h"
#include "exec/schema_scanner/schema_schema_privileges_scanner.h"
#include "exec/schema_scanner/schema_schemata_scanner.h"
#include "exec/schema_scanner/schema_stream_loads_scanner.h"
#include "exec/schema_scanner/schema_table_privileges_scanner.h"
#include "exec/schema_scanner/schema_tables_config_scanner.h"
#include "exec/schema_scanner/schema_tables_scanner.h"
#include "exec/schema_scanner/schema_tablet_reshard_jobs_scanner.h"
#include "exec/schema_scanner/schema_task_runs_scanner.h"
#include "exec/schema_scanner/schema_tasks_scanner.h"
#include "exec/schema_scanner/schema_temp_tables_scanner.h"
#include "exec/schema_scanner/schema_user_privileges_scanner.h"
#include "exec/schema_scanner/schema_variables_scanner.h"
#include "exec/schema_scanner/schema_views_scanner.h"
#include "exec/schema_scanner/schema_warehouse_metrics.h"
#include "exec/schema_scanner/schema_warehouse_queries.h"
#include "exec/schema_scanner/starrocks_grants_to_scanner.h"
#include "exec/schema_scanner/starrocks_role_edges_scanner.h"
#include "exec/schema_scanner/sys_fe_locks.h"
#include "exec/schema_scanner/sys_fe_memory_usage.h"
#include "exec/schema_scanner/sys_object_dependencies.h"

namespace starrocks {

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
    case TSchemaTableType::SCH_FE_METRICS:
        return std::make_unique<SchemaFeMetricsScanner>();
    case TSchemaTableType::SCH_FE_THREADS:
        return std::make_unique<SchemaFeThreadsScanner>();
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
    case TSchemaTableType::SCH_BE_TABLET_WRITE_LOG:
        return std::make_unique<SchemaBeTabletWriteLogScanner>();
    case TSchemaTableType::STARROCKS_ROLE_EDGES:
        return std::make_unique<StarrocksRoleEdgesScanner>();
    case TSchemaTableType::STARROCKS_GRANT_TO_ROLES:
        return std::make_unique<StarrocksGrantsToScanner>(TGrantsToType::ROLE);
    case TSchemaTableType::STARROCKS_GRANT_TO_USERS:
        return std::make_unique<StarrocksGrantsToScanner>(TGrantsToType::USER);
    case TSchemaTableType::STARROCKS_OBJECT_DEPENDENCIES:
        return std::make_unique<SysObjectDependencies>();
    case TSchemaTableType::SCH_ROUTINE_LOAD_JOBS:
        return std::make_unique<SchemaRoutineLoadJobsScanner>();
    case TSchemaTableType::SCH_STREAM_LOADS:
        return std::make_unique<SchemaStreamLoadsScanner>();
    case TSchemaTableType::SCH_PIPE_FILES:
        return std::make_unique<SchemaTablePipeFiles>();
    case TSchemaTableType::SCH_PIPES:
        return std::make_unique<SchemaTablePipes>();
    case TSchemaTableType::SYS_FE_LOCKS:
        return std::make_unique<SysFeLocks>();
    case TSchemaTableType::SCH_BE_DATACACHE_METRICS:
        return std::make_unique<SchemaBeDataCacheMetricsScanner>();
    case TSchemaTableType::SCH_PARTITIONS_META:
        return std::make_unique<SchemaPartitionsMetaScanner>();
    case TSchemaTableType::SYS_FE_MEMORY_USAGE:
        return std::make_unique<SysFeMemoryUsage>();
    case TSchemaTableType::SCH_TEMP_TABLES:
        return std::make_unique<SchemaTempTablesScanner>();
    case TSchemaTableType::SCH_RECYCLEBIN_CATALOGS:
        return std::make_unique<SchemaRecycleBinCatalogs>();
    case TSchemaTableType::SCH_COLUMN_STATS_USAGE:
        return std::make_unique<SchemaColumnStatsUsageScanner>();
    case TSchemaTableType::SCH_ANALYZE_STATUS:
        return std::make_unique<SchemaAnalyzeStatus>();
    case TSchemaTableType::SCH_CLUSTER_SNAPSHOTS:
        return std::make_unique<SchemaClusterSnapshotsScanner>();
    case TSchemaTableType::SCH_CLUSTER_SNAPSHOT_JOBS:
        return std::make_unique<SchemaClusterSnapshotJobsScanner>();
    case TSchemaTableType::SCH_APPLICABLE_ROLES:
        return std::make_unique<SchemaApplicableRolesScanner>();
    case TSchemaTableType::SCH_KEYWORDS:
        return std::make_unique<SchemaKeywordsScanner>();
    case TSchemaTableType::SCH_WAREHOUSE_METRICS:
        return std::make_unique<WarehouseMetricsScanner>();
    case TSchemaTableType::SCH_WAREHOUSE_QUERIES:
        return std::make_unique<WarehouseQueriesScanner>();
    case TSchemaTableType::SCH_TABLET_RESHARD_JOBS:
        return std::make_unique<SchemaTabletReshardJobsScanner>();
    default:
        return std::make_unique<SchemaDummyScanner>();
    }
}

} // namespace starrocks
