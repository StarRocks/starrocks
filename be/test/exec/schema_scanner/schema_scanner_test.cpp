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

#include <gtest/gtest.h>

#include "exec/builtin_schema_scanner_factory.h"
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
#include "exec/schema_scanner/schema_collation_character_set_applicability_scanner.h"
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
#include "exec/schema_scanner/schema_materialized_view_refresh_jobs_scanner.h"
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
#include "gen_cpp/Descriptors_types.h"

namespace starrocks {

namespace {

template <typename Expected>
void expect_scanner_type(const SchemaScannerFactory& factory, TSchemaTableType::type type) {
    auto scanner = factory.create(type);
    ASSERT_NE(nullptr, scanner);
    EXPECT_NE(nullptr, dynamic_cast<Expected*>(scanner.get()));
}

void expect_variable_scanner(const SchemaScannerFactory& factory, TSchemaTableType::type table_type,
                             TVarType::type variable_type) {
    auto scanner = factory.create(table_type);
    ASSERT_NE(nullptr, scanner);
    auto* variables = dynamic_cast<SchemaVariablesScanner*>(scanner.get());
    ASSERT_NE(nullptr, variables);
    EXPECT_EQ(variable_type, variables->_type);
}

void expect_grants_scanner(const SchemaScannerFactory& factory, TSchemaTableType::type table_type,
                           TGrantsToType::type grants_type) {
    auto scanner = factory.create(table_type);
    ASSERT_NE(nullptr, scanner);
    auto* grants = dynamic_cast<StarrocksGrantsToScanner*>(scanner.get());
    ASSERT_NE(nullptr, grants);
    EXPECT_EQ(grants_type, grants->_type);
}

} // namespace

TEST(SchemaScannerBuiltinTest, CreatesExpectedConcreteScannerForEveryTableType) {
    auto factory = create_builtin_schema_scanner_factory();
    ASSERT_NE(nullptr, factory);

    expect_scanner_type<SchemaTablesScanner>(*factory, TSchemaTableType::SCH_TABLES);
    expect_scanner_type<SchemaSchemataScanner>(*factory, TSchemaTableType::SCH_SCHEMATA);
    expect_scanner_type<SchemaColumnsScanner>(*factory, TSchemaTableType::SCH_COLUMNS);
    expect_scanner_type<SchemaCharsetsScanner>(*factory, TSchemaTableType::SCH_CHARSETS);
    expect_scanner_type<SchemaCollationsScanner>(*factory, TSchemaTableType::SCH_COLLATIONS);
    expect_scanner_type<SchemaCollationCharacterSetApplicabilityScanner>(
            *factory, TSchemaTableType::SCH_COLLATION_CHARACTER_SET_APPLICABILITY);
    expect_variable_scanner(*factory, TSchemaTableType::SCH_GLOBAL_VARIABLES, TVarType::GLOBAL);
    expect_variable_scanner(*factory, TSchemaTableType::SCH_SESSION_VARIABLES, TVarType::SESSION);
    expect_variable_scanner(*factory, TSchemaTableType::SCH_VARIABLES, TVarType::SESSION);
    expect_scanner_type<SchemaUserPrivilegesScanner>(*factory, TSchemaTableType::SCH_USER_PRIVILEGES);
    expect_scanner_type<SchemaSchemaPrivilegesScanner>(*factory, TSchemaTableType::SCH_SCHEMA_PRIVILEGES);
    expect_scanner_type<SchemaTablePrivilegesScanner>(*factory, TSchemaTableType::SCH_TABLE_PRIVILEGES);
    expect_scanner_type<SchemaViewsScanner>(*factory, TSchemaTableType::SCH_VIEWS);
    expect_scanner_type<SchemaTasksScanner>(*factory, TSchemaTableType::SCH_TASKS);
    expect_scanner_type<SchemaTaskRunsScanner>(*factory, TSchemaTableType::SCH_TASK_RUNS);
    expect_scanner_type<SchemaMaterializedViewsScanner>(*factory, TSchemaTableType::SCH_MATERIALIZED_VIEWS);
    expect_scanner_type<SchemaMaterializedViewRefreshJobsScanner>(*factory,
                                                                  TSchemaTableType::SCH_MATERIALIZED_VIEW_REFRESH_JOBS);
    expect_scanner_type<SchemaLoadsScanner>(*factory, TSchemaTableType::SCH_LOADS);
    expect_scanner_type<SchemaLoadTrackingLogsScanner>(*factory, TSchemaTableType::SCH_LOAD_TRACKING_LOGS);
    expect_scanner_type<SchemaTablesConfigScanner>(*factory, TSchemaTableType::SCH_TABLES_CONFIG);
    expect_variable_scanner(*factory, TSchemaTableType::SCH_VERBOSE_SESSION_VARIABLES, TVarType::VERBOSE);
    expect_scanner_type<SchemaBeTabletsScanner>(*factory, TSchemaTableType::SCH_BE_TABLETS);
    expect_scanner_type<SchemaBeMetricsScanner>(*factory, TSchemaTableType::SCH_BE_METRICS);
    expect_scanner_type<SchemaFeMetricsScanner>(*factory, TSchemaTableType::SCH_FE_METRICS);
    expect_scanner_type<SchemaFeThreadsScanner>(*factory, TSchemaTableType::SCH_FE_THREADS);
    expect_scanner_type<SchemaBeTxnsScanner>(*factory, TSchemaTableType::SCH_BE_TXNS);
    expect_scanner_type<SchemaBeConfigsScanner>(*factory, TSchemaTableType::SCH_BE_CONFIGS);
    expect_scanner_type<SchemaBeThreadsScanner>(*factory, TSchemaTableType::SCH_BE_THREADS);
    expect_scanner_type<SchemaBeLogsScanner>(*factory, TSchemaTableType::SCH_BE_LOGS);
    expect_scanner_type<SchemaFeTabletSchedulesScanner>(*factory, TSchemaTableType::SCH_FE_TABLET_SCHEDULES);
    expect_scanner_type<SchemaBeCompactionsScanner>(*factory, TSchemaTableType::SCH_BE_COMPACTIONS);
    expect_scanner_type<SchemaBeBvarsScanner>(*factory, TSchemaTableType::SCH_BE_BVARS);
    expect_scanner_type<SchemaBeCloudNativeCompactionsScanner>(*factory,
                                                               TSchemaTableType::SCH_BE_CLOUD_NATIVE_COMPACTIONS);
    expect_scanner_type<SchemaBeTabletWriteLogScanner>(*factory, TSchemaTableType::SCH_BE_TABLET_WRITE_LOG);
    expect_scanner_type<StarrocksRoleEdgesScanner>(*factory, TSchemaTableType::STARROCKS_ROLE_EDGES);
    expect_grants_scanner(*factory, TSchemaTableType::STARROCKS_GRANT_TO_ROLES, TGrantsToType::ROLE);
    expect_grants_scanner(*factory, TSchemaTableType::STARROCKS_GRANT_TO_USERS, TGrantsToType::USER);
    expect_scanner_type<SysObjectDependencies>(*factory, TSchemaTableType::STARROCKS_OBJECT_DEPENDENCIES);
    expect_scanner_type<SchemaRoutineLoadJobsScanner>(*factory, TSchemaTableType::SCH_ROUTINE_LOAD_JOBS);
    expect_scanner_type<SchemaStreamLoadsScanner>(*factory, TSchemaTableType::SCH_STREAM_LOADS);
    expect_scanner_type<SchemaTablePipeFiles>(*factory, TSchemaTableType::SCH_PIPE_FILES);
    expect_scanner_type<SchemaTablePipes>(*factory, TSchemaTableType::SCH_PIPES);
    expect_scanner_type<SysFeLocks>(*factory, TSchemaTableType::SYS_FE_LOCKS);
    expect_scanner_type<SchemaBeDataCacheMetricsScanner>(*factory, TSchemaTableType::SCH_BE_DATACACHE_METRICS);
    expect_scanner_type<SchemaPartitionsMetaScanner>(*factory, TSchemaTableType::SCH_PARTITIONS_META);
    expect_scanner_type<SysFeMemoryUsage>(*factory, TSchemaTableType::SYS_FE_MEMORY_USAGE);
    expect_scanner_type<SchemaTempTablesScanner>(*factory, TSchemaTableType::SCH_TEMP_TABLES);
    expect_scanner_type<SchemaRecycleBinCatalogs>(*factory, TSchemaTableType::SCH_RECYCLEBIN_CATALOGS);
    expect_scanner_type<SchemaColumnStatsUsageScanner>(*factory, TSchemaTableType::SCH_COLUMN_STATS_USAGE);
    expect_scanner_type<SchemaAnalyzeStatus>(*factory, TSchemaTableType::SCH_ANALYZE_STATUS);
    expect_scanner_type<SchemaClusterSnapshotsScanner>(*factory, TSchemaTableType::SCH_CLUSTER_SNAPSHOTS);
    expect_scanner_type<SchemaClusterSnapshotJobsScanner>(*factory, TSchemaTableType::SCH_CLUSTER_SNAPSHOT_JOBS);
    expect_scanner_type<SchemaApplicableRolesScanner>(*factory, TSchemaTableType::SCH_APPLICABLE_ROLES);
    expect_scanner_type<SchemaKeywordsScanner>(*factory, TSchemaTableType::SCH_KEYWORDS);
    expect_scanner_type<WarehouseMetricsScanner>(*factory, TSchemaTableType::SCH_WAREHOUSE_METRICS);
    expect_scanner_type<WarehouseQueriesScanner>(*factory, TSchemaTableType::SCH_WAREHOUSE_QUERIES);
    expect_scanner_type<SchemaTabletReshardJobsScanner>(*factory, TSchemaTableType::SCH_TABLET_RESHARD_JOBS);
}

TEST(SchemaScannerBuiltinTest, UnknownTableTypeCreatesDummyScanner) {
    auto factory = create_builtin_schema_scanner_factory();
    ASSERT_NE(nullptr, factory);
    expect_scanner_type<SchemaDummyScanner>(*factory, static_cast<TSchemaTableType::type>(-1));
}

} // namespace starrocks
