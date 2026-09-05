# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/schema_scanner")
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/schema_scanner")

set(SCHEMA_SCANNER_FILES
    schema_analyze_status.cpp
    schema_applicable_roles_scanner.cpp
    schema_be_bvars_scanner.cpp
    schema_be_cloud_native_compactions_scanner.cpp
    schema_be_compactions_scanner.cpp
    schema_be_configs_scanner.cpp
    schema_be_datacache_metrics_scanner.cpp
    schema_be_logs_scanner.cpp
    schema_be_metrics_scanner.cpp
    schema_be_tablet_write_log_scanner.cpp
    schema_be_tablets_scanner.cpp
    schema_be_threads_scanner.cpp
    schema_be_txns_scanner.cpp
    schema_charsets_scanner.cpp
    schema_cluster_snapshot_jobs_scanner.cpp
    schema_cluster_snapshots_scanner.cpp
    schema_collation_character_set_applicability_scanner.cpp
    schema_collations_scanner.cpp
    schema_column_stats_usage_scanner.cpp
    schema_columns_scanner.cpp
    schema_dummy_scanner.cpp
    schema_events_scanner.cpp
    schema_fe_metrics_scanner.cpp
    schema_fe_tablet_schedules_scanner.cpp
    schema_fe_threads_scanner.cpp
    schema_helper.cpp
    schema_keywords_scanner.cpp
    schema_load_tracking_logs_scanner.cpp
    schema_loads_scanner.cpp
    schema_materialized_view_refresh_jobs_scanner.cpp
    schema_materialized_views_scanner.cpp
    schema_partitions_meta_scanner.cpp
    schema_pipe_files.cpp
    schema_pipes.cpp
    schema_recyclebin_catalogs.cpp
    schema_routine_load_jobs_scanner.cpp
    schema_schema_privileges_scanner.cpp
    schema_schemata_scanner.cpp
    schema_statistics_scanner.cpp
    schema_stream_loads_scanner.cpp
    schema_table_privileges_scanner.cpp
    schema_tables_config_scanner.cpp
    schema_tables_scanner.cpp
    schema_tablet_reshard_jobs_scanner.cpp
    schema_task_runs_scanner.cpp
    schema_tasks_scanner.cpp
    schema_temp_tables_scanner.cpp
    schema_triggers_scanner.cpp
    schema_user_privileges_scanner.cpp
    schema_variables_scanner.cpp
    schema_views_scanner.cpp
    schema_warehouse_metrics.cpp
    schema_warehouse_queries.cpp
    starrocks_grants_to_scanner.cpp
    starrocks_role_edges_scanner.cpp
    sys_fe_locks.cpp
    sys_fe_memory_usage.cpp
    sys_object_dependencies.cpp
)

ADD_BE_LIB(SchemaScanners
    ${SCHEMA_SCANNER_FILES}
)

target_link_libraries(SchemaScanners PUBLIC
    ExecSchemaScannerCore
    Storage
    StoragePrimitive
    Cache
    Platform
    Expr
    Runtime
    ChunkCore
    ColumnCore
    Types
    Common
    Base
    Gutil
    StarRocksGen
)

ADD_BE_LIB(SchemaScannerBuiltin
    builtin_schema_scanner_factory.cpp
)

target_link_libraries(SchemaScannerBuiltin PUBLIC
    ExecSchemaScannerCore
)

target_link_libraries(SchemaScannerBuiltin PRIVATE
    SchemaScanners
)
