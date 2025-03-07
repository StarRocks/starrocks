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

package com.starrocks.catalog.system;

// used for system table.
// cannot bigger than 10000(GlobalStateMgr.NEXT_ID_INIT_VALUE)
public class SystemId {

    public static final long INFORMATION_SCHEMA_DB_ID = 1L;

    public static final long TABLES_ID = 2L;

    public static final long TABLE_PRIVILEGES_ID = 3L;

    public static final long REFERENTIAL_CONSTRAINTS_ID = 4L;

    public static final long KEY_COLUMN_USAGE_ID = 5L;

    public static final long ROUTINES_ID = 6L;

    public static final long SCHEMATA_ID = 7L;

    public static final long SESSION_VARIABLES_ID = 8L;

    public static final long GLOBAL_VARIABLES_ID = 9L;

    public static final long COLUMNS_ID = 10L;

    public static final long CHARACTER_SETS_ID = 11L;

    public static final long COLLATIONS_ID = 12L;

    public static final long TABLE_CONSTRAINTS_ID = 13L;

    public static final long ENGINES_ID = 14L;

    public static final long USER_PRIVILEGES_ID = 15L;

    public static final long SCHEMA_PRIVILEGES_ID = 16L;

    public static final long STATISTICS_ID = 17L;

    public static final long TRIGGERS_ID = 18L;

    public static final long EVENTS_ID = 19L;

    public static final long VIEWS_ID = 20L;

    public static final long TASKS_ID = 21L;

    public static final long TASK_RUNS_ID = 22L;

    public static final long MATERIALIZED_VIEWS_ID = 23L;

    public static final long TABLES_CONFIG_ID = 24L;

    public static final long VERBOSE_SESSION_VARIABLES_ID = 25L;

    public static final long BE_TABLETS_ID = 26L;

    public static final long BE_METRICS_ID = 27L;

    public static final long BE_TXNS_ID = 28L;

    public static final long BE_CONFIGS_ID = 29L;

    public static final long PARTITIONS_ID = 30L;

    public static final long COLUMN_PRIVILEGES_ID = 31L;

    public static final long LOADS_ID = 32L;

    public static final long LOAD_TRACKING_LOGS_ID = 33L;

    public static final long FE_SCHEDULES_ID = 34L;

    public static final long BE_COMPACTIONS_ID = 35L;

    public static final long BE_THREADS_ID = 36L;

    public static final long BE_LOGS_ID = 37L;

    public static final long BE_BVARS_ID = 38L;

    public static final long BE_CLOUD_NATIVE_COMPACTIONS = 39L;

    public static final long ROUTINE_LOAD_JOBS_ID = 40L;

    public static final long STREAM_LOADS_ID = 41L;

    public static final long FE_METRICS_ID = 42L;

    public static final long TEMP_TABLES_ID = 43L;

    public static final long KEYWORDS_ID = 44L;

    public static final long SYS_DB_ID = 100L;

    public static final long ROLE_EDGES_ID = 101L;
    public static final long GRANTS_TO_ROLES_ID = 102L;
    public static final long GRANTS_TO_USERS_ID = 103L;
    public static final long OBJECT_DEPENDENCIES = 104L;
    public static final long FE_LOCKS_ID = 105L;
    public static final long MEMORY_USAGE_ID = 106L;
    public static final long PIPE_FILES_ID = 120L;
    public static final long PIPES_ID = 121L;
    public static final long BE_DATACACHE_METRICS = 130L;
    // Remain for other datacache manage table

    public static final long PARTITIONS_META_ID = 140L;

    // ====================  Statistics  =========================== //
    public static final long COLUMN_STATS_USAGE = 150L;
    public static final long ANALYZE_STATUS = 151L;

    // ==================  Cluster Snapshot  ======================= //
    public static final long CLUSTER_SNAPSHOTS_ID = 160L;
    public static final long CLUSTER_SNAPSHOT_JOBS_ID = 161L;
}
