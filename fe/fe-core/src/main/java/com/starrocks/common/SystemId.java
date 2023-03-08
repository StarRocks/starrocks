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

package com.starrocks.common;

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

}
