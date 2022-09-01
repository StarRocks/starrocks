// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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

}
