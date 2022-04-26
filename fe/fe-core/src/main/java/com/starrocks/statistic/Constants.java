// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.starrocks.system.SystemInfoService;

public class Constants {
    public static final String StatisticsDBName =
            SystemInfoService.DEFAULT_CLUSTER + ":" + "_statistics_";
    public static final String StatisticsTableName = "table_statistic_v1";

    public static final String INFORMATION_SCHEMA = SystemInfoService.DEFAULT_CLUSTER + ":information_schema";

    public static final int CrossJoinCostPenalty = 100000000;

    public enum AnalyzeType {
        SAMPLE,
        FULL,
    }

    public enum ScheduleType {
        ONCE,
        SCHEDULE,
    }

    public enum ScheduleStatus {
        PENDING,
        RUNNING,
        // only use for ScheduleType.ONCE
        FINISH,
    }

    // PENDING -> RUNNING -> FAILED
    //                    -> SUCCESS
    //                    -> PARTIAL_SUCCESS
    //                    -> CANCELED
    //         -> CANCELED
    public enum MaterializedViewJobStatus {
        PENDING,
        RUNNING,
        FAILED,
        // all tasks are executed, but some task failed
        PARTIAL_SUCCESS,
        CANCELED,
        SUCCESS,
    }

    // PENDING -> RUNNING -> FAILED
    //                    -> SUCCESS
    //                    -> CANCELED
    //         -> CANCELED
    public enum MaterializedViewTaskStatus {
        PENDING,
        RUNNING,
        CANCELED,
        FAILED,
        SUCCESS
    }

    public enum MaterializedViewRefreshTriggerType {
        AUTO,
        MANUAL
    }

    public enum MaterializedViewRefreshMode {
        SYNC,
        ASYNC
    }

}
