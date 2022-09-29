// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.statistic;

public class StatsConstants {
    public static final long DEFAULT_ALL_ID = -1;

    public static final int STATISTIC_DATA_VERSION = 1;
    public static final int STATISTIC_DICT_VERSION = 101;
    public static final int STATISTIC_HISTOGRAM_VERSION = 2;

    public static final String STATISTICS_DB_NAME = "_statistics_";
    public static final String SAMPLE_STATISTICS_TABLE_NAME = "table_statistic_v1";
    public static final String FULL_STATISTICS_TABLE_NAME = "column_statistics";
    public static final String HISTOGRAM_STATISTICS_TABLE_NAME = "histogram_statistics";

    public static final String INFORMATION_SCHEMA = "information_schema";

    public static final int CROSS_JOIN_COST_PENALTY = 100 * 10000;
    public static final int CROSS_JOIN_RIGHT_COST_PENALTY = 10 * 10000;
    public static final int BROADCAST_JOIN_MEM_EXCEED_PENALTY = 1000;

    //Statistics collection threshold
    public static final String STATISTIC_AUTO_COLLECT_RATIO = "statistic_auto_collect_ratio";
    public static final String STATISTIC_SAMPLE_COLLECT_ROWS = "statistic_sample_collect_rows";

    //Histogram Statistics properties
    public static final String HISTOGRAM_BUCKET_NUM = "histogram_bucket_num";
    public static final String HISTOGRAM_MCV_SIZE = "histogram_mcv_size";
    public static final String HISTOGRAM_SAMPLE_RATIO = "histogram_sample_ratio";

    /**
     * Deprecated stats properties
     */
    public static final String PRO_SAMPLE_RATIO = "sample_ratio";
    public static final String PROP_UPDATE_INTERVAL_SEC_KEY = "update_interval_sec";
    public static final String PROP_COLLECT_INTERVAL_SEC_KEY = "collect_interval_sec";

    public static final String TABLE_PROPERTY_SEPARATOR = ",\n\"";

    public enum AnalyzeType {
        SAMPLE,
        FULL,
        HISTOGRAM
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
        FAILED
    }
}
