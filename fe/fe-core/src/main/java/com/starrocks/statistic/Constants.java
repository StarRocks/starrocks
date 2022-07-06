// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.starrocks.system.SystemInfoService;

public class Constants {
    public static final int STATISTIC_DATA_VERSION = 1;
    public static final int STATISTIC_DICT_VERSION = 101;
    public static final int STATISTIC_HISTOGRAM_VERSION = 2;

    public static final String StatisticsDBName = SystemInfoService.DEFAULT_CLUSTER + ":" + "_statistics_";
    public static final String SampleStatisticsTableName = "table_statistic_v1";
    public static final String FullStatisticsTableName = "column_statistics";
    public static final String HistogramStatisticsTableName = "histogram_statistics";

    public static final String INFORMATION_SCHEMA = SystemInfoService.DEFAULT_CLUSTER + ":information_schema";

    public static final int CrossJoinCostPenalty = 100000000;
    public static final int BroadcastJoinMemExceedPenalty = 1000;

    // Sample ratio in total rows
    public static final String PRO_SAMPLE_RATIO = "sample_ratio";

    //Statistics collection threshold
    public static final String PRO_AUTO_COLLECT_STATISTICS_RATIO = "auto_collect_statistics_ratio";

    public static final String PRO_BUCKET_NUM = "bucket_num";

    public static final String PROP_UPDATE_INTERVAL_SEC_KEY = "update_interval_sec";
    public static final String PROP_SAMPLE_COLLECT_ROWS_KEY = "sample_collect_rows";

    @Deprecated
    public static final String PROP_COLLECT_INTERVAL_SEC_KEY = "collect_interval_sec";

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
