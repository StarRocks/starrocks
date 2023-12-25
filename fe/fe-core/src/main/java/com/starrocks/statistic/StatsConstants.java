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


package com.starrocks.statistic;

import com.google.common.collect.Maps;

import java.util.Map;

public class StatsConstants {
    public static final long DEFAULT_ALL_ID = -1;

    public static final int STATISTIC_DATA_VERSION = 1;
    public static final int STATISTIC_DICT_VERSION = 101;
    public static final int STATISTIC_HISTOGRAM_VERSION = 2;
    public static final int STATISTIC_TABLE_VERSION = 3;
    public static final int STATISTIC_BATCH_VERSION = 4;
    public static final int STATISTIC_EXTERNAL_VERSION = 5;


    public static final int STATISTICS_PARTITION_UPDATED_THRESHOLD = 10;
    public static final String STATISTICS_DB_NAME = "_statistics_";
    public static final String SAMPLE_STATISTICS_TABLE_NAME = "table_statistic_v1";
    public static final String FULL_STATISTICS_TABLE_NAME = "column_statistics";
    public static final String EXTERNAL_FULL_STATISTICS_TABLE_NAME = "external_column_statistics";
    public static final String HISTOGRAM_STATISTICS_TABLE_NAME = "histogram_statistics";

    public static final String INFORMATION_SCHEMA = "information_schema";

    //Statistics collection threshold
    public static final String STATISTIC_AUTO_COLLECT_RATIO = "statistic_auto_collect_ratio";
    public static final String STATISTIC_SAMPLE_COLLECT_ROWS = "statistic_sample_collect_rows";
    public static final String STATISTIC_EXCLUDE_PATTERN = "statistic_exclude_pattern";
    public static final String STATISTIC_AUTO_COLLECT_INTERVAL = "statistic_auto_collect_interval";

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

    // use this to distinguish the initial sample collect job from sample job requested by client
    public static final String INIT_SAMPLE_STATS_JOB = "init_stats_sample_job";

    public static final String INIT_SAMPLE_STATS_PROPERTY = "('" + INIT_SAMPLE_STATS_JOB + "' = 'true')";

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

    public static Map<String, String> buildInitStatsProp() {
        Map<String, String> map = Maps.newHashMap();
        map.put(INIT_SAMPLE_STATS_JOB, "true");
        return map;
    }
}
