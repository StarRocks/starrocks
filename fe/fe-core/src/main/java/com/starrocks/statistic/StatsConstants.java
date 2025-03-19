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

import com.google.common.collect.ImmutableSet;
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
    public static final int STATISTIC_EXTERNAL_QUERY_VERSION = 6;
    public static final int STATISTIC_EXTERNAL_HISTOGRAM_VERSION = 7;
    public static final int STATISTIC_EXTERNAL_QUERY_V2_VERSION = 8;
    public static final int STATISTIC_PARTITION_VERSION = 11;
    public static final int STATISTIC_BATCH_VERSION_V5 = 9;
    public static final int STATISTIC_DATA_VERSION_V2 = 10;
    public static final int STATISTIC_MULTI_COLUMN_VERSION = 12;
    public static final int STATISTIC_QUERY_MULTI_COLUMN_VERSION = 13;



    public static final ImmutableSet<Integer> STATISTIC_SUPPORTED_VERSION =
            ImmutableSet.<Integer>builder()
                    .add(STATISTIC_DATA_VERSION)
                    .add(STATISTIC_DICT_VERSION)
                    .add(STATISTIC_HISTOGRAM_VERSION)
                    .add(STATISTIC_TABLE_VERSION)
                    .add(STATISTIC_BATCH_VERSION)
                    .add(STATISTIC_EXTERNAL_VERSION)
                    .add(STATISTIC_EXTERNAL_QUERY_VERSION)
                    .add(STATISTIC_EXTERNAL_HISTOGRAM_VERSION)
                    .add(STATISTIC_EXTERNAL_QUERY_V2_VERSION)
                    .add(STATISTIC_PARTITION_VERSION)
                    .add(STATISTIC_BATCH_VERSION_V5)
                    .add(STATISTIC_DATA_VERSION_V2)
                    .add(STATISTIC_MULTI_COLUMN_VERSION)
                    .add(STATISTIC_QUERY_MULTI_COLUMN_VERSION)
                    .build();

    public static final int STATISTICS_PARTITION_UPDATED_THRESHOLD = 10;
    public static final String STATISTICS_DB_NAME = "_statistics_";
    public static final String SAMPLE_STATISTICS_TABLE_NAME = "table_statistic_v1";
    public static final String FULL_STATISTICS_TABLE_NAME = "column_statistics";
    public static final String EXTERNAL_FULL_STATISTICS_TABLE_NAME = "external_column_statistics";
    public static final String HISTOGRAM_STATISTICS_TABLE_NAME = "histogram_statistics";
    public static final String EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME = "external_histogram_statistics";
    public static final String MULTI_COLUMN_STATISTICS_TABLE_NAME = "multi_column_statistics";


    public static final String INFORMATION_SCHEMA = "information_schema";

    // Statistics collection threshold
    public static final String STATISTIC_AUTO_COLLECT_RATIO = "statistic_auto_collect_ratio";
    public static final String STATISTIC_SAMPLE_COLLECT_ROWS = "statistic_sample_collect_rows";
    public static final String STATISTIC_EXCLUDE_PATTERN = "statistic_exclude_pattern";
    public static final String STATISTIC_AUTO_COLLECT_INTERVAL = "statistic_auto_collect_interval";

    // Sample statistics tablet sample ratio
    public static final String HIGH_WEIGHT_SAMPLE_RATIO = "high_weight_sample_ratio";

    public static final String MEDIUM_HIGH_WEIGHT_SAMPLE_RATIO = "medium_high_weight_sample_ratio";

    public static final String MEDIUM_LOW_WEIGHT_SAMPLE_RATIO = "medium_low_weight_sample_ratio";

    public static final String LOW_WEIGHT_SAMPLE_RATIO = "low_weight_sample_ratio";

    public static final String MAX_SAMPLE_TABLET_NUM = "max_sample_tablet_num";

    public static final String STATISTIC_SAMPLE_COLLECT_PARTITIONS = "statistic_sample_collect_partitions";

    // Histogram Statistics properties
    public static final String HISTOGRAM_BUCKET_NUM = "histogram_bucket_num";
    public static final String HISTOGRAM_MCV_SIZE = "histogram_mcv_size";
    public static final String HISTOGRAM_SAMPLE_RATIO = "histogram_sample_ratio";

    // SQL plan manager table
    public static final String SPM_BASELINE_TABLE_NAME = "spm_baselines";

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
    public static final String COLUMN_ID_SEPARATOR = "#";

    public enum AnalyzeType {
        SAMPLE,
        FULL,
        // For compatibility with older versions， we can't drop HISTOGRAM from this enum.
        HISTOGRAM,
    }

    // used to record statistics type for multi-columns.
    // For version compatibility, single-column statistics are not recorded StatisticsType
    public enum StatisticsType {
        // for single column statistics
        COMMON,
        // for single column histogram
        HISTOGRAM,
        // for multi-column combined ndv
        MCDISTINCT
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
