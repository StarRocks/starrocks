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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    public static final int STATISTIC_PARTITION_VERSION_V2 = 20;



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
                    .add(STATISTIC_PARTITION_VERSION_V2)
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

    // Bounded-cost external-table statistics-scan budgets. Per-statement overrides for the matching
    // connector_table_analyze_scan_*_cap Config: an explicit property wins over the global Config, an
    // absent property falls back to the Config default. Each <= 0 disables that dimension.
    public static final String EXTERNAL_ANALYZE_SCAN_BYTES_CAP = "scan_bytes_cap";
    public static final String EXTERNAL_ANALYZE_SCAN_FILES_CAP = "scan_files_cap";
    public static final String EXTERNAL_ANALYZE_SCAN_ROWS_CAP = "scan_rows_cap";

    // Histogram Statistics properties
    public static final String HISTOGRAM_BUCKET_NUM = "histogram_bucket_num";
    public static final String HISTOGRAM_MCV_SIZE = "histogram_mcv_size";
    public static final String HISTOGRAM_SAMPLE_RATIO = "histogram_sample_ratio";
    public static final String HISTOGRAM_STATS_SCOPE = "histogram_stats_scope";
    public static final String HISTOGRAM_STATS_SCOPE_MCV = "mcv";
    public static final String HISTOGRAM_STATS_SCOPE_BUCKETS = "buckets";

    public static final String HISTOGRAM_COLLECT_BUCKET_NDV_MODE = "histogram_collect_bucket_ndv_mode";

    // SQL plan manager table
    public static final String SPM_BASELINE_TABLE_NAME = "spm_baselines";
    public static final String QUERY_HISTORY_TABLE_NAME = "query_history";

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

    public static final String FULL_ONCE_TIMES = "full_once_times";
    public static final String FULL_SCHEDULE_TIMES = "full_schedule_times";
    public static final String SAMPLE_ONCE_TIMES = "sample_once_times";
    public static final String SAMPLE_SCHEDULE_TIMES = "sample_schedule_times";

    public static final List<String> STATISTICS_TABLES = List.of(
            QUERY_HISTORY_TABLE_NAME,
            SPM_BASELINE_TABLE_NAME,
            FULL_STATISTICS_TABLE_NAME,
            SAMPLE_STATISTICS_TABLE_NAME,
            EXTERNAL_FULL_STATISTICS_TABLE_NAME,
            MULTI_COLUMN_STATISTICS_TABLE_NAME,
            HISTOGRAM_STATISTICS_TABLE_NAME,
            EXTERNAL_HISTOGRAM_STATISTICS_TABLE_NAME
    );

    public enum AnalyzeType {
        SAMPLE,
        FULL,
        // For compatibility with older versions， we can't drop HISTOGRAM from this enum.
        HISTOGRAM,
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

    public enum HistogramCollectBucketNdvMode {
        NONE,
        SAMPLE,
        HLL
    }

    // The kinds of statistic a histogram job can collect. Modelled as a set rather than an enum of
    // combinations so that adding a third kind does not multiply the accepted property values.
    public enum HistogramStatKind {
        MCV(HISTOGRAM_STATS_SCOPE_MCV),
        BUCKETS(HISTOGRAM_STATS_SCOPE_BUCKETS);

        private final String propertyValue;

        HistogramStatKind(String propertyValue) {
            this.propertyValue = propertyValue;
        }

        public String propertyValue() {
            return propertyValue;
        }
    }

    public static final String HISTOGRAM_STATS_SCOPE_VALUES = Arrays.stream(HistogramStatKind.values())
            .map(HistogramStatKind::propertyValue)
            .collect(Collectors.joining("', '", "'", "'"));

    /**
     * Parse the {@link #HISTOGRAM_STATS_SCOPE} property into the set of statistic kinds to collect.
     * A null value - the property is absent - means "collect every kind", so callers do not need to
     * materialise a default. Otherwise the value is a comma-separated set, e.g. {@code "mcv"} or
     * {@code "mcv,buckets"}. A value that is present but names no kind is an error rather than a
     * silent "everything": spelling it out empty is a mistake worth reporting.
     *
     * @throws IllegalArgumentException if a kind is unknown, or the value is present but names no kind
     */
    public static EnumSet<HistogramStatKind> parseHistogramStatsScope(String rawScope) {
        if (rawScope == null) {
            return EnumSet.allOf(HistogramStatKind.class);
        }

        EnumSet<HistogramStatKind> kinds = EnumSet.noneOf(HistogramStatKind.class);
        for (String token : rawScope.split(",", -1)) {
            String kind = token.trim();
            if (kind.isEmpty()) {
                continue;
            }
            kinds.add(Arrays.stream(HistogramStatKind.values())
                    .filter(candidate -> candidate.propertyValue().equalsIgnoreCase(kind))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "unknown histogram statistic kind '" + kind + "'")));
        }

        if (kinds.isEmpty()) {
            throw new IllegalArgumentException("no histogram statistic kind specified");
        }
        return kinds;
    }

    public static Map<String, String> buildInitStatsProp() {
        Map<String, String> map = Maps.newHashMap();
        map.put(INIT_SAMPLE_STATS_JOB, "true");
        return map;
    }
}
