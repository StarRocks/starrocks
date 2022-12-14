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


package com.starrocks.metric;

import com.google.common.collect.Lists;
import com.starrocks.metric.Metric.MetricUnit;

import java.util.List;

public final class TableMetricsEntity {

    protected static final String TABLE_SCAN_BYTES = "table_scan_bytes";
    protected static final String TABLE_SCAN_BYTES_COMMENT = "total scanned bytes of a table";
    protected static final String TABLE_SCAN_ROWS = "table_scan_rows";
    protected static final String TABLE_SCAN_ROWS_COMMENT = "total scanned rows of a table";
    protected static final String TABLE_SCAN_FINISHED = "table_scan_finished";
    protected static final String TABLE_SCAN_FINISHED_COMMENT = "total scanned times of a table";

    public static final String TABLE_LOAD_BYTES = "table_load_bytes";
    private static final String TABLE_LOAD_BYTES_COMMENT = "total loaded bytes of a table";
    public static final String TABLE_LOAD_ROWS = "table_load_rows";
    private static final String TABLE_LOAD_ROWS_COMMENT = "total loaded rows of a table";
    public static final String TABLE_LOAD_ERROR_ROWS = "table_load_error_rows";
    private static final String TABLE_LOAD_ERROR_COMMENT = "total error rows in load job of a table";
    public static final String TABLE_LOAD_UNSELECTED_ROWS = "table_load_unselected_rows";
    private static final String TABLE_LOAD_UNSELECTED_COMMENT = "total unselected rows in load job of a table";
    public static final String TABLE_LOAD_FINISHED = "table_load_finished";
    private static final String TABLE_LOAD_FINISHED_COMMENT = "total loaded times of this table";

    private List<Metric> metrics;

    public LongCounterMetric counterScanBytesTotal;
    public LongCounterMetric counterScanRowsTotal;
    public LongCounterMetric counterScanFinishedTotal;

    public LongCounterMetric counterStreamLoadBytesTotal;
    public LongCounterMetric counterStreamLoadRowsTotal;
    public LongCounterMetric counterStreamLoadFinishedTotal;

    public LongCounterMetric counterRoutineLoadBytesTotal;
    public LongCounterMetric counterRoutineLoadRowsTotal;
    public LongCounterMetric counterRoutineLoadErrorRowsTotal;
    public LongCounterMetric counterRoutineLoadUnselectedRowsTotal;
    public LongCounterMetric counterRoutineLoadFinishedTotal;

    public LongCounterMetric counterInsertLoadBytesTotal;
    public LongCounterMetric counterInsertLoadRowsTotal;
    public LongCounterMetric counterInsertLoadFinishedTotal;

    public LongCounterMetric counterBrokerLoadBytesTotal;
    public LongCounterMetric counterBrokerLoadRowsTotal;
    public LongCounterMetric counterBrokerLoadFinishedTotal;

    public LongCounterMetric counterSparkLoadBytesTotal;
    public LongCounterMetric counterSparkLoadRowsTotal;
    public LongCounterMetric counterSparkLoadFinishedTotal;

    public TableMetricsEntity() {
        initTableMetrics();
    }

    public List<Metric> getMetrics() {
        return metrics;
    }

    protected void initTableMetrics() {
        metrics = Lists.newArrayList();

        // scan metrics
        counterScanBytesTotal = new LongCounterMetric(TABLE_SCAN_BYTES, MetricUnit.BYTES, TABLE_SCAN_BYTES_COMMENT);
        metrics.add(counterScanBytesTotal);
        counterScanRowsTotal = new LongCounterMetric(TABLE_SCAN_ROWS, MetricUnit.ROWS, TABLE_SCAN_ROWS_COMMENT);
        metrics.add(counterScanRowsTotal);
        counterScanFinishedTotal =
                new LongCounterMetric(TABLE_SCAN_FINISHED, MetricUnit.REQUESTS, TABLE_SCAN_FINISHED_COMMENT);
        metrics.add(counterScanFinishedTotal);

        // load metrics
        counterStreamLoadBytesTotal =
                new LongCounterMetric(TABLE_LOAD_BYTES, MetricUnit.BYTES, TABLE_LOAD_BYTES_COMMENT);
        counterStreamLoadBytesTotal.addLabel(new MetricLabel("type", "stream_load"));
        metrics.add(counterStreamLoadBytesTotal);
        counterStreamLoadRowsTotal =
                new LongCounterMetric(TABLE_LOAD_ROWS, MetricUnit.ROWS, TABLE_LOAD_ROWS_COMMENT);
        counterStreamLoadRowsTotal.addLabel(new MetricLabel("type", "stream_load"));
        metrics.add(counterStreamLoadRowsTotal);
        counterStreamLoadFinishedTotal =
                new LongCounterMetric(TABLE_LOAD_FINISHED, MetricUnit.REQUESTS, TABLE_LOAD_FINISHED_COMMENT);
        counterStreamLoadFinishedTotal.addLabel(new MetricLabel("type", "stream_load"));
        metrics.add(counterStreamLoadFinishedTotal);

        counterRoutineLoadBytesTotal =
                new LongCounterMetric(TABLE_LOAD_BYTES, MetricUnit.BYTES, TABLE_LOAD_BYTES_COMMENT);
        counterRoutineLoadBytesTotal.addLabel(new MetricLabel("type", "routine_load"));
        metrics.add(counterRoutineLoadBytesTotal);
        counterRoutineLoadRowsTotal =
                new LongCounterMetric(TABLE_LOAD_ROWS, MetricUnit.ROWS, TABLE_LOAD_ROWS_COMMENT);
        counterRoutineLoadRowsTotal.addLabel(new MetricLabel("type", "routine_load"));
        metrics.add(counterRoutineLoadRowsTotal);
        counterRoutineLoadErrorRowsTotal =
                new LongCounterMetric(TABLE_LOAD_ERROR_ROWS, MetricUnit.ROWS,  TABLE_LOAD_ERROR_COMMENT);
        counterRoutineLoadErrorRowsTotal.addLabel(new MetricLabel("type", "routine_load"));
        metrics.add(counterRoutineLoadErrorRowsTotal);
        counterRoutineLoadUnselectedRowsTotal =
                new LongCounterMetric(TABLE_LOAD_UNSELECTED_ROWS, MetricUnit.ROWS, TABLE_LOAD_UNSELECTED_COMMENT);
        counterRoutineLoadUnselectedRowsTotal.addLabel(new MetricLabel("type", "routine_load"));
        metrics.add(counterRoutineLoadUnselectedRowsTotal);
        counterRoutineLoadFinishedTotal =
                new LongCounterMetric(TABLE_LOAD_FINISHED, MetricUnit.REQUESTS, TABLE_LOAD_FINISHED_COMMENT);
        counterRoutineLoadFinishedTotal.addLabel(new MetricLabel("type", "routine_load"));
        metrics.add(counterRoutineLoadFinishedTotal);

        counterBrokerLoadBytesTotal =
                new LongCounterMetric(TABLE_LOAD_BYTES, MetricUnit.BYTES, TABLE_LOAD_BYTES_COMMENT);
        counterBrokerLoadBytesTotal.addLabel(new MetricLabel("type", "broker_load"));
        metrics.add(counterBrokerLoadBytesTotal);
        counterBrokerLoadRowsTotal =
                new LongCounterMetric(TABLE_LOAD_ROWS, MetricUnit.ROWS, TABLE_LOAD_ROWS_COMMENT);
        counterBrokerLoadRowsTotal.addLabel(new MetricLabel("type", "broker_load"));
        metrics.add(counterBrokerLoadRowsTotal);
        counterBrokerLoadFinishedTotal =
                new LongCounterMetric(TABLE_LOAD_FINISHED, MetricUnit.REQUESTS, TABLE_LOAD_FINISHED_COMMENT);
        counterBrokerLoadFinishedTotal.addLabel(new MetricLabel("type", "broker_load"));
        metrics.add(counterBrokerLoadFinishedTotal);

        counterSparkLoadBytesTotal =
                new LongCounterMetric(TABLE_LOAD_BYTES, MetricUnit.BYTES, TABLE_LOAD_BYTES_COMMENT);
        counterSparkLoadBytesTotal.addLabel(new MetricLabel("type", "spark_load"));
        metrics.add(counterSparkLoadBytesTotal);
        counterSparkLoadRowsTotal =
                new LongCounterMetric(TABLE_LOAD_ROWS, MetricUnit.ROWS, TABLE_LOAD_ROWS_COMMENT);
        counterSparkLoadRowsTotal.addLabel(new MetricLabel("type", "spark_load"));
        metrics.add(counterSparkLoadRowsTotal);
        counterSparkLoadFinishedTotal =
                new LongCounterMetric(TABLE_LOAD_FINISHED, MetricUnit.REQUESTS, TABLE_LOAD_FINISHED_COMMENT);
        counterSparkLoadFinishedTotal.addLabel(new MetricLabel("type", "spark_load"));
        metrics.add(counterSparkLoadFinishedTotal);

        counterInsertLoadBytesTotal =
                new LongCounterMetric(TABLE_LOAD_BYTES, MetricUnit.BYTES, TABLE_LOAD_BYTES_COMMENT);
        counterInsertLoadBytesTotal.addLabel(new MetricLabel("type", "insert_into"));
        metrics.add(counterInsertLoadBytesTotal);
        counterInsertLoadRowsTotal =
                new LongCounterMetric(TABLE_LOAD_ROWS, MetricUnit.ROWS, TABLE_LOAD_ROWS_COMMENT);
        counterInsertLoadRowsTotal.addLabel(new MetricLabel("type", "insert_into"));
        metrics.add(counterInsertLoadRowsTotal);
        counterInsertLoadFinishedTotal =
                new LongCounterMetric(TABLE_LOAD_FINISHED, MetricUnit.REQUESTS, TABLE_LOAD_FINISHED_COMMENT);
        counterInsertLoadFinishedTotal.addLabel(new MetricLabel("type", "insert_into"));
        metrics.add(counterInsertLoadFinishedTotal);
    }
}

