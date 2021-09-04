// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.metric;

import avro.shaded.com.google.common.collect.Lists;
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
    public static final String TABLE_LOAD_FINISHED = "table_load_finished";
    private static final String TABLE_LOAD_FINISHED_COMMENT = "total loaded times of this table";

    private List<Metric> metrics;

    public LongCounterMetric COUNTER_SCAN_BYTES_TOTAL;
    public LongCounterMetric COUNTER_SCAN_ROWS_TOTAL;
    public LongCounterMetric COUNTER_SCAN_FINISHED_TOTAL;

    public LongCounterMetric COUNTER_STREAM_LOAD_BYTES_TOTAL;
    public LongCounterMetric COUNTER_STREAM_LOAD_ROWS_TOTAL;
    public LongCounterMetric COUNTER_STREAM_LOAD_FINISHED_TOTAL;

    public LongCounterMetric COUNTER_ROUTINE_LOAD_BYTES_TOTAL;
    public LongCounterMetric COUNTER_ROUTINE_LOAD_ROWS_TOTAL;
    public LongCounterMetric COUNTER_ROUTINE_LOAD_FINISHED_TOTAL;

    public LongCounterMetric COUNTER_INSERT_LOAD_BYTES_TOTAL;
    public LongCounterMetric COUNTER_INSERT_LOAD_ROWS_TOTAL;
    public LongCounterMetric COUNTER_INSERT_LOAD_FINISHED_TOTAL;

    public LongCounterMetric COUNTER_BROKER_LOAD_BYTES_TOTAL;
    public LongCounterMetric COUNTER_BROKER_LOAD_ROWS_TOTAL;
    public LongCounterMetric COUNTER_BROKER_LOAD_FINISHED_TOTAL;

    public LongCounterMetric COUNTER_SPARK_LOAD_BYTES_TOTAL;
    public LongCounterMetric COUNTER_SPARK_LOAD_ROWS_TOTAL;
    public LongCounterMetric COUNTER_SPARK_LOAD_FINISHED_TOTAL;

    public TableMetricsEntity() {
        initTableMetrics();
    }

    public List<Metric> getMetrics() {
        return metrics;
    }

    protected void initTableMetrics() {
        metrics = Lists.newArrayList();

        // scan metrics
        COUNTER_SCAN_BYTES_TOTAL = new LongCounterMetric(TABLE_SCAN_BYTES, MetricUnit.BYTES, TABLE_SCAN_BYTES_COMMENT);
        metrics.add(COUNTER_SCAN_BYTES_TOTAL);
        COUNTER_SCAN_ROWS_TOTAL = new LongCounterMetric(TABLE_SCAN_ROWS, MetricUnit.ROWS, TABLE_SCAN_ROWS_COMMENT);
        metrics.add(COUNTER_SCAN_ROWS_TOTAL);
        COUNTER_SCAN_FINISHED_TOTAL =
                new LongCounterMetric(TABLE_SCAN_FINISHED, MetricUnit.REQUESTS, TABLE_SCAN_FINISHED_COMMENT);
        metrics.add(COUNTER_SCAN_FINISHED_TOTAL);

        // load metrics
        COUNTER_STREAM_LOAD_BYTES_TOTAL =
                new LongCounterMetric(TABLE_LOAD_BYTES, MetricUnit.BYTES, TABLE_LOAD_BYTES_COMMENT);
        COUNTER_STREAM_LOAD_BYTES_TOTAL.addLabel(new MetricLabel("type", "stream_load"));
        metrics.add(COUNTER_STREAM_LOAD_BYTES_TOTAL);
        COUNTER_STREAM_LOAD_ROWS_TOTAL =
                new LongCounterMetric(TABLE_LOAD_ROWS, MetricUnit.ROWS, TABLE_LOAD_ROWS_COMMENT);
        COUNTER_STREAM_LOAD_ROWS_TOTAL.addLabel(new MetricLabel("type", "stream_load"));
        metrics.add(COUNTER_STREAM_LOAD_ROWS_TOTAL);
        COUNTER_STREAM_LOAD_FINISHED_TOTAL =
                new LongCounterMetric(TABLE_LOAD_FINISHED, MetricUnit.REQUESTS, TABLE_LOAD_FINISHED_COMMENT);
        COUNTER_STREAM_LOAD_FINISHED_TOTAL.addLabel(new MetricLabel("type", "stream_load"));
        metrics.add(COUNTER_STREAM_LOAD_FINISHED_TOTAL);

        COUNTER_ROUTINE_LOAD_BYTES_TOTAL =
                new LongCounterMetric(TABLE_LOAD_BYTES, MetricUnit.BYTES, TABLE_LOAD_BYTES_COMMENT);
        COUNTER_ROUTINE_LOAD_BYTES_TOTAL.addLabel(new MetricLabel("type", "routine_load"));
        metrics.add(COUNTER_ROUTINE_LOAD_BYTES_TOTAL);
        COUNTER_ROUTINE_LOAD_ROWS_TOTAL =
                new LongCounterMetric(TABLE_LOAD_ROWS, MetricUnit.ROWS, TABLE_LOAD_ROWS_COMMENT);
        COUNTER_ROUTINE_LOAD_ROWS_TOTAL.addLabel(new MetricLabel("type", "routine_load"));
        metrics.add(COUNTER_ROUTINE_LOAD_ROWS_TOTAL);
        COUNTER_ROUTINE_LOAD_FINISHED_TOTAL =
                new LongCounterMetric(TABLE_LOAD_FINISHED, MetricUnit.REQUESTS, TABLE_LOAD_FINISHED_COMMENT);
        COUNTER_ROUTINE_LOAD_FINISHED_TOTAL.addLabel(new MetricLabel("type", "routine_load"));
        metrics.add(COUNTER_ROUTINE_LOAD_FINISHED_TOTAL);

        COUNTER_BROKER_LOAD_BYTES_TOTAL =
                new LongCounterMetric(TABLE_LOAD_BYTES, MetricUnit.BYTES, TABLE_LOAD_BYTES_COMMENT);
        COUNTER_BROKER_LOAD_BYTES_TOTAL.addLabel(new MetricLabel("type", "broker_load"));
        metrics.add(COUNTER_BROKER_LOAD_BYTES_TOTAL);
        COUNTER_BROKER_LOAD_ROWS_TOTAL =
                new LongCounterMetric(TABLE_LOAD_ROWS, MetricUnit.ROWS, TABLE_LOAD_ROWS_COMMENT);
        COUNTER_BROKER_LOAD_ROWS_TOTAL.addLabel(new MetricLabel("type", "broker_load"));
        metrics.add(COUNTER_BROKER_LOAD_ROWS_TOTAL);
        COUNTER_BROKER_LOAD_FINISHED_TOTAL =
                new LongCounterMetric(TABLE_LOAD_FINISHED, MetricUnit.REQUESTS, TABLE_LOAD_FINISHED_COMMENT);
        COUNTER_BROKER_LOAD_FINISHED_TOTAL.addLabel(new MetricLabel("type", "broker_load"));
        metrics.add(COUNTER_BROKER_LOAD_FINISHED_TOTAL);

        COUNTER_SPARK_LOAD_BYTES_TOTAL =
                new LongCounterMetric(TABLE_LOAD_BYTES, MetricUnit.BYTES, TABLE_LOAD_BYTES_COMMENT);
        COUNTER_SPARK_LOAD_BYTES_TOTAL.addLabel(new MetricLabel("type", "spark_load"));
        metrics.add(COUNTER_SPARK_LOAD_BYTES_TOTAL);
        COUNTER_SPARK_LOAD_ROWS_TOTAL =
                new LongCounterMetric(TABLE_LOAD_ROWS, MetricUnit.ROWS, TABLE_LOAD_ROWS_COMMENT);
        COUNTER_SPARK_LOAD_ROWS_TOTAL.addLabel(new MetricLabel("type", "spark_load"));
        metrics.add(COUNTER_SPARK_LOAD_ROWS_TOTAL);
        COUNTER_SPARK_LOAD_FINISHED_TOTAL =
                new LongCounterMetric(TABLE_LOAD_FINISHED, MetricUnit.REQUESTS, TABLE_LOAD_FINISHED_COMMENT);
        COUNTER_SPARK_LOAD_FINISHED_TOTAL.addLabel(new MetricLabel("type", "spark_load"));
        metrics.add(COUNTER_SPARK_LOAD_FINISHED_TOTAL);

        COUNTER_INSERT_LOAD_BYTES_TOTAL =
                new LongCounterMetric(TABLE_LOAD_BYTES, MetricUnit.BYTES, TABLE_LOAD_BYTES_COMMENT);
        COUNTER_INSERT_LOAD_BYTES_TOTAL.addLabel(new MetricLabel("type", "insert_into"));
        metrics.add(COUNTER_INSERT_LOAD_BYTES_TOTAL);
        COUNTER_INSERT_LOAD_ROWS_TOTAL =
                new LongCounterMetric(TABLE_LOAD_ROWS, MetricUnit.ROWS, TABLE_LOAD_ROWS_COMMENT);
        COUNTER_INSERT_LOAD_ROWS_TOTAL.addLabel(new MetricLabel("type", "insert_into"));
        metrics.add(COUNTER_INSERT_LOAD_ROWS_TOTAL);
        COUNTER_INSERT_LOAD_FINISHED_TOTAL =
                new LongCounterMetric(TABLE_LOAD_FINISHED, MetricUnit.REQUESTS, TABLE_LOAD_FINISHED_COMMENT);
        COUNTER_INSERT_LOAD_FINISHED_TOTAL.addLabel(new MetricLabel("type", "insert_into"));
        metrics.add(COUNTER_INSERT_LOAD_FINISHED_TOTAL);
    }
}

