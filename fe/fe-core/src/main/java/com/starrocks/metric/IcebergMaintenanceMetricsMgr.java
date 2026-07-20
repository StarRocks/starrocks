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

import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.connector.iceberg.procedure.IcebergMaintenanceTaskStats;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Metrics manager for iceberg auto metadata maintenance (AMM).
 * All metrics carry a `catalog` label; counters are created lazily on first use.
 *
 * Metric naming: iceberg_amm_{metric}, e.g. iceberg_amm_check_total,
 * iceberg_amm_orphan_file_removed_total. The planning-latency histogram is the
 * exception: it is registered as iceberg_planning_latency_ms (no amm prefix)
 * because it is driven by the query path and covers every catalog, not just the
 * ones with auto maintenance enabled.
 */
public class IcebergMaintenanceMetricsMgr {

    public static final String ACTION_EXPIRE_SNAPSHOTS = "expire_snapshots";
    public static final String ACTION_REMOVE_ORPHAN_FILES = "remove_orphan_files";
    public static final String ACTION_REWRITE_MANIFESTS = "rewrite_manifests";

    private static final String LABEL_CATALOG = "catalog";
    private static final String LABEL_ACTION = "action";
    private static final String LABEL_STATUS = "status";

    // keyed by "catalog" or "catalog|action|status"
    private static final ConcurrentHashMap<String, LongCounterMetric> CHECK_TOTAL = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> CHECK_DURATION_MS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> EXECUTE_TOTAL = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> EXECUTE_DURATION_MS = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> SNAPSHOT_COUNT_INPUT = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> SNAPSHOT_COUNT_OUTPUT = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> MANIFEST_COUNT_INPUT = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> MANIFEST_COUNT_OUTPUT = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> MANIFEST_BYTES_OUTPUT = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> MANIFEST_SMALL_FILES_OUTPUT =
            new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> ORPHAN_FILES_REMOVED = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, LongCounterMetric> ORPHAN_BYTES_REMOVED = new ConcurrentHashMap<>();

    /**
     * One maintenance check pass over a catalog (table listing + due decision).
     */
    public static void recordCheck(String catalog, long durationMs) {
        catalogCounter(CHECK_TOTAL, "iceberg_amm_check_total", Metric.MetricUnit.REQUESTS,
                "total iceberg auto maintenance check passes by catalog", catalog)
                .increase(1L);
        catalogCounter(CHECK_DURATION_MS, "iceberg_amm_check_duration_ms_total", Metric.MetricUnit.MILLISECONDS,
                "total duration in milliseconds of iceberg auto maintenance checks by catalog", catalog)
                .increase(durationMs);
    }

    /**
     * One maintenance action on one table (auto path only). {@code status} is the
     * task's final status — success / skipped / failed / partial — used verbatim as
     * the metric label; {@code skipped} means the action ran but had nothing to do.
     */
    public static void recordExecute(String catalog, String action, String status, long durationMs) {
        String key = catalog + "|" + action + "|" + status;
        EXECUTE_TOTAL.computeIfAbsent(key, k -> {
            LongCounterMetric metric = new LongCounterMetric("iceberg_amm_execute_total",
                    Metric.MetricUnit.REQUESTS,
                    "total iceberg auto maintenance executions by catalog, action and status");
            metric.addLabel(new MetricLabel(LABEL_CATALOG, catalog));
            metric.addLabel(new MetricLabel(LABEL_ACTION, action));
            metric.addLabel(new MetricLabel(LABEL_STATUS, status));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(1L);

        String durationKey = catalog + "|" + action;
        EXECUTE_DURATION_MS.computeIfAbsent(durationKey, k -> {
            LongCounterMetric metric = new LongCounterMetric("iceberg_amm_execute_duration_ms_total",
                    Metric.MetricUnit.MILLISECONDS,
                    "total duration in milliseconds of iceberg auto maintenance executions by catalog and action");
            metric.addLabel(new MetricLabel(LABEL_CATALOG, catalog));
            metric.addLabel(new MetricLabel(LABEL_ACTION, action));
            MetricRepo.addMetric(metric);
            return metric;
        }).increase(durationMs);
    }

    /**
     * Physical effect of one executed maintenance action; recorded from the auto
     * maintenance path only. The effect counters reflect actual change while
     * skip visibility is left to iceberg_amm_execute_total{status="skipped"}.
     */
    public static void reportEffectMetrics(String catalog, IcebergMaintenanceTaskStats stats) {
        if (stats == null || stats.getOperation() == null) {
            return;
        }
        if (stats.getOperation() != IcebergTableOperation.REMOVE_ORPHAN_FILES && !stats.isCommitted()) {
            return;
        }
        if (!stats.hasMaterialChange()) {
            return;
        }
        switch (stats.getOperation()) {
            case EXPIRE_SNAPSHOTS:
                increaseIfSet(SNAPSHOT_COUNT_INPUT, "iceberg_amm_snapshot_count_input",
                        Metric.MetricUnit.NOUNIT,
                        "total snapshot count before iceberg expire_snapshots executions by catalog", catalog,
                        stats.getSnapshotCountInput());
                increaseIfSet(SNAPSHOT_COUNT_OUTPUT, "iceberg_amm_snapshot_count_output",
                        Metric.MetricUnit.NOUNIT,
                        "total snapshot count after iceberg expire_snapshots executions by catalog", catalog,
                        stats.getSnapshotCountOutput());
                break;
            case REWRITE_MANIFESTS:
                increaseIfSet(MANIFEST_COUNT_INPUT, "iceberg_amm_manifest_file_count_input",
                        Metric.MetricUnit.NOUNIT,
                        "total manifest file count before iceberg rewrite_manifests executions by catalog", catalog,
                        stats.getManifestCountInput());
                increaseIfSet(MANIFEST_COUNT_OUTPUT, "iceberg_amm_manifest_file_count_output",
                        Metric.MetricUnit.NOUNIT,
                        "total manifest file count after iceberg rewrite_manifests executions by catalog", catalog,
                        stats.getManifestCountOutput());
                increaseIfSet(MANIFEST_BYTES_OUTPUT, "iceberg_amm_manifest_bytes_total_output",
                        Metric.MetricUnit.BYTES,
                        "total manifest bytes after iceberg rewrite_manifests executions by catalog", catalog,
                        stats.getManifestBytesOutput());
                increaseIfSet(MANIFEST_SMALL_FILES_OUTPUT,
                        "iceberg_amm_rewrite_manifest_small_files_total_output", Metric.MetricUnit.NOUNIT,
                        "total output manifest files smaller than the target size after iceberg "
                                + "rewrite_manifests executions by catalog", catalog,
                        stats.getManifestSmallFilesOutput());
                break;
            case REMOVE_ORPHAN_FILES:
                catalogCounter(ORPHAN_FILES_REMOVED, "iceberg_amm_orphan_file_removed_total",
                        Metric.MetricUnit.NOUNIT,
                        "total orphan files removed by iceberg remove_orphan_files executions by catalog", catalog)
                        .increase(stats.getOrphanFilesRemoved());
                catalogCounter(ORPHAN_BYTES_REMOVED, "iceberg_amm_orphan_bytes_removed_total",
                        Metric.MetricUnit.BYTES,
                        "total bytes reclaimed by iceberg remove_orphan_files executions by catalog", catalog)
                        .increase(stats.getOrphanBytesRemoved());
                break;
            default:
                break;
        }
    }

    /**
     * Record one iceberg split planning latency sample into the per-catalog histogram
     * iceberg_planning_latency_ms (same pattern as catalog_query_latency_ms). The
     * histogram exposes count + quantiles, letting PromQL compute time-windowed
     * averages/percentiles of the planning-latency trend as maintenance takes effect.
     */
    public static void recordPlanningLatencyMs(String catalog, long latencyMs) {
        MetricRepo.getOrCreateIcebergPlanningLatencyHistogram(catalog).update(latencyMs);
    }

    private static LongCounterMetric catalogCounter(ConcurrentHashMap<String, LongCounterMetric> holder,
                                                    String name, Metric.MetricUnit unit, String description,
                                                    String catalog) {
        return holder.computeIfAbsent(catalog, k -> {
            LongCounterMetric metric = new LongCounterMetric(name, unit, description);
            metric.addLabel(new MetricLabel(LABEL_CATALOG, catalog));
            MetricRepo.addMetric(metric);
            return metric;
        });
    }

    /**
     * Report a "measured" effect value (snapshot/manifest counts and bytes). A -1 value is the
     * sentinel for "not collected" (e.g. the output side before a successful commit); in that case
     * the counter is neither incremented nor even created
     */
    private static void increaseIfSet(ConcurrentHashMap<String, LongCounterMetric> holder,
                                      String name, Metric.MetricUnit unit, String description,
                                      String catalog, long value) {
        if (value >= 0) {
            catalogCounter(holder, name, unit, description, catalog).increase(value);
        }
    }
}
