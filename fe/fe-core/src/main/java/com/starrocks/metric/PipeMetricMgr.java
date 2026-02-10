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

import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.load.pipe.Pipe;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.server.GlobalStateMgr;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;

/**
 * PipeMetricMgr manages the lifecycle and runtime metrics for Pipes in StarRocks.
 *
 * <p>Design & Implementation Decisions:</p>
 *
 * <p>1. Metric Scope & Labels:</p>
 * <ul>
 *    <li>All Pipe metrics are tagged with `db_id` to allow breakdown by database.
 *    This is crucial for multi-tenant monitoring.</li>
 *    <li>`pipe_type` (FILE, KAFKA) distinguishes the source type.</li>
 *    <li>Lifecycle counters: `creation`, `drop`, `alter` track user DDL operations.</li>
 *    <li>Runtime metrics: `schedule` tracks the schedule frequency.</li>
 *    <li>Data metrics: `loaded_files`, `loaded_bytes`, `loaded_rows` track total throughput.</li>
 * </ul>
 *
 * <p>2. LEADER-ONLY Metrics:</p>
 * <ul>
 *    <li>These metrics are collected <b>ONLY</b> on the FE Leader node.</li>
 *    <li>Integration points are placed in {@link com.starrocks.load.pipe.PipeManager} (create/drop/alter)
 *         which handles user requests, and {@link com.starrocks.load.pipe.Pipe} (task build/finish)
 *         which handles task scheduling.</li>
 *    <li>FE Followers/Observers replay states via {@link com.starrocks.load.pipe.PipeRepo} which <b>bypasses</b>
 *        these metric calls.
 *        This ensures proper behavior: metrics reflect the active scheduler state (Leader), and Followers
 *        report 0/empty metrics instead of duplicate or stale values.</li>
 * </ul>
 *
 * <p>3. Memory Management & Auto-Expiration:</p>
 * <ul>
 *    <li>Counters are stored in {@code METRIC_MAP} wrapper to track {@code lastAccessTime}.</li>
 *    <li>A background {@code CLEANUP_TIMER} runs periodically (every 10 min) to scan and remove metrics
 *        that haven't been touched for {@link Config#pipe_metric_expire_minutes} (default 3 days).</li>
 *    <li>This prevents memory leaks from dropped databases or inactive pipes in long-running FE processes,
 *        without requiring explicit cleanup hooks for every possible exit path.</li>
 * </ul>
 */
public class PipeMetricMgr {
    private static final String LABEL_DB_ID = "db_id";

    // FILE, KAFKA
    private static final String LABEL_PIPE_TYPE = "pipe_type";

    // SUSPEND, RUNNING, FINISHED, ERROR
    private static final String LABEL_STATUS = "status";

    // SUCCESS, FAILED
    private static final String LABEL_DONE_STATUS = "done_status";

    // Map for DB-level counters to allow manual expiration of inactive metrics
    private static final Map<MetricKey, MetricHolder> METRIC_MAP = Maps.newConcurrentMap();

    // Wrapper to track last access time for LRU-like expiration
    private static class MetricHolder {
        LongCounterMetric metric;
        long lastAccessTime;

        MetricHolder(LongCounterMetric metric) {
            this.metric = metric;
            this.lastAccessTime = System.currentTimeMillis();
        }
    }

    // Timer to clean up expired metrics based on Config.pipe_metric_expire_minutes
    // This allows dynamic adjustment of retention period without restarting FE
    private static Timer CLEANUP_TIMER;

    public static void startPipeMetricCleanTimer() {
        if (!FeConstants.runningUnitTest) {
            CLEANUP_TIMER = new Timer("PipeMetricCleanup", true);
            CLEANUP_TIMER.schedule(new TimerTask() {
                @Override
                public void run() {
                    cleanupExpiredMetrics();
                }
            }, 600000, 600000); // Check every 10 minutes
        } else {
            CLEANUP_TIMER = null;
        }
    }

    // Visible for testing
    static void cleanupExpiredMetrics() {
        long now = System.currentTimeMillis();
        long expireMs = Config.pipe_metric_expire_minutes * 60 * 1000L;
        Iterator<Map.Entry<MetricKey, MetricHolder>> it = METRIC_MAP.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MetricKey, MetricHolder> entry = it.next();
            if (now - entry.getValue().lastAccessTime > expireMs) {
                MetricRepo.removeMetric(entry.getValue().metric);
                it.remove();
            }
        }
        // Also clean up expired state gauges
        cleanupExpiredStateGauges(now, expireMs);
    }

    private static class MetricKey {
        String group;
        long dbId;
        String labelValue;

        public MetricKey(String group, long dbId, String labelValue) {
            this.group = group;
            this.dbId = dbId;
            this.labelValue = labelValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MetricKey metricKey = (MetricKey) o;
            return dbId == metricKey.dbId && Objects.equals(group, metricKey.group) &&
                    Objects.equals(labelValue, metricKey.labelValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(group, dbId, labelValue);
        }
    }

    // --- Lifecycle ---

    public static void incPipeCreation(long dbId, String pipeType) {
        getOrCreateCounter("creation", "pipe_creation", "number of pipes created",
                MetricUnit.REQUESTS, dbId, LABEL_PIPE_TYPE, pipeType)
                .increase(1L);
    }

    public static void incPipeSchedule(long dbId, String pipeType) {
        getOrCreateCounter("schedule", "pipe_schedule_count", "number of pipe schedule calls",
                MetricUnit.REQUESTS, dbId, LABEL_PIPE_TYPE, pipeType)
                .increase(1L);
    }

    public static void incPipeDrop(long dbId, String pipeType) {
        getOrCreateCounter("drop", "pipe_drop", "number of pipes dropped",
                MetricUnit.REQUESTS, dbId, LABEL_PIPE_TYPE, pipeType)
                .increase(1L);
    }

    public static void incPipeAlter(long dbId, String pipeType) {
        getOrCreateCounter("alter", "pipe_alter", "number of pipes altered",
                MetricUnit.REQUESTS, dbId, LABEL_PIPE_TYPE, pipeType)
                .increase(1L);
    }

    // --- Running State ---

    // Map for pipe state gauges, keyed by (dbId, pipeType, state)
    private static final Map<MetricKey, GaugeHolder> STATE_GAUGE_MAP = Maps.newConcurrentMap();

    private static class GaugeHolder {
        GaugeMetricImpl<Long> metric;
        long lastAccessTime;

        GaugeHolder(GaugeMetricImpl<Long> metric) {
            this.metric = metric;
            this.lastAccessTime = System.currentTimeMillis();
        }
    }

    /**
     * Refresh pipe state gauges by iterating all pipes and updating counts.
     * This should be called on each scheduler run to keep gauges up-to-date.
     * Gauges are created/updated per (dbId, pipeType, state) combination.
     */
    public static void refreshPipeStateGauges() {
        try {
            List<Pipe> pipes = GlobalStateMgr.getCurrentState().getPipeManager().getAllPipes();

            // Count pipes by (dbId, pipeType, state)
            Map<MetricKey, Long> counts = Maps.newHashMap();
            for (Pipe pipe : pipes) {
                long dbId = pipe.getPipeId().getDbId();
                String pipeType = pipe.getTypeName();
                String state = pipe.getState().name();
                MetricKey key = new MetricKey("state_" + state, dbId, pipeType);
                counts.merge(key, 1L, Long::sum);
            }

            // Update or create gauges for active combinations
            for (Map.Entry<MetricKey, Long> entry : counts.entrySet()) {
                MetricKey key = entry.getKey();
                long count = entry.getValue();
                String state = key.group.substring("state_".length());

                GaugeHolder holder = STATE_GAUGE_MAP.computeIfAbsent(key, k -> {
                    GaugeMetricImpl<Long> metric = new GaugeMetricImpl<>("pipe_state_count", MetricUnit.NOUNIT,
                            "number of pipes in this state");
                    metric.addLabel(new MetricLabel(LABEL_DB_ID, String.valueOf(key.dbId)));
                    metric.addLabel(new MetricLabel(LABEL_PIPE_TYPE, key.labelValue));
                    metric.addLabel(new MetricLabel(LABEL_STATUS, state));
                    metric.setValue(0L);
                    MetricRepo.addMetric(metric);
                    return new GaugeHolder(metric);
                });
                holder.metric.setValue(count);
                holder.lastAccessTime = System.currentTimeMillis();
            }

            // Set gauges not in counts to 0 (but don't remove, let cleanup handle expiration)
            for (Map.Entry<MetricKey, GaugeHolder> entry : STATE_GAUGE_MAP.entrySet()) {
                if (!counts.containsKey(entry.getKey())) {
                    entry.getValue().metric.setValue(0L);
                }
            }
        } catch (Exception e) {
            // Ignore errors during refresh
        }
    }

    /**
     * Clean up expired state gauges. Called by cleanupExpiredMetrics().
     */
    private static void cleanupExpiredStateGauges(long now, long expireMs) {
        Iterator<Map.Entry<MetricKey, GaugeHolder>> it = STATE_GAUGE_MAP.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MetricKey, GaugeHolder> entry = it.next();
            GaugeHolder holder = entry.getValue();
            // Only expire if value is 0 and not accessed for a long time
            if (holder.metric.getValue() == 0 && now - holder.lastAccessTime > expireMs) {
                MetricRepo.removeMetric(holder.metric);
                it.remove();
            }
        }
    }

    public static void incPipeCompleteTasks(long dbId, String pipeType, String doneStatus, long delta) {
        if (delta < 0) {
            return;
        }
        // Use group="complete_tasks_" + status to allow partitioning by status in the key,
        // and labelValue=type to allow removal by type.
        String group = "complete_tasks_" + doneStatus;
        MetricKey key = new MetricKey(group, dbId, pipeType);

        MetricHolder holder = METRIC_MAP.computeIfAbsent(key, k -> {
            LongCounterMetric metric = new LongCounterMetric("pipe_complete_tasks", MetricUnit.REQUESTS,
                    "number of completed pipe subtasks");
            metric.addLabel(new MetricLabel(LABEL_DB_ID, String.valueOf(dbId)));
            metric.addLabel(new MetricLabel(LABEL_PIPE_TYPE, pipeType));
            metric.addLabel(new MetricLabel(LABEL_DONE_STATUS, doneStatus));
            MetricRepo.addMetric(metric);
            return new MetricHolder(metric);
        });
        holder.lastAccessTime = System.currentTimeMillis();
        holder.metric.increase(delta);
    }

    // --- Data Load State ---

    public static void incPipeLoadedFiles(long dbId, String pipeType, long delta) {
        if (delta < 0) {
            return;
        }
        getOrCreateCounter("loaded_files", "pipe_loaded_files", "total loaded files",
                MetricUnit.NOUNIT, dbId, LABEL_PIPE_TYPE, pipeType)
                .increase(delta);
    }

    public static void incPipeLoadedBytes(long dbId, String pipeType, long delta) {
        if (delta < 0) {
            return;
        }
        getOrCreateCounter("loaded_bytes", "pipe_loaded_bytes", "total loaded bytes",
                MetricUnit.BYTES, dbId, LABEL_PIPE_TYPE, pipeType)
                .increase(delta);
    }

    public static void incPipeLoadedRows(long dbId, String pipeType, long delta) {
        if (delta < 0) {
            return;
        }
        getOrCreateCounter("loaded_rows", "pipe_loaded_rows", "total loaded rows",
                MetricUnit.ROWS, dbId, LABEL_PIPE_TYPE, pipeType)
                .increase(delta);
    }

    // --- Helpers ---

    private static LongCounterMetric getOrCreateCounter(String group,
                                                        String name, String desc,
                                                        MetricUnit unit,
                                                        long dbId, String labelName, String labelValue) {
        MetricKey key = new MetricKey(group, dbId, labelValue);
        MetricHolder holder = METRIC_MAP.computeIfAbsent(key, k -> {
            LongCounterMetric metric = new LongCounterMetric(name, unit, desc);
            metric.addLabel(new MetricLabel(LABEL_DB_ID, String.valueOf(dbId)));
            metric.addLabel(new MetricLabel(labelName, labelValue));
            MetricRepo.addMetric(metric);
            return new MetricHolder(metric);
        });
        holder.lastAccessTime = System.currentTimeMillis();
        return holder.metric;
    }
}
