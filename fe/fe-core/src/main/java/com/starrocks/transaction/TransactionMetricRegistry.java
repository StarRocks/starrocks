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

package com.starrocks.transaction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.Config;
import com.starrocks.metric.LeaderAwareHistogramMetric;
import com.starrocks.metric.MetricLabel;
import com.starrocks.metric.MetricVisitor;
import com.starrocks.server.GlobalStateMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * A central registry for collecting and managing transaction latency metrics.
 *
 * <p>This class tracks performance characteristics of different transaction phases, such as
 * write, publish, and total duration. It is used within the StarRocks frontend to monitor
 * transaction performance, helping developers and administrators identify bottlenecks in various
 * load jobs.
 *
 * <p>The class maintains groups of histogram metrics, with a default aggregated "all" group
 * and other groups that can be dynamically enabled or disabled via the
 * {@code Config.txn_latency_metric_report_groups} configuration. It follows a singleton pattern
 * and registers a listener to respond to configuration changes at runtime.
 *
 * <p><b>Metric Grouping Example:</b>
 * This registry aggregates various transaction source types into logical groups for unified
 * reporting. By default, an "all" group is always enabled, which aggregates metrics
 * across all transaction types. Additionally, specific groups can be enabled via configuration.
 * For instance, transactions from {@code BACKEND_STREAMING}, {@code FRONTEND_STREAMING},
 * and {@code MULTI_STATEMENT_STREAMING} are all categorized under the "stream_load" group.
 * When metrics are exported (e.g., to Prometheus), they will include a 'type' label
 * corresponding to the group name. A sample metric might look like:
 * <pre>
 *   starrocks_fe_txn_total_latency_ms{type="all", quantile="0.99"} 150.78
 *   starrocks_fe_txn_total_latency_ms{type="stream_load", quantile="0.99"} 123.45
 *   starrocks_fe_txn_total_latency_ms{type="routine_load", quantile="0.99"} 234.56
 * </pre>
 * This allows for monitoring both the overall transaction performance (using "all") and the
 * performance of specific load types like "stream_load", abstracting away the specific
 * submission mechanism.
 */
public class TransactionMetricRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionMetricRegistry.class);

    private static volatile TransactionMetricRegistry INSTANCE;

    // Aggregated metrics across all source types
    private final TransactionMetricGroup allTxnGroup;
    // Mapping from group name (e.g. "stream_load") to the metric group
    private final Map<String, TransactionMetricGroup> metricGroupsByName;
    // Fast index from source type ordinal to its owning group
    private final TransactionMetricGroup[] sourceTypeIndexToGroup;

    // Last seen config string used to determine report groups
    @Nullable private volatile String prevReportGroupsConfig;
    // Snapshot of groups currently enabled for reporting; may be null/empty
    @Nullable private volatile ImmutableList<TransactionMetricGroup> reportGroups;

    /**
     * Returns the singleton instance of the TransactionMetricRegistry.
     *
     * @return The singleton {@link TransactionMetricRegistry} instance.
     */
    public static TransactionMetricRegistry getInstance() {
        TransactionMetricRegistry inst = INSTANCE;
        if (inst == null) {
            synchronized (TransactionMetricRegistry.class) {
                if (INSTANCE == null) {
                    INSTANCE = new TransactionMetricRegistry();
                }
                inst = INSTANCE;
            }
        }
        return inst;
    }

    /** Initializes groups and registers a config refresh listener. */
    private TransactionMetricRegistry() {
        this(true);
    }

    /** Internal constructor with option to skip registering config listener (for tests). */
    private TransactionMetricRegistry(boolean registerConfigListener) {
        this.allTxnGroup = new TransactionMetricGroup("all", TransactionState.LoadJobSourceType.values());
        this.allTxnGroup.metrics = Optional.of(buildLatencyMetrics(allTxnGroup.name));
        this.metricGroupsByName = buildMetricGroups();
        this.sourceTypeIndexToGroup = new TransactionMetricGroup[TransactionState.LoadJobSourceType.values().length];
        for (TransactionMetricGroup group : metricGroupsByName.values()) {
            for (TransactionState.LoadJobSourceType sourceType : group.sourceTypes) {
                sourceTypeIndexToGroup[sourceType.ordinal()] = group;
            }
        }
        updateConfig();
        if (registerConfigListener) {
            GlobalStateMgr.getCurrentState().getConfigRefreshDaemon().registerListener(this::updateConfig);
        }
    }

    /**
     * Create a new instance for tests to avoid interfering with the global singleton and listeners.
     */
    @VisibleForTesting
    static TransactionMetricRegistry createForTest() {
        return new TransactionMetricRegistry(false);
    }

    /**
     * Updates the relevant latency histograms based on the timing information from a completed transaction.
     *
     * <p>This method only processes transactions that have reached the {@link TransactionStatus#VISIBLE}
     * status. It calculates latencies for total duration, write phase, and various publish sub-phases
     * from the transaction state's timestamps. It then updates both the aggregated "all" group and the
     * specific group corresponding to the transaction's source type.
     *
     * @param txnState The {@code TransactionState} object of a finished transaction, containing the timing data.
     */
    public void update(TransactionState txnState) {
        try {
            if (txnState.getTransactionStatus() != TransactionStatus.VISIBLE) {
                return;
            }
            long prepareTime = txnState.getPrepareTime();
            long commitTime = txnState.getCommitTime();
            long publishVersionTime = txnState.getPublishVersionTime();
            long publishVersionFinishTime = txnState.getPublishVersionFinishTime();
            long finishTime = txnState.getFinishTime();
            updateLatencyMetrics(allTxnGroup.metrics,
                    prepareTime, commitTime, publishVersionTime, publishVersionFinishTime, finishTime);
            TransactionState.LoadJobSourceType sourceType = txnState.getSourceType();
            if (sourceType == null || sourceType.ordinal() >= sourceTypeIndexToGroup.length) {
                return;
            }
            updateLatencyMetrics(sourceTypeIndexToGroup[sourceType.ordinal()].metrics,
                    prepareTime, commitTime, publishVersionTime, publishVersionFinishTime, finishTime);
        } catch (Exception e) {
            LOG.debug("failed to update transaction metrics, {}", txnState, e);
        }
    }

    private void updateLatencyMetrics(Optional<TransactionMetrics> metricsOptional, long prepareTime, long commitTime,
                                      long publishVersionTime, long publishVersionFinishTime, long finishTime) {
        TransactionMetrics metrics = metricsOptional.orElse(null);
        if (metrics != null) {
            recordLatencyIfValid(metrics.totalLatencyMs, prepareTime, finishTime);
            recordLatencyIfValid(metrics.writeLatencyMs, prepareTime, commitTime);
            recordLatencyIfValid(metrics.publishLatencyMs, commitTime, finishTime);
            recordLatencyIfValid(metrics.publishScheduleLatencyMs, commitTime, publishVersionTime);
            recordLatencyIfValid(metrics.publishExecuteLatencyMs, publishVersionTime, publishVersionFinishTime);
            recordLatencyIfValid(metrics.publishAckLatencyMs, publishVersionFinishTime, finishTime);
        }
    }

    private void recordLatencyIfValid(LeaderAwareHistogramMetric metric, long fromTime, long toTime) {
        if (fromTime > 0 && fromTime <= toTime) {
            metric.update(toTime - fromTime);
        }
    }

    /**
     * Reports all enabled transaction metric histograms to a metric visitor.
     *
     * <p>This method first reports the metrics for the mandatory "all" group, and then iterates through
     * the current snapshot of enabled groups to report their respective metrics.
     *
     * @param visitor The {@link MetricVisitor} to which the histogram metrics will be reported.
     */
    public void report(MetricVisitor visitor) {
        try {
            reportGroupMetrics(visitor, allTxnGroup);
            ImmutableList<TransactionMetricGroup> txnGroups = reportGroups;
            if (txnGroups == null) {
                return;
            }
            for (TransactionMetricGroup txnGroup : txnGroups) {
                reportGroupMetrics(visitor, txnGroup);
            }
        } catch (Exception e) {
            LOG.debug("failed to report transaction metrics", e);
        }
    }

    /** Report histograms for a single group if enabled. */
    private void reportGroupMetrics(MetricVisitor visitor, TransactionMetricGroup txnGroup) {
        TransactionMetrics metrics = txnGroup.metrics.orElse(null);
        if (metrics == null) {
            return;
        }
        visitor.visitHistogram(metrics.totalLatencyMs);
        visitor.visitHistogram(metrics.writeLatencyMs);
        visitor.visitHistogram(metrics.publishLatencyMs);
        visitor.visitHistogram(metrics.publishScheduleLatencyMs);
        visitor.visitHistogram(metrics.publishExecuteLatencyMs);
        visitor.visitHistogram(metrics.publishAckLatencyMs);
    }

    /** Build all possible metric groups and map them by normalized name. */
    private Map<String, TransactionMetricGroup> buildMetricGroups() {
        Map<String, TransactionMetricGroup> groups = new HashMap<>();
        for (TransactionState.LoadJobSourceType sourceType : TransactionState.LoadJobSourceType.values()) {
            String groupName = switch (sourceType) {
                // normal stream load / transaction stream load / parallel transaction stream load / merge commit / multiple statement
                case BACKEND_STREAMING, FRONTEND_STREAMING, MULTI_STATEMENT_STREAMING -> "stream_load";
                case ROUTINE_LOAD_TASK -> "routine_load";
                case BATCH_LOAD_JOB -> "broker_load";
                case INSERT_STREAMING -> "insert";
                case LAKE_COMPACTION -> "compaction";
                default -> sourceType.name().toLowerCase();
            };
            TransactionMetricGroup group = groups.computeIfAbsent(groupName, TransactionMetricGroup::new);
            group.sourceTypes.add(sourceType);
        }
        return groups;
    }

    /** Create the six latency histograms with a shared type label for a group. */
    private TransactionMetrics buildLatencyMetrics(String groupName) {
        LeaderAwareHistogramMetric totalLatencyMs = createHistogram("txn_total_latency_ms", groupName);
        LeaderAwareHistogramMetric writeLatencyMs = createHistogram("txn_write_latency_ms", groupName);
        LeaderAwareHistogramMetric publishLatencyMs = createHistogram("txn_publish_latency_ms", groupName);
        LeaderAwareHistogramMetric publishScheduleLatencyMs = createHistogram("txn_publish_schedule_latency_ms", groupName);
        LeaderAwareHistogramMetric publishExecuteLatencyMs = createHistogram("txn_publish_execute_latency_ms", groupName);
        LeaderAwareHistogramMetric publishAckLatencyMs = createHistogram("txn_publish_ack_latency_ms", groupName);
        return new TransactionMetrics(totalLatencyMs, writeLatencyMs, publishLatencyMs,
                publishScheduleLatencyMs, publishExecuteLatencyMs, publishAckLatencyMs);

    }

    /** Create a type-labeled leader-aware histogram metric. */
    private LeaderAwareHistogramMetric createHistogram(String name, String typeLabel) {
        LeaderAwareHistogramMetric histogram = new LeaderAwareHistogramMetric(name);
        histogram.addLabel(new MetricLabel("type", typeLabel));
        return histogram;
    }

    /**
     * Refreshes the set of enabled metric groups based on the current system configuration.
     *
     * <p>It reads the {@code Config.txn_latency_metric_report_groups} comma-separated string, parses the
     * group names, and updates the internal list of enabled groups. This method creates metric
     * objects for newly enabled groups and clears them for disabled groups to manage resource
     * usage. The method is synchronized to prevent concurrent modification issues.
     */
    @VisibleForTesting
    synchronized void updateConfig() {
        String currentConfig = Config.txn_latency_metric_report_groups;
        if (Objects.equals(prevReportGroupsConfig, currentConfig)) {
            return;
        }
        prevReportGroupsConfig = currentConfig;
        try {
            Set<String> groupNames = Arrays.stream(currentConfig.split(",")).map(name -> name.trim().toLowerCase())
                    .filter(name -> !name.isEmpty()).collect(Collectors.toSet());
            ImmutableList.Builder<TransactionMetricGroup> reportMetricsBuilder = ImmutableList.builder();
            for (TransactionMetricGroup group : metricGroupsByName.values()) {
                if (groupNames.contains(group.name)) {
                    if (group.metrics.isEmpty()) {
                        group.metrics = Optional.of(buildLatencyMetrics(group.name));
                    }
                    reportMetricsBuilder.add(group);
                    LOG.info("enable txn group [{}]", group.name);
                } else {
                    if (group.metrics.isPresent()) {
                        group.metrics = Optional.empty();
                        LOG.info("disable txn group [{}]", group.name);
                    }
                }
            }
            this.reportGroups = reportMetricsBuilder.build();
        } catch (Exception e) {
            LOG.error("failed to update to new config [{}]", currentConfig, e);
        }
    }

    /**
     * A group of transaction source types sharing one set of latency histograms.
     *
     * <p>This class is used to aggregate similar load job types (e.g., various stream load
     * types into a single "stream_load" group) for unified metric reporting.
     *
     * <p>It holds a name, a set of source types, and an {@link Optional} {@link TransactionMetrics}
     * instance, which is present only when the group is enabled for reporting.
     */
    private static class TransactionMetricGroup {
        /** The normalized, unique name for the metric group (e.g., "stream_load"). */
        final String name;
        /** The set of {@link TransactionState.LoadJobSourceType}s that belong to this group. */
        final EnumSet<TransactionState.LoadJobSourceType> sourceTypes;
        /** Holds the metrics for this group, which is empty if metrics are not enabled. */
        volatile Optional<TransactionMetrics> metrics;

        public TransactionMetricGroup(String name, TransactionState.LoadJobSourceType... sourceTypes) {
            this.name = name;
            this.sourceTypes = EnumSet.noneOf(TransactionState.LoadJobSourceType.class);
            this.sourceTypes.addAll(Arrays.asList(sourceTypes));
            this.metrics = Optional.empty();
        }
    }

    /**
     * A data holder for the specific latency histogram metrics tracked for a transaction group.
     *
     * <p>An instance of this class is created for each active {@link TransactionMetricGroup} to store its
     * associated histogram objects.
     *
     * <p>The latencies have the following relationship:
     * <ul>
     *   <li>{@code totalLatencyMs = writeLatencyMs + publishLatencyMs}</li>
     *   <li>{@code publishLatencyMs = publishScheduleLatencyMs + publishExecuteLatencyMs + publishAckLatencyMs}</li>
     * </ul>
     */
    private static class TransactionMetrics {
        /** Histogram for the total transaction latency, from prepare to finish. */
        final LeaderAwareHistogramMetric totalLatencyMs;
        /** Histogram for the write phase latency, from prepare to commit. */
        final LeaderAwareHistogramMetric writeLatencyMs;
        /** Histogram for the publish phase latency, from commit to finish. */
        final LeaderAwareHistogramMetric publishLatencyMs;
        /** Histogram for the publish schedule latency, from commit to when the publish task is taken. */
        final LeaderAwareHistogramMetric publishScheduleLatencyMs;
        /** Histogram for the publish execution latency, from when the task is taken to when it finishes. */
        final LeaderAwareHistogramMetric publishExecuteLatencyMs;
        /**
         * Histogram for the publish acknowledgment latency, from when the task finishes to
         * when the transaction is marked visible.
         */
        final LeaderAwareHistogramMetric publishAckLatencyMs;

        public TransactionMetrics(LeaderAwareHistogramMetric totalLatencyMs,
                                  LeaderAwareHistogramMetric writeLatencyMs,
                                  LeaderAwareHistogramMetric publishLatencyMs,
                                  LeaderAwareHistogramMetric publishScheduleLatencyMs,
                                  LeaderAwareHistogramMetric publishExecuteLatencyMs,
                                  LeaderAwareHistogramMetric publishAckLatencyMs) {
            this.totalLatencyMs = totalLatencyMs;
            this.writeLatencyMs = writeLatencyMs;
            this.publishLatencyMs = publishLatencyMs;
            this.publishScheduleLatencyMs = publishScheduleLatencyMs;
            this.publishExecuteLatencyMs = publishExecuteLatencyMs;
            this.publishAckLatencyMs = publishAckLatencyMs;
        }
    }
}
