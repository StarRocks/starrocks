// Copyright 2025-present StarRocks, Inc. All rights reserved.
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

package com.starrocks.common.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Pair;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Enhanced profile formatter with tier-based display, metric grouping, and rule-of-thumb guidance.
 * Implements requirements from issue #60952 for improved query profile readability.
 */
public class TieredProfileFormatter implements RuntimeProfile.ProfileFormatter {
    
    public enum MetricTier {
        BASIC(0),      // ≤5 most critical metrics per operator
        ADVANCED(1),   // Additional performance metrics for experienced users
        TRACE(2);      // Developer and debugging metrics
        
        private final int level;
        
        MetricTier(int level) {
            this.level = level;
        }
        
        public int getLevel() { return level; }
        
        public boolean includes(MetricTier other) {
            return this.level >= other.level;
        }
    }
    
    public enum MetricGroup {
        TIMING("Timing Metrics"),
        MEMORY("Memory Metrics"), 
        IO("I/O Metrics"),
        NETWORK("Network Metrics"),
        RUNTIME_FILTER("Runtime Filter Metrics"),
        HASH_TABLE("Hash Table Metrics"),
        SPILL("Spill Metrics"),
        ROWS("Row Processing Metrics"),
        GENERAL("General Metrics");
        
        private final String displayName;
        
        MetricGroup(String displayName) {
            this.displayName = displayName;
        }
        
        public String getDisplayName() { return displayName; }
    }
    
    private final StringBuilder builder;
    private final MetricTier maxTier;
    private final boolean showZeroValues;
    private final boolean showRuleOfThumb;
    private final boolean groupMetrics;
    
    public TieredProfileFormatter() {
        this(MetricTier.ADVANCED, false, true, true);
    }
    
    public TieredProfileFormatter(MetricTier maxTier, boolean showZeroValues, 
                                 boolean showRuleOfThumb, boolean groupMetrics) {
        this.builder = new StringBuilder();
        this.maxTier = maxTier;
        this.showZeroValues = showZeroValues;
        this.showRuleOfThumb = showRuleOfThumb;
        this.groupMetrics = groupMetrics;
    }
    
    @Override
    public String format(RuntimeProfile profile, String prefix) {
        this.doFormat(profile, prefix);
        return builder.toString();
    }
    
    @Override
    public String format(RuntimeProfile profile) {
        this.doFormat(profile, "");
        return builder.toString();
    }
    
    private void doFormat(RuntimeProfile profile, String prefix) {
        Map<String, RuntimeProfile.Counter> counterMap = profile.getCounterMap();
        RuntimeProfile.Counter totalTimeCounter = counterMap.get(RuntimeProfile.TOTAL_TIME_COUNTER);
        
        if (totalTimeCounter == null) {
            builder.append(prefix).append(profile.getName()).append("\n");
            return;
        }
        
        // Profile header with timing info
        builder.append(prefix).append(profile.getName()).append(":");
        
        if (totalTimeCounter.getValue() != 0) {
            builder.append("(Active: ")
                   .append(RuntimeProfile.printCounter(totalTimeCounter.getValue(), totalTimeCounter.getType()));
            if (DebugUtil.THOUSAND < totalTimeCounter.getValue()) {
                builder.append("[").append(totalTimeCounter.getValue()).append("ns]");
            }
            builder.append(", % non-child: ")
                   .append(String.format("%.2f", profile.getLocalTimePercent()))
                   .append("%)");
        }
        builder.append("\n");
        
        // Print info strings
        for (Map.Entry<String, String> infoPair : profile.getInfoStrings().entrySet()) {
            builder.append(prefix).append("   - ").append(infoPair.getKey())
                   .append(": ").append(infoPair.getValue()).append("\n");
        }
        
        // Collect all metrics for rule-of-thumb evaluation
        Map<String, Long> allMetrics = Maps.newHashMap();
        for (Map.Entry<String, RuntimeProfile.Counter> entry : counterMap.entrySet()) {
            allMetrics.put(entry.getKey(), entry.getValue().getValue());
            String friendlyName = MetricNameMapper.getFriendlyName(entry.getKey());
            if (!friendlyName.equals(entry.getKey())) {
                allMetrics.put(friendlyName, entry.getValue().getValue());
            }
        }
        
        // Group and filter metrics by tier and group
        Map<MetricGroup, List<MetricEntry>> groupedMetrics = groupAndFilterMetrics(counterMap, allMetrics);
        
        // Print metrics by group
        printGroupedMetrics(groupedMetrics, prefix, allMetrics);
        
        // Recursively format children
        for (Pair<RuntimeProfile, Boolean> childPair : profile.getChildList()) {
            RuntimeProfile child = childPair.first;
            boolean indent = childPair.second;
            doFormat(child, prefix + (indent ? "  " : ""));
        }
    }
    
    private Map<MetricGroup, List<MetricEntry>> groupAndFilterMetrics(
            Map<String, RuntimeProfile.Counter> counterMap, Map<String, Long> allMetrics) {
        
        Map<MetricGroup, List<MetricEntry>> groupedMetrics = Maps.newLinkedHashMap();
        
        for (Map.Entry<String, RuntimeProfile.Counter> entry : counterMap.entrySet()) {
            String metricName = entry.getKey();
            RuntimeProfile.Counter counter = entry.getValue();
            
            // Skip total time (already displayed in header)
            if (RuntimeProfile.TOTAL_TIME_COUNTER.equals(metricName)) {
                continue;
            }
            
            // Check tier filtering
            MetricTier tier = getMetricTier(metricName);
            if (!maxTier.includes(tier)) {
                continue;
            }
            
            // Check zero value filtering
            boolean hasValue = counter.getValue() != 0;
            boolean shouldDisplayZero = shouldDisplayWhenZero(metricName, tier);
            
            if (!hasValue && !showZeroValues && !shouldDisplayZero) {
                continue;
            }
            
            // Get user-friendly name and group
            String friendlyName = MetricNameMapper.getFriendlyName(metricName);
            MetricGroup group = getMetricGroup(metricName);
            
            // Evaluate rule-of-thumb if enabled
            MetricRuleEngine.MetricEvaluation evaluation = null;
            if (showRuleOfThumb) {
                evaluation = MetricRuleEngine.evaluateMetric(metricName, counter.getValue(), allMetrics);
                if (evaluation == null) {
                    evaluation = MetricRuleEngine.evaluateMetric(friendlyName, counter.getValue(), allMetrics);
                }
            }
            
            MetricEntry metricEntry = new MetricEntry(metricName, friendlyName, counter, tier, evaluation);
            groupedMetrics.computeIfAbsent(group, k -> Lists.newArrayList()).add(metricEntry);
        }
        
        // Sort metrics within each group by tier (basic first) then by name
        for (List<MetricEntry> metrics : groupedMetrics.values()) {
            metrics.sort((a, b) -> {
                int tierCompare = Integer.compare(a.tier.getLevel(), b.tier.getLevel());
                if (tierCompare != 0) return tierCompare;
                return a.friendlyName.compareTo(b.friendlyName);
            });
        }
        
        return groupedMetrics;
    }
    
    private void printGroupedMetrics(Map<MetricGroup, List<MetricEntry>> groupedMetrics, 
                                    String prefix, Map<String, Long> allMetrics) {
        
        boolean multipleGroups = groupedMetrics.size() > 1;
        
        for (Map.Entry<MetricGroup, List<MetricEntry>> groupEntry : groupedMetrics.entrySet()) {
            MetricGroup group = groupEntry.getKey();
            List<MetricEntry> metrics = groupEntry.getValue();
            
            if (metrics.isEmpty()) continue;
            
            // Print group header if we have multiple groups and grouping is enabled
            if (groupMetrics && multipleGroups && group != MetricGroup.GENERAL) {
                builder.append(prefix).append("   # ").append(group.getDisplayName()).append("\n");
            }
            
            // Print metrics in this group
            for (MetricEntry metric : metrics) {
                printMetric(metric, prefix, allMetrics);
            }
        }
    }
    
    private void printMetric(MetricEntry metric, String prefix, Map<String, Long> allMetrics) {
        builder.append(prefix).append("   - ").append(metric.friendlyName).append(": ")
               .append(RuntimeProfile.printCounter(metric.counter.getValue(), metric.counter.getType()));
        
        // Add tier indicator for non-basic metrics
        if (metric.tier != MetricTier.BASIC) {
            String tierIndicator = metric.tier == MetricTier.ADVANCED ? " [ADV]" : " [TRACE]";
            builder.append(tierIndicator);
        }
        
        // Add rule-of-thumb evaluation if available
        if (metric.evaluation != null) {
            builder.append(" ").append(metric.evaluation.getFormattedMessage());
        }
        
        builder.append("\n");
    }
    
    private MetricTier getMetricTier(String metricName) {
        // Basic tier metrics (≤5 per operator type)
        Set<String> basicMetrics = Sets.newHashSet(
            "TotalTime", "QueryExecutionWallTime", "QueryPeakMemoryUsagePerNode", 
            "QuerySpillBytes", "QueryCumulativeCpuTime", "RowsRead", "BytesRead", 
            "ScanTime", "BuildHashTableTime", "SearchHashTableTime", 
            "HashTableMemoryUsage", "probeCount", "HashTableSize", 
            "OperatorTotalTime", "NetworkTime", "SerializeChunkTime"
        );
        
        // Trace tier metrics (developer/debugging)
        Set<String> traceMetrics = Sets.newHashSet(
            "PrepareTime", "CloseTime", "SetFinishingTime", "SetFinishedTime",
            "BufferUnplugCount", "PullChunkNum", "PushChunkNum", "PullTotalTime", "PushTotalTime",
            "DriverPrepareTime", "FragmentInstancePrepareTime", "prepare-query-ctx",
            "prepare-fragment-ctx", "prepare-runtime-state", "ColumnResizeTime",
            "RemoveUnusedRowsCount", "DeploySerializeConcurrencyTime", "DeployStageByStageTime",
            "FileWriterCloseTime", "GetDelVec", "PartitionNums", "PartitionProbeOverhead",
            "AccessPathHits", "AccessPathUnhits", "NonPushdownPredicates", "PushdownPredicates",
            "PushdownAccessPaths", "ScannerTotalTime", "ReadCounter"
        );
        
        if (basicMetrics.contains(metricName)) {
            return MetricTier.BASIC;
        } else if (traceMetrics.contains(metricName)) {
            return MetricTier.TRACE;
        } else {
            return MetricTier.ADVANCED;
        }
    }
    
    private MetricGroup getMetricGroup(String metricName) {
        // Group metrics logically for better organization
        if (metricName.contains("Time") || metricName.contains("Duration")) {
            return MetricGroup.TIMING;
        } else if (metricName.contains("Memory") || metricName.contains("Buffer")) {
            return MetricGroup.MEMORY;
        } else if (metricName.contains("BytesRead") || metricName.contains("Scan") || metricName.contains("IO")) {
            return MetricGroup.IO;
        } else if (metricName.contains("Network") || metricName.contains("Serialize")) {
            return MetricGroup.NETWORK;
        } else if (metricName.contains("RuntimeFilter") || metricName.contains("Filter")) {
            return MetricGroup.RUNTIME_FILTER;
        } else if (metricName.contains("HashTable") || metricName.contains("Hash") || metricName.contains("Join")) {
            return MetricGroup.HASH_TABLE;
        } else if (metricName.contains("Spill")) {
            return MetricGroup.SPILL;
        } else if (metricName.contains("Rows") || metricName.contains("Count")) {
            return MetricGroup.ROWS;
        } else {
            return MetricGroup.GENERAL;
        }
    }
    
    private boolean shouldDisplayWhenZero(String metricName, MetricTier tier) {
        // Basic tier metrics should always be displayed, even when zero
        if (tier == MetricTier.BASIC) {
            return true;
        }
        
        // Always display critical metrics even when zero
        Set<String> alwaysDisplay = Sets.newHashSet(
            "QuerySpillBytes", "QueryPeakMemoryUsagePerNode", "RowsRead", "OperatorTotalTime"
        );
        
        return alwaysDisplay.contains(metricName);
    }
    
    private static class MetricEntry {
        final String originalName;
        final String friendlyName;
        final RuntimeProfile.Counter counter;
        final MetricTier tier;
        final MetricRuleEngine.MetricEvaluation evaluation;
        
        MetricEntry(String originalName, String friendlyName, RuntimeProfile.Counter counter, 
                   MetricTier tier, MetricRuleEngine.MetricEvaluation evaluation) {
            this.originalName = originalName;
            this.friendlyName = friendlyName;
            this.counter = counter;
            this.tier = tier;
            this.evaluation = evaluation;
        }
    }
}