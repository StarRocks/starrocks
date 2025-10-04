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

import java.util.*;

/**
 * Aggregates metrics across multiple nodes/instances to provide statistical distributions.
 * Shows min/max/p50/p90 for metrics that have significant variance across nodes.
 * Implements requirements from issue #60952.
 */
public class MetricStatisticsAggregator {
    
    public static class MetricStatistics {
        private final long min;
        private final long max;
        private final long p50;
        private final long p90;
        private final double mean;
        private final int count;
        private final boolean hasVariance;
        
        public MetricStatistics(long min, long max, long p50, long p90, double mean, int count, boolean hasVariance) {
            this.min = min;
            this.max = max;
            this.p50 = p50;
            this.p90 = p90;
            this.mean = mean;
            this.count = count;
            this.hasVariance = hasVariance;
        }
        
        public long getMin() { return min; }
        public long getMax() { return max; }
        public long getP50() { return p50; }
        public long getP90() { return p90; }
        public double getMean() { return mean; }
        public int getCount() { return count; }
        public boolean hasVariance() { return hasVariance; }
        
        public String formatDistribution(String unit) {
            if (!hasVariance || count <= 1) {
                return RuntimeProfile.printCounter(min, unit);
            }
            
            StringBuilder sb = new StringBuilder();
            sb.append(RuntimeProfile.printCounter(p50, unit)).append(" (p50)");
            sb.append(" | ").append(RuntimeProfile.printCounter(min, unit)).append(" (min)");
            sb.append(" | ").append(RuntimeProfile.printCounter(max, unit)).append(" (max)");
            if (count > 2) {
                sb.append(" | ").append(RuntimeProfile.printCounter(p90, unit)).append(" (p90)");
            }
            sb.append(" [").append(count).append(" nodes]");
            return sb.toString();
        }
    }
    
    /**
     * Metrics that should display statistical distributions when they vary significantly across nodes
     */
    private static final Set<String> DISTRIBUTABLE_METRICS = new HashSet<>(Arrays.asList(
        // Timing metrics that can vary significantly across nodes
        "DriverTotalTime", "ActiveTime", "ScanTime", "OperatorTotalTime",
        "BuildHashTableTime", "SearchHashTableTime", "IOTaskExecTime", 
        "NetworkTime", "SerializeChunkTime", "DeserializeChunkTime",
        
        // Memory metrics that show node variance
        "QueryPeakMemoryUsagePerNode", "HashTableMemoryUsage", "PeakChunkBufferMemoryUsage",
        
        // I/O metrics with potential variance
        "BytesRead", "CompressedBytesRead", "UncompressedBytesRead",
        
        // Row processing metrics
        "RowsRead", "RawRowsRead", "probeCount",
        
        // User-friendly names
        "Data Scan Time", "Query Duration", "Peak Memory per Node", "Join Memory Usage",
        "Network Transfer Time", "Data Serialization Time", "Operator Execution Time",
        "Total Data Read", "Rows Returned (after filters)", "Hash Table Probes"
    ));
    
    /**
     * Aggregate metric values from multiple profile instances
     */
    public static Map<String, MetricStatistics> aggregateMetrics(List<RuntimeProfile> profiles) {
        Map<String, List<Long>> metricValues = new HashMap<>();
        
        // Collect all metric values across profiles
        for (RuntimeProfile profile : profiles) {
            collectMetricsFromProfile(profile, metricValues);
        }
        
        // Calculate statistics for each metric
        Map<String, MetricStatistics> statistics = new HashMap<>();
        for (Map.Entry<String, List<Long>> entry : metricValues.entrySet()) {
            String metricName = entry.getKey();
            List<Long> values = entry.getValue();
            
            if (shouldCalculateDistribution(metricName, values)) {
                statistics.put(metricName, calculateStatistics(values));
            }
        }
        
        return statistics;
    }
    
    /**
     * Check if a metric should display statistical distribution
     */
    public static boolean shouldShowDistribution(String metricName) {
        return DISTRIBUTABLE_METRICS.contains(metricName) || 
               DISTRIBUTABLE_METRICS.contains(MetricNameMapper.getFriendlyName(metricName));
    }
    
    private static void collectMetricsFromProfile(RuntimeProfile profile, Map<String, List<Long>> metricValues) {
        // Collect metrics from current profile
        for (Map.Entry<String, RuntimeProfile.Counter> entry : profile.getCounterMap().entrySet()) {
            String metricName = entry.getKey();
            long value = entry.getValue().getValue();
            
            metricValues.computeIfAbsent(metricName, k -> new ArrayList<>()).add(value);
            
            // Also track by friendly name
            String friendlyName = MetricNameMapper.getFriendlyName(metricName);
            if (!friendlyName.equals(metricName)) {
                metricValues.computeIfAbsent(friendlyName, k -> new ArrayList<>()).add(value);
            }
        }
        
        // Recursively collect from child profiles
        for (com.starrocks.common.Pair<RuntimeProfile, Boolean> child : profile.getChildList()) {
            collectMetricsFromProfile(child.first, metricValues);
        }
    }
    
    private static boolean shouldCalculateDistribution(String metricName, List<Long> values) {
        if (!shouldShowDistribution(metricName) || values.size() <= 1) {
            return false;
        }
        
        // Check if there's significant variance (coefficient of variation > 0.1)
        double mean = values.stream().mapToLong(Long::longValue).average().orElse(0.0);
        if (mean == 0) return false;
        
        double variance = values.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .average().orElse(0.0);
        double stdDev = Math.sqrt(variance);
        double coefficientOfVariation = stdDev / mean;
        
        return coefficientOfVariation > 0.1; // Show distribution if CV > 10%
    }
    
    private static MetricStatistics calculateStatistics(List<Long> values) {
        if (values.isEmpty()) {
            return new MetricStatistics(0, 0, 0, 0, 0.0, 0, false);
        }
        
        List<Long> sortedValues = new ArrayList<>(values);
        Collections.sort(sortedValues);
        
        long min = sortedValues.get(0);
        long max = sortedValues.get(sortedValues.size() - 1);
        double mean = values.stream().mapToLong(Long::longValue).average().orElse(0.0);
        
        // Calculate percentiles
        long p50 = calculatePercentile(sortedValues, 50);
        long p90 = calculatePercentile(sortedValues, 90);
        
        // Check for significant variance
        double variance = values.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .average().orElse(0.0);
        double stdDev = Math.sqrt(variance);
        double coefficientOfVariation = mean > 0 ? stdDev / mean : 0;
        boolean hasVariance = coefficientOfVariation > 0.1;
        
        return new MetricStatistics(min, max, p50, p90, mean, values.size(), hasVariance);
    }
    
    private static long calculatePercentile(List<Long> sortedValues, double percentile) {
        if (sortedValues.isEmpty()) return 0;
        
        double index = (percentile / 100.0) * (sortedValues.size() - 1);
        int lowerIndex = (int) Math.floor(index);
        int upperIndex = (int) Math.ceil(index);
        
        if (lowerIndex == upperIndex) {
            return sortedValues.get(lowerIndex);
        }
        
        long lowerValue = sortedValues.get(lowerIndex);
        long upperValue = sortedValues.get(upperIndex);
        double weight = index - lowerIndex;
        
        return Math.round(lowerValue + (upperValue - lowerValue) * weight);
    }
}