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

import java.util.HashMap;
import java.util.Map;

/**
 * Rule-of-thumb evaluation engine for StarRocks query metrics.
 * Provides performance guidance and warnings based on metric values.
 * Implements requirements from issue #60952.
 */
public class MetricRuleEngine {
    
    public enum IndicatorLevel {
        NORMAL("🟢", "Normal"),
        WARNING("🟡", "Warning"), 
        CRITICAL("🔴", "Critical");
        
        private final String emoji;
        private final String text;
        
        IndicatorLevel(String emoji, String text) {
            this.emoji = emoji;
            this.text = text;
        }
        
        public String getEmoji() { return emoji; }
        public String getText() { return text; }
    }
    
    public static class MetricEvaluation {
        private final IndicatorLevel level;
        private final String message;
        
        public MetricEvaluation(IndicatorLevel level, String message) {
            this.level = level;
            this.message = message;
        }
        
        public IndicatorLevel getLevel() { return level; }
        public String getMessage() { return message; }
        
        public String getFormattedMessage() {
            return level.getEmoji() + " " + message;
        }
    }
    
    // Rule-of-thumb thresholds (configurable)
    private static final long SPILL_CRITICAL_BYTES = 1024L * 1024L * 1024L; // 1GB
    private static final double MEMORY_WARNING_PERCENT = 0.80; // 80% 
    private static final double MEMORY_CRITICAL_PERCENT = 0.95; // 95%
    private static final double NETWORK_WARNING_PERCENT = 0.30; // 30% of total time
    private static final double IO_WARNING_PERCENT = 0.50; // 50% of total time
    private static final long LARGE_SCAN_ROWS = 100_000_000L; // 100M rows
    private static final long SLOW_QUERY_THRESHOLD_MS = 10_000L; // 10 seconds
    
    private static final Map<String, MetricRule> METRIC_RULES = new HashMap<>();
    
    static {
        initializeRules();
    }
    
    @FunctionalInterface
    private interface MetricRule {
        MetricEvaluation evaluate(long value, Map<String, Long> allMetrics);
    }
    
    /**
     * Evaluate a metric value and return performance guidance
     */
    public static MetricEvaluation evaluateMetric(String metricName, long value, Map<String, Long> allMetrics) {
        MetricRule rule = METRIC_RULES.get(metricName);
        if (rule != null) {
            return rule.evaluate(value, allMetrics);
        }
        return null; // No rule defined for this metric
    }
    
    /**
     * Check if a metric has evaluation rules
     */
    public static boolean hasRule(String metricName) {
        return METRIC_RULES.containsKey(metricName);
    }
    
    private static void initializeRules() {
        // Data Spill Rules
        METRIC_RULES.put("QuerySpillBytes", (value, allMetrics) -> {
            if (value == 0) {
                return new MetricEvaluation(IndicatorLevel.NORMAL, "No spill - good memory utilization");
            } else if (value >= SPILL_CRITICAL_BYTES) {
                return new MetricEvaluation(IndicatorLevel.CRITICAL, 
                    "High disk spill detected - consider increasing memory_limit or enabling spill optimization");
            } else {
                return new MetricEvaluation(IndicatorLevel.WARNING, 
                    "Some data spilled to disk - monitor memory usage");
            }
        });
        
        METRIC_RULES.put("Data Spilled to Disk", (value, allMetrics) -> {
            return METRIC_RULES.get("QuerySpillBytes").evaluate(value, allMetrics);
        });
        
        // Memory Usage Rules
        METRIC_RULES.put("QueryPeakMemoryUsagePerNode", (value, allMetrics) -> {
            // This would need system memory info to calculate percentage
            // For now, use absolute thresholds as approximation
            long memoryGB = value / (1024L * 1024L * 1024L);
            if (memoryGB > 100) {
                return new MetricEvaluation(IndicatorLevel.CRITICAL, 
                    "Very high memory usage (" + memoryGB + "GB) - risk of OOM errors");
            } else if (memoryGB > 50) {
                return new MetricEvaluation(IndicatorLevel.WARNING, 
                    "High memory usage (" + memoryGB + "GB) - monitor for memory pressure");
            }
            return new MetricEvaluation(IndicatorLevel.NORMAL, "Memory usage within normal range");
        });
        
        METRIC_RULES.put("Peak Memory per Node", (value, allMetrics) -> {
            return METRIC_RULES.get("QueryPeakMemoryUsagePerNode").evaluate(value, allMetrics);
        });
        
        // Network Performance Rules
        METRIC_RULES.put("NetworkTime", (value, allMetrics) -> {
            Long totalTime = allMetrics.get("QueryExecutionWallTime");
            if (totalTime == null) totalTime = allMetrics.get("TotalTime");
            
            if (totalTime != null && totalTime > 0) {
                double networkPercent = (double) value / totalTime;
                if (networkPercent > NETWORK_WARNING_PERCENT) {
                    return new MetricEvaluation(IndicatorLevel.WARNING, 
                        String.format("Network bottleneck detected (%.1f%% of total time) - consider data locality optimization", 
                                    networkPercent * 100));
                }
            }
            return new MetricEvaluation(IndicatorLevel.NORMAL, "Network performance acceptable");
        });
        
        METRIC_RULES.put("Network Transfer Time", (value, allMetrics) -> {
            return METRIC_RULES.get("NetworkTime").evaluate(value, allMetrics);
        });
        
        // I/O Performance Rules  
        METRIC_RULES.put("ScanTime", (value, allMetrics) -> {
            Long totalTime = allMetrics.get("QueryExecutionWallTime");
            if (totalTime == null) totalTime = allMetrics.get("TotalTime");
            
            if (totalTime != null && totalTime > 0) {
                double scanPercent = (double) value / totalTime;
                if (scanPercent > IO_WARNING_PERCENT) {
                    return new MetricEvaluation(IndicatorLevel.WARNING, 
                        String.format("I/O bound query (%.1f%% scan time) - consider indexing or partitioning", 
                                    scanPercent * 100));
                }
            }
            return new MetricEvaluation(IndicatorLevel.NORMAL, "Scan performance acceptable");
        });
        
        METRIC_RULES.put("Data Scan Time", (value, allMetrics) -> {
            return METRIC_RULES.get("ScanTime").evaluate(value, allMetrics);
        });
        
        // Row Processing Rules
        METRIC_RULES.put("RowsRead", (value, allMetrics) -> {
            if (value > LARGE_SCAN_ROWS) {
                return new MetricEvaluation(IndicatorLevel.WARNING, 
                    String.format("Large scan (%,d rows) - consider adding filters or limit clauses", value));
            }
            return new MetricEvaluation(IndicatorLevel.NORMAL, "Row processing volume acceptable");
        });
        
        METRIC_RULES.put("Rows Returned (after filters)", (value, allMetrics) -> {
            return METRIC_RULES.get("RowsRead").evaluate(value, allMetrics);
        });
        
        // Query Duration Rules
        METRIC_RULES.put("QueryExecutionWallTime", (value, allMetrics) -> {
            long queryTimeMs = value / 1_000_000L; // Convert nanoseconds to milliseconds
            if (queryTimeMs > SLOW_QUERY_THRESHOLD_MS) {
                return new MetricEvaluation(IndicatorLevel.WARNING, 
                    String.format("Slow query detected (%.2fs) - consider optimization", queryTimeMs / 1000.0));
            }
            return new MetricEvaluation(IndicatorLevel.NORMAL, "Query execution time acceptable");
        });
        
        METRIC_RULES.put("Query Duration", (value, allMetrics) -> {
            return METRIC_RULES.get("QueryExecutionWallTime").evaluate(value, allMetrics);
        });
        
        // Hash Join Rules
        METRIC_RULES.put("HashTableMemoryUsage", (value, allMetrics) -> {
            long memoryMB = value / (1024L * 1024L);
            if (memoryMB > 10_000) { // 10GB
                return new MetricEvaluation(IndicatorLevel.WARNING, 
                    String.format("Large hash table (%,dMB) - consider join optimization or partitioning", memoryMB));
            }
            return new MetricEvaluation(IndicatorLevel.NORMAL, "Hash table size acceptable");
        });
        
        METRIC_RULES.put("Join Memory Usage", (value, allMetrics) -> {
            return METRIC_RULES.get("HashTableMemoryUsage").evaluate(value, allMetrics);
        });
        
        // CPU Utilization Rules
        METRIC_RULES.put("QueryCumulativeCpuTime", (value, allMetrics) -> {
            Long wallTime = allMetrics.get("QueryExecutionWallTime");
            if (wallTime != null && wallTime > 0) {
                double cpuRatio = (double) value / wallTime;
                if (cpuRatio < 0.5) {
                    return new MetricEvaluation(IndicatorLevel.WARNING, 
                        String.format("Low CPU utilization (%.1fx) - query may be I/O or network bound", cpuRatio));
                } else if (cpuRatio > 8.0) {
                    return new MetricEvaluation(IndicatorLevel.WARNING, 
                        String.format("High CPU utilization (%.1fx) - consider parallel optimization", cpuRatio));
                }
            }
            return new MetricEvaluation(IndicatorLevel.NORMAL, "CPU utilization balanced");
        });
        
        METRIC_RULES.put("Total CPU Time", (value, allMetrics) -> {
            return METRIC_RULES.get("QueryCumulativeCpuTime").evaluate(value, allMetrics);
        });
    }
}