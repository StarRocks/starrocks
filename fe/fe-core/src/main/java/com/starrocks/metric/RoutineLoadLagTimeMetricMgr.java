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
import com.starrocks.metric.Metric.MetricUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Manager for Routine Load Lag Time Metrics
 * Handles collection and emission of lag metrics for Kafka routine load jobs
 */
public class RoutineLoadLagTimeMetricMgr {
    private static final Logger LOG = LogManager.getLogger(RoutineLoadLagTimeMetricMgr.class);
    private static final long STALE_THRESHOLD_MS = 5L * 60 * 1000; // 5 minutes; to prevent memory leaks
    
    private static final RoutineLoadLagTimeMetricMgr INSTANCE = new RoutineLoadLagTimeMetricMgr();
    
    // Use composite key (dbId_jobName) to uniquely identify jobs
    private final Map<String, RoutineLoadLagTimeMetric> jobLagTimeMap = Maps.newConcurrentMap();
    
    private RoutineLoadLagTimeMetricMgr() {}
    
    public static RoutineLoadLagTimeMetricMgr getInstance() {
        return INSTANCE;
    }

    private String getJobKey(long dbId, String jobName) {
        return dbId + "." + jobName;
    }

    private static class RoutineLoadLagTimeMetric {
        private long updateTimestamp;
        private final String jobName;
        
        private final Map<Integer, Long> partitionLagTimes;
        private final LeaderAwareGaugeMetricLong maxLagTimeMetric;
        
        public RoutineLoadLagTimeMetric(String jobName) {
            this.updateTimestamp = -1;
            this.jobName = jobName;
            this.partitionLagTimes = Maps.newHashMap();
            this.maxLagTimeMetric = new LeaderAwareGaugeMetricLong(
                    "routine_load_max_lag_time_of_partition",
                    MetricUnit.SECONDS,
                    "Maximum lag time across all partitions for routine load job"
            ) {
                @Override
                public Long getValueLeader() {
                    return partitionLagTimes.values().stream()
                            .mapToLong(Long::longValue)
                            .max()
                            .orElse(0L);
                }
            };
            this.maxLagTimeMetric.addLabel(new MetricLabel("job_name", jobName));
            
            LOG.debug("Initialized empty lag time metric structure for job {}", jobName);
        }
        
        public void updateMetrics(Map<Integer, Long> newPartitionLagTimes, long newUpdateTimestamp) {
            this.updateTimestamp = newUpdateTimestamp;
            this.partitionLagTimes.clear();
            this.partitionLagTimes.putAll(newPartitionLagTimes);
            
            LOG.debug("Updated metrics for job {}: partitions={}", 
                     jobName, newPartitionLagTimes.size());
        }

        public Map<Integer, Long> getPartitionLagTimes() {
            return Maps.newHashMap(partitionLagTimes);
        }

        public LeaderAwareGaugeMetricLong getMaxLagTimeMetric() {
            return maxLagTimeMetric;
        }
        
        public boolean isStale(long currentTime, long staleThresholdMs) {
            return (currentTime - updateTimestamp) > staleThresholdMs;
        }
        
        public boolean hasData() {
            return !partitionLagTimes.isEmpty() && updateTimestamp > 0;
        }
    }

    public void updateRoutineLoadLagTimeMetric(long dbId, String jobName, Map<Integer, Long> partitionLagTimes) {
        try {
            if (partitionLagTimes.isEmpty()) {
                LOG.debug("No partition lag times available for job {}", jobName);
                return;
            }
            
            // Get or create the metric structure using composite key
            String jobKey = getJobKey(dbId, jobName);
            RoutineLoadLagTimeMetric lagTimeMetric =
                    jobLagTimeMap.computeIfAbsent(jobKey, RoutineLoadLagTimeMetric::new);
            
            // Update the metric with new values
            long now = System.currentTimeMillis();
            lagTimeMetric.updateMetrics(partitionLagTimes, now);
            
            LOG.debug("Updated lag time data for Kafka job {}: partitions={}", 
                     jobKey, partitionLagTimes.size());
            
        } catch (Exception e) {
            LOG.warn("Failed to update lag time data for Kafka job {}: {}", getJobKey(dbId, jobName), e.getMessage());
        }
    }
    
    /**
     * Clean up stale lag time metrics to prevent memory leaks
     */
    public void cleanupStaleMetrics() {
        if (!Config.enable_routine_load_lag_time_metrics) {
            return;
        }
        
        try {
            long now = System.currentTimeMillis();
            long staleThresholdMs = STALE_THRESHOLD_MS;
            
            // Clean up stale data
            Iterator<Map.Entry<String, RoutineLoadLagTimeMetric>> jobLagTimeIterator = jobLagTimeMap.entrySet().iterator();
            while (jobLagTimeIterator.hasNext()) {
                Map.Entry<String, RoutineLoadLagTimeMetric> entry = jobLagTimeIterator.next();
                String jobKey = entry.getKey();
                RoutineLoadLagTimeMetric lagTimeMetric = entry.getValue();
                
                // Remove stale data
                if (lagTimeMetric.isStale(now, staleThresholdMs)) {
                    LOG.debug("Removing stale lag time data for job {}", jobKey);
                    jobLagTimeIterator.remove();
                }
            }
            
        } catch (Exception e) {
            LOG.warn("Failed to cleanup stale routine load lag time metrics", e);
        }
    }

    /**
     * Collect routine load lag time metrics - now uses stored data instead of calculating on-demand
     */
    public void collectRoutineLoadLagTimeMetrics(MetricVisitor visitor) {
        if (!Config.enable_routine_load_lag_time_metrics) {
            return;
        }
        
        try {
            // Emit metrics for all jobs with data
            for (Map.Entry<String, RoutineLoadLagTimeMetric> entry : jobLagTimeMap.entrySet()) {
                String jobKey = entry.getKey();
                RoutineLoadLagTimeMetric lagTimeMetric = entry.getValue();
                
                // Skip metrics that don't have data yet
                if (!lagTimeMetric.hasData()) {
                    LOG.debug("Skipping job {} - no data available yet", jobKey);
                    continue;
                }
                
                // Emit metrics for this job
                emitJobMetrics(jobKey, lagTimeMetric, visitor);
            }
            
        } catch (Exception e) {
            LOG.warn("Failed to collect routine load lag time metrics", e);
        }
    }
    
    private void emitJobMetrics(String jobKey, RoutineLoadLagTimeMetric lagTimeMetric, MetricVisitor visitor) {
        try {            
            if (lagTimeMetric.hasData()) {
                // only max lag is emitted; partition-level lag is only available through `SHOW ROUTINE LOAD`
                visitor.visit(lagTimeMetric.getMaxLagTimeMetric());
            }            
        } catch (Exception e) {
            LOG.warn("Failed to emit metrics for job {}: {}", jobKey, e.getMessage());
        }
    }

    public Map<Integer, Long> getPartitionLagTimes(long dbId, String jobName) {
        try {
            String jobKey = getJobKey(dbId, jobName);
            RoutineLoadLagTimeMetric lagTimeMetric = jobLagTimeMap.get(jobKey);
            
            if (lagTimeMetric == null || !lagTimeMetric.hasData()) {
                LOG.debug("No lag time metric found for job {} ({})", jobKey, jobName);
                return Collections.emptyMap();
            }
            
            Map<Integer, Long> lagTimes = lagTimeMetric.getPartitionLagTimes();
            LOG.debug("Retrieved {} partition lag times for job {} ({})", 
                     lagTimes.size(), jobKey, jobName);
            return lagTimes;
            
        } catch (Exception e) {
            LOG.warn("Failed to get partition lag times for job {} ({}): {}", 
                     getJobKey(dbId, jobName), jobName, e.getMessage());
            return Collections.emptyMap();
        }
    }
}
