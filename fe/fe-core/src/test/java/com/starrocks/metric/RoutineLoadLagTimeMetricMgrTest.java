// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License. 

package com.starrocks.metric;

import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

public class RoutineLoadLagTimeMetricMgrTest {

    private RoutineLoadLagTimeMetricMgr metricMgr;
    private AutoCloseable closeable;
    
    @Mock
    private MetricVisitor mockVisitor;

    @BeforeEach
    public void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        metricMgr = RoutineLoadLagTimeMetricMgr.getInstance();
        // Enable routine load lag time metrics for testing
        Config.enable_routine_load_lag_time_metrics = true;
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (closeable != null) {
            closeable.close();
        }
    }

    @Test
    public void testMultipleJobsMetrics() {

        // Setup: Different lag times for each job
        Map<Integer, Long> job1LagTimes = Maps.newHashMap();
        job1LagTimes.put(0, 30L);
        job1LagTimes.put(1, 45L);

        Map<Integer, Long> job2LagTimes = Maps.newHashMap();
        job2LagTimes.put(0, 60L);
        job2LagTimes.put(1, 75L);
        job2LagTimes.put(2, 90L);

        // Execute: Update metrics for both jobs
        metricMgr.updateRoutineLoadLagTimeMetric(1L, "test_multiple_job1", job1LagTimes);
        metricMgr.updateRoutineLoadLagTimeMetric(2L, "test_multiple_job2", job2LagTimes);

        // Verify: Each job has its own metrics
        Map<Integer, Long> retrieved1 = metricMgr.getPartitionLagTimes(1L, "test_multiple_job1");
        Map<Integer, Long> retrieved2 = metricMgr.getPartitionLagTimes(2L, "test_multiple_job2");

        Assertions.assertEquals(2, retrieved1.size(), "Job 1 should have 2 partitions");
        Assertions.assertEquals(3, retrieved2.size(), "Job 2 should have 3 partitions");
        Assertions.assertEquals(Long.valueOf(30L), retrieved1.get(0), "Job 1 partition 0");
        Assertions.assertEquals(Long.valueOf(60L), retrieved2.get(0), "Job 2 partition 0");
    }

    @Test
    public void testUpdateRoutineLoadLagTimeMetricWithEmptyPartitions() {
        // Setup: Empty partition lag times
        Map<Integer, Long> partitionLagTimes = Maps.newHashMap();

        // Execute: Update metrics with empty map
        metricMgr.updateRoutineLoadLagTimeMetric(1L, "test_empty_job", partitionLagTimes);

        // Verify: No metrics stored
        Map<Integer, Long> retrievedLagTimes = metricMgr.getPartitionLagTimes(1L, "test_empty_job");
        Assertions.assertTrue(retrievedLagTimes.isEmpty(), "Should return empty map for empty input");
    }

    @Test
    public void testUpdateRoutineLoadLagTimeMetricOverwrite() {
        // Setup: Initial partition lag times
        Map<Integer, Long> initialLagTimes = Maps.newHashMap();
        initialLagTimes.put(0, 30L);
        initialLagTimes.put(1, 45L);

        // Execute: First update
        metricMgr.updateRoutineLoadLagTimeMetric(1L, "test_overwrite_job", initialLagTimes);

        // Setup: Updated partition lag times
        Map<Integer, Long> updatedLagTimes = Maps.newHashMap();
        updatedLagTimes.put(0, 35L); // Updated lag for partition 0
        updatedLagTimes.put(1, 50L); // Updated lag for partition 1
        updatedLagTimes.put(2, 25L); // New partition 2

        // Execute: Second update
        metricMgr.updateRoutineLoadLagTimeMetric(1L, "test_overwrite_job", updatedLagTimes);

        // Verify: Metrics were updated, old partitions removed
        Map<Integer, Long> retrievedLagTimes = metricMgr.getPartitionLagTimes(1L, "test_overwrite_job");
        Assertions.assertEquals(3, retrievedLagTimes.size(), "Should have 3 partitions after update");
        Assertions.assertEquals(Long.valueOf(35L), retrievedLagTimes.get(0), "Partition 0 lag time updated");
        Assertions.assertEquals(Long.valueOf(50L), retrievedLagTimes.get(1), "Partition 1 lag time updated");
        Assertions.assertEquals(Long.valueOf(25L), retrievedLagTimes.get(2), "Partition 2 lag time added");
    }

    @Test
    public void testGetPartitionLagTimesForNonExistentJob() {
        // Execute: Try to get lag times for non-existent job
        Map<Integer, Long> retrievedLagTimes = metricMgr.getPartitionLagTimes(999L, "test_nonexistent_job");

        // Verify: Returns empty map
        Assertions.assertTrue(retrievedLagTimes.isEmpty(), "Should return empty map for non-existent job");
    }

    @Test
    public void testCollectMetrics() throws Exception {
        // Setup: Add metrics
        Map<Integer, Long> partitionLagTimes = Maps.newHashMap();
        partitionLagTimes.put(0, 30L);
        partitionLagTimes.put(1, 45L);
        metricMgr.updateRoutineLoadLagTimeMetric(1L, "test_collect_job", partitionLagTimes);

        // Verify: Check that metrics were actually stored
        Map<Integer, Long> retrievedLagTimes = metricMgr.getPartitionLagTimes(1L, "test_collect_job");
        Assertions.assertFalse(retrievedLagTimes.isEmpty(), "Metrics should be stored before collection");

        // Execute: Collect metrics
        metricMgr.collectRoutineLoadLagTimeMetrics(mockVisitor);

        // Verify: Visitor was called for partition metrics and max metric
        verify(mockVisitor, atLeast(1)).visit(any());
    }

    @Test
    public void testMaxLagTimeCalculation() {
        // Setup: Partition lag times with different values
        Map<Integer, Long> partitionLagTimes = Maps.newHashMap();
        partitionLagTimes.put(0, 30L);  // min
        partitionLagTimes.put(1, 75L);  // max
        partitionLagTimes.put(2, 45L);  // middle

        // Execute: Update metrics
        metricMgr.updateRoutineLoadLagTimeMetric(1L, "test_maxlag_job", partitionLagTimes);

        // Verify: Max lag time is calculated correctly
        Map<Integer, Long> retrieved = metricMgr.getPartitionLagTimes(1L, "test_maxlag_job");
        Assertions.assertEquals(3, retrieved.size(), "Should maintain all partition data");
        
        // Verify max value is among the retrieved values
        Long maxValue = retrieved.values().stream().max(Long::compareTo).orElse(0L);
        Assertions.assertEquals(Long.valueOf(75L), maxValue, "Max lag time should be 75");
    }

    @Test
    public void testIsStaleMethodWhenStale() {
        // Setup: Add a job with lag time metrics
        Map<Integer, Long> partitionLagTimes = Maps.newHashMap();
        partitionLagTimes.put(0, 30L);
        metricMgr.updateRoutineLoadLagTimeMetric(1L, "test_stale_job", partitionLagTimes);

        // Get the internal RoutineLoadLagTimeMetric object
        Map<String, Object> jobLagTimeMap = Deencapsulation.getField(metricMgr, "jobLagTimeMap");
        Object lagTimeMetric = jobLagTimeMap.get("1.test_stale_job");
        Assertions.assertNotNull(lagTimeMetric, "Lag time metric should exist");

        // Test: Check if metric is stale (6 minutes ago)
        long currentTime = System.currentTimeMillis();
        long staleThresholdMs = 5L * 60 * 1000; // 5 minutes
        long staleUpdateTime = currentTime - (6L * 60 * 1000); // 6 minutes ago

        // Set the update timestamp to 6 minutes ago
        Deencapsulation.setField(lagTimeMetric, "updateTimestamp", staleUpdateTime);

        // Verify: Metric should be stale
        boolean isStale = Deencapsulation.invoke(lagTimeMetric, "isStale", currentTime, staleThresholdMs);
        Assertions.assertTrue(isStale, "Metric should be stale after 6 minutes");
    }

    @Test
    public void testIsStaleMethodWhenFresh() {
        // Setup: Add a job with lag time metrics
        Map<Integer, Long> partitionLagTimes = Maps.newHashMap();
        partitionLagTimes.put(0, 30L);
        metricMgr.updateRoutineLoadLagTimeMetric(1L, "test_fresh_job", partitionLagTimes);

        // Get the internal RoutineLoadLagTimeMetric object
        Map<String, Object> jobLagTimeMap = Deencapsulation.getField(metricMgr, "jobLagTimeMap");
        Object lagTimeMetric = jobLagTimeMap.get("1.test_fresh_job");
        Assertions.assertNotNull(lagTimeMetric, "Lag time metric should exist");

        // Test: Check if metric is fresh (2 minutes ago)
        long currentTime = System.currentTimeMillis();
        long staleThresholdMs = 5L * 60 * 1000; // 5 minutes
        long freshUpdateTime = currentTime - (2L * 60 * 1000); // 2 minutes ago

        // Set the update timestamp to 2 minutes ago
        Deencapsulation.setField(lagTimeMetric, "updateTimestamp", freshUpdateTime);

        // Verify: Metric should not be stale
        boolean isStale = Deencapsulation.invoke(lagTimeMetric, "isStale", currentTime, staleThresholdMs);
        Assertions.assertFalse(isStale, "Metric should not be stale after 2 minutes");
    }

    @Test
    public void testCollectRoutineLoadLagTimeMetricsExceptionHandling() {
        // Setup: Add a job with metrics
        Map<Integer, Long> partitionLagTimes = Maps.newHashMap();
        partitionLagTimes.put(0, 30L);
        metricMgr.updateRoutineLoadLagTimeMetric(1L, "test_collect_exception_job", partitionLagTimes);

        // Mock visitor to throw exception
        doThrow(new RuntimeException("Simulated visitor exception")).when(mockVisitor).visit(any());

        // Execute: This should handle the exception gracefully
        Assertions.assertDoesNotThrow(() -> {
            metricMgr.collectRoutineLoadLagTimeMetrics(mockVisitor);
        }, "collectRoutineLoadLagTimeMetrics should handle exceptions gracefully");
    }

    @Test
    public void testGetPartitionLagTimesExceptionHandling() {
        // Setup: First add a job to have some data
        Map<Integer, Long> partitionLagTimes = Maps.newHashMap();
        partitionLagTimes.put(0, 30L);
        metricMgr.updateRoutineLoadLagTimeMetric(1L, "test_get_exception_job", partitionLagTimes);

        // Get the internal jobLagTimeMap and replace it with a map that throws exception
        Map<String, Object> originalJobLagTimeMap = Deencapsulation.getField(metricMgr, "jobLagTimeMap");
        
        // Create a custom map that throws exception when get() is called
        Map<String, Object> faultyMap = new java.util.HashMap<String, Object>() {
            @Override
            public Object get(Object key) {
                throw new RuntimeException("Simulated map access exception");
            }
        };
        
        // Replace the internal map with our faulty map
        Deencapsulation.setField(metricMgr, "jobLagTimeMap", faultyMap);

        // Execute: This should handle the exception gracefully
        Map<Integer, Long> result = Assertions.assertDoesNotThrow(() -> {
            return metricMgr.getPartitionLagTimes(1L, "test_get_exception_job");
        }, "getPartitionLagTimes should handle exceptions gracefully");

        // Verify: Should return empty map on exception
        Assertions.assertTrue(result.isEmpty(), "Should return empty map when exception occurs");
        
        // Restore the original map for other tests
        Deencapsulation.setField(metricMgr, "jobLagTimeMap", originalJobLagTimeMap);
    }

    @Test
    public void testEmitJobMetricsExceptionHandling() {
        // Setup: Add a job with metrics
        Map<Integer, Long> partitionLagTimes = Maps.newHashMap();
        partitionLagTimes.put(0, 30L);
        metricMgr.updateRoutineLoadLagTimeMetric(1L, "test_emit_exception_job", partitionLagTimes);

        // Get the internal objects
        Map<String, Object> jobLagTimeMap = Deencapsulation.getField(metricMgr, "jobLagTimeMap");
        Object lagTimeMetric = jobLagTimeMap.get("1.test_emit_exception_job");
        Assertions.assertNotNull(lagTimeMetric, "Lag time metric should exist");

        // Mock visitor to throw exception
        doThrow(new RuntimeException("Simulated emit exception")).when(mockVisitor).visit(any());

        // Execute: This should handle the exception gracefully
        Assertions.assertDoesNotThrow(() -> {
            Deencapsulation.invoke(metricMgr, "emitJobMetrics", "1.test_emit_exception_job", lagTimeMetric, mockVisitor);
        }, "emitJobMetrics should handle exceptions gracefully");
    }
}
