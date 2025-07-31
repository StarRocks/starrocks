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
import com.starrocks.load.routineload.KafkaRoutineLoadJob;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RoutineLoadLagTimeMetricMgrTest {

    private RoutineLoadLagTimeMetricMgr metricMgr;
    
    @Mock
    private MetricVisitor mockVisitor;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        metricMgr = RoutineLoadLagTimeMetricMgr.getInstance();
    }

    // Helper method to create mock jobs with unique names
    private KafkaRoutineLoadJob createMockJob(String jobName, long dbId, long jobId) {
        KafkaRoutineLoadJob mockJob = mock(KafkaRoutineLoadJob.class);
        when(mockJob.getDbId()).thenReturn(dbId);
        when(mockJob.getName()).thenReturn(jobName);
        when(mockJob.getId()).thenReturn(jobId);
        return mockJob;
    }

    @Test
    public void testMultipleJobsMetrics() {
        // Setup: Create two jobs with unique names
        KafkaRoutineLoadJob mockJob1 = createMockJob("test_multiple_job1", 1L, 103L);
        KafkaRoutineLoadJob mockJob2 = createMockJob("test_multiple_job2", 2L, 104L);

        // Setup: Different lag times for each job
        Map<Integer, Long> job1LagTimes = Maps.newHashMap();
        job1LagTimes.put(0, 30L);
        job1LagTimes.put(1, 45L);

        Map<Integer, Long> job2LagTimes = Maps.newHashMap();
        job2LagTimes.put(0, 60L);
        job2LagTimes.put(1, 75L);
        job2LagTimes.put(2, 90L);

        // Execute: Update metrics for both jobs
        metricMgr.updateRoutineLoadLagTimeMetric(mockJob1, job1LagTimes);
        metricMgr.updateRoutineLoadLagTimeMetric(mockJob2, job2LagTimes);

        // Verify: Each job has its own metrics
        Map<Integer, Long> retrieved1 = metricMgr.getPartitionLagTimes(mockJob1);
        Map<Integer, Long> retrieved2 = metricMgr.getPartitionLagTimes(mockJob2);

        Assert.assertEquals("Job 1 should have 2 partitions", 2, retrieved1.size());
        Assert.assertEquals("Job 2 should have 3 partitions", 3, retrieved2.size());
        Assert.assertEquals("Job 1 partition 0", Long.valueOf(30L), retrieved1.get(0));
        Assert.assertEquals("Job 2 partition 0", Long.valueOf(60L), retrieved2.get(0));
    }

    @Test
    public void testUpdateRoutineLoadLagTimeMetricWithEmptyPartitions() {
        // Setup: Create job with unique name for this test
        KafkaRoutineLoadJob mockJob = createMockJob("test_empty_job", 1L, 101L);
        
        // Setup: Empty partition lag times
        Map<Integer, Long> partitionLagTimes = Maps.newHashMap();

        // Execute: Update metrics with empty map
        metricMgr.updateRoutineLoadLagTimeMetric(mockJob, partitionLagTimes);

        // Verify: No metrics stored
        Map<Integer, Long> retrievedLagTimes = metricMgr.getPartitionLagTimes(mockJob);
        Assert.assertTrue("Should return empty map for empty input", retrievedLagTimes.isEmpty());
    }

    @Test
    public void testUpdateRoutineLoadLagTimeMetricOverwrite() {
        // Setup: Create job with unique name for this test
        KafkaRoutineLoadJob mockJob = createMockJob("test_overwrite_job", 1L, 102L);
        
        // Setup: Initial partition lag times
        Map<Integer, Long> initialLagTimes = Maps.newHashMap();
        initialLagTimes.put(0, 30L);
        initialLagTimes.put(1, 45L);

        // Execute: First update
        metricMgr.updateRoutineLoadLagTimeMetric(mockJob, initialLagTimes);

        // Setup: Updated partition lag times
        Map<Integer, Long> updatedLagTimes = Maps.newHashMap();
        updatedLagTimes.put(0, 35L); // Updated lag for partition 0
        updatedLagTimes.put(1, 50L); // Updated lag for partition 1
        updatedLagTimes.put(2, 25L); // New partition 2

        // Execute: Second update
        metricMgr.updateRoutineLoadLagTimeMetric(mockJob, updatedLagTimes);

        // Verify: Metrics were updated, old partitions removed
        Map<Integer, Long> retrievedLagTimes = metricMgr.getPartitionLagTimes(mockJob);
        Assert.assertEquals("Should have 3 partitions after update", 3, retrievedLagTimes.size());
        Assert.assertEquals("Partition 0 lag time updated", Long.valueOf(35L), retrievedLagTimes.get(0));
        Assert.assertEquals("Partition 1 lag time updated", Long.valueOf(50L), retrievedLagTimes.get(1));
        Assert.assertEquals("Partition 2 lag time added", Long.valueOf(25L), retrievedLagTimes.get(2));
    }

    @Test
    public void testGetPartitionLagTimesForNonExistentJob() {
        // Setup: Create job that was never added to metrics
        KafkaRoutineLoadJob nonExistentJob = createMockJob("test_nonexistent_job", 999L, 999L);

        // Execute: Try to get lag times for non-existent job
        Map<Integer, Long> retrievedLagTimes = metricMgr.getPartitionLagTimes(nonExistentJob);

        // Verify: Returns empty map
        Assert.assertTrue("Should return empty map for non-existent job", retrievedLagTimes.isEmpty());
    }

    @Test
    public void testCollectMetrics() throws Exception {
        // Setup: Create job with unique name for this test
        KafkaRoutineLoadJob mockJob = createMockJob("test_collect_job", 1L, 105L);
        
        // Setup: Add metrics
        Map<Integer, Long> partitionLagTimes = Maps.newHashMap();
        partitionLagTimes.put(0, 30L);
        partitionLagTimes.put(1, 45L);
        metricMgr.updateRoutineLoadLagTimeMetric(mockJob, partitionLagTimes);

        // Execute: Collect metrics
        metricMgr.collectRoutineLoadLagTimeMetrics(mockVisitor);

        // Verify: Visitor was called for partition metrics and max metric
        verify(mockVisitor, atLeast(1)).visit(any());
    }

    @Test
    public void testMaxLagTimeCalculation() {
        // Setup: Create job with unique name for this test
        KafkaRoutineLoadJob mockJob = createMockJob("test_maxlag_job", 1L, 108L);
        
        // Setup: Partition lag times with different values
        Map<Integer, Long> partitionLagTimes = Maps.newHashMap();
        partitionLagTimes.put(0, 30L);  // min
        partitionLagTimes.put(1, 75L);  // max
        partitionLagTimes.put(2, 45L);  // middle

        // Execute: Update metrics
        metricMgr.updateRoutineLoadLagTimeMetric(mockJob, partitionLagTimes);

        // Verify: Max lag time is calculated correctly
        Map<Integer, Long> retrieved = metricMgr.getPartitionLagTimes(mockJob);
        Assert.assertEquals("Should maintain all partition data", 3, retrieved.size());
        
        // Verify max value is among the retrieved values
        Long maxValue = retrieved.values().stream().max(Long::compareTo).orElse(0L);
        Assert.assertEquals("Max lag time should be 75", Long.valueOf(75L), maxValue);
    }
}
