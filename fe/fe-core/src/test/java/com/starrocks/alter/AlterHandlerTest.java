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

package com.starrocks.alter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for AlterHandler, specifically testing getActiveTxnIdOfTable behavior
 * with multiple concurrent alter jobs.
 */
public class AlterHandlerTest {

    private SchemaChangeHandler handler;

    @BeforeEach
    public void setUp() {
        handler = new SchemaChangeHandler();
    }

    @Test
    public void testGetActiveTxnIdOfTableReturnsEmptyWhenNoJobs() {
        Optional<Long> result = handler.getActiveTxnIdOfTable(123L);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetActiveTxnIdOfTableReturnsEmptyWhenNoMatchingTable() {
        AlterJobV2 job = createMockJob(1L, 100L, 1000L, AlterJobV2.JobState.RUNNING);
        handler.addAlterJobV2(job);

        // Query for a different table
        Optional<Long> result = handler.getActiveTxnIdOfTable(999L);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetActiveTxnIdOfTableReturnsTxnIdForSingleJob() {
        long tableId = 100L;
        long txnId = 5000L;
        AlterJobV2 job = createMockJob(1L, tableId, txnId, AlterJobV2.JobState.RUNNING);
        handler.addAlterJobV2(job);

        Optional<Long> result = handler.getActiveTxnIdOfTable(tableId);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(txnId, result.get());
    }

    @Test
    public void testGetActiveTxnIdOfTableIgnoresFinishedJobs() {
        long tableId = 100L;
        AlterJobV2 finishedJob = createMockJob(1L, tableId, 5000L, AlterJobV2.JobState.FINISHED);
        handler.addAlterJobV2(finishedJob);

        Optional<Long> result = handler.getActiveTxnIdOfTable(tableId);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetActiveTxnIdOfTableIgnoresCancelledJobs() {
        long tableId = 100L;
        AlterJobV2 cancelledJob = createMockJob(1L, tableId, 5000L, AlterJobV2.JobState.CANCELLED);
        handler.addAlterJobV2(cancelledJob);

        Optional<Long> result = handler.getActiveTxnIdOfTable(tableId);
        Assertions.assertTrue(result.isEmpty());
    }

    /**
     * Test that getActiveTxnIdOfTable returns the MINIMUM transaction ID
     * when multiple concurrent jobs exist for the same table.
     * This is critical for vacuum correctness - we must use the smallest
     * txn ID to ensure we don't delete data needed by any active job.
     */
    @Test
    public void testGetActiveTxnIdOfTableReturnsMinimumForMultipleConcurrentJobs() {
        long tableId = 100L;

        // Create multiple concurrent jobs with different transaction IDs
        // Simulating Config.max_running_rollup_job_num_per_table > 1
        AlterJobV2 job1 = createMockJob(1L, tableId, 3000L, AlterJobV2.JobState.RUNNING);
        AlterJobV2 job2 = createMockJob(2L, tableId, 1000L, AlterJobV2.JobState.RUNNING);  // smallest
        AlterJobV2 job3 = createMockJob(3L, tableId, 2000L, AlterJobV2.JobState.WAITING_TXN);

        handler.addAlterJobV2(job1);
        handler.addAlterJobV2(job2);
        handler.addAlterJobV2(job3);

        Optional<Long> result = handler.getActiveTxnIdOfTable(tableId);
        Assertions.assertTrue(result.isPresent());
        // Must return the minimum (1000L), not just the first one found
        Assertions.assertEquals(1000L, result.get());
    }

    /**
     * Test with multiple jobs where some are finished/cancelled.
     * Should only consider active jobs when finding the minimum.
     */
    @Test
    public void testGetActiveTxnIdOfTableReturnsMinimumIgnoringFinalStateJobs() {
        long tableId = 100L;

        // Mix of active and final-state jobs
        AlterJobV2 activeJob1 = createMockJob(1L, tableId, 5000L, AlterJobV2.JobState.RUNNING);
        AlterJobV2 finishedJob =
                createMockJob(2L, tableId, 1000L, AlterJobV2.JobState.FINISHED);  // smaller but finished
        AlterJobV2 cancelledJob =
                createMockJob(3L, tableId, 500L, AlterJobV2.JobState.CANCELLED);  // smallest but cancelled
        AlterJobV2 activeJob2 = createMockJob(4L, tableId, 3000L, AlterJobV2.JobState.PENDING);

        handler.addAlterJobV2(activeJob1);
        handler.addAlterJobV2(finishedJob);
        handler.addAlterJobV2(cancelledJob);
        handler.addAlterJobV2(activeJob2);

        Optional<Long> result = handler.getActiveTxnIdOfTable(tableId);
        Assertions.assertTrue(result.isPresent());
        // Should return 3000L (minimum among active jobs), not 500L or 1000L
        Assertions.assertEquals(3000L, result.get());
    }

    /**
     * Test with jobs across multiple tables.
     * Should only consider jobs for the requested table.
     */
    @Test
    public void testGetActiveTxnIdOfTableFiltersCorrectlyAcrossMultipleTables() {
        long tableId1 = 100L;
        long tableId2 = 200L;

        // Jobs for table1
        AlterJobV2 table1Job1 = createMockJob(1L, tableId1, 5000L, AlterJobV2.JobState.RUNNING);
        AlterJobV2 table1Job2 = createMockJob(2L, tableId1, 3000L, AlterJobV2.JobState.RUNNING);

        // Jobs for table2 with smaller txn IDs
        AlterJobV2 table2Job1 = createMockJob(3L, tableId2, 1000L, AlterJobV2.JobState.RUNNING);
        AlterJobV2 table2Job2 = createMockJob(4L, tableId2, 500L, AlterJobV2.JobState.RUNNING);

        handler.addAlterJobV2(table1Job1);
        handler.addAlterJobV2(table1Job2);
        handler.addAlterJobV2(table2Job1);
        handler.addAlterJobV2(table2Job2);

        // Query for table1 should return 3000L (min for table1), not 500L
        Optional<Long> result1 = handler.getActiveTxnIdOfTable(tableId1);
        Assertions.assertTrue(result1.isPresent());
        Assertions.assertEquals(3000L, result1.get());

        // Query for table2 should return 500L (min for table2)
        Optional<Long> result2 = handler.getActiveTxnIdOfTable(tableId2);
        Assertions.assertTrue(result2.isPresent());
        Assertions.assertEquals(500L, result2.get());
    }

    /**
     * Test with jobs that have empty transaction IDs.
     * Should only consider jobs with present transaction IDs.
     */
    @Test
    public void testGetActiveTxnIdOfTableHandlesEmptyTransactionIds() {
        long tableId = 100L;

        // Job without a transaction ID (e.g., PENDING state before txn assigned)
        AlterJobV2 jobNoTxn = createMockJobWithEmptyTxn(1L, tableId, AlterJobV2.JobState.PENDING);
        AlterJobV2 jobWithTxn = createMockJob(2L, tableId, 2000L, AlterJobV2.JobState.RUNNING);

        handler.addAlterJobV2(jobNoTxn);
        handler.addAlterJobV2(jobWithTxn);

        Optional<Long> result = handler.getActiveTxnIdOfTable(tableId);
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(2000L, result.get());
    }

    /**
     * Test with all jobs having empty transaction IDs.
     */
    @Test
    public void testGetActiveTxnIdOfTableReturnsEmptyWhenAllJobsHaveNoTxnId() {
        long tableId = 100L;

        AlterJobV2 job1 = createMockJobWithEmptyTxn(1L, tableId, AlterJobV2.JobState.PENDING);
        AlterJobV2 job2 = createMockJobWithEmptyTxn(2L, tableId, AlterJobV2.JobState.PENDING);

        handler.addAlterJobV2(job1);
        handler.addAlterJobV2(job2);

        Optional<Long> result = handler.getActiveTxnIdOfTable(tableId);
        Assertions.assertTrue(result.isEmpty());
    }

    /**
     * Helper method to create a mock AlterJobV2 with specified properties.
     */
    private AlterJobV2 createMockJob(long jobId, long tableId, long txnId, AlterJobV2.JobState state) {
        AlterJobV2 job = mock(AlterJobV2.class);
        when(job.getJobId()).thenReturn(jobId);
        when(job.getTableId()).thenReturn(tableId);
        when(job.getTransactionId()).thenReturn(Optional.of(txnId));
        when(job.getJobState()).thenReturn(state);
        return job;
    }

    /**
     * Helper method to create a mock AlterJobV2 with empty transaction ID.
     */
    private AlterJobV2 createMockJobWithEmptyTxn(long jobId, long tableId, AlterJobV2.JobState state) {
        AlterJobV2 job = mock(AlterJobV2.class);
        when(job.getJobId()).thenReturn(jobId);
        when(job.getTableId()).thenReturn(tableId);
        when(job.getTransactionId()).thenReturn(Optional.empty());
        when(job.getJobState()).thenReturn(state);
        return job;
    }
}

