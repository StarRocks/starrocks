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

package com.starrocks.qe;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.LakeMetaVersionNotFoundException;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.RuntimeFilterDescription;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.proto.AIExecutionStatisticsPB;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.scheduler.QueryRuntimeProfile;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.ExecutionSchedule;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.FrontendServiceVersion;
import com.starrocks.thrift.TAIExecutionStatistics;
import com.starrocks.thrift.TAuditStatistics;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TReportAuditStatisticsParams;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.compress.utils.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class CoordinatorTest extends PlanTestBase {
    ConnectContext ctx;
    DefaultCoordinator coordinator;
    CoordinatorPreprocessor coordinatorPreprocessor;

    @BeforeEach
    public void setUp() {
        super.setUp();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.setExecutionId(new TUniqueId(0xdeadbeef, 0xdeadbeef));
        ConnectContext.threadLocalInfo.set(ctx);

        coordinator = new DefaultCoordinator.Factory().createQueryScheduler(ctx, Lists.newArrayList(), Lists.newArrayList(),
                new TDescriptorTable(), null);
        coordinatorPreprocessor = coordinator.getPrepareInfo();
    }

    private PlanFragment genFragment() {
        ArrayList<TupleId> tupleIdArrayList = new ArrayList<>();
        tupleIdArrayList.add(new TupleId(1));
        PlanFragment fragment =
                new PlanFragment(new PlanFragmentId(1), new EmptySetNode(new PlanNodeId(1), tupleIdArrayList),
                        new DataPartition(TPartitionType.RANDOM));
        return fragment;
    }

    @Test
    public void testCancellationDuringPreparationPreventsDeployment() throws Exception {
        ctx.setNeedQueued(false);
        ExecutionSchedule schedule = mock(ExecutionSchedule.class);
        ResultReceiver receiver = mock(ResultReceiver.class);
        Deencapsulation.setField(coordinator, "scheduler", schedule);
        new MockUp<DefaultCoordinator>() {
            @Mock
            public void prepareExec() {
                // Cancellation can arrive before prepareResultSink publishes the receiver.
                coordinator.cancel(PPlanFragmentCancelReason.USER_CANCEL, "cancel during preparation");
                Deencapsulation.setField(coordinator, "receiver", receiver);
            }
        };

        StarRocksException error = Assertions.assertThrows(StarRocksException.class, coordinator::exec);
        Assertions.assertTrue(error.getMessage().contains("cancel during preparation"));
        Assertions.assertTrue(coordinator.getExecStatus().isCancelled());
        verify(receiver).cancel();
        verify(schedule, never()).prepareSchedule(any(), any(), any());
        verify(schedule, never()).schedule(any());
        verify(schedule, never()).continueSchedule(any());
    }

    @Test
    public void testCancellationWhileWaitingForDeploymentLock() throws Exception {
        ctx.setNeedQueued(false);
        ExecutionSchedule schedule = mock(ExecutionSchedule.class);
        Deencapsulation.setField(coordinator, "scheduler", schedule);
        CountDownLatch prepared = new CountDownLatch(1);
        new MockUp<DefaultCoordinator>() {
            @Mock
            public void prepareExec() {
                prepared.countDown();
            }
        };
        ReentrantLock deploymentLock = Deencapsulation.getField(coordinator, "lock");
        ExecutorService worker = Executors.newSingleThreadExecutor();
        deploymentLock.lock();
        try {
            Future<?> execution = worker.submit(() -> {
                coordinator.exec();
                return null;
            });
            Assertions.assertTrue(prepared.await(5, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!deploymentLock.hasQueuedThreads() && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            Assertions.assertTrue(deploymentLock.hasQueuedThreads(), "deployment must be waiting for the lock");
            coordinator.cancel(PPlanFragmentCancelReason.USER_CANCEL, "cancel before deployment lock");
            deploymentLock.unlock();

            ExecutionException error = Assertions.assertThrows(ExecutionException.class,
                    () -> execution.get(5, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(StarRocksException.class, error.getCause());
            Assertions.assertTrue(error.getCause().getMessage().contains("cancel before deployment lock"));
            verify(schedule, never()).prepareSchedule(any(), any(), any());
            verify(schedule, never()).schedule(any());
            verify(schedule, never()).continueSchedule(any());
        } finally {
            if (deploymentLock.isHeldByCurrentThread()) {
                deploymentLock.unlock();
            }
            worker.shutdownNow();
            Assertions.assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testAuditStatisticsSnapshotsAreDetached() {
        Assertions.assertNull(coordinator.getAuditStatistics());
        coordinator.updateAuditStatistics(aiAuditReport(1));
        PQueryStatistics first = coordinator.getAuditStatistics();
        coordinator.updateAuditStatistics(aiAuditReport(2));
        PQueryStatistics second = coordinator.getAuditStatistics();
        Assertions.assertEquals(10L, first.aiStatistics.promptTokens);
        Assertions.assertEquals(1L, first.aiStatistics.promptUsageCount);
        Assertions.assertEquals(30L, second.aiStatistics.promptTokens);
        Assertions.assertEquals(3L, second.aiStatistics.promptUsageCount);
        Assertions.assertNotSame(first, second);
        Assertions.assertNotSame(first.aiStatistics, second.aiStatistics);

        second.aiStatistics.promptTokens = 999L;
        second.scanRows = 999L;
        PQueryStatistics third = coordinator.getAuditStatistics();
        Assertions.assertEquals(30L, third.aiStatistics.promptTokens);
        Assertions.assertEquals(3L, third.scanRows);
    }

    @Test
    public void testConcurrentAuditSnapshotsKeepUsagePairsConsistent() throws Exception {
        coordinator.updateAuditStatistics(aiAuditReport(1));
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> writer = workers.submit(() -> {
                start.await();
                for (int i = 0; i < 1000; i++) {
                    coordinator.updateAuditStatistics(aiAuditReport(1));
                }
                return null;
            });
            Future<?> reader = workers.submit(() -> {
                start.await();
                for (int i = 0; i < 1000; i++) {
                    AIExecutionStatisticsPB ai = coordinator.getAuditStatistics().aiStatistics;
                    Assertions.assertEquals(ai.promptUsageCount * 10, ai.promptTokens);
                    Assertions.assertEquals(ai.completionUsageCount * 20, ai.completionTokens);
                    Assertions.assertEquals(ai.totalUsageCount * 30, ai.totalTokens);
                }
                return null;
            });
            start.countDown();
            writer.get(30, TimeUnit.SECONDS);
            reader.get(30, TimeUnit.SECONDS);
        } finally {
            workers.shutdownNow();
            Assertions.assertTrue(workers.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    private static TReportAuditStatisticsParams aiAuditReport(long count) {
        TAIExecutionStatistics ai = new TAIExecutionStatistics()
                .setPrompt_tokens(10 * count).setPrompt_usage_count(count)
                .setCompletion_tokens(20 * count).setCompletion_usage_count(count)
                .setTotal_tokens(30 * count).setTotal_usage_count(count);
        return new TReportAuditStatisticsParams().setAudit_statistics(
                new TAuditStatistics().setScan_rows(count).setAi_statistics(ai));
    }

    private void testComputeBucketSeq2InstanceOrdinal(JoinNode.DistributionMode mode)
            throws IOException, StarRocksException {
        PlanFragment fragment = genFragment();
        ExecutionFragment execFragment = new ExecutionFragment(null, fragment, 0);
        FragmentInstance instance0 = new FragmentInstance(null, execFragment);
        FragmentInstance instance1 = new FragmentInstance(null, execFragment);
        FragmentInstance instance2 = new FragmentInstance(null, execFragment);
        instance0.addBucketSeq(2);
        instance0.addBucketSeq(0);
        instance1.addBucketSeq(1);
        instance1.addBucketSeq(4);
        instance2.addBucketSeq(3);
        instance2.addBucketSeq(5);

        execFragment.addInstance(instance0);
        execFragment.addInstance(instance1);
        execFragment.addInstance(instance2);

        OlapTable table = new OlapTable();
        table.maySetDatabaseId(1L);
        table.setBaseIndexMetaId(1L);
        table.setIndexMeta(1L, "base", Collections.singletonList(new Column("c0", IntegerType.INT)),
                0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setDefaultDistributionInfo(new HashDistributionInfo(6, Collections.emptyList()));
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        OlapScanNode scanNode = new OlapScanNode(new PlanNodeId(0), desc, "test-scan-node", table.getBaseIndexMetaId());
        scanNode.setSelectedPartitionIds(ImmutableList.of(0L, 1L));
        execFragment.getOrCreateColocatedAssignment(scanNode);

        RuntimeFilterDescription rf = new RuntimeFilterDescription(ctx.sessionVariable);
        rf.setJoinMode(mode);
        fragment.getBuildRuntimeFilters().put(1, rf);
        Assertions.assertTrue(rf.getBucketSeqToInstance() == null || rf.getBucketSeqToInstance().isEmpty());
        execFragment.setLayoutInfosForRuntimeFilters();
        Assertions.assertEquals(Arrays.asList(0, 1, 0, 2, 1, 2), rf.getBucketSeqToInstance());
    }

    @Test
    public void testLoadResourceUsageFromExecStatusReports() {
        Assertions.assertEquals(0, coordinator.getLoadCpuNs());
        Assertions.assertEquals(0, coordinator.getLoadPeakMemBytes());

        QueryRuntimeProfile queryProfile = coordinator.getQueryRuntimeProfile();
        // backend 1 reports twice (two instances), the second report carries the larger
        // cumulative values; backend 2 reports once
        TReportExecStatusParams be1First = new TReportExecStatusParams(FrontendServiceVersion.V1);
        be1First.setBackend_id(1L);
        be1First.setLoad_cpu_ns(100L);
        be1First.setLoad_peak_mem_bytes(500L);
        queryProfile.updateLoadInformation(null, be1First);
        TReportExecStatusParams be1Second = new TReportExecStatusParams(FrontendServiceVersion.V1);
        be1Second.setBackend_id(1L);
        be1Second.setLoad_cpu_ns(300L);
        be1Second.setLoad_peak_mem_bytes(700L);
        queryProfile.updateLoadInformation(null, be1Second);
        TReportExecStatusParams be2 = new TReportExecStatusParams(FrontendServiceVersion.V1);
        be2.setBackend_id(2L);
        be2.setLoad_cpu_ns(50L);
        be2.setLoad_peak_mem_bytes(600L);
        queryProfile.updateLoadInformation(null, be2);

        // cpu: max per backend, summed over backends; peak: the largest per-backend total
        Assertions.assertEquals(350L, coordinator.getLoadCpuNs());
        Assertions.assertEquals(700L, coordinator.getLoadPeakMemBytes());

        // a backend that only hosts a load channel shows up through the sink's observation
        TReportExecStatusParams channelOnly = new TReportExecStatusParams(FrontendServiceVersion.V1);
        channelOnly.setBackend_id(1L);
        channelOnly.setLoad_channel_peak_mem_bytes(900L);
        queryProfile.updateLoadInformation(null, channelOnly);
        Assertions.assertEquals(900L, coordinator.getLoadPeakMemBytes());
        Assertions.assertEquals(350L, coordinator.getLoadCpuNs());

        // audit statistics never lower what the reports established
        TAuditStatistics audit = new TAuditStatistics();
        audit.setCpu_cost_ns(1L);
        audit.setMem_cost_bytes(1L);
        TReportAuditStatisticsParams auditParams = new TReportAuditStatisticsParams();
        auditParams.setAudit_statistics(audit);
        coordinator.updateAuditStatistics(auditParams);
        Assertions.assertEquals(350L, coordinator.getLoadCpuNs());
        Assertions.assertEquals(900L, coordinator.getLoadPeakMemBytes());
    }

    @Test
    public void testLoadResourceUsageFallsBackToAuditStatistics() {
        Assertions.assertEquals(0, coordinator.getLoadCpuNs());
        Assertions.assertEquals(0, coordinator.getLoadPeakMemBytes());

        // two backends report audit statistics only: cpu time is summed, memory is the largest peak
        TAuditStatistics first = new TAuditStatistics();
        first.setCpu_cost_ns(100L);
        first.setMem_cost_bytes(50L);
        TReportAuditStatisticsParams firstParams = new TReportAuditStatisticsParams();
        firstParams.setAudit_statistics(first);
        coordinator.updateAuditStatistics(firstParams);

        TAuditStatistics second = new TAuditStatistics();
        second.setCpu_cost_ns(200L);
        second.setMem_cost_bytes(30L);
        TReportAuditStatisticsParams secondParams = new TReportAuditStatisticsParams();
        secondParams.setAudit_statistics(second);
        coordinator.updateAuditStatistics(secondParams);

        Assertions.assertEquals(300L, coordinator.getLoadCpuNs());
        Assertions.assertEquals(50L, coordinator.getLoadPeakMemBytes());
    }

    /**
     * Rolling upgrade, a load with a scan backend and a sink backend where only one of them
     * reports its own CPU time. The audit statistics the sink backend sends are already merged
     * over the scan backend, so they are the total and must not be added to a backend's report.
     */
    @Test
    public void testLoadCpuInMixedVersionsNeverCountsABackendTwice() {
        long scanCpu = 400L;
        long sinkCpu = 100L;
        TAuditStatistics audit = new TAuditStatistics();
        audit.setCpu_cost_ns(scanCpu + sinkCpu);
        TReportAuditStatisticsParams auditParams = new TReportAuditStatisticsParams();
        auditParams.setAudit_statistics(audit);
        QueryRuntimeProfile queryProfile = coordinator.getQueryRuntimeProfile();

        // only the scan backend is upgraded: its report is part of the audit total, not on top of it
        TReportExecStatusParams scan = new TReportExecStatusParams(FrontendServiceVersion.V1);
        scan.setBackend_id(1L);
        scan.setLoad_cpu_ns(scanCpu);
        queryProfile.updateLoadResourceUsage(null, scan);
        // before the audit statistics arrive the reported part is all that is known
        Assertions.assertEquals(scanCpu, coordinator.getLoadCpuNs());
        coordinator.updateAuditStatistics(auditParams);
        Assertions.assertEquals(scanCpu + sinkCpu, coordinator.getLoadCpuNs());

        // the sink backend reports too (every backend upgraded): still the same total
        TReportExecStatusParams sink = new TReportExecStatusParams(FrontendServiceVersion.V1);
        sink.setBackend_id(2L);
        sink.setLoad_cpu_ns(sinkCpu);
        queryProfile.updateLoadResourceUsage(null, sink);
        Assertions.assertEquals(scanCpu + sinkCpu, coordinator.getLoadCpuNs());
    }

    @Test
    public void testLoadCpuInMixedVersionsKeepsTheOldBackendsShare() {
        // only the sink backend is upgraded: its own report is 100, the audit total it sends
        // also carries the old scan backend's 400
        TReportExecStatusParams sink = new TReportExecStatusParams(FrontendServiceVersion.V1);
        sink.setBackend_id(2L);
        sink.setLoad_cpu_ns(100L);
        coordinator.getQueryRuntimeProfile().updateLoadResourceUsage(null, sink);
        TAuditStatistics audit = new TAuditStatistics();
        audit.setCpu_cost_ns(500L);
        TReportAuditStatisticsParams auditParams = new TReportAuditStatisticsParams();
        auditParams.setAudit_statistics(audit);
        coordinator.updateAuditStatistics(auditParams);

        Assertions.assertEquals(500L, coordinator.getLoadCpuNs());
    }

    @Test
    public void testColocateRuntimeFilter() throws IOException, StarRocksException {
        testComputeBucketSeq2InstanceOrdinal(JoinNode.DistributionMode.COLOCATE);
    }

    @Test
    public void testBucketShuffleRuntimeFilter() throws IOException, StarRocksException {
        testComputeBucketSeq2InstanceOrdinal(JoinNode.DistributionMode.LOCAL_HASH_BUCKET);
    }

    @Test
    public void testTimeoutHintUsesTableQueryTimeout() {
        // Cover DefaultCoordinator timeout hint branch:
        // DefaultCoordinator.java:960-963, 967-975

        // Prepare an executor with table timeout info
        StmtExecutor executor = new StmtExecutor(ctx, new QueryStatement(ValuesRelation.newDualRelation()));
        Deencapsulation.setField(executor, "tableQueryTimeoutTableName", "test.t0");
        Deencapsulation.setField(executor, "tableQueryTimeoutValue", 120);
        ctx.setExecutor(executor);

        // Make jobSpec.query_timeout match the table timeout, so DefaultCoordinator uses table_query_timeout hint.
        JobSpec jobSpec = Deencapsulation.getField(coordinator, "jobSpec");
        jobSpec.getQueryOptions().setQuery_timeout(120);

        Status timeoutStatus = new Status(TStatusCode.TIMEOUT, "timeout");

        com.starrocks.common.TimeoutException ex = Assertions.assertThrows(
                com.starrocks.common.TimeoutException.class,
                () -> Deencapsulation.invoke(coordinator, "dealStatusToTryRetry", timeoutStatus));
        Assertions.assertTrue(ex.getMessage().contains("table_query_timeout"));
        Assertions.assertTrue(ex.getMessage().contains("please increase"));
    }

    @Test
    public void testTimeoutHintFallbackWhenBuildHintThrows() {
        // Force DefaultCoordinator.java:967-969 (catch) and 975 (reportTimeoutException) to execute.
        // Ensure executor is present so the code enters the try-block.
        StmtExecutor executor = new StmtExecutor(ctx, new QueryStatement(ValuesRelation.newDualRelation()));
        ctx.setExecutor(executor);
        new Expectations(executor) {
            {
                executor.getTableQueryTimeoutInfo();
                result = new RuntimeException("mock exception for hint building");
                minTimes = 0;
            }
        };

        JobSpec jobSpec = Deencapsulation.getField(coordinator, "jobSpec");
        jobSpec.getQueryOptions().setQuery_timeout(120);

        Status timeoutStatus = new Status(TStatusCode.TIMEOUT, "timeout");
        com.starrocks.common.TimeoutException ex = Assertions.assertThrows(
                com.starrocks.common.TimeoutException.class,
                () -> Deencapsulation.invoke(coordinator, "dealStatusToTryRetry", timeoutStatus));
        // After catch, hint falls back to session variable query_timeout.
        Assertions.assertTrue(ex.getMessage().contains("query_timeout"));
        Assertions.assertTrue(ex.getMessage().contains("please increase"));
    }

    @Test
    public void testTimeoutHintUsesMetadataCollectQueryTimeoutForMetadataContext() {
        ctx.setMetadataContext(true);

        JobSpec jobSpec = Deencapsulation.getField(coordinator, "jobSpec");
        jobSpec.getQueryOptions().setQuery_timeout(300);

        Status timeoutStatus = new Status(TStatusCode.TIMEOUT, "timeout");
        com.starrocks.common.TimeoutException ex = Assertions.assertThrows(
                com.starrocks.common.TimeoutException.class,
                () -> Deencapsulation.invoke(coordinator, "dealStatusToTryRetry", timeoutStatus));
        Assertions.assertTrue(ex.getMessage().contains(SessionVariable.METADATA_COLLECT_QUERY_TIMEOUT));
        Assertions.assertFalse(ex.getMessage().contains("'" + SessionVariable.QUERY_TIMEOUT + "'"));
    }

    @Test
    public void testTimeoutHintUsesInsertTimeoutForLoad() {
        // In practice INSERT/CTAS timeouts are intercepted FE-side in StmtExecutor (which renders a richer
        // hint including the load timeout property). This test covers the fallback when BE returns TIMEOUT
        // before that polling fires: dealStatusToTryRetry must still name insert_timeout, not query_timeout.
        StmtExecutor executor = new StmtExecutor(ctx, new QueryStatement(ValuesRelation.newDualRelation()));
        new Expectations(executor) {
            {
                executor.isExecLoadType();
                result = true;
                minTimes = 0;
            }
        };
        ctx.setExecutor(executor);

        JobSpec jobSpec = Deencapsulation.getField(coordinator, "jobSpec");
        jobSpec.getQueryOptions().setQuery_timeout(300);

        Status timeoutStatus = new Status(TStatusCode.TIMEOUT, "timeout");
        com.starrocks.common.TimeoutException ex = Assertions.assertThrows(
                com.starrocks.common.TimeoutException.class,
                () -> Deencapsulation.invoke(coordinator, "dealStatusToTryRetry", timeoutStatus));
        Assertions.assertTrue(ex.getMessage().contains(SessionVariable.INSERT_TIMEOUT));
        Assertions.assertFalse(ex.getMessage().contains("'" + SessionVariable.QUERY_TIMEOUT + "'"));
    }

    @Test
    public void testLakeMetaVersionNotFoundStatusRaisesRetryableException() {
        String errorMsg = "lake tablet metadata version not found, tablet_id=10001, partition_id=10002, "
                + "version=144847: Not found";
        Status status = new Status(TStatusCode.LAKE_META_VERSION_NOT_FOUND, errorMsg);

        LakeMetaVersionNotFoundException ex = Assertions.assertThrows(LakeMetaVersionNotFoundException.class,
                () -> Deencapsulation.invoke(coordinator, "dealStatusToTryRetry", status));
        Assertions.assertEquals(errorMsg, ex.getMessage());
    }

    @Test
    public void testRetryableStatusReplacesRecordedCancelledStatus() {
        // A fragment that was merely cancelled can report before the one that actually failed. Letting
        // the CANCELLED stand would bury a retryable cause and fail a query that a retry would answer.
        Status queryStatus = Deencapsulation.getField(coordinator, "queryStatus");
        queryStatus.setStatus(Status.CANCELLED);
        queryStatus.setErrorMsg("Cancelled");

        Deencapsulation.invoke(coordinator, "updateStatus",
                new Status(TStatusCode.LAKE_META_VERSION_NOT_FOUND, "lake tablet metadata version not found"),
                new TUniqueId(1, 1));

        Status updated = Deencapsulation.getField(coordinator, "queryStatus");
        Assertions.assertEquals(TStatusCode.LAKE_META_VERSION_NOT_FOUND, updated.getErrorCode());
    }

    @Test
    public void testNonRetryableStatusKeepsRecordedCancelledStatus() {
        Status queryStatus = Deencapsulation.getField(coordinator, "queryStatus");
        queryStatus.setStatus(Status.CANCELLED);
        queryStatus.setErrorMsg("Cancelled");

        Deencapsulation.invoke(coordinator, "updateStatus",
                new Status(TStatusCode.INTERNAL_ERROR, "some other failure"), new TUniqueId(1, 1));

        Status updated = Deencapsulation.getField(coordinator, "queryStatus");
        Assertions.assertEquals(TStatusCode.CANCELLED, updated.getErrorCode());
    }

    @Test
    public void testDeliberateCancelIsNotReplacedByRetryableStatus() {
        // A KILL (or timeout, or limit reached) is final: a fragment error arriving afterwards must not
        // turn the stopped query into a retry.
        coordinator.cancel(PPlanFragmentCancelReason.USER_CANCEL, "cancelled by user");

        Deencapsulation.invoke(coordinator, "updateStatus",
                new Status(TStatusCode.LAKE_META_VERSION_NOT_FOUND, "lake tablet metadata version not found"),
                new TUniqueId(1, 1));

        Status updated = Deencapsulation.getField(coordinator, "queryStatus");
        Assertions.assertEquals(TStatusCode.CANCELLED, updated.getErrorCode());
    }

    @Test
    public void testKillAfterFragmentCancellationStillBlocksRetryableStatus() {
        // Ordering that the first version of this guard got wrong: a fragment records CANCELLED first,
        // so the user's KILL takes cancel()'s "we can't cancel twice" early return. The deliberate stop
        // must be recorded anyway, or a retryable fragment error arriving next would replace the
        // CANCELLED and make StmtExecutor replan and rerun a query the user explicitly stopped.
        Status queryStatus = Deencapsulation.getField(coordinator, "queryStatus");
        queryStatus.setStatus(Status.CANCELLED);
        queryStatus.setErrorMsg("Cancelled");

        coordinator.cancel(PPlanFragmentCancelReason.USER_CANCEL, "cancelled by user");

        Deencapsulation.invoke(coordinator, "updateStatus",
                new Status(TStatusCode.LAKE_META_VERSION_NOT_FOUND, "lake tablet metadata version not found"),
                new TUniqueId(1, 1));

        Status updated = Deencapsulation.getField(coordinator, "queryStatus");
        Assertions.assertEquals(TStatusCode.CANCELLED, updated.getErrorCode());
    }

    private static java.lang.reflect.Method handleErrorExecutionMethod() throws NoSuchMethodException {
        java.lang.reflect.Method m = DefaultCoordinator.class.getDeclaredMethod(
                "handleErrorExecution", Status.class,
                com.starrocks.qe.scheduler.dag.FragmentInstanceExecState.class, Throwable.class);
        m.setAccessible(true);
        return m;
    }

    @Test
    public void testHandleErrorExecutionSuppressesCancelAfterEos() throws Exception {
        // After the receiver got EOS (returnedAllResults=true), an in-flight stage-2 deploy that races
        // with our QUERY_FINISHED cancel returns CANCELLED on the BE. Those are not real errors and must
        // not surface as "[reason=INTERNAL_ERROR] [msg=null]" to the client.
        // Covers DefaultCoordinator.handleErrorExecution guard at line ~766.
        Deencapsulation.setField(coordinator, "returnedAllResults", true);
        java.lang.reflect.Method handle = handleErrorExecutionMethod();

        for (String beMsg : new String[] {"Query terminates prematurely", "QueryFinished", "Cancelled"}) {
            ctx.getState().reset();
            Status cancelled = new Status(TStatusCode.CANCELLED, beMsg);
            Assertions.assertDoesNotThrow(
                    () -> handle.invoke(coordinator, cancelled, null, null),
                    "handleErrorExecution must swallow post-EOS cancel: " + beMsg);
            Assertions.assertTrue(
                    ctx.getState().getErrorMessage() == null || ctx.getState().getErrorMessage().isEmpty(),
                    "post-EOS cancel must not set client error, msg was: " + ctx.getState().getErrorMessage());
        }
    }

    @Test
    public void testHandleErrorExecutionStillThrowsBeforeEos() throws Exception {
        // Sanity: when EOS has not been delivered, a non-internal CANCELLED still falls through to the
        // default branch and surfaces — i.e. the new guard must not swallow real pre-EOS errors.
        Deencapsulation.setField(coordinator, "returnedAllResults", false);
        java.lang.reflect.Method handle = handleErrorExecutionMethod();
        Status cancelled = new Status(TStatusCode.CANCELLED, "some real cancel reason");
        java.lang.reflect.InvocationTargetException ite = Assertions.assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> handle.invoke(coordinator, cancelled, null, null));
        Assertions.assertInstanceOf(StarRocksException.class, ite.getCause());
    }

    @Test
    public void testClearExternalResourcesOnlyOnce() {
        AtomicInteger clearCount = new AtomicInteger();
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        ScanNode scanNode = new ScanNode(new PlanNodeId(0), desc, "counting-scan") {
            @Override
            public void clear() {
                clearCount.incrementAndGet();
            }

            @Override
            public java.util.List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
                return Collections.emptyList();
            }

            @Override
            protected void toThrift(TPlanNode msg) {
            }
        };
        DefaultCoordinator coordinatorWithScan = new DefaultCoordinator.Factory().createQueryScheduler(
                ctx, Lists.newArrayList(), Collections.singletonList(scanNode), new TDescriptorTable(), null);

        coordinatorWithScan.clearExternalResources();
        coordinatorWithScan.clearExternalResources();

        Assertions.assertEquals(1, clearCount.get());
    }

}
