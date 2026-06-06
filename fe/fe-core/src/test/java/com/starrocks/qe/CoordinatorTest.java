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
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import org.apache.commons.compress.utils.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

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
