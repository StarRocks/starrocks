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

package com.starrocks.connector.iceberg;

import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.iceberg.IcebergMetadata.IcebergSinkExtra;
import com.starrocks.connector.iceberg.IcebergRewriteDataJob;
import com.starrocks.connector.iceberg.IcebergRewriteDataJob.FinishArgs;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.IcebergRewriteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OriginStatement;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TSinkCommitInfo;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IcebergRewriteDataJobTest {

    private IcebergRewriteDataJob newJob() {
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        AlterTableStmt alter = Mockito.mock(AlterTableStmt.class);
        return new IcebergRewriteDataJob(
                "insert into t select 1",
                false,
                0L,
                10L,
                1L,
                ctx,
                alter
        );
    }

    @Test
    public void testFinishArgsGettersAndToString() {
        IcebergRewriteDataJob job = newJob();

        List<TSinkCommitInfo> commits = Arrays.asList(new TSinkCommitInfo(), new TSinkCommitInfo());
        Object extra = new Object();

        IcebergRewriteDataJob.FinishArgs fa =
                job.new FinishArgs("c1", "db1", "t1", commits, "branchA", extra);

        // getters
        Assertions.assertEquals("c1", fa.getCatalog());
        Assertions.assertEquals("db1", fa.getDb());
        Assertions.assertEquals("t1", fa.getTable());
        Assertions.assertSame(commits, fa.getCommitInfos());
        Assertions.assertEquals("branchA", fa.getBranch());
        Assertions.assertSame(extra, fa.getExtra());

        Assertions.assertEquals("FinishArgs{c1.db1.t1, commits=2}", fa.toString());
    }

    @Test
    public void testFinishArgsNullCommitInfosShouldThrow() {
        IcebergRewriteDataJob job = newJob();
        Assertions.assertThrows(NullPointerException.class, () ->
                job.new FinishArgs("c1", "db1", "t1", null, "branchA", new Object())
        );
    }

    @Test
    public void execute_shouldThrowWhenNotPrepared() {
        ConnectContext ctx = mock(ConnectContext.class, Mockito.RETURNS_DEEP_STUBS);
        AlterTableStmt alter = mock(AlterTableStmt.class);
        when(alter.getTableName()).thenReturn("t");

        IcebergRewriteDataJob job = new IcebergRewriteDataJob(
                "insert into t select 1", false, 0L, 10L, 1L, ctx, alter);

        IcebergScanNode scanNode = mock(IcebergScanNode.class);
        Deencapsulation.setField(job, "scanNodes", Collections.singletonList(scanNode));

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> job.execute());
        assertEquals("Must call prepare() before execute()", ex.getMessage());
    }

    private static ExecPlan mockPlanWithOneIcebergScan(IcebergScanNode scanNode) {
        ExecPlan plan = mock(ExecPlan.class);
        PlanFragment fragment = mock(PlanFragment.class);
        Map<PlanNodeId, ScanNode> scanMap = new HashMap<>();
        scanMap.put(new PlanNodeId(1), scanNode);

        ArrayList<PlanFragment> fragments = new ArrayList<>();
        fragments.add(fragment);

        when(plan.getFragments()).thenReturn(fragments);
        when(fragment.collectScanNodes()).thenReturn(scanMap);
        when(scanNode.getPlanNodeName()).thenReturn("IcebergScanNode");
        return plan;
    }

    @Test
    public void execute_happyPath_shouldRebuildScanRange_and_FinishSink() throws Exception {
        // ---- Base context & deps ----
        ConnectContext ctx = mock(ConnectContext.class, Mockito.RETURNS_DEEP_STUBS);
        QueryState state = mock(QueryState.class);
        when(ctx.getState()).thenReturn(state);
        when(ctx.getQueryId()).thenReturn(UUID.randomUUID());

        AlterTableStmt alter = mock(AlterTableStmt.class);
        when(alter.getTableName()).thenReturn("t");
        when(alter.getCatalogName()).thenReturn("c");
        when(alter.getDbName()).thenReturn("db");

        GlobalStateMgr gsm = mock(GlobalStateMgr.class);
        MetadataMgr metadata = mock(MetadataMgr.class);
        when(ctx.getGlobalStateMgr()).thenReturn(gsm);
        when(gsm.getMetadataMgr()).thenReturn(metadata);

        SessionVariable sv = mock(SessionVariable.class);
        when(ctx.getSessionVariable()).thenReturn(sv);
        when(sv.clone()).thenReturn(sv);

        IcebergRewriteDataJob job = new IcebergRewriteDataJob(
                "insert into t select 1", false, 0L, 10L, 1L, ctx, alter);

        // ---- Prepare minimal fields required by execute() ----
        InsertStmt parsedInsert = mock(InsertStmt.class);
        Deencapsulation.setField(job, "parsedStmt", parsedInsert);

        IcebergScanNode scanNode = mock(IcebergScanNode.class);
        ExecPlan mainPlan = mockPlanWithOneIcebergScan(scanNode);
        Deencapsulation.setField(job, "execPlan", mainPlan);
        Deencapsulation.setField(job, "scanNodes", Collections.singletonList(scanNode));

        IcebergRewriteData rewriteData = mock(IcebergRewriteData.class);
        when(rewriteData.hasMoreTaskGroup()).thenReturn(true).thenReturn(false);
        List<RemoteFileInfo> oneGroup = new ArrayList<>();
        RemoteFileInfo rfi = mock(RemoteFileInfo.class);
        oneGroup.add(rfi);
        when(rewriteData.nextTaskGroup()).thenReturn(oneGroup);
        Deencapsulation.setField(job, "rewriteData", rewriteData);

        IcebergScanNode localScanNode = mock(IcebergScanNode.class);
        ExecPlan localPlan = mockPlanWithOneIcebergScan(localScanNode);

        new MockUp<StatementPlanner>() {
            @Mock
            public ExecPlan plan(StatementBase stmt, ConnectContext session) {
                return localPlan;
            }
        };

        new MockUp<IcebergScanNode>() {
            @Mock
            public void rebuildScanRange(List<RemoteFileInfo> splits) { /* no-op */ }
        };
        
        StmtExecutor executor = mock(StmtExecutor.class);

        new MockUp<StmtExecutor>() {
            private static StmtExecutor HOLDER;
            { HOLDER = executor; }
            @Mock
            public static StmtExecutor newInternalExecutor(ConnectContext c, StatementBase s) {
                return HOLDER;
            }
        };

        IcebergRewriteStmt rewriteStmt = Mockito.mock(IcebergRewriteStmt.class);
        Deencapsulation.setField(job, "rewriteStmt", rewriteStmt);  
        InsertStmt fakeInsertStmt = Mockito.mock(InsertStmt.class);
        PartitionNames pn = mock(PartitionNames.class);
        when(fakeInsertStmt.getTableName()).thenReturn(new TableName("c", "db", "t"));
        when(fakeInsertStmt.getTargetPartitionNames()).thenReturn(pn);
        when(fakeInsertStmt.getLabel()).thenReturn("label_123");
        when(fakeInsertStmt.getTargetColumnNames()).thenReturn(Collections.emptyList());
        when(fakeInsertStmt.getQueryStatement()).thenReturn(null);
        when(fakeInsertStmt.isOverwrite()).thenReturn(false);
        when(fakeInsertStmt.getProperties()).thenReturn(new HashMap<>());
        OriginStatement base = mock(OriginStatement.class);
        when(fakeInsertStmt.getOrigStmt()).thenReturn(base);
        Deencapsulation.setField(job, "parsedStmt", fakeInsertStmt);
        new mockit.Expectations() {
            {
                new com.starrocks.sql.ast.IcebergRewriteStmt(fakeInsertStmt, anyBoolean);
                result = rewriteStmt;
                minTimes = 0;
            }
        };
        ConcurrentLinkedQueue<IcebergRewriteDataJob.FinishArgs> finishArg = new ConcurrentLinkedQueue<>();
        finishArg.add(job.new FinishArgs(
                "c", "db", "t",
                Arrays.asList(new TSinkCommitInfo(), new TSinkCommitInfo()),
                "main",
                new IcebergSinkExtra()
        ));
        Deencapsulation.setField(job, "collected", finishArg);
        // ---- run ----
        job.execute();

        // ---- verify ----
        verify(rewriteData, times(2)).hasMoreTaskGroup();
        verify(rewriteData, times(1)).nextTaskGroup();

        verify(scanNode, never()).rebuildScanRange(anyList()); 
        verify(localScanNode, times(1)).rebuildScanRange(eq(oneGroup));

        verify(executor, times(1)).handleDMLStmt(eq(localPlan), isA(com.starrocks.sql.ast.IcebergRewriteStmt.class));

        verify(state, never()).setError(anyString());
    }
}