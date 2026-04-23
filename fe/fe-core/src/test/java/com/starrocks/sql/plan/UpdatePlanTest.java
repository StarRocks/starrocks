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

package com.starrocks.sql.plan;

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ProjectNode;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPartitionType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UpdatePlanTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    @Test
    public void testUpdate() throws Exception {
        String explainString = getUpdateExecPlan("update tprimary set v1 = 'aaa' where pk = 1");
        Assertions.assertTrue(explainString.contains("PREDICATES: 1: pk = 1"));
        Assertions.assertTrue(explainString.contains("<slot 4> : 'aaa'"));

        explainString = getUpdateExecPlan("update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        Assertions.assertTrue(explainString.contains("v1 = 'aaa'"));
        Assertions.assertTrue(explainString.contains("CAST(CAST(3: v2 AS BIGINT) + 1 AS INT)"));

        testExplain("explain update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        testExplain("explain update tprimary set v2 = DEFAULT where v1 = 'aaa'");
        testExplain("explain update tprimary_auto_increment set v2 = DEFAULT where v1 = '123'");
        testExplain("explain verbose update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        testExplain("explain costs update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
    }

    @Test
    public void testColumnPartialUpdate() throws Exception {
        String oldVal = connectContext.getSessionVariable().getPartialUpdateMode();
        connectContext.getSessionVariable().setPartialUpdateMode("column");
        testExplain("explain update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        testExplain("explain update tprimary set v2 = DEFAULT where v1 = 'aaa'");
        testExplain("explain update tprimary_auto_increment set v2 = DEFAULT where v1 = '123'");
        testExplain("explain verbose update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        testExplain("explain costs update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        connectContext.getSessionVariable().setPartialUpdateMode(oldVal);
    }

    @Test
    public void testIcebergUpdateExplainContainsRowDeltaSink() throws Exception {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        String explainString = getIcebergUpdateExecPlanString(sql);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"),
                "Explain plan should contain ICEBERG ROW DELTA SINK");
    }

    @Test
    public void testIcebergUpdateExplainContainsIcebergScan() throws Exception {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        String explainString = getIcebergUpdateExecPlanString(sql);
        assertTrue(explainString.contains("IcebergScanNode"),
                "Explain plan should contain IcebergScanNode");
    }

    @Test
    public void testPartitionedIcebergUpdateHasHashShuffle() throws Exception {
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET data = 'new' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);

        assertNotNull(execPlan);
        assertNotNull(execPlan.getFragments());
        assertSame(TPartitionType.HASH_PARTITIONED,
                execPlan.getFragments().get(1).getOutputPartition().getType());
    }

    @Test
    public void testUnpartitionedIcebergUpdateHasRowDeltaSink() throws Exception {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ROW DELTA"),
                "Explain plan should contain ROW DELTA");
    }

    @Test
    public void testIcebergUpdateScanNodeUsedForDelete() throws Exception {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);

        List<PlanFragment> fragments = execPlan.getFragments();
        boolean hasDeleteScanNode = false;
        for (PlanFragment fragment : fragments) {
            PlanNode root = fragment.getPlanRoot();
            if (root instanceof IcebergScanNode) {
                IcebergScanNode icebergScanNode = (IcebergScanNode) root;
                if (icebergScanNode.isUsedForDelete()) {
                    hasDeleteScanNode = true;
                    break;
                }
            } else if (root instanceof ProjectNode) {
                PlanNode child = root.getChild(0);
                if (child instanceof IcebergScanNode) {
                    IcebergScanNode icebergScanNode = (IcebergScanNode) child;
                    if (icebergScanNode.isUsedForDelete()) {
                        hasDeleteScanNode = true;
                        break;
                    }
                }
            }
        }
        assertTrue(hasDeleteScanNode,
                "UPDATE plan should have IcebergScanNode with usedForDelete=true");
    }

    @Test
    public void testIcebergUpdateExplain() throws Exception {
        String explainStmt = "explain UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.getState().reset();
        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(explainStmt, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statements.get(0));
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.EOF, connectContext.getState().getStateType());
    }

    private void testExplain(String explainStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.getState().reset();
        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(explainStmt, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statements.get(0));
        stmtExecutor.execute();
        Assertions.assertEquals(connectContext.getState().getStateType(), QueryState.MysqlStateType.EOF);
    }

    private static String getUpdateExecPlan(String originStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable().getSqlMode())
                        .get(0);
        connectContext.getDumpInfo().setOriginStmt(originStmt);
        ExecPlan execPlan = new StatementPlanner().plan(statementBase, connectContext);

        String ret = execPlan.getExplainString(TExplainLevel.NORMAL);
        return ret;
    }

    private static ExecPlan getIcebergUpdateExecPlan(String originStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable().getSqlMode())
                        .get(0);
        connectContext.getDumpInfo().setOriginStmt(originStmt);
        return StatementPlanner.plan(statementBase, connectContext);
    }

    private static String getIcebergUpdateExecPlanString(String originStmt) throws Exception {
        ExecPlan execPlan = getIcebergUpdateExecPlan(originStmt);
        return execPlan.getExplainString(TExplainLevel.NORMAL);
    }

    @Test
    public void testIcebergUpdateMultipleColumns() throws Exception {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new', date = '2024-06-01' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"),
                "Multi-column update should produce ICEBERG ROW DELTA SINK");
    }

    @Test
    public void testIcebergUpdatePartitionedTablePlan() throws Exception {
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET data = 'new' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"),
                "Partitioned update should produce ICEBERG ROW DELTA SINK");
    }

    @Test
    public void testIcebergUpdateVerboseExplain() throws Exception {
        String explainStmt = "explain verbose UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.getState().reset();
        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(explainStmt, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statements.get(0));
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.EOF, connectContext.getState().getStateType());
    }

    @Test
    public void testIcebergUpdateCostsExplain() throws Exception {
        String explainStmt = "explain costs UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.getState().reset();
        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(explainStmt, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statements.get(0));
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.EOF, connectContext.getState().getStateType());
    }

    @Test
    public void testIcebergUpdateFragmentSinkType() throws Exception {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);
        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        assertNotNull(sinkFragment.getSink());
        assertTrue(sinkFragment.getSink() instanceof com.starrocks.planner.IcebergRowDeltaSink,
                "Sink should be IcebergRowDeltaSink");
    }

    @Test
    public void testIcebergUpdatePartitionedMultiColumnPlan() throws Exception {
        // Partitioned table + multi-column SET exercises the partition-shuffle property,
        // multi-expr cast path, and the shared RowDelta sink construction together.
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET data = 'new' WHERE id < 10";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);

        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"));
        assertTrue(explainString.contains("IcebergScanNode"));

        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        assertTrue(sinkFragment.getSink() instanceof com.starrocks.planner.IcebergRowDeltaSink);
    }

    @Test
    public void testIcebergUpdateSessionVariableRestoredAfterPlan() throws Exception {
        // UpdatePlanner flips enable_local_shuffle_agg off while planning, then
        // restores it in the finally block. Confirm we return to the caller's value.
        boolean prev = connectContext.getSessionVariable().isEnableLocalShuffleAgg();
        try {
            connectContext.getSessionVariable().setEnableLocalShuffleAgg(true);
            String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
            ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
            assertNotNull(execPlan);
            Assertions.assertTrue(connectContext.getSessionVariable().isEnableLocalShuffleAgg(),
                    "enable_local_shuffle_agg should be restored to true");
        } finally {
            connectContext.getSessionVariable().setEnableLocalShuffleAgg(prev);
        }
    }

    @Test
    public void testIcebergUpdateSinkFragmentHasIcebergTableSink() throws Exception {
        // The sink fragment should be flagged as having an Iceberg table sink
        // so pipeline scheduling picks the right execution strategy.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);
        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        assertTrue(sinkFragment.hasIcebergTableSink(),
                "Sink fragment should be marked with hasIcebergTableSink");
    }

    @Test
    public void testIcebergUpdateWithComplexPredicate() throws Exception {
        // Non-trivial WHERE (AND + comparison) exercises conflict-detection filter
        // synthesis inside setupIcebergRowDeltaSink.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id > 5 AND id < 100";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"));
    }

    @Test
    public void testIcebergUpdatePipelineDopIsSet() throws Exception {
        // The Iceberg sink pipeline configurer must set a concrete pipeline DOP
        // on the sink fragment (>= 1) regardless of adaptive sink DOP setting.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        Assertions.assertTrue(sinkFragment.getPipelineDop() >= 1,
                "Sink fragment pipeline DOP must be >= 1");
    }

    @Test
    public void testIcebergUpdateOutputExprsMatchSchema() throws Exception {
        // The sink tuple slots are built from execPlan.getOutputExprs() — verify
        // the planner produces the expected 6 output exprs for a V2 unpartitioned
        // update: [_file, _pos, id, data, date, op_code].
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan.getOutputExprs());
        Assertions.assertEquals(6, execPlan.getOutputExprs().size());
    }

    @Test
    public void testIcebergUpdatePartitionedShuffleUsesPartitionColumn() throws Exception {
        // The partition shuffle property must hash on the partition column id.
        // The upstream fragment (fragment[1]) is the one that feeds the sink with
        // a HASH_PARTITIONED output partition for partitioned tables.
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET data = 'x' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);
        assertSame(TPartitionType.HASH_PARTITIONED,
                execPlan.getFragments().get(1).getOutputPartition().getType());
    }
}