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
import com.starrocks.planner.EnforceUniqueNode;
import com.starrocks.planner.IcebergRowDeltaSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.planner.TupleId;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MergeIntoPlanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    private static ExecPlan getMergeExecPlan(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(sql, connectContext.getSessionVariable().getSqlMode())
                        .get(0);
        connectContext.getDumpInfo().setOriginStmt(sql);
        return StatementPlanner.plan(statementBase, connectContext);
    }

    private static String getMergeExecPlanString(String sql) throws Exception {
        ExecPlan execPlan = getMergeExecPlan(sql);
        return execPlan.getExplainString(TExplainLevel.NORMAL);
    }

    @Test
    public void testMergePlanContainsEnforceUnique() throws Exception {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        // The sink fragment root should be an EnforceUniqueNode
        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        PlanNode root = sinkFragment.getPlanRoot();
        assertTrue(root instanceof EnforceUniqueNode,
                "Sink fragment root should be EnforceUniqueNode, was: " + root.getClass().getSimpleName());
    }

    @Test
    public void testMergePlanContainsRowDeltaSink() throws Exception {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        assertNotNull(sinkFragment.getSink());
        assertTrue(sinkFragment.getSink() instanceof IcebergRowDeltaSink,
                "Sink should be IcebergRowDeltaSink");
    }

    @Test
    public void testMergePlanExplainContainsRowDelta() throws Exception {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        String explainString = getMergeExecPlanString(sql);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"),
                "Explain plan should contain ICEBERG ROW DELTA SINK");
    }

    @Test
    public void testMergePlanContainsEnforceUniqueInExplain() throws Exception {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN DELETE";
        String explainString = getMergeExecPlanString(sql);
        assertTrue(explainString.contains("ENFORCE UNIQUE"),
                "Explain plan should contain ENFORCE UNIQUE: " + explainString);
    }

    @Test
    public void testMergePlanPartitionedTableHasShuffle() throws Exception {
        // t1_v2 is partitioned by 'date' — verify plan generates correctly for partitioned tables.
        // Note: with a trivial constant source, the optimizer may choose NESTLOOP JOIN with
        // broadcast rather than HASH shuffle. The partition shuffle property is set correctly
        // by the planner, but the optimizer may produce a different distribution based on cost.
        // This test verifies correctness (plan generates, sink is correct), not specific distribution.
        String sql = "MERGE INTO iceberg0.partitioned_db.t1_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ENFORCE UNIQUE"),
                "Partitioned MERGE should contain ENFORCE UNIQUE node");
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"),
                "Partitioned MERGE should produce ICEBERG ROW DELTA SINK");
    }

    @Test
    public void testMergePlanContainsIcebergScanNode() throws Exception {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        String explainString = getMergeExecPlanString(sql);
        assertTrue(explainString.contains("IcebergScanNode"),
                "Explain plan should contain IcebergScanNode");
    }

    @Test
    public void testMergePlanAllClauses() throws Exception {
        // All three clause types in a single MERGE statement
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED AND s.data = 'delete' THEN DELETE " +
                "WHEN MATCHED THEN UPDATE SET data = s.data " +
                "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"),
                "All-clauses MERGE should produce ICEBERG ROW DELTA SINK");
        assertTrue(explainString.contains("ENFORCE UNIQUE"),
                "All-clauses MERGE should have ENFORCE UNIQUE node");
    }

    @Test
    public void testMergePlanPartitionedTableExplainContainsSink() throws Exception {
        String sql = "MERGE INTO iceberg0.partitioned_db.t1_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        String explainString = getMergeExecPlanString(sql);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"),
                "Partitioned MERGE should produce ICEBERG ROW DELTA SINK");
    }

    @Test
    public void testMergePlanNotMatchedInsert() throws Exception {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 2 AS id, 'inserted' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        assertNotNull(sinkFragment.getSink());
        assertTrue(sinkFragment.getSink() instanceof IcebergRowDeltaSink,
                "NOT MATCHED INSERT MERGE should use IcebergRowDeltaSink");
    }

    @Test
    public void testSelfMergePlanGeneratesSuccessfully() throws Exception {
        // Self-merge: MERGE INTO t USING t — both scans are the same table.
        // Verify the plan generates successfully (conflict filter should use
        // the target scan identified by isUsedForDelete, not the source scan).
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING iceberg0.unpartitioned_db.t0_v2 AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ENFORCE UNIQUE"),
                "Self-merge should contain ENFORCE UNIQUE node");
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"),
                "Self-merge should produce ICEBERG ROW DELTA SINK");
        // Should have two IcebergScanNode instances (one for source, one for target)
        int scanCount = explainString.split("IcebergScanNode").length - 1;
        assertTrue(scanCount >= 2,
                "Self-merge should have at least 2 IcebergScanNodes, found: " + scanCount);
    }

    @Test
    public void testMergePlanPositionalInsertValues() throws Exception {
        // INSERT VALUES without column list — positional mapping to schema order
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 99 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.data, s.date)";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"),
                "Positional INSERT MERGE should produce ICEBERG ROW DELTA SINK");
    }

    @Test
    public void testMergeDuplicateSetLastWins() throws Exception {
        // Regression for dup-SET precedence: when the SET list targets the
        // same column twice, the LAST assignment must reach the plan — matching
        // StarRocks OLAP UPDATE and MySQL. Prior to the fix, the analyzer's
        // emission layer (getMatchedColumnValue) returned on the FIRST match
        // while the validation-layer Map.put() was last-wins, so the two
        // layers silently disagreed.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = 'first_wins', data = 'last_wins'";
        String explainString = getMergeExecPlanString(sql);
        assertTrue(explainString.contains("'last_wins'"),
                "Duplicate SET should emit the LAST literal in the plan; " +
                        "got:\n" + explainString);
        assertTrue(!explainString.contains("'first_wins'"),
                "Duplicate SET must NOT emit the first literal in the plan; " +
                        "got:\n" + explainString);
    }

    @Test
    public void testEnforceUniqueKeyIndicesMatchPhysicalLayout() throws Exception {
        // Regression for the `[0, 1]` hardcoding bug (and its common-sub-slot
        // follow-up): EnforceUniqueNode's uniqueKeyColIndices must reflect the
        // ACTUAL physical positions of _file and _pos in the plan-root's chunk
        // output, NOT a hardcoded pair.
        //
        // We construct a MERGE whose source is a multi-branch UNION ALL. This
        // forces source-side ColumnRefOperators (and common sub-expressions
        // from the MergeIntoAnalyzer's `_file IS NOT NULL` CASE predicate) to
        // get slot IDs, and gives the optimizer enough room to assign some of
        // them before _file / _pos. The emitted ExecPlan must:
        //
        //   - find the EnforceUniqueNode we inserted
        //   - compute physicalOrder = materialized slots of the plan root's
        //     tuple(s), sorted ascending by slot ID
        //   - verify uniqueKeyColIndices[0] lands on the slot referenced by
        //     outputExprs[0] (i.e. _file), and [1] lands on outputExprs[1] (_pos)
        //
        // Locks in: slot-ID-based resolution + materialized-only filter.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (" +
                "  SELECT 1 AS id, 'a' AS data, '2024-01-01' AS date " +
                "  UNION ALL SELECT 2 AS id, 'b' AS data, '2024-01-01' AS date " +
                "  UNION ALL SELECT 3 AS id, 'c' AS data, '2024-01-01' AS date " +
                ") AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data " +
                "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        PlanNode root = sinkFragment.getPlanRoot();
        assertTrue(root instanceof EnforceUniqueNode,
                "Sink fragment root must be EnforceUniqueNode; was: " + root.getClass().getSimpleName());
        EnforceUniqueNode enforceNode = (EnforceUniqueNode) root;
        List<Integer> indices = enforceNode.getUniqueKeyColIndices();
        assertEquals(2, indices.size(), "Expected exactly 2 unique-key indices (_file, _pos)");
        assertNotEquals(indices.get(0), indices.get(1), "_file and _pos indices must differ");

        // Compute the expected physical layout the same way the planner does:
        // materialized slots of the EnforceUnique child's tuple, sorted by slot-ID.
        PlanNode child = enforceNode.getChild(0);
        List<SlotId> physicalOrder = new ArrayList<>();
        for (TupleId tid : child.getTupleIds()) {
            TupleDescriptor tupleDesc = execPlan.getDescTbl().getTupleDesc(tid);
            if (tupleDesc == null) {
                continue;
            }
            for (SlotDescriptor slot : tupleDesc.getSlots()) {
                if (slot.isMaterialized()) {
                    physicalOrder.add(slot.getId());
                }
            }
        }
        physicalOrder.sort(Comparator.comparingInt(SlotId::asInt));

        // outputExprs[0] is _file, outputExprs[1] is _pos (MergeIntoAnalyzer order).
        // The key-col indices must resolve back to those same slot IDs.
        List<com.starrocks.sql.ast.expression.Expr> outputExprs = execPlan.getOutputExprs();
        SlotId expectedFileSlot = ((com.starrocks.sql.ast.expression.SlotRef) outputExprs.get(0)).getSlotId();
        SlotId expectedPosSlot = ((com.starrocks.sql.ast.expression.SlotRef) outputExprs.get(1)).getSlotId();
        assertEquals(expectedFileSlot, physicalOrder.get(indices.get(0)),
                "uniqueKeyColIndices[0] must point at the _file slot. " +
                        "indices=" + indices + ", physicalOrder=" + physicalOrder +
                        ", expected _file slot=" + expectedFileSlot);
        assertEquals(expectedPosSlot, physicalOrder.get(indices.get(1)),
                "uniqueKeyColIndices[1] must point at the _pos slot. " +
                        "indices=" + indices + ", physicalOrder=" + physicalOrder +
                        ", expected _pos slot=" + expectedPosSlot);
    }

    @Test
    public void testMergeRejectsNonPipelineEngine() {
        // The BE EnforceUniqueNode is pipeline-only; planning must fail fast
        // with a clear message instead of erroring out on the BE at runtime.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        boolean prev = connectContext.getSessionVariable().isEnablePipelineEngine();
        connectContext.getSessionVariable().setEnablePipelineEngine(false);
        try {
            Exception e = assertThrows(Exception.class, () -> getMergeExecPlan(sql));
            assertTrue(e.getMessage().contains("requires the pipeline engine"),
                    "Error should mention pipeline engine: " + e.getMessage());
        } finally {
            connectContext.getSessionVariable().setEnablePipelineEngine(prev);
        }
    }
}
