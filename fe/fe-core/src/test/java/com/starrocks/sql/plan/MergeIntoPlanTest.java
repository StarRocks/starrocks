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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.EmptySetNode;
import com.starrocks.planner.EnforceUniqueRowLocatorNode;
import com.starrocks.planner.HashJoinNode;
import com.starrocks.planner.IcebergRowDeltaSink;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.JoinNode;
import com.starrocks.planner.NestLoopJoinNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.MergeIntoPlanner;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.ExprToSql;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlan;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    private static boolean containsEnforceUnique(ExecPlan execPlan) {
        // The EnforceUniqueRowLocatorNode sits in the merge join's fragment, which is
        // not necessarily the sink fragment, so search across all fragments.
        return findNode(execPlan, EnforceUniqueRowLocatorNode.class) != null;
    }

    private static boolean containsPreserveShuffleJoin(OptExpression optExpression) {
        if (optExpression.getOp() instanceof PhysicalJoinOperator joinOperator &&
                joinOperator.isPreserveShuffleColumns()) {
            return true;
        }
        return optExpression.getInputs().stream().anyMatch(MergeIntoPlanTest::containsPreserveShuffleJoin);
    }

    private static List<Integer> findPreservedJoinInputShuffleColumnCounts(OptExpression optExpression) {
        if (optExpression.getOp() instanceof PhysicalJoinOperator joinOperator &&
                joinOperator.isPreserveShuffleColumns()) {
            return optExpression.getInputs().stream()
                    .map(MergeIntoPlanTest::findFirstShuffleDistributionColumnCount)
                    .collect(Collectors.toList());
        }
        for (OptExpression input : optExpression.getInputs()) {
            List<Integer> counts = findPreservedJoinInputShuffleColumnCounts(input);
            if (!counts.isEmpty()) {
                return counts;
            }
        }
        return List.of();
    }

    private static Integer findFirstShuffleDistributionColumnCount(OptExpression optExpression) {
        if (optExpression.getOp() instanceof PhysicalDistributionOperator distributionOperator &&
                distributionOperator.getDistributionSpec() instanceof HashDistributionSpec hashDistributionSpec) {
            return hashDistributionSpec.getHashDistributionDesc().getDistributionCols().size();
        }
        for (OptExpression input : optExpression.getInputs()) {
            Integer count = findFirstShuffleDistributionColumnCount(input);
            if (count != null) {
                return count;
            }
        }
        return null;
    }

    @Test
    public void testMergePlanContainsEnforceUnique() throws Exception {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        // The sink fragment root should be an EnforceUniqueRowLocatorNode
        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        PlanNode root = sinkFragment.getPlanRoot();
        assertTrue(root instanceof EnforceUniqueRowLocatorNode,
                "Sink fragment root should be EnforceUniqueRowLocatorNode, was: " + root.getClass().getSimpleName());
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
    public void testMergeElidesEnforceUniqueWhenSourceDistinctKeysCoverOnKeys() throws Exception {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT DISTINCT id, date FROM iceberg0.unpartitioned_db.t0_v2) AS s " +
                "ON t.id = s.id AND t.date = s.date " +
                "WHEN MATCHED THEN UPDATE SET data = 'updated'";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertFalse(containsEnforceUnique(execPlan),
                "MERGE should elide ENFORCE UNIQUE when source DISTINCT keys are covered by ON keys");
    }

    @Test
    public void testMergeKeepsEnforceUniqueWhenSourceDistinctKeysDoNotCoverOnKeys() throws Exception {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT DISTINCT id, data FROM iceberg0.unpartitioned_db.t0_v2) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertTrue(containsEnforceUnique(execPlan),
                "MERGE must keep ENFORCE UNIQUE when ON keys do not cover source DISTINCT keys");
    }

    @Test
    public void testMergeElidesEnforceUniqueWhenSourceGroupByKeysCoverOnKeys() throws Exception {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT id, date FROM iceberg0.unpartitioned_db.t0_v2 GROUP BY id, date) AS s " +
                "ON t.id = s.id AND t.date = s.date " +
                "WHEN MATCHED THEN UPDATE SET data = 'updated'";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertFalse(containsEnforceUnique(execPlan),
                "MERGE should elide ENFORCE UNIQUE when source GROUP BY keys are covered by ON keys");
    }

    @Test
    public void testMergeKeepsEnforceUniqueForGroupingSetsSource() throws Exception {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT id, date FROM iceberg0.unpartitioned_db.t0_v2 " +
                "       GROUP BY GROUPING SETS ((id, date), (id))) AS s " +
                "ON t.id = s.id AND t.date = s.date " +
                "WHEN MATCHED THEN UPDATE SET data = 'updated'";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertTrue(containsEnforceUnique(execPlan),
                "MERGE must keep ENFORCE UNIQUE for grouping sets source");
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
    public void testMergeJoinPreservesShuffleColumns() throws Exception {
        String sql = "MERGE INTO iceberg0.partitioned_db.t1_v2 AS t " +
                "USING (SELECT id, data, date FROM iceberg0.partitioned_db.t1_v2) AS s " +
                "ON t.id = s.id AND t.data = s.data AND t.date = s.date " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        ExecPlan execPlan = getMergeExecPlan(sql);

        assertTrue(containsPreserveShuffleJoin(execPlan.getPhysicalPlan()),
                "MERGE top join should keep the preserve-shuffle-columns anti-skew guard");
        List<Integer> shuffleColumnCounts =
                findPreservedJoinInputShuffleColumnCounts(execPlan.getPhysicalPlan());
        assertEquals(List.of(3, 3), shuffleColumnCounts,
                "MERGE top join inputs should keep all three ON-key shuffle columns (not pruned to one) "
                        + "to avoid runtime skew before EnforceUnique / the row-delta sink");
    }

    @Test
    public void testMergeJoinPreservesShuffleColumnsWithMergeJoinImplementation() throws Exception {
        String prevJoinImplementationMode = connectContext.getSessionVariable().getJoinImplementationMode();
        try {
            connectContext.getSessionVariable().setJoinImplementationMode("merge");
            String sql = "MERGE INTO iceberg0.partitioned_db.t1_v2 AS t " +
                    "USING (SELECT id, data, date FROM iceberg0.partitioned_db.t1_v2) AS s " +
                    "ON t.id = s.id AND t.data = s.data AND t.date = s.date " +
                    "WHEN MATCHED THEN UPDATE SET data = s.data";
            ExecPlan execPlan = getMergeExecPlan(sql);

            assertTrue(containsPreserveShuffleJoin(execPlan.getPhysicalPlan()),
                    "MERGE physical merge join should keep the preserve-shuffle-columns anti-skew guard");
            List<Integer> shuffleColumnCounts =
                    findPreservedJoinInputShuffleColumnCounts(execPlan.getPhysicalPlan());
            assertEquals(List.of(3, 3), shuffleColumnCounts,
                    "MERGE physical merge join inputs should keep all three ON-key shuffle columns (not pruned "
                            + "to one) to avoid runtime skew before EnforceUnique / the row-delta sink");
        } finally {
            connectContext.getSessionVariable().setJoinImplementationMode(prevJoinImplementationMode);
        }
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
    public void testMergeDerivedTargetPredicatePushedDownToTargetIcebergScan() throws Exception {
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT id, data, date FROM iceberg0.unpartitioned_db.t0_v2 WHERE id % 20 = 0) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        ExecPlan execPlan = getMergeExecPlan(sql);
        IcebergScanNode targetScan = findTargetIcebergScan(execPlan);
        assertNotNull(targetScan, "MERGE plan should have a target IcebergScanNode marked usedForDelete");

        String targetPredicates = targetScan.getConjuncts().stream()
                .map(ExprToSql::toSql)
                .map(String::toLowerCase)
                .collect(Collectors.joining(", "));
        assertTrue(targetPredicates.contains("id % 20 = 0"),
                "Derived source predicate should be pushed to target IcebergScanNode PREDICATES; got: "
                        + targetPredicates);
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
        // Verify the plan generates successfully.
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
    public void testSelfMergeSourceMetadataUsesTargetScanForConflictFilter() throws Exception {
        // The source side deliberately reads _file so its IcebergScanNode is also
        // marked usedForDelete. The conflict filter must still come from the MERGE
        // target side (t.date predicate), not from the source scan (s.id predicate).
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (" +
                "  SELECT id, data, date, _file AS src_file " +
                "  FROM iceberg0.unpartitioned_db.t0_v2 " +
                "  WHERE id = 1" +
                ") AS s " +
                "ON t.id = s.id AND t.date = '2024-01-02' " +
                "WHEN MATCHED AND s.src_file IS NOT NULL THEN UPDATE SET data = s.data";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        List<IcebergScanNode> scanNodes = findNodes(execPlan, IcebergScanNode.class);
        long usedForDeleteCount = scanNodes.stream().filter(IcebergScanNode::isUsedForDelete).count();
        assertTrue(usedForDeleteCount >= 2,
                "Both source metadata scan and target row-locator scan should be marked usedForDelete");

        IcebergRowDeltaSink sink = (IcebergRowDeltaSink) execPlan.getFragments().get(0).getSink();
        IcebergMetadata.IcebergSinkExtra extra = sink.getSinkExtraInfo();
        assertNotNull(extra);
        assertNotNull(extra.getConflictDetectionFilter(),
                "Target-side date predicate should produce a conflict detection filter");
        String filter = extra.getConflictDetectionFilter().toString();
        assertTrue(filter.contains("date"),
                "Conflict filter must come from target scan, not source metadata scan: " + filter);
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
    public void testEnforceUniqueKeySlotIdsMatchOutputExprs() throws Exception {
        // EnforceUniqueRowLocatorNode hands the BE the SLOT IDS of _file and _pos; the BE
        // resolves the chunk columns through the chunk's slot-id map. The UNION
        // ALL source forces source-side slots to interleave with _file/_pos slot
        // ids, which under the old physical-index contract used to shift the key
        // positions away from [0, 1].
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
        assertTrue(root instanceof EnforceUniqueRowLocatorNode,
                "Sink fragment root must be EnforceUniqueRowLocatorNode; was: " + root.getClass().getSimpleName());
        EnforceUniqueRowLocatorNode enforceNode = (EnforceUniqueRowLocatorNode) root;
        List<Integer> slotIds = enforceNode.getUniqueKeySlotIds();
        assertEquals(2, slotIds.size(), "Expected exactly 2 unique-key slots (_file, _pos)");
        assertNotEquals(slotIds.get(0), slotIds.get(1), "_file and _pos slots must differ");

        // outputExprs[0] is _file, outputExprs[1] is _pos (MergeIntoAnalyzer order).
        List<com.starrocks.sql.ast.expression.Expr> outputExprs = execPlan.getOutputExprs();
        SlotId expectedFileSlot = ((com.starrocks.sql.ast.expression.SlotRef) outputExprs.get(0)).getSlotId();
        SlotId expectedPosSlot = ((com.starrocks.sql.ast.expression.SlotRef) outputExprs.get(1)).getSlotId();
        assertEquals(expectedFileSlot.asInt(), slotIds.get(0).intValue(),
                "uniqueKeySlotIds[0] must be the _file slot; got " + slotIds);
        assertEquals(expectedPosSlot.asInt(), slotIds.get(1).intValue(),
                "uniqueKeySlotIds[1] must be the _pos slot; got " + slotIds);
    }

    @Test
    public void testEnforceUniqueSitsAboveShuffleMergeJoinInSameFragment() throws Exception {
        // The duplicate-match check is a LOCAL check whose correctness rests on the
        // merge join: the analyzer pins a SHUFFLE hint so the target side is never
        // broadcast (each target row owned by one instance), and the planner places
        // the check in the join's fragment, before any write-distribution exchange.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (" +
                "  SELECT 1 AS id, 'a' AS data, '2024-01-01' AS date " +
                "  UNION ALL SELECT 2 AS id, 'b' AS data, '2024-01-01' AS date " +
                ") AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data " +
                "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        EnforceUniqueRowLocatorNode enforceNode = findNode(execPlan, EnforceUniqueRowLocatorNode.class);
        assertNotNull(enforceNode, "Plan must contain an EnforceUniqueRowLocatorNode");
        HashJoinNode joinNode = findNode(execPlan, HashJoinNode.class);
        assertNotNull(joinNode, "Plan must contain the merge HashJoinNode");

        assertEquals(JoinNode.DistributionMode.PARTITIONED, joinNode.getDistrMode(),
                "Merge join must be a shuffle join (target side never broadcast)");
        assertEquals(enforceNode.getFragment(), joinNode.getFragment(),
                "The duplicate check must sit in the merge join's fragment, before any "
                        + "write-distribution exchange");
    }

    @Test
    public void testMergeHashJoinDisablesPostJoinPassthrough() throws Exception {
        boolean previous = setHashJoinInterpolatePassthrough(true);
        try {
            String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                    "USING (" +
                    "  SELECT 1 AS id, 'a' AS data, '2024-01-01' AS date " +
                    "  UNION ALL SELECT 2 AS id, 'b' AS data, '2024-01-01' AS date " +
                    ") AS s " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET data = s.data " +
                    "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
            ExecPlan execPlan = getMergeExecPlan(sql);
            HashJoinNode joinNode = findNode(execPlan, HashJoinNode.class);
            assertNotNull(joinNode, "Plan must contain the merge HashJoinNode");
            assertFalse(joinNode.getCanLocalShuffle(),
                    "MERGE planner must suppress post-join passthrough on the target join");

            TPlanNode thriftJoin = findThriftNode(joinNode.treeToThrift(), joinNode.getId().asInt());
            assertNotNull(thriftJoin, "Join node must be serialized to thrift");
            assertFalse(thriftJoin.hash_join_node.isSetInterpolate_passthrough()
                            && thriftJoin.hash_join_node.isInterpolate_passthrough(),
                    "MERGE duplicate checking relies on the join's local key distribution; "
                            + "post-join passthrough must be disabled");
        } finally {
            setHashJoinInterpolatePassthrough(previous);
        }
    }

    @Test
    public void testDuplicateConstantSourceMergeContainsEnforceUnique() throws Exception {
        int previousPipelineDop = connectContext.getSessionVariable().getPipelineDop();
        int previousPipelineSinkDop = connectContext.getSessionVariable().getPipelineSinkDop();
        connectContext.getSessionVariable().setPipelineDop(4);
        connectContext.getSessionVariable().setPipelineSinkDop(4);
        try {
            String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                    "USING (" +
                    "  SELECT 1 AS id, 'a' AS data, '2024-01-01' AS date " +
                    "  UNION ALL SELECT 1 AS id, 'b' AS data, '2024-01-01' AS date " +
                    ") AS s " +
                    "ON t.id = s.id " +
                    "WHEN MATCHED THEN UPDATE SET data = s.data";
            ExecPlan execPlan = getMergeExecPlan(sql);
            assertNotNull(execPlan);

            String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
            EnforceUniqueRowLocatorNode enforceNode = findNode(execPlan, EnforceUniqueRowLocatorNode.class);
            assertNotNull(enforceNode, "Plan must contain an EnforceUniqueRowLocatorNode:\n" + explainString);
            assertTrue(explainString.contains("ENFORCE UNIQUE"),
                    "Explain plan should contain ENFORCE UNIQUE:\n" + explainString);
            HashJoinNode joinNode = findNode(execPlan, HashJoinNode.class);
            assertNotNull(joinNode, "Plan must contain the merge HashJoinNode:\n" + explainString);
            assertEquals(JoinNode.DistributionMode.PARTITIONED, joinNode.getDistrMode(),
                    "Merge join must be a shuffle join");
        } finally {
            connectContext.getSessionVariable().setPipelineDop(previousPipelineDop);
            connectContext.getSessionVariable().setPipelineSinkDop(previousPipelineSinkDop);
        }
    }

    @Test
    public void testMergeTargetLocatorRejectsRowLocatorSlotsWithoutTargetScan() throws Exception {
        ExecPlan execPlan = new ExecPlan();
        DescriptorTable descTbl = execPlan.getDescTbl();

        TupleDescriptor sourceTuple = descTbl.createTupleDescriptor("synthetic-source");
        SlotDescriptor sourceSlot = descTbl.addSlotDescriptor(sourceTuple);
        sourceSlot.setColumn(new Column("id", IntegerType.INT));
        sourceSlot.setIsMaterialized(true);
        sourceTuple.computeMemLayout();

        TupleDescriptor targetTuple = descTbl.createTupleDescriptor("synthetic-target-row-locator");
        SlotDescriptor fileSlot = descTbl.addSlotDescriptor(targetTuple);
        fileSlot.setColumn(new Column("_file", VarcharType.VARCHAR));
        fileSlot.setIsMaterialized(true);
        SlotDescriptor posSlot = descTbl.addSlotDescriptor(targetTuple);
        posSlot.setColumn(new Column("_pos", IntegerType.BIGINT));
        posSlot.setIsMaterialized(true);
        targetTuple.computeMemLayout();

        PlanNode source = new EmptySetNode(new PlanNodeId(0), new ArrayList<>(List.of(sourceTuple.getId())));
        PlanNode rowLocatorOnlyTarget =
                new EmptySetNode(new PlanNodeId(1), new ArrayList<>(List.of(targetTuple.getId())));
        HashJoinNode joinNode = new HashJoinNode(new PlanNodeId(2), source, rowLocatorOnlyTarget,
                JoinOperator.LEFT_OUTER_JOIN, Collections.emptyList(), Collections.emptyList());
        execPlan.getFragments().add(new PlanFragment(new PlanFragmentId(0), joinNode, DataPartition.UNPARTITIONED));
        execPlan.getOutputExprs().add(new SlotRef(fileSlot));
        execPlan.getOutputExprs().add(new SlotRef(posSlot));

        Method method = MergeIntoPlanner.class.getDeclaredMethod(
                "findMergeTargetContext", ExecPlan.class, IcebergTable.class);
        method.setAccessible(true);
        IcebergTable targetTable = new IcebergTable(1, "srTableName", "iceberg0", "resource_name",
                "unpartitioned_db", "t0_v2", "", Collections.emptyList(), null, new HashMap<>());

        InvocationTargetException exception = assertThrows(InvocationTargetException.class,
                () -> method.invoke(null, execPlan, targetTable));
        assertTrue(exception.getCause() instanceof IllegalStateException,
                "Row-locator slots without target scan must fail loud: " + exception.getCause());
        assertTrue(exception.getCause().getMessage().contains("target scan"),
                "Error should mention missing target scan: " + exception.getCause().getMessage());
    }

    @Test
    public void testMergeRejectsNonEquiOnClause() {
        // Without a target-source equality predicate the join degrades to a
        // (broadcast-only) nestloop join. With a SCANNED source that replicates
        // target rows across instances and would let duplicate matches escape the
        // local check; the planner must reject it instead of silently planning it.
        // (A pure-constant source is exempt: its fragment runs single-instance.)
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING iceberg0.unpartitioned_db.t0_v2 AS s " +
                "ON t.id > s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        Exception e = assertThrows(Exception.class, () -> getMergeExecPlan(sql));
        assertTrue(e.getMessage().contains("equality predicate"),
                "Error should mention the equality predicate requirement: " + e.getMessage());
    }

    @Test
    public void testConstantSourceNestLoopMergeIsAccepted() throws Exception {
        // Constant folding rewrites the ON equality against a literal source into a
        // pushed-down predicate (t.id = 1), leaving a nestloop join with no equality
        // predicate. Its probe side is a pure constant relation, which the scheduler
        // runs as a single instance, so the local duplicate check stays sound and the
        // planner must ACCEPT this shape. Locks in the constant-source exemption of
        // validateMergeJoinKeepsTargetUnreplicated; if a future optimizer keeps the
        // hash join for constant sources, relax the nestloop assertion accordingly.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        NestLoopJoinNode nestLoopJoin = findNode(execPlan, NestLoopJoinNode.class);
        assertNotNull(nestLoopJoin,
                "Constant-source MERGE is expected to fold the ON equality and plan a nestloop join");
        assertFalse(nestLoopJoin.getCanLocalShuffle(),
                "MERGE planner must suppress post-join passthrough on the target join");
        EnforceUniqueRowLocatorNode enforceNode = findNode(execPlan, EnforceUniqueRowLocatorNode.class);
        assertNotNull(enforceNode, "Plan must contain an EnforceUniqueRowLocatorNode");
        assertEquals(enforceNode.getFragment(), nestLoopJoin.getFragment(),
                "The duplicate check must sit in the merge join's fragment");
    }

    @Test
    public void testEmptySourceMergeSkipsEnforceUnique() throws Exception {
        // A provably-empty source (a constant relation with a false filter) lets the
        // optimizer eliminate the merge join entirely. With no source rows there are no
        // matches and thus no possible duplicate match, so the planner must skip the
        // duplicate-match check and still produce a valid (no-op) plan, rather than
        // failing with "MERGE INTO physical plan must contain a join".
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (SELECT 1 AS id, 'new' AS data, '2024-01-01' AS date WHERE 1 = 0) AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN DELETE " +
                "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        assertNull(findNode(execPlan, JoinNode.class),
                "Empty-source MERGE should let the optimizer eliminate the merge join");
        assertNull(findNode(execPlan, EnforceUniqueRowLocatorNode.class),
                "With no merge join the duplicate-match check must be skipped, not inserted");
    }

    @Test
    public void testInsertOnlyMergeDoesNotTreatSourceJoinAsMergeJoin() throws Exception {
        // ON 1=0 lets the optimizer remove the target side of the MERGE join, but the
        // USING subquery still has its own join whose right side scans the target
        // table and reads Iceberg row-locator metadata. The duplicate-match check
        // must not attach to that source join.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (" +
                "  SELECT cast(s2._pos AS INT) AS id, s2._file AS data, s1.date " +
                "  FROM iceberg0.partitioned_db.t1_v2 AS s1 " +
                "  JOIN iceberg0.unpartitioned_db.t0_v2 AS s2 ON s1.id = s2.id" +
                ") AS s " +
                "ON 1 = 0 " +
                "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        assertNotNull(findNode(execPlan, JoinNode.class),
                "The source subquery join should remain in the physical plan");
        assertNull(findNode(execPlan, EnforceUniqueRowLocatorNode.class),
                "Insert-only MERGE must not insert duplicate-match check above a source join");
    }

    @Test
    public void testMergeRejectsMultiRowConstantNestLoop() {
        // A multi-row constant source with no source-target equality (ON references
        // only the target) plans a broadcast nestloop join. Both source rows match
        // every qualifying target row, so a target row is matched more than once. The
        // broadcast target is replicated and the multi-row probe is split across
        // drivers under pipeline_dop > 1, so the per-driver seen-sets would miss the
        // duplicate — the planner must reject this shape rather than plan it.
        String sql = "MERGE INTO iceberg0.unpartitioned_db.t0_v2 AS t " +
                "USING (" +
                "  SELECT 1 AS id, 'a' AS data, '2024-01-01' AS date " +
                "  UNION ALL SELECT 2 AS id, 'b' AS data, '2024-01-01' AS date " +
                ") AS s " +
                "ON t.id = 1 " +
                "WHEN MATCHED THEN UPDATE SET data = s.data";
        Exception e = assertThrows(Exception.class, () -> getMergeExecPlan(sql));
        assertTrue(e.getMessage().contains("equality predicate"),
                "Error should mention the equality predicate requirement: " + e.getMessage());
    }

    @Test
    public void testPartitionedMergeChecksInJoinFragmentBeforePartitionShuffle() throws Exception {
        // For partitioned targets the sink fragment is hash-partitioned by the
        // partition columns (write clustering). The duplicate check must stay in the
        // JOIN's fragment, BELOW that exchange, so the write distribution is free to
        // redistribute rows without affecting check correctness.
        String sql = "MERGE INTO iceberg0.partitioned_db.t1_v2 AS t " +
                "USING (" +
                "  SELECT 1 AS id, 'a' AS data, '2024-01-01' AS date " +
                "  UNION ALL SELECT 2 AS id, 'b' AS data, '2024-01-02' AS date " +
                ") AS s " +
                "ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET data = s.data " +
                "WHEN NOT MATCHED THEN INSERT (id, data, date) VALUES (s.id, s.data, s.date)";
        ExecPlan execPlan = getMergeExecPlan(sql);
        assertNotNull(execPlan);

        EnforceUniqueRowLocatorNode enforceNode = findNode(execPlan, EnforceUniqueRowLocatorNode.class);
        assertNotNull(enforceNode, "Plan must contain an EnforceUniqueRowLocatorNode");
        HashJoinNode joinNode = findNode(execPlan, HashJoinNode.class);
        assertNotNull(joinNode, "Plan must contain the merge HashJoinNode");

        assertEquals(JoinNode.DistributionMode.PARTITIONED, joinNode.getDistrMode(),
                "Merge join must be a shuffle join (target side never broadcast)");
        assertEquals(enforceNode.getFragment(), joinNode.getFragment(),
                "The duplicate check must sit in the merge join's fragment");
        assertNotEquals(execPlan.getFragments().get(0), enforceNode.getFragment(),
                "For partitioned targets the check must sit BELOW the partition-column "
                        + "shuffle, not in the sink fragment");
    }

    // Seed the BFS from EVERY fragment's plan root, not just the sink fragment.
    // After insertEnforceUniqueRowLocatorNode calls joinFragment.setPlanRoot(enforce),
    // the new root sits ABOVE the join, but the receiving ExchangeNode in the parent
    // fragment still links to the OLD root — so a getChildren() walk seeded only from
    // the sink fragment would never reach the enforce node. (The BE serializes each
    // fragment from its own planRoot, so this divergence is purely an FE-traversal
    // artifact and does not affect execution.)
    private static <T extends PlanNode> T findNode(ExecPlan execPlan, Class<T> clazz) {
        Queue<PlanNode> queue = new ArrayDeque<>();
        for (PlanFragment fragment : execPlan.getFragments()) {
            queue.add(fragment.getPlanRoot());
        }
        while (!queue.isEmpty()) {
            PlanNode node = queue.poll();
            if (clazz.isInstance(node)) {
                return clazz.cast(node);
            }
            queue.addAll(node.getChildren());
        }
        return null;
    }

    private static <T extends PlanNode> List<T> findNodes(ExecPlan execPlan, Class<T> clazz) {
        List<T> matches = new ArrayList<>();
        Queue<PlanNode> queue = new ArrayDeque<>();
        for (PlanFragment fragment : execPlan.getFragments()) {
            queue.add(fragment.getPlanRoot());
        }
        while (!queue.isEmpty()) {
            PlanNode node = queue.poll();
            if (clazz.isInstance(node)) {
                matches.add(clazz.cast(node));
            }
            queue.addAll(node.getChildren());
        }
        return matches;
    }

    private static TPlanNode findThriftNode(TPlan plan, int nodeId) {
        return plan.getNodes().stream()
                .filter(node -> node.node_id == nodeId)
                .findFirst()
                .orElse(null);
    }

    private static boolean setHashJoinInterpolatePassthrough(boolean value) throws Exception {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        Field field = SessionVariable.class.getDeclaredField("hashJoinInterpolatePassthrough");
        field.setAccessible(true);
        boolean previous = field.getBoolean(sessionVariable);
        field.setBoolean(sessionVariable, value);
        return previous;
    }

    @Test
    public void testMergeRejectsNonPipelineEngine() {
        // The BE EnforceUniqueRowLocatorNode is pipeline-only; planning must fail fast
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

    private static IcebergScanNode findTargetIcebergScan(ExecPlan execPlan) {
        for (PlanFragment fragment : execPlan.getFragments()) {
            for (PlanNode node : fragment.collectScanNodes().values()) {
                if (node instanceof IcebergScanNode icebergScanNode && icebergScanNode.isUsedForDelete()) {
                    return icebergScanNode;
                }
            }
        }
        return null;
    }
}
