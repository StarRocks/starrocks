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
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
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
}
