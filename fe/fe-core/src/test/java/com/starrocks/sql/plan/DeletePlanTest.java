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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DeletePlanTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    @Test
    public void testDelete() throws Exception {
        String explainString = getDeleteExecPlanString("delete from tprimary where pk = 1");
        assertTrue(explainString.contains("PREDICATES: 1: pk = 1"));

        testExplain("explain delete from tprimary where pk = 1");
        testExplain("explain verbose delete from tprimary where pk = 1");
        testExplain("explain costs delete from tprimary where pk = 1");
    }

    @Test
    public void testIcebergDeleteWithMultipleConditions() throws Exception {
        // Test DELETE with multiple WHERE conditions
        String[] deleteStatements = {
                "delete from iceberg0.unpartitioned_db.t0 where id = 1",
                "delete from iceberg0.unpartitioned_db.t0 where id > 100",
                "delete from iceberg0.unpartitioned_db.t0 where id between 10 and 100",
                "delete from iceberg0.unpartitioned_db.t0 where data like '%test%'"
        };

        for (String sql : deleteStatements) {
            String explainString = getDeleteExecPlanString(sql);
            assertNotNull(explainString, "Explain plan should not be null for SQL: " + sql);
            assertTrue(explainString.contains("ICEBERG DELETE SINK"));
        }
    }

    @Test
    public void testPartitionedIcebergTableDelete() throws Exception {
        // Test DELETE on partitioned Iceberg tables
        String[] deleteStatements = {
                "delete from iceberg0.partitioned_db.t1 where id = 1",
                "delete from iceberg0.partitioned_db.t1 where `date` = '2023-01-01'",
                "delete from iceberg0.partitioned_db.t1 where id > 100 and `date` >= '2023-01-01'"
        };

        for (String sql : deleteStatements) {
            ExecPlan execPlan = getDeleteExecPlan(sql);

            assertNotNull(execPlan);
            assertNotNull(execPlan.getFragments());
            assertFalse(execPlan.getFragments().isEmpty());
            assertSame(TPartitionType.HASH_PARTITIONED, execPlan.getFragments().get(1).getOutputPartition().getType());
        }
    }

    @Test
    public void testIcebergDeleteWithSubquery() throws Exception {
        // Test DELETE with subquery (like: DELETE WHERE id IN (SELECT id FROM other_table))
        String sql = "delete from iceberg0.unpartitioned_db.t0 where id in (select id from iceberg0.partitioned_db.t1)";
        String explainString = getDeleteExecPlanString(sql);
        assertTrue(explainString.contains("ICEBERG DELETE SINK"));
    }

    @Test
    public void testIcebergDeleteOutputColumns() throws Exception {
        // Test that DELETE returns _file and _pos columns for Iceberg position delete
        String sql = "delete from iceberg0.unpartitioned_db.t0 where id = 1";
        ExecPlan execPlan = getDeleteExecPlan(sql);

        assertNotNull(execPlan);
        assertNotNull(execPlan.getOutputExprs());
        assertTrue(execPlan.getOutputExprs().size() >= 2, "Should output _file and _pos");
        assertTrue(execPlan.getOutputExprs().get(0).debugString().contains("_file"));
        assertTrue(execPlan.getOutputExprs().get(1).debugString().contains("_pos"));
    }

    @Test
    public void testIsIcebergDeleteOperation() throws Exception {
        // Test case 1: Output columns contain _file and _pos
        String sql1 = "delete from iceberg0.unpartitioned_db.t0 where id = 1";
        ExecPlan execPlan1 = getDeleteExecPlan(sql1);
        assertNotNull(execPlan1);
        
        // Test case 2: Regular select statement (should not be considered DELETE)
        String sql2 = "select * from iceberg0.unpartitioned_db.t0 where id = 1";
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        List<StatementBase> statements = com.starrocks.sql.parser.SqlParser.parse(
                sql2, connectContext.getSessionVariable().getSqlMode());
        ExecPlan execPlan2 = StatementPlanner.plan(statements.get(0), connectContext);
        assertNotNull(execPlan2);
        
        // Verify that the delete plan has the DELETE flag set
        List<PlanFragment> deleteFragments = execPlan1.getFragments();
        boolean hasDeleteScanNode = false;
        for (PlanFragment fragment : deleteFragments) {
            PlanNode root = fragment.getPlanRoot();
            if (root instanceof IcebergScanNode) {
                IcebergScanNode icebergScanNode = (IcebergScanNode) root;
                if (icebergScanNode.isUsedForDelete()) {
                    hasDeleteScanNode = true;
                    break;
                }
            } else if (root instanceof ProjectNode) {
                // Check child nodes
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
        assertTrue(hasDeleteScanNode, "DELETE plan should have IcebergScanNode with usedForDelete=true");
        
        // Verify that the select plan does not have the DELETE flag set
        List<PlanFragment> selectFragments = execPlan2.getFragments();
        boolean hasSelectDeleteScanNode = false;
        for (PlanFragment fragment : selectFragments) {
            PlanNode root = fragment.getPlanRoot();
            if (root instanceof IcebergScanNode) {
                IcebergScanNode icebergScanNode = (IcebergScanNode) root;
                if (icebergScanNode.isUsedForDelete()) {
                    hasSelectDeleteScanNode = true;
                    break;
                }
            } else if (root instanceof ProjectNode) {
                // Check child nodes
                PlanNode child = root.getChild(0);
                if (child instanceof IcebergScanNode) {
                    IcebergScanNode icebergScanNode = (IcebergScanNode) child;
                    if (icebergScanNode.isUsedForDelete()) {
                        hasSelectDeleteScanNode = true;
                        break;
                    }
                }
            }
        }
        assertFalse(hasSelectDeleteScanNode, "SELECT plan should not have IcebergScanNode with usedForDelete=true");
    }

    private void testExplain(String explainStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.getState().reset();
        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(explainStmt, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statements.get(0));
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.EOF, connectContext.getState().getStateType());
    }

    private static ExecPlan getDeleteExecPlan(String originStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable().getSqlMode())
                        .get(0);
        connectContext.getDumpInfo().setOriginStmt(originStmt);
        return StatementPlanner.plan(statementBase, connectContext);
    }

    private static String getDeleteExecPlanString(String originStmt) throws Exception {
        ExecPlan execPlan = getDeleteExecPlan(originStmt);
        return execPlan.getExplainString(TExplainLevel.NORMAL);
    }
}