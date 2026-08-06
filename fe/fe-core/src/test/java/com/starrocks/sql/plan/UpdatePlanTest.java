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

import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.UpdatePlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UpdatePlanTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE update_shadow_generated_column (\n" +
                "  pk bigint NOT NULL,\n" +
                "  v int NOT NULL,\n" +
                "  g bigint NULL AS (v + 1)\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY (pk)\n" +
                "DISTRIBUTED BY HASH (pk) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
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
    public void testUpdateSinkExcludesStaleGeneratedColumnsOutsideSchemaChange() throws Exception {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(connectContext.getDatabase()).getTable("update_shadow_generated_column");
        List<Column> originalFullSchema = table.getFullSchema();
        OlapTable.OlapTableState originalState = table.getState();
        List<Column> schemaWithStaleGeneratedColumns = new ArrayList<>(originalFullSchema);
        for (Column column : table.getBaseSchema()) {
            if (column.isGeneratedColumn()) {
                schemaWithStaleGeneratedColumns.add(column.deepCopy());
            }
        }
        try {
            for (OlapTable.OlapTableState state :
                    List.of(OlapTable.OlapTableState.OPTIMIZE, OlapTable.OlapTableState.NORMAL)) {
                table.setState(state);
                table.setNewFullSchema(originalFullSchema);
                String sql = "update update_shadow_generated_column set v = 2 where pk = 1";
                connectContext.setQueryId(UUIDUtil.genUUID());
                connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
                connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
                connectContext.getDumpInfo().setOriginStmt(sql);
                UpdateStmt updateStmt = (UpdateStmt) com.starrocks.sql.parser.SqlParser
                        .parse(sql, connectContext.getSessionVariable().getSqlMode()).get(0);
                Analyzer.analyze(updateStmt, connectContext);

                // Simulate a stale fullSchema becoming visible between analysis and sink planning.
                table.setNewFullSchema(schemaWithStaleGeneratedColumns);
                ExecPlan execPlan = new UpdatePlanner().plan(updateStmt, connectContext);
                OlapTableSink sink = (OlapTableSink) execPlan.getFragments().get(0).getSink();
                assertEquals(execPlan.getOutputExprs().size(), sink.getTupleDescriptor().getSlots().size());
            }
        } finally {
            table.setState(originalState);
            table.setNewFullSchema(originalFullSchema);
        }
    }

    @Test
    public void testUpdateSinkPreservesShadowColumnsDuringSchemaChange() throws Exception {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb(connectContext.getDatabase()).getTable("update_shadow_generated_column");
        List<Column> originalFullSchema = table.getFullSchema();
        OlapTable.OlapTableState originalState = table.getState();
        List<Column> schemaWithShadowGeneratedColumns = new ArrayList<>(originalFullSchema);
        for (Column column : table.getBaseSchema()) {
            if (column.isGeneratedColumn()) {
                Column shadowColumn = column.deepCopy();
                shadowColumn.setName(SchemaChangeHandler.SHADOW_NAME_PREFIX + column.getName());
                schemaWithShadowGeneratedColumns.add(shadowColumn);
            }
        }
        table.setNewFullSchema(schemaWithShadowGeneratedColumns);
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);

        try {
            ExecPlan execPlan = getUpdateExecPlanObject(
                    "update update_shadow_generated_column set v = 2 where pk = 1");
            OlapTableSink sink = (OlapTableSink) execPlan.getFragments().get(0).getSink();
            assertEquals(schemaWithShadowGeneratedColumns.size(), sink.getTupleDescriptor().getSlots().size());
            Assertions.assertTrue(sink.getTupleDescriptor().getSlots().stream()
                    .anyMatch(slot -> slot.getColumn().isShadowColumn()));
        } finally {
            table.setState(originalState);
            table.setNewFullSchema(originalFullSchema);
        }
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
        return getUpdateExecPlanObject(originStmt).getExplainString(TExplainLevel.NORMAL);
    }

    private static ExecPlan getUpdateExecPlanObject(String originStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable().getSqlMode())
                        .get(0);
        connectContext.getDumpInfo().setOriginStmt(originStmt);
        return new StatementPlanner().plan(statementBase, connectContext);
    }
}
