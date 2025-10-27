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

package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CompactionClause;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AnalyzeAlterTableStatementTest {
    private static ConnectContext connectContext;
    private static AlterTableClauseAnalyzer clauseAnalyzerVisitor;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        connectContext = AnalyzeTestUtil.getConnectContext();
        clauseAnalyzerVisitor = new AlterTableClauseAnalyzer(null);
    }

    @Test
    public void testTableRename() {
        AlterTableStmt alterTableStmt = (AlterTableStmt) analyzeSuccess("alter table t0 rename test1");
        Assertions.assertEquals(alterTableStmt.getAlterClauseList().size(), 1);
        Assertions.assertTrue(alterTableStmt.getAlterClauseList().get(0) instanceof TableRenameClause);
        analyzeFail("alter table test rename");
    }

    @Test
    public void testEmptyNewTableName() {
        assertThrows(SemanticException.class, () -> {
            TableRenameClause clause = new TableRenameClause("");
            clauseAnalyzerVisitor.analyze(connectContext, clause);
        });
    }

    @Test
    public void testNoClause() {
        assertThrows(SemanticException.class, () -> {
            List<AlterClause> ops = Lists.newArrayList();
            AlterTableStmt alterTableStmt = new AlterTableStmt(new TableName("testDb", "testTbl"), ops);
            AlterTableStatementAnalyzer.analyze(alterTableStmt, AnalyzeTestUtil.getConnectContext());
        });
    }

    @Test
    public void testCompactionClause() {
        assertThrows(SemanticException.class, () -> {
            new MockUp<RunMode>() {
                @Mock
                public RunMode getCurrentRunMode() {
                    return RunMode.SHARED_DATA;
                }
            };

            List<AlterClause> ops = Lists.newArrayList();
            NodePosition pos = new NodePosition(1, 23, 1, 48);
            ops.add(new CompactionClause(true, pos));
            AlterTableStmt alterTableStmt = new AlterTableStmt(new TableName("testDb", "testTbl"), ops);
            AlterTableStatementAnalyzer.analyze(alterTableStmt, AnalyzeTestUtil.getConnectContext());
        });
    }

    @Test
    public void testCreateIndex() throws Exception {
        String sql = "CREATE INDEX index1 ON `test`.`t0` (`v1`) USING BITMAP COMMENT 'balabala'";
        analyzeSuccess(sql);

        sql = "alter table t0 add index index1 (v2)";
        analyzeSuccess(sql);

        AnalyzeTestUtil.getStarRocksAssert().withTable("CREATE TABLE test.bitmapTable\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ") AGGREGATE KEY (k1, k2)\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        // create bitmap index on v1
        sql = "CREATE INDEX index1 ON `test`.`bitmapTable` (`v1`) USING BITMAP COMMENT 'balabala'";
        StatementBase statement = SqlParser.parseSingleStatement(sql, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
        stmtExecutor.execute();
        Assertions.assertEquals(connectContext.getState().getErrType(), QueryState.ErrType.INTERNAL_ERR);
        connectContext.getState().getErrorMessage()
                .contains(
                        "BITMAP index only used in columns of " +
                                "DUP_KEYS/PRIMARY_KEYS table or key columns of UNIQUE_KEYS/AGG_KEYS table");
    }

    @Test
    public void testDropIndex() {
        String sql = "DROP INDEX index1 ON test.t0";
        analyzeSuccess(sql);

        sql = "alter table t0 drop index index1";
        analyzeSuccess(sql);
    }

    @Test
    public void testModifyTableProperties() {
        analyzeSuccess("ALTER TABLE test.t0 SET (\"default.replication_num\" = \"2\");");
        analyzeSuccess("ALTER TABLE test.t0 SET (\"datacache.partition_duration\" = \"10 days\");");
        analyzeFail("ALTER TABLE test.t0 SET (\"datacache.partition_duration\" = \"abcd\");", "Cannot parse text to Duration");
        analyzeFail("ALTER TABLE test.t0 SET (\"default.replication_num\" = \"2\", \"dynamic_partition.enable\" = \"true\");",
                "Can only set one table property at a time");
        analyzeFail("ALTER TABLE test.t0 SET (\"abc\" = \"2\");",
                "Unknown properties: {abc=2}");
        analyzeFail("ALTER TABLE test.t0 SET (\"send_clear_alter_tasks\" = \"FALSE\");",
                "Property send_clear_alter_tasks should be set to true");
        analyzeFail("ALTER TABLE test.t0 SET (\"tablet_type\" = \"V1\");",
                "Alter tablet type not supported");
    }

    @Test
    public void testRenameMaterializedViewPartition() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().withTable("CREATE TABLE test.table_to_create_mv\n" +
                "(\n" +
                "    k1 date,\n" +
                "    k2 int,\n" +
                "    v1 int sum\n" +
                ")\n" +
                "PARTITION BY RANGE(k1)\n" +
                "(\n" +
                "    PARTITION p1 values less than('2020-02-01'),\n" +
                "    PARTITION p2 values less than('2020-03-01')\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');");
        AnalyzeTestUtil.getStarRocksAssert().withMaterializedView("CREATE MATERIALIZED VIEW mv1_partition_by_column \n" +
                "PARTITION BY k1 \n" +
                "distributed by hash(k2) \n" +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) \n" +
                "PROPERTIES('replication_num' = '1') \n" +
                "as select k1, k2 from table_to_create_mv;");
        String renamePartition = "alter table mv1_partition_by_column rename partition p00000101_20200201 pbase;";
        StatementBase statement = SqlParser.parseSingleStatement(renamePartition,
                connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statement);
        stmtExecutor.execute();
        Assertions.assertEquals(connectContext.getState().getErrType(), QueryState.ErrType.ANALYSIS_ERR);
        connectContext.getState().getErrorMessage()
                .contains("cannot be alter by 'ALTER TABLE', because 'mv1_partition_by_column' is a materialized view");

    }

    @Test
    public void testRollup() {
        analyzeSuccess("alter table t0 drop rollup test1");
        analyzeSuccess("alter table t0 drop rollup test1, test2");
    }

    @Test
    public void testAlterTableComment() {
        analyzeSuccess("alter table t0 comment = \"new comment\"");
    }

    @Test
    public void testAlterWithTimeType() {
        analyzeFail("alter table t0 add column testcol TIME");
        analyzeFail("alter table t0 modify column v0 TIME");
    }

    @Test
    public void testAddBitmapColumnWithoutDefault() throws Exception {
        AnalyzeTestUtil.getStarRocksAssert().withTable("CREATE TABLE test.pk_bitmap_base (\n" +
                "  k1 INT NOT NULL,\n" +
                "  k2 DATETIME NOT NULL,\n" +
                "  v1 BIGINT\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\"\n" +
                ");");

        try {
            AlterTableStmt alter = (AlterTableStmt) AnalyzeTestUtil.analyzeSuccess(
                    "ALTER TABLE test.pk_bitmap_base ADD COLUMN (v_bitmap BITMAP NOT NULL, v_bitmap_nullable BITMAP)");
            Assertions.assertEquals(1, alter.getAlterClauseList().size());
            AlterClause clause = alter.getAlterClauseList().get(0);
            Assertions.assertTrue(clause instanceof AddColumnsClause);
            AddColumnsClause addColumnsClause = (AddColumnsClause) clause;
            Assertions.assertEquals(2, addColumnsClause.getColumns().size());

            Column nonNullColumn = addColumnsClause.getColumns().get(0);
            Assertions.assertFalse(nonNullColumn.isAllowNull());
            Assertions.assertNotNull(nonNullColumn.getDefaultValue());
            Assertions.assertNotNull(addColumnsClause.getColumns().get(1).getDefaultValue());
        } finally {
            AnalyzeTestUtil.getStarRocksAssert().dropTable("test.pk_bitmap_base");
        }
    }

    @Test
    public void testColumnWithRowUpdate() {
        String sql = "alter table tmcwr add column testcol int";
        analyzeSuccess(sql);
        sql = "alter table tmcwr drop column name";
        analyzeSuccess(sql);
    }

}
