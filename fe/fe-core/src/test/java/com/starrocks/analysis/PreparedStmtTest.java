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

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PreparedStmtTest{
    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;
    private static String createTable = "CREATE TABLE `prepare_stmt` (\n" +
            "  `c0` varchar(24) NOT NULL COMMENT \"\",\n" +
            "  `c1` decimal128(24, 5) NOT NULL COMMENT \"\",\n" +
            "  `c2` decimal128(24, 2) NOT NULL COMMENT \"\"\n" +
            ") ENGINE=OLAP \n" +
            "DUPLICATE KEY(`c0`)\n" +
            "COMMENT \"OLAP\"\n" +
            "DISTRIBUTED BY HASH(`c0`) BUCKETS 1 \n" +
            "PROPERTIES (\n" +
            "\"replication_num\" = \"1\",\n" +
            "\"in_memory\" = \"false\",\n" +
            "\"storage_format\" = \"DEFAULT\",\n" +
            "\"enable_persistent_index\" = \"true\",\n" +
            "\"replicated_storage\" = \"true\",\n" +
            "\"compression\" = \"LZ4\"\n" +
            "); ";


    @BeforeAll
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("demo").useDatabase("demo");
        starRocksAssert.withTable(createTable);
    }

    @Test
    public void testParser() throws Exception {
        String sql1 = "PREPARE stmt2 FROM select * from demo.prepare_stmt where c1 = ? and c2 = ?;";
        String sql2 = "PREPARE stmt3 FROM 'select * from demo.prepare_stmt';";
        String sql3 = "execute stmt3;";
        String sql4 = "execute stmt2 using @i;";

        PrepareStmt stmt1 = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
        PrepareStmt stmt2 = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);
        Assertions.assertEquals(2, stmt1.getParameters().size());
        Assertions.assertEquals(0, stmt2.getParameters().size());
        Assertions.assertThrows(StarRocksPlannerException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql3, ctx));

        ctx.putPreparedStmt("stmt2", new PrepareStmtContext(stmt2, ctx, null));
        Assertions.assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql4, ctx));
    }

    @Test
    public void testIsQuery() throws Exception {
        String selectSql = "select * from demo.prepare_stmt";
        QueryStatement queryStatement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(selectSql, ctx);
        Assertions.assertEquals(true, ctx.isQueryStmt(queryStatement));

        String prepareSql = "PREPARE stmt FROM select * from demo.prepare_stmt";
        PrepareStmt prepareStmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(prepareSql, ctx);
        Assertions.assertEquals(false, ctx.isQueryStmt(prepareStmt));

        ctx.putPreparedStmt("stmt", new PrepareStmtContext(prepareStmt, ctx, null));
        Assertions.assertEquals(true, ctx.isQueryStmt(new ExecuteStmt("stmt", null)));
        Assertions.assertEquals(false, ctx.isQueryStmt(new ExecuteStmt("stmt1", null)));
    }

    @Test
    public void testPrepareEnable() {
        ctx.getSessionVariable().setEnablePrepareStmt(false);
        String prepareSql = "PREPARE stmt1 FROM insert into demo.prepare_stmt values (?, ?, ?, ?);";
        String executeSql = "execute stmt1 using @i, @i;";
        Assertions.assertThrows(StarRocksPlannerException.class, () -> starRocksAssert.query(prepareSql).explainQuery());
        Assertions.assertThrows(StarRocksPlannerException.class, () -> starRocksAssert.query(executeSql).explainQuery());
        ctx.getSessionVariable().setEnablePrepareStmt(true);
        assertDoesNotThrow(() -> starRocksAssert.query(prepareSql));

        // TODO support forward leader for fe
        StatementBase statement = SqlParser.parse(prepareSql, ctx.getSessionVariable()).get(0);
        StmtExecutor executor = new StmtExecutor(ctx, statement);
        Assertions.assertFalse(executor.isForwardToLeader());
    }

    @Test
    public void testPrepareWithSelectConst() throws Exception {
        String sql = "PREPARE stmt1 FROM select ?, ?, ?;";
        PrepareStmt stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assertions.assertEquals(3, stmt.getParameters().size());

        HashSet<Integer> idSet = new HashSet<Integer>();
        for (Expr expr : stmt.getParameters()) {
            Assertions.assertEquals(true, idSet.add(expr.hashCode()));
        }

        Assertions.assertEquals(false, stmt.getParameters().get(0).equals(stmt.getParameters().get(1)));
        Assertions.assertEquals(false, stmt.getParameters().get(1).equals(stmt.getParameters().get(2)));
        Assertions.assertEquals(false, stmt.getParameters().get(0).equals(stmt.getParameters().get(2)));
    }

    @Test
    public void testPrepareStatementParser() {
        String sql = "PREPARE stmt1 FROM insert into demo.prepare_stmt values (?, ?, ?, ?);";
        Exception e = assertThrows(AnalysisException.class, () -> UtFrameUtils.parseStmtWithNewParser(sql, ctx));
        assertEquals("Getting analyzing error. Detail message: This command is not supported in the " +
                "prepared statement protocol yet.", e.getMessage());
    }

    @Test
    public void testPrepareStatementParserWithHavingClause() {
        String sql = "PREPARE stmt1 FROM SELECT prepare_stmt.c0 from prepare_stmt GROUP BY prepare_stmt.c0 HAVING COUNT(*) = ?";
        try {
            PrepareStmt stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            Assertions.fail("should not reach here");
        }

        sql = "PREPARE stmt1 FROM SELECT prepare_stmt.c0 from prepare_stmt GROUP BY prepare_stmt.c0 HAVING c0 > ?";
        try {
            PrepareStmt stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        } catch (Exception e) {
            Assertions.fail("should not reach here");
        }
    }

    @Test
    public void testPrepareStmtWithCte() throws Exception {
        String sql = "PREPARE stmt FROM with cte as (select * from prepare_stmt where c0 = ?) select * from cte where c1 = ?";
        PrepareStmt stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        QueryStatement queryStmt = (QueryStatement) stmt.getInnerStmt();
        Assertions.assertTrue(stmt.getParameters().get(1) ==
                ((SelectRelation) queryStmt.getQueryRelation()).getPredicate().getChild(1));

        sql = "PREPARE stmt FROM select *, ? from (with cte as " +
                "(select * from prepare_stmt where c0 = ?) select * from cte where c1 = ?) t where c2 = ?";
        stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        queryStmt = (QueryStatement) stmt.getInnerStmt();
        Assertions.assertTrue(stmt.getParameters().get(0) ==
                ((SelectRelation) queryStmt.getQueryRelation()).getSelectList().getItems().get(1).getExpr());
        Assertions.assertTrue(stmt.getParameters().get(3) ==
                ((SelectRelation) queryStmt.getQueryRelation()).getPredicate().getChild(1));
    }

}
