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


package com.starrocks.sql.parser;

import com.google.common.collect.Lists;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.qe.VariableMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.starrocks.sql.plan.PlanTestBase.assertContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

class ParserTest {

    @Test
    void tokensExceedLimitTest() {
        String sql = "select 1";
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setParseTokensLimit(1);
        try {
            SqlParser.parse(sql, sessionVariable);
        } catch (Exception e) {
            assertContains(e.getMessage(), "Getting syntax error. Detail message: " +
                    "Statement exceeds maximum length limit");
        }
    }

    @Test
    void sqlParseErrorInfoTest() {
        String sql = "select 1 form tbl";
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable);
            fail("sql should fail to parse.");
        } catch (Exception e) {
            assertContains(e.getMessage(), "Getting syntax error at line 1, column 14. " +
                    "Detail message: Unexpected input 'tbl', the most similar input is {<EOF>, ';'}.");
        }
    }

    /**
     * Test that FE code can parse queries for databases in the MySQL-family that support SQL 2011's system versioning
     * temporal queries. Although MySQL doesn't yet support this syntax directly, multiple MySQL compatible databases do.
     */
    @Test
    void sqlParseTemporalQueriesTest() {
        String[] temporalQueries = new String[] {
                // DoltDB temporal query syntax
                // https://docs.dolthub.com/sql-reference/version-control/querying-history
                "SELECT * FROM t AS OF 'kfvpgcf8pkd6blnkvv8e0kle8j6lug7a';",
                "SELECT * FROM t AS OF 'myBranch';",
                "SELECT * FROM t AS OF 'HEAD^2';",
                "SELECT * FROM t AS OF TIMESTAMP('2020-01-01');",
                "SELECT * from `mydb/ia1ibijq8hq1llr7u85uivsi5lh3310p`.myTable;",

                // MariaDB temporal query syntax
                // https://mariadb.com/kb/en/system-versioned-tables/
                "SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '2016-10-09 08:07:06';",
                "SELECT * FROM t FOR SYSTEM_TIME BETWEEN (NOW() - INTERVAL 1 YEAR) AND NOW();",
                "SELECT * FROM t FOR SYSTEM_TIME FROM '2016-01-01 00:00:00' TO '2017-01-01 00:00:00';",
                "SELECT * FROM t FOR SYSTEM_TIME ALL;",
        };

        for (String query : temporalQueries) {
            try {
                com.starrocks.sql.parser.SqlParser.parse(query, 0).get(0);
            } catch (ParsingException e) {
                fail("Unexpected parsing exception for query: " + query);
            } catch (Exception e) {
                fail("Unexpected exception for query: " + query);
            }
        }
    }

    @Test
    void testInvalidDbName() {
        String sql = "use a.b.c";
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable);
            fail("sql should fail to parse.");
        } catch (Exception e) {
            assertContains(e.getMessage(), "Getting syntax error from line 1, column 4 to line 1, column 8. " +
                    "Detail message: Invalid db name format 'a.b.c'.");
        }
    }

    @Test
    void testInvalidTaskName() {
        String sql = "submit task a.b.c as create table a.b (v1, v2) as select * from t1";
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable);
            fail("sql should fail to parse.");
        } catch (Exception e) {
            assertContains(e.getMessage(), "Getting syntax error from line 1, column 12 to line 1, column 16." +
                    " Detail message: Invalid task name format 'a.b.c'.");
        }
    }

    @Test
    void testNonReservedWords_1() {
        String sql = "select anti, authentication, auto_increment, cancel, distributed, enclose, escape, export," +
                "host, incremental, minus, nodes, optimizer, privileges, qualify, skip_header, semi, trace, trim_space " +
                "from tbl left anti join t1 on ture left semi join t2 on false full join t3 on true minus select * from tbl";
        SessionVariable sessionVariable = new SessionVariable();
        try {
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
        } catch (Exception e) {
            fail("sql should success. errMsg: " +  e.getMessage());
        }
    }

    @Test
    void testNonReservedWords_2() {
        // semi and anti are table names
        String sql = "select * from semi semi join anti anti on anti.col join t1 on true";
        SessionVariable sessionVariable = new SessionVariable();
        try {
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
            JoinRelation topJoinRelation = (JoinRelation) ((SelectRelation) stmt.getQueryRelation()).getRelation();
            Assert.assertEquals(JoinOperator.INNER_JOIN, topJoinRelation.getJoinOp());

            JoinRelation bottomJoinRelation = (JoinRelation) topJoinRelation.getLeft();
            Assert.assertEquals("semi", bottomJoinRelation.getLeft().getResolveTableName().getTbl());
            Assert.assertEquals("anti", bottomJoinRelation.getRight().getResolveTableName().getTbl());
            Assert.assertEquals(JoinOperator.INNER_JOIN, bottomJoinRelation.getJoinOp());
        } catch (Exception e) {
            fail("sql should success. errMsg: " +  e.getMessage());
        }
    }

    @Test
    void testParseLargeDecimal() {
        String sql = "select cast(1 as decimal(65,0))";
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();
        SessionVariable sessionVariable = ctx.getSessionVariable();
        try {
            sessionVariable.setSqlDialect("sr");
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
            Assert.fail();
        } catch (Throwable err) {
            Assert.assertTrue(err.getMessage().contains("DECIMAL's precision should range from 1 to 38"));
        }

        try {
            sessionVariable.setSqlDialect("trino");
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
            Assert.fail();
        } catch (Throwable err) {
            Assert.assertTrue(err.getMessage().contains("DECIMAL's precision should range from 1 to 38"));
        }

        try {
            sessionVariable.setSqlDialect("sr");
            sessionVariable.setLargeDecimalUnderlyingType("double");
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
            Analyzer.analyze(stmt, ctx);
            Type type = stmt.getQueryRelation().getOutputExpression().get(0).getType();
            Assert.assertTrue(type.isDouble());
        } catch (Throwable err) {
            Assert.fail(err.getMessage());
        }

        try {
            sessionVariable.setSqlDialect("trino");
            sessionVariable.setLargeDecimalUnderlyingType("double");
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
            Analyzer.analyze(stmt, ctx);
            Type type = stmt.getQueryRelation().getOutputExpression().get(0).getType();
            Assert.assertTrue(type.isDouble());
        } catch (Throwable err) {
            Assert.fail(err.getMessage());
        }

        try {
            sessionVariable.setSqlDialect("sr");
            sessionVariable.setLargeDecimalUnderlyingType("decimal");
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
            Analyzer.analyze(stmt, ctx);
            Type type = stmt.getQueryRelation().getOutputExpression().get(0).getType();
            Assert.assertEquals(type, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0));
        } catch (Throwable err) {
            Assert.fail(err.getMessage());
        }

        try {
            sessionVariable.setSqlDialect("trino");
            sessionVariable.setLargeDecimalUnderlyingType("decimal");
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
            Analyzer.analyze(stmt, ctx);
            Type type = stmt.getQueryRelation().getOutputExpression().get(0).getType();
            Assert.assertEquals(type, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0));
        } catch (Throwable err) {
            Assert.fail(err.getMessage());
        }
        try {
            sessionVariable.setLargeDecimalUnderlyingType("foobar");
            Assert.fail();
        } catch (Throwable error) {

        }
    }

    @Test
    void testDecimalTypeDeclarationMysqlCompatibility() {
        String sql = "select cast(1 as decimal(65)),cast(1 as decimal)";
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();
        SessionVariable sessionVariable = ctx.getSessionVariable();
        sessionVariable.setSqlDialect("sr");
        sessionVariable.setLargeDecimalUnderlyingType("decimal");
        QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
        Analyzer.analyze(stmt, ctx);
        Type type1 = stmt.getQueryRelation().getOutputExpression().get(0).getType();
        Type type2 = stmt.getQueryRelation().getOutputExpression().get(1).getType();
        Assert.assertEquals(type1, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0));
        Assert.assertEquals(type2, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 0));
    }

    @Test
    void testSettingSqlMode() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        Object lock = new Object();
        String sql = "select 'a' || 'b'";
        final Expr[] exprs = new Expr[2];
        Thread t1 = new Thread(() -> {
            synchronized (lock) {
                StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
                lexer.setSqlMode(SqlModeHelper.MODE_DEFAULT);
                CommonTokenStream tokenStream = new CommonTokenStream(lexer);
                StarRocksParser parser = new StarRocksParser(tokenStream);
                parser.removeErrorListeners();
                parser.addErrorListener(new BaseErrorListener());
                parser.removeParseListeners();
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
                List<StarRocksParser.SingleStatementContext> sqlStatements = parser.sqlStatements().singleStatement();
                QueryStatement statement = (QueryStatement) new AstBuilder(SqlModeHelper.MODE_DEFAULT)
                        .visitSingleStatement(sqlStatements.get(0));
                SelectList item = ((SelectRelation) statement.getQueryRelation()).getSelectList();
                exprs[0] = item.getItems().get(0).getExpr();
                latch.countDown();

            }
        });

        Thread t2 = new Thread(() -> {
            synchronized (lock) {
                StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
                long sqlMode = SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_PIPES_AS_CONCAT;
                lexer.setSqlMode(sqlMode);
                CommonTokenStream tokenStream = new CommonTokenStream(lexer);
                StarRocksParser parser = new StarRocksParser(tokenStream);
                parser.removeErrorListeners();
                parser.addErrorListener(new BaseErrorListener());
                parser.removeParseListeners();
                List<StarRocksParser.SingleStatementContext> sqlStatements = parser.sqlStatements().singleStatement();
                QueryStatement statement = (QueryStatement) new AstBuilder(sqlMode)
                        .visitSingleStatement(sqlStatements.get(0));
                SelectList item = ((SelectRelation) statement.getQueryRelation()).getSelectList();
                exprs[1] = item.getItems().get(0).getExpr();
                lock.notify();
                latch.countDown();
            }
        });

        t1.start();
        Thread.sleep(100);
        t2.start();
        latch.await(10, TimeUnit.SECONDS);
        Assert.assertTrue(exprs[0].toSql() + "should be a compound or predicate",
                exprs[0] instanceof CompoundPredicate);
        Assert.assertTrue(exprs[1].toSql() + "should be a concat function call",
                exprs[1] instanceof FunctionCallExpr);
    }

    @ParameterizedTest
    @MethodSource("keyWordSqls")
    void testNodeReservedWords_3(String sql) {
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable).get(0);
        } catch (Exception e) {
            fail("sql should success. errMsg: " +  e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("reservedWordSqls")
    void testReservedWords(String sql) {
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable).get(0);
            fail("Not quoting reserved words. sql should fail.");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof ParsingException);
        }
    }

    @ParameterizedTest
    @MethodSource("multipleStatements")
    void testMultipleStatements(String sql, boolean isValid) {
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable).get(0);
            if (!isValid) {
                fail("sql should fail.");
            }
        } catch (Exception e) {
            if (isValid) {
                fail("sql should success. errMsg: " +  e.getMessage());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("setQuantifierInAggFunc")
    void testSetQuantifierInAggFunc(String sql, boolean isValid) {
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable).get(0);
            if (!isValid) {
                fail("sql should fail.");
            }
        } catch (Exception e) {
            if (isValid) {
                fail("sql should success. errMsg: " +  e.getMessage());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("unexpectedTokenSqls")
    void testUnexpectedTokenSqls(String sql, String expecting) {
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable).get(0);
            fail("sql should fail.");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            assertContains(e.getMessage(), expecting);
        }
    }

    @Test
    void testWrongVariableName() {
        String res = VariableMgr.findSimilarVarNames("disable_coloce_join");
        assertContains(res, "{'disable_colocate_join', 'disable_join_reorder', 'disable_function_fold_constants'}");

        res = VariableMgr.findSimilarVarNames("SQL_AUTO_NULL");
        assertContains(res, "{'SQL_AUTO_IS_NULL', 'sql_dialect', 'sql_mode_v2'}");

        res = VariableMgr.findSimilarVarNames("pipeline");
        assertContains(res, "{'pipeline_dop', 'pipeline_sink_dop', 'pipeline_profile_level'}");

        res = VariableMgr.findSimilarVarNames("disable_joinreorder");
        assertContains(res, "{'disable_join_reorder', 'disable_colocate_join'");
    }

    @Test
    void testModOperator() {
        String sql = "select 100 MOD 2";
        List<StatementBase> stmts = SqlParser.parse(sql, new SessionVariable());
        String newSql = AstToSQLBuilder.toSQL(stmts.get(0));
        assertEquals("SELECT 100 % 2", newSql);
    }

    private static Stream<Arguments> keyWordSqls() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("select current_role()");
        sqls.add("select current_role");
        sqls.add("SHOW ALL AUTHENTICATION ");
        sqls.add("CANCEL BACKUP from tbl");
        sqls.add("select current_role() from tbl");
        sqls.add("grant all privileges on DATABASE db1 to test");
        sqls.add("revoke export on DATABASE db1 from test");
        sqls.add("ALTER SYSTEM MODIFY BACKEND HOST '1' to '1'");
        sqls.add("SHOW COMPUTE NODES");
        sqls.add("trace times select 1");
        sqls.add("select anti from t1 left anti join t2 on true");
        sqls.add("select anti, semi from t1 left semi join t2 on true");
        sqls.add("select * from tbl1 MINUS select * from tbl2");
        return sqls.stream().map(e -> Arguments.of(e));
    }


    private static Stream<Arguments> reservedWordSqls() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("select * from current_role ");
        sqls.add("select * from full full join anti anti on anti.col join t1 on true");
        return sqls.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> multipleStatements() {
        List<Pair<String, Boolean>> sqls = Lists.newArrayList();
        sqls.add(Pair.create("select 1;;;;;;select 2", true));
        sqls.add(Pair.create("select 1;;;;;select 2 ; ; ;;  select 3;; ;", true));
        sqls.add(Pair.create("select 1, abc from--comments\n tbl;; select 1 -- comments\n from tbl;", true));
        sqls.add(Pair.create("select abc from tbl", true));
        sqls.add(Pair.create("select abc from tbl--comments", true));
        sqls.add(Pair.create(";;;;;;-----;;;;", true));

        sqls.add(Pair.create("select 1 select 2", false));
        sqls.add(Pair.create("select 1 xxx select 2 xxx", false));
        return sqls.stream().map(e -> Arguments.of(e.first, e.second));
    }

    private static Stream<Arguments> setQuantifierInAggFunc() {
        List<Pair<String, Boolean>> sqls = Lists.newArrayList();
        sqls.add(Pair.create("select count(v1) from t1", true));
        sqls.add(Pair.create("select count(all v1) from t1", true));
        sqls.add(Pair.create("select count(distinct v1) from t1", true));
        sqls.add(Pair.create("select sum(abs(v1)) from t1", true));
        sqls.add(Pair.create("select sum(all abs(v1)) from t1", true));
        sqls.add(Pair.create("select sum(distinct abs(v1)) from t1", true));

        sqls.add(Pair.create("select count(all *) from t1", false));
        sqls.add(Pair.create("select count(distinct *) from t1", false));
        sqls.add(Pair.create("select abs(all v1) from t1", false));
        sqls.add(Pair.create("select abs(distinct v1) from t1", false));
        return sqls.stream().map(e -> Arguments.of(e.first, e.second));
    }


    private static Stream<Arguments> unexpectedTokenSqls() {
        List<Arguments> arguments = Lists.newArrayList();

        arguments.add(Arguments.of("selct * from tbl", "SELECT"));
        arguments.add(Arguments.of("select , from tbl", "a legal identifier"));
        arguments.add(Arguments.of("CREATE TABLE IF NOT EXISTS timetest (\n" +
                "  `v1` int(11) NOT NULL,\n" +
                "  `v2` int(11) NOT NULL,\n" +
                "  `v3` int(11) NOT NULL\n" +
                " ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");", ")"));
        arguments.add(Arguments.of("analyze table tt abc", "';'"));
        arguments.add(Arguments.of("select 1,, from tbl", "a legal identifier"));
        arguments.add(Arguments.of("INSTALL PLUGIN FRO xxx", "FROM"));
        arguments.add(Arguments.of("select (1 + 1) + 1) from tbl", "';'"));
        arguments.add(Arguments.of("CREATE TABLE IF NOT EXISTS timetest (\n" +
                "  `v1` int(11) NOT NULL,\n" +
                "  `v2` int(11) NOT NULL,\n" +
                "  `v3` int(11) NOT NULL\n" +
                ")ENGINE=OLAPDUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");", "the most similar input is {<EOF>, ';'}"));
        arguments.add(Arguments.of("create MATERIALIZED VIEW  as select * from (t1 join t2);",
                "the most similar input is {a legal identifier}."));
        return arguments.stream();
    }

}


