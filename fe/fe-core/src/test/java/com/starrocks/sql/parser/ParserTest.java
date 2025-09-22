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
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.qe.VariableMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.utframe.UtFrameUtils;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.starrocks.sql.plan.PlanTestBase.assertContains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class ParserTest {

    @Test
    void test() {
        String sql = "alter plan advisor add " +
                "select count(*) from customer join " +
                "(select * from skew_tbl where c_custkey_skew = 100) t on abs(c_custkey) = c_custkey_skew;";
        SqlParser.parse(sql, new SessionVariable());
        System.out.println();
    }

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
     * Test that FE code can parse queries for databases in the MySQL-family that
     * support SQL 2011's system versioning
     * temporal queries. Although MySQL doesn't yet support this syntax directly,
     * multiple MySQL compatible databases do.
     */
    @Test
    void sqlParseTemporalQueriesTest() {
        String[] temporalQueries = new String[] {
                // MariaDB temporal query syntax
                // https://mariadb.com/kb/en/system-versioned-tables/
                "SELECT * FROM t FOR SYSTEM_TIME AS OF '2016-10-09 08:07:06';",
                "SELECT * FROM t FOR SYSTEM_TIME BETWEEN (NOW() - INTERVAL 1 YEAR) AND NOW();",
                "SELECT * FROM t FOR SYSTEM_TIME FROM '2016-01-01 00:00:00' TO '2017-01-01 00:00:00';",
                "SELECT * FROM t FOR SYSTEM_TIME ALL;",
                "SELECT * FROM t FOR VERSION AS OF 123345456321;",
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
                "host, incremental, minus, nodes, optimizer, privileges, qualify, skip_header, semi, trace, trim_space "
                +
                "from tbl left anti join t1 on ture left semi join t2 on false full join t3 on true minus select * from tbl";
        SessionVariable sessionVariable = new SessionVariable();
        try {
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
        } catch (Exception e) {
            fail("sql should success. errMsg: " + e.getMessage());
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
            Assertions.assertEquals(JoinOperator.INNER_JOIN, topJoinRelation.getJoinOp());

            JoinRelation bottomJoinRelation = (JoinRelation) topJoinRelation.getLeft();
            Assertions.assertEquals("semi", bottomJoinRelation.getLeft().getResolveTableName().getTbl());
            Assertions.assertEquals("anti", bottomJoinRelation.getRight().getResolveTableName().getTbl());
            Assertions.assertEquals(JoinOperator.INNER_JOIN, bottomJoinRelation.getJoinOp());
        } catch (Exception e) {
            fail("sql should success. errMsg: " + e.getMessage());
        }
    }

    @Test
    void testParseLargeDecimal() {
        String sql = "select cast(1 as decimal(85,0))";
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();
        SessionVariable sessionVariable = ctx.getSessionVariable();
        try {
            sessionVariable.setSqlDialect("sr");
            SqlParser.parse(sql, sessionVariable);
            Assertions.fail();
        } catch (Throwable err) {
            Assertions.assertTrue(err.getMessage().contains("DECIMAL's precision should range from 1 to 76"));
        }

        try {
            sessionVariable.setSqlDialect("trino");
            SqlParser.parse(sql, sessionVariable);
            Assertions.fail();
        } catch (Throwable err) {
            Assertions.assertTrue(err.getMessage().contains("DECIMAL's precision should range from 1 to 76"));
        }

        try {
            sessionVariable.setSqlDialect("sr");
            sessionVariable.setLargeDecimalUnderlyingType("double");
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
            Analyzer.analyze(stmt, ctx);
            Type type = stmt.getQueryRelation().getOutputExpression().get(0).getType();
            Assertions.assertTrue(type.isDouble());
        } catch (Throwable err) {
            Assertions.fail(err.getMessage());
        }

        try {
            sessionVariable.setSqlDialect("trino");
            sessionVariable.setLargeDecimalUnderlyingType("double");
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
            Analyzer.analyze(stmt, ctx);
            Type type = stmt.getQueryRelation().getOutputExpression().get(0).getType();
            Assertions.assertTrue(type.isDouble());
        } catch (Throwable err) {
            Assertions.fail(err.getMessage());
        }

        try {
            sessionVariable.setSqlDialect("sr");
            sessionVariable.setLargeDecimalUnderlyingType("decimal");
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
            Analyzer.analyze(stmt, ctx);
            Type type = stmt.getQueryRelation().getOutputExpression().get(0).getType();
            Assertions.assertEquals(type, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL256, 76, 0));
        } catch (Throwable err) {
            Assertions.fail(err.getMessage());
        }

        try {
            sessionVariable.setSqlDialect("trino");
            sessionVariable.setLargeDecimalUnderlyingType("decimal");
            QueryStatement stmt = (QueryStatement) SqlParser.parse(sql, sessionVariable).get(0);
            Analyzer.analyze(stmt, ctx);
            Type type = stmt.getQueryRelation().getOutputExpression().get(0).getType();
            Assertions.assertEquals(type, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL256, 76, 0));
        } catch (Throwable err) {
            Assertions.fail(err.getMessage());
        }
        try {
            sessionVariable.setLargeDecimalUnderlyingType("foobar");
            Assertions.fail();
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
        Assertions.assertEquals(type1, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL256, 65, 0));
        Assertions.assertEquals(type2, ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 10, 0));
    }

    @Test
    void testSettingSqlMode() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        Object lock = new Object();
        String sql = "select 'a' || 'b'";
        final Expr[] exprs = new Expr[2];
        Thread t1 = new Thread(() -> {
            synchronized (lock) {
                com.starrocks.sql.parser.StarRocksLexer lexer =
                        new com.starrocks.sql.parser.StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
                lexer.setSqlMode(SqlModeHelper.MODE_DEFAULT);
                CommonTokenStream tokenStream = new CommonTokenStream(lexer);
                com.starrocks.sql.parser.StarRocksParser parser = new com.starrocks.sql.parser.StarRocksParser(tokenStream);
                parser.removeErrorListeners();
                parser.addErrorListener(new BaseErrorListener());
                parser.removeParseListeners();
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
                List<com.starrocks.sql.parser.StarRocksParser.SingleStatementContext> sqlStatements =
                        parser.sqlStatements().singleStatement();
                QueryStatement statement =
                        (QueryStatement) new AstBuilder(SqlModeHelper.MODE_DEFAULT, GlobalVariable.enableTableNameCaseInsensitive,
                                new IdentityHashMap<>())
                                .visitSingleStatement(sqlStatements.get(0));
                SelectList item = ((SelectRelation) statement.getQueryRelation()).getSelectList();
                exprs[0] = item.getItems().get(0).getExpr();
                latch.countDown();

            }
        });

        Thread t2 = new Thread(() -> {
            synchronized (lock) {
                com.starrocks.sql.parser.StarRocksLexer lexer =
                        new com.starrocks.sql.parser.StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
                long sqlMode = SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_PIPES_AS_CONCAT;
                lexer.setSqlMode(sqlMode);
                CommonTokenStream tokenStream = new CommonTokenStream(lexer);
                com.starrocks.sql.parser.StarRocksParser parser = new com.starrocks.sql.parser.StarRocksParser(tokenStream);
                parser.removeErrorListeners();
                parser.addErrorListener(new BaseErrorListener());
                parser.removeParseListeners();
                List<com.starrocks.sql.parser.StarRocksParser.SingleStatementContext> sqlStatements =
                        parser.sqlStatements().singleStatement();
                QueryStatement statement = (QueryStatement) new AstBuilder(sqlMode, GlobalVariable.enableTableNameCaseInsensitive,
                        new IdentityHashMap<>())
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
        Assertions.assertTrue(exprs[0] instanceof CompoundPredicate,
                exprs[0].toSql() + "should be a compound or predicate");
        Assertions.assertTrue(exprs[1] instanceof FunctionCallExpr,
                exprs[1].toSql() + "should be a concat function call");
    }

    @ParameterizedTest
    @MethodSource("keyWordSqls")
    void testNodeReservedWords_3(String sql) {
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable).get(0);
        } catch (Exception e) {
            fail("sql should success. errMsg: " + e.getMessage());
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
            Assertions.assertTrue(e instanceof ParsingException);
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
                fail("sql should success. errMsg: " + e.getMessage());
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
                fail("sql should success. errMsg: " + e.getMessage());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("unexpectedTokenSqls")
    void testUnexpectedTokenSqls(String sql, String expecting) {
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable).get(0);
            fail("sql should fail: " + sql);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            assertContains(e.getMessage(), expecting);
        }
    }

    @Test
    void testWrongVariableName() {
        VariableMgr variableMgr = new VariableMgr();
        String res = variableMgr.findSimilarVarNames("disable_coloce_join");
        assertContains(res, "{'disable_colocate_join', 'disable_colocate_set', 'disable_join_reorder'");

        res = variableMgr.findSimilarVarNames("SQL_AUTO_NULL");
        assertContains(res, "{'SQL_AUTO_IS_NULL', 'sql_dialect', 'spill_storage_volume'}");

        res = variableMgr.findSimilarVarNames("pipeline");
        assertContains(res, "{'pipeline_dop', 'pipeline_sink_dop', 'pipeline_profile_level'}");

        res = variableMgr.findSimilarVarNames("disable_joinreorder");
        assertContains(res, "{'disable_join_reorder', 'disable_colocate_join'");
    }

    @Test
    void testModOperator() {
        String sql = "select 100 MOD 2";
        List<StatementBase> stmts = SqlParser.parse(sql, new SessionVariable());
        String newSql = AstToSQLBuilder.toSQL(stmts.get(0));
        assertEquals("SELECT 100 % 2", newSql);
    }

    @Test
    void testComplexExpr() {
        String exprString = " not X1 + 1  >  X2 and not X3 + 2 > X4 and not X5 + 3 > X6  and not X7 + 1 = X8 " +
                "and not X9 + X10 < X11 + X12 ";
        StringBuilder builder = new StringBuilder();
        builder.append(exprString);
        for (int i = 0; i < 500; i++) {
            builder.append("or");
            builder.append(exprString);
        }

        AstBuilder astBuilder = new AstBuilder(SqlModeHelper.MODE_DEFAULT, GlobalVariable.enableTableNameCaseInsensitive,
                new IdentityHashMap<>());
        com.starrocks.sql.parser.StarRocksLexer lexer = new com.starrocks.sql.parser.StarRocksLexer(
                new CaseInsensitiveStream(CharStreams.fromString(builder.toString())));
        lexer.setSqlMode(SqlModeHelper.MODE_DEFAULT);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        com.starrocks.sql.parser.StarRocksParser parser = new com.starrocks.sql.parser.StarRocksParser(tokenStream);
        parser.getInterpreter().setPredictionMode(PredictionMode.LL);
        long start = System.currentTimeMillis();
        com.starrocks.sql.parser.StarRocksParser.ExpressionContext context1 = parser.expression();
        Expr expr1 = (Expr) astBuilder.visit(context1);
        long end = System.currentTimeMillis();
        long timeOfLL = end - start;

        parser.getTokenStream().seek(0);
        parser.reset();
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        start = System.currentTimeMillis();
        com.starrocks.sql.parser.StarRocksParser.ExpressionContext context2 = parser.expression();
        Expr expr2 = (Expr) astBuilder.visit(context2);
        end = System.currentTimeMillis();
        long timeOfSLL = end - start;

        Assertions.assertEquals(expr1, expr2);
        Assertions.assertTrue(timeOfLL > timeOfSLL);
    }

    @Test
    void testPivot() {
        List<String> sqls = Lists.newArrayList();
        sqls.add("select * from t pivot (sum(v1) for v2 in (1, 2, 3))");
        sqls.add("select * from t pivot (sum(v1) as s1 for (v2, v3) in ((1, 2) as 'a', (3,4) as b, (5,6) as 'c'))");
        sqls.add("select * from t " +
                "pivot (sum(v1) as s1, count(v2) as c1, avg(v3) as c3 " +
                "for (v2, v3) in ((1, 2) as 'a', (3,4) as b, (5,6) as 'c'))");

        List<String> expects = Lists.newArrayList();
        expects.add("SELECT *\n" +
                "FROM `t` PIVOT (sum(v1) " +
                "FOR v2 IN (1, 2, 3)" +
                ")");
        expects.add("SELECT *\n" +
                "FROM `t` PIVOT (sum(v1) AS s1 " +
                "FOR (v2, v3) IN ((1, 2) AS a, (3, 4) AS b, (5, 6) AS c)" +
                ")");
        expects.add("SELECT *\n" +
                "FROM `t` PIVOT (sum(v1) AS s1, count(v2) AS c1, avg(v3) AS c3 " +
                "FOR (v2, v3) IN ((1, 2) AS a, (3, 4) AS b, (5, 6) AS c)" +
                ")");
        for (String sql : sqls) {
            try {
                StatementBase stmt = SqlParser.parse(sql, new SessionVariable()).get(0);
                String newSql = AstToSQLBuilder.toSQL(stmt);
                assertEquals(expects.get(sqls.indexOf(sql)), newSql);
            } catch (Exception e) {
                e.printStackTrace();
                fail("sql should success. errMsg: " + e.getMessage());
            }
        }
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

    @Test
    public void testTranslateFunction() {
        String sql = "select translate('abcabc', 'ab', '12') as test;";
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable);
        } catch (Exception e) {
            Assertions.fail("sql should success. errMsg: " + e.getMessage());
        }
    }

    @Test
    public void testSplitTabletClause() {
        {
            String sql = "ALTER TABLE test_db.test_table\n" + //
                    "SPLIT TABLET\n" + //
                    "PROPERTIES (\n" + //
                    "    \"dynamic_tablet_split_size\"=\"1024\")";

            SessionVariable sessionVariable = new SessionVariable();
            try {
                List<StatementBase> stmts = SqlParser.parse(sql, sessionVariable);
                Assertions.assertEquals(1, stmts.size());

                AlterTableStmt alterTableStmt = (AlterTableStmt) stmts.get(0);
                Assertions.assertEquals("test_db", alterTableStmt.getDbName());
                Assertions.assertEquals("test_table", alterTableStmt.getTableName());

                List<AlterClause> alterClauses = alterTableStmt.getAlterClauseList();
                Assertions.assertEquals(1, alterClauses.size());

                SplitTabletClause splitTabletClause = (SplitTabletClause) alterClauses.get(0);
                Assertions.assertEquals(null, splitTabletClause.getPartitionNames());
                Assertions.assertEquals(null, splitTabletClause.getTabletList());
                Assertions.assertEquals(Map.of("dynamic_tablet_split_size", "1024"), splitTabletClause.getProperties());
                Assertions.assertNotNull(splitTabletClause.toString());
            } catch (Exception e) {
                Assertions.fail("sql should success. errMsg: " + e.getMessage());
            }
        }

        {
            String sql = "ALTER TABLE test_db.test_table\n" + //
                    "SPLIT TABLET\n" + //
                    "    PARTITION (partiton_name1, partition_name2)\n" + //
                    "PROPERTIES (\n" + //
                    "    \"dynamic_tablet_split_size\"=\"1024\")";

            SessionVariable sessionVariable = new SessionVariable();
            try {
                List<StatementBase> stmts = SqlParser.parse(sql, sessionVariable);
                Assertions.assertEquals(1, stmts.size());

                AlterTableStmt alterTableStmt = (AlterTableStmt) stmts.get(0);
                Assertions.assertEquals("test_db", alterTableStmt.getDbName());
                Assertions.assertEquals("test_table", alterTableStmt.getTableName());

                List<AlterClause> alterClauses = alterTableStmt.getAlterClauseList();
                Assertions.assertEquals(1, alterClauses.size());

                SplitTabletClause splitTabletClause = (SplitTabletClause) alterClauses.get(0);
                Assertions.assertEquals(Lists.newArrayList("partiton_name1", "partition_name2"),
                        splitTabletClause.getPartitionNames().getPartitionNames());
                Assertions.assertEquals(null, splitTabletClause.getTabletList());
                Assertions.assertEquals(Map.of("dynamic_tablet_split_size", "1024"), splitTabletClause.getProperties());
                Assertions.assertNotNull(splitTabletClause.toString());
            } catch (Exception e) {
                Assertions.fail("sql should success. errMsg: " + e.getMessage());
            }
        }

        {
            String sql = "ALTER TABLE test_db.test_table\n" + //
                    "SPLIT TABLET (1, 2, 3)\n" + //
                    "PROPERTIES (\n" + //
                    "    \"dynamic_tablet_split_size\"=\"1024\")";

            SessionVariable sessionVariable = new SessionVariable();
            try {
                List<StatementBase> stmts = SqlParser.parse(sql, sessionVariable);
                Assertions.assertEquals(1, stmts.size());

                AlterTableStmt alterTableStmt = (AlterTableStmt) stmts.get(0);
                Assertions.assertEquals("test_db", alterTableStmt.getDbName());
                Assertions.assertEquals("test_table", alterTableStmt.getTableName());

                List<AlterClause> alterClauses = alterTableStmt.getAlterClauseList();
                Assertions.assertEquals(1, alterClauses.size());

                SplitTabletClause splitTabletClause = (SplitTabletClause) alterClauses.get(0);
                Assertions.assertEquals(null, splitTabletClause.getPartitionNames());
                Assertions.assertEquals(Lists.newArrayList(1L, 2L, 3L),
                        splitTabletClause.getTabletList().getTabletIds());
                Assertions.assertEquals(Map.of("dynamic_tablet_split_size", "1024"), splitTabletClause.getProperties());
                Assertions.assertNotNull(splitTabletClause.toString());
            } catch (Exception e) {
                Assertions.fail("sql should success. errMsg: " + e.getMessage());
            }
        }

        SplitTabletClause splitTabletClause = new SplitTabletClause(null, null, null);
        Assertions.assertEquals(null, splitTabletClause.getPartitionNames());
    }
}
