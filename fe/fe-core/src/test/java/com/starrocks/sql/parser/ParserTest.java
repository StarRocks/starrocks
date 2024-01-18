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
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
<<<<<<< HEAD
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
=======
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectRelation;
>>>>>>> branch-2.5-mrs
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
<<<<<<< HEAD
import static org.junit.Assert.assertEquals;
=======
>>>>>>> branch-2.5-mrs
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

class ParserTest {

    @Test
    void tokensExceedLimitTest() {
        String sql = "select 1";
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setParseTokensLimit(1);
        assertThrows(OperationNotAllowedException.class, () -> SqlParser.parse(sql, sessionVariable));
    }

    @Test
    void sqlParseErrorInfoTest() {
        String sql = "select 1 form tbl";
        SessionVariable sessionVariable = new SessionVariable();
        try {
            SqlParser.parse(sql, sessionVariable);
            fail("sql should fail to parse.");
        } catch (Exception e) {
            assertContains(e.getMessage(), "You have an error in your SQL syntax");
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

<<<<<<< HEAD
    @Test
    void testModOperator() {
        String sql = "select 100 MOD 2";
        List<StatementBase> stmts = SqlParser.parse(sql, new SessionVariable());
        Analyzer.analyze(stmts.get(0), UtFrameUtils.createDefaultCtx());
        String newSql = AstToSQLBuilder.toSQL(stmts.get(0));
        assertEquals("SELECT 100 % 2 AS `100 % 2`", newSql);
    }

=======
>>>>>>> branch-2.5-mrs
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
}


