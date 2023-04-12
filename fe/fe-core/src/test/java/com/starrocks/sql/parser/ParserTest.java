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
import com.starrocks.analysis.SelectList;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
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
import static org.junit.Assert.fail;

class ParserTest {

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
=======
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
        sqls.add("trace optimizer select 1");
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

>>>>>>> 7c1efe58e ([BugFix] support all setQuantifier in special agg functions in new parser (#21413))
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
}


