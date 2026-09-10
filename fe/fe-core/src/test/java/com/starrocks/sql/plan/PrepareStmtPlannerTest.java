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

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.sql.PrepareStmtPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.common.AIModelConfigs;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class PrepareStmtPlannerTest extends PlanTestBase {
    private static final AtomicInteger NEXT_STMT_ID = new AtomicInteger();
    private static String oldEndpoint;
    private static String oldModel;
    private static String oldProvider;

    @BeforeAll
    public static void setUpAIConfiguration() throws Exception {
        oldEndpoint = Config.ai_default_chat_endpoint;
        oldModel = Config.ai_default_chat_model;
        oldProvider = Config.ai_default_chat_provider;
        Config.ai_default_chat_endpoint = "https://unit.test.example/v1/chat/completions";
        Config.ai_default_chat_model = "unit-test-model";
        Config.ai_default_chat_provider = AIModelConfigs.OPENAI_COMPATIBLE_PROVIDER;

        starRocksAssert.withTable("CREATE TABLE `pq_dup3` (\n" +
                "  `k1` int NOT NULL,\n" +
                "  `k2` int NOT NULL,\n" +
                "  `k3` int NOT NULL,\n" +
                "  `v` string NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `pq_pk3` (\n" +
                "  `k1` int NOT NULL,\n" +
                "  `k2` int NOT NULL,\n" +
                "  `k3` int NOT NULL,\n" +
                "  `v` string NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`k1`, `k2`, `k3`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `pq_dup2` (\n" +
                "  `k1` int NOT NULL,\n" +
                "  `k2` int NOT NULL,\n" +
                "  `v` string NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `pq_dup4` (\n" +
                "  `k1` int NOT NULL,\n" +
                "  `k2` int NOT NULL,\n" +
                "  `k3` int NOT NULL,\n" +
                "  `k4` int NOT NULL,\n" +
                "  `v` string NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `pq_varchar` (\n" +
                "  `k1` varchar(32) NOT NULL,\n" +
                "  `v` string NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `pq_str` (\n" +
                "  `k` varchar(10) NOT NULL,\n" +
                "  `v` varchar(20) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`k`)\n" +
                "DISTRIBUTED BY HASH(`k`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `pq_strdt` (\n" +
                "  `k` varchar(20) NOT NULL,\n" +
                "  `v` varchar(20) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`k`)\n" +
                "DISTRIBUTED BY HASH(`k`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `pq_dec` (\n" +
                "  `k` decimal(10,2) NOT NULL,\n" +
                "  `v` varchar(20) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k`)\n" +
                "DISTRIBUTED BY HASH(`k`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `pq_date` (\n" +
                "  `k` date NOT NULL,\n" +
                "  `v` varchar(20) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k`)\n" +
                "DISTRIBUTED BY HASH(`k`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `pq_datetime` (\n" +
                "  `k` datetime NOT NULL,\n" +
                "  `v` varchar(20) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k`)\n" +
                "DISTRIBUTED BY HASH(`k`) BUCKETS 1\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `pq_stale` (\n" +
                "  `k1` int NOT NULL,\n" +
                "  `k2` int NOT NULL,\n" +
                "  `k3` int NOT NULL,\n" +
                "  `v` string NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `pq_part` (\n" +
                "  `k1` int NOT NULL,\n" +
                "  `k2` int NOT NULL,\n" +
                "  `v` string NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "PARTITION BY RANGE(`k1`) (\n" +
                "  PARTITION p1 VALUES [(\"1\"), (\"10\")),\n" +
                "  PARTITION p2 VALUES [(\"10\"), (\"20\"))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 3\n" +
                "PROPERTIES (\"replication_num\" = \"1\");");
    }

    @AfterAll
    public static void restoreAIConfiguration() {
        Config.ai_default_chat_endpoint = oldEndpoint;
        Config.ai_default_chat_model = oldModel;
        Config.ai_default_chat_provider = oldProvider;
    }

    @Test
    public void testOrdinaryPointQueryStillUsesPreparedPlanCache() throws Exception {
        PreparedQuery prepared = prepare("select v1 from tprimary where pk = ?");

        ExecPlan first = execute(prepared, bigint(1));
        ExecPlan second = execute(prepared, bigint(2));

        Assertions.assertAll(
                () -> Assertions.assertTrue(prepared.context().isCached()),
                () -> Assertions.assertSame(first.getPhysicalPlan(), second.getPhysicalPlan()),
                () -> Assertions.assertEquals(2, scanPredicateValue(second)));
    }

    @Test
    public void testAIPromptAndPointPredicateUseFreshPlans() throws Exception {
        assertAIPreparedStatementUsesFreshPlans("select ai_complete(?) from tprimary where pk = ?");
    }

    @Test
    public void testNestedAIPromptAndPointPredicateUseFreshPlans() throws Exception {
        assertAIPreparedStatementUsesFreshPlans(
                "select concat(ai_complete(?), '!') from tprimary where pk = ?");
    }

    private static void assertAIPreparedStatementUsesFreshPlans(String sql) throws Exception {
        PreparedQuery prepared = prepare(sql);

        ExecPlan first = execute(prepared, new StringLiteral("first prompt"), bigint(1));
        ExecPlan second = execute(prepared, new StringLiteral("second prompt"), bigint(2));

        Assertions.assertAll(
                () -> Assertions.assertFalse(prepared.context().isCached()),
                () -> Assertions.assertNotSame(first.getPhysicalPlan(), second.getPhysicalPlan()),
                () -> Assertions.assertEquals("first prompt", aiPrompt(first)),
                () -> Assertions.assertEquals(1, scanPredicateValue(first)),
                () -> Assertions.assertEquals("second prompt", aiPrompt(second)),
                () -> Assertions.assertEquals(2, scanPredicateValue(second)));
    }

    @Test
    public void testComparisonSemanticsHoldAcrossKeyAndParameterTypes() throws Exception {
        assertCachedPlanMatchesFreshPlan("select v from pq_dec where k = ?",
                () -> List.of(new DecimalLiteral("1.00")),
                () -> List.of(new DecimalLiteral("1.005")),
                () -> List.of(intLit(1)),
                () -> List.of(new StringLiteral("1.00")),
                () -> List.of(doubleLit(1.0)));
        assertCachedPlanMatchesFreshPlan("select v from pq_date where k = ?",
                () -> List.of(new DateLiteral(2020, 1, 1)),
                () -> List.of(new DateLiteral(2020, 1, 2, 3, 4, 5, 0)),
                () -> List.of(new StringLiteral("2020-01-03")),
                () -> List.of(intLit(20200104)));
        assertCachedPlanMatchesFreshPlan("select v from pq_datetime where k = ?",
                () -> List.of(new DateLiteral(2020, 1, 1, 2, 3, 4, 0)),
                () -> List.of(new DateLiteral(2020, 1, 2)),
                () -> List.of(new StringLiteral("2020-01-03 01:02:03")));
    }

    @Test
    public void testDateParameterOnVarcharKeyDoesNotKeepStringComparison() throws Exception {
        assertCachedPlanMatchesFreshPlan("select v from pq_strdt where k = ?",
                () -> List.of(new StringLiteral("2020-01-02")),
                () -> List.of(new DateLiteral(2020, 1, 2)),
                () -> List.of(new StringLiteral("2020-01-02")),
                () -> List.of(new DateLiteral(2020, 1, 2, 3, 4, 5, 0)));
    }

    @Test
    public void testFloatingParameterOnVarcharKeyDoesNotKeepStringComparison() throws Exception {
        assertCachedPlanMatchesFreshPlan("select v from pq_str where k = ?",
                () -> List.of(new StringLiteral("1")),
                () -> List.of(doubleLit(1.0)),
                () -> List.of(new StringLiteral("1")));
    }

    @Test
    public void testExactNumericParameterOnVarcharKeyStillRebinds() throws Exception {
        assertCachedPlanMatchesFreshPlan("select v from pq_str where k = ?",
                () -> List.of(new StringLiteral("1")),
                () -> List.of(intLit(1)),
                () -> List.of(new DecimalLiteral("1")));
    }

    // the common JDBC shape: numeric key columns, every parameter sent as a string. Planning folds the string into
    // the column type and casts nothing, so these must keep hitting the cache
    @Test
    public void testStringParameterOnIntegerKeyStillRebinds() throws Exception {
        String sql = "select v from pq_dup3 where k1 = ? and k2 = ? and k3 = ?";
        PreparedQuery prepared = prepare(sql);
        ExecPlan first = execute(prepared, List.of(intLit(1), intLit(2), intLit(3)));
        List<Expr> strings = List.of(new StringLiteral("4"), new StringLiteral("5"), new StringLiteral("6"));
        ExecPlan second = execute(prepared, strings);
        Map<String, String> fresh = scanBindings(execute(prepare(sql),
                List.of(new StringLiteral("4"), new StringLiteral("5"), new StringLiteral("6"))));

        Assertions.assertAll(
                () -> Assertions.assertTrue(prepared.context().isCached()),
                () -> Assertions.assertSame(first.getPhysicalPlan(), second.getPhysicalPlan()),
                () -> Assertions.assertEquals(Map.of("k1", "4", "k2", "5", "k3", "6"), scanBindings(second)),
                () -> Assertions.assertEquals(fresh, scanBindings(second)));
    }

    @Test
    public void testThreeKeyPointQueryRebindsEveryParameter() throws Exception {
        for (String table : List.of("pq_dup3", "pq_pk3")) {
            assertCachedPlanMatchesFreshPlan("select v from " + table + " where k1 = ? and k2 = ? and k3 = ?",
                    () -> List.of(intLit(1), intLit(2), intLit(3)),
                    () -> List.of(intLit(4), intLit(5), intLit(6)),
                    () -> List.of(intLit(1), intLit(2), intLit(3)),
                    () -> List.of(intLit(7), intLit(8), intLit(9)));
        }
    }

    @Test
    public void testFourKeysBoundWhenPredicateOrderDiffersFromKeyOrder() throws Exception {
        assertCachedPlanMatchesFreshPlan("select v from pq_dup4 where k3 = ? and k1 = ? and k4 = ? and k2 = ?",
                () -> List.of(intLit(3), intLit(1), intLit(4), intLit(2)),
                () -> List.of(intLit(30), intLit(10), intLit(40), intLit(20)),
                () -> List.of(intLit(3), intLit(1), intLit(4), intLit(2)));
    }

    @Test
    public void testParameterOnTheLeftOfEqualityStillBindsByColumn() throws Exception {
        assertCachedPlanMatchesFreshPlan("select v from pq_dup3 where ? = k1 and ? = k2 and ? = k3",
                () -> List.of(intLit(1), intLit(2), intLit(3)),
                () -> List.of(intLit(4), intLit(5), intLit(6)));
    }

    @Test
    public void testFractionalParameterIsNotTruncatedOntoAnIntKey() throws Exception {
        PreparedQuery prepared = prepare("select v from pq_dup3 where k1 = ? and k2 = ? and k3 = ?");
        execute(prepared, List.of(intLit(1), intLit(2), intLit(3)));

        List<Expr> fractional = List.of(doubleLit(1.9), intLit(2), intLit(3));
        ExecPlan second = execute(prepared, fractional);
        ExecPlan fresh = execute(prepare("select v from pq_dup3 where k1 = ? and k2 = ? and k3 = ?"),
                List.of(doubleLit(1.9), intLit(2), intLit(3)));

        Assertions.assertAll(
                () -> Assertions.assertNotEquals("1", scanBindings(second).get("k1")),
                () -> Assertions.assertEquals(scanBindings(fresh), scanBindings(second)));
    }

    @Test
    public void testNullParameterDoesNotMatchTheStringNull() throws Exception {
        PreparedQuery prepared = prepare("select v from pq_varchar where k1 = ?");
        execute(prepared, new StringLiteral("abc"));

        ExecPlan second = execute(prepared, new NullLiteral());
        ExecPlan fresh = execute(prepare("select v from pq_varchar where k1 = ?"), List.of(new NullLiteral()));

        Assertions.assertAll(
                // k1 = NULL matches nothing, so planning folds the query away; the old code bound the string 'NULL'
                () -> Assertions.assertFalse(second.getPhysicalPlan().getOp() instanceof PhysicalOlapScanOperator,
                        "expected the NULL parameter to leave no scan"),
                () -> Assertions.assertEquals(fresh.getExplainString(TExplainLevel.NORMAL),
                        second.getExplainString(TExplainLevel.NORMAL)));
    }

    @Test
    public void testRejectedRebindLeavesTheCachedPredicateUntouched() throws Exception {
        PreparedQuery prepared = prepare("select v from pq_dup3 where k1 = ? and k2 = ? and k3 = ?");
        execute(prepared, List.of(intLit(1), intLit(2), intLit(3)));
        ExecPlan cachedPlan = prepared.context().getExecPlan();

        execute(prepared, List.of(intLit(4), intLit(5), doubleLit(6.5)));

        Assertions.assertEquals(Map.of("k1", "1", "k2", "2", "k3", "3"), logicalFilterBindings(cachedPlan));
    }

    @Test
    public void testSelectLimitSessionVariableKeepsThePlanOutOfTheCache() throws Exception {
        long oldLimit = connectContext.getSessionVariable().getSqlSelectLimit();
        connectContext.getSessionVariable().setSqlSelectLimit(100);
        try {
            PreparedQuery prepared = prepare("select v from pq_dup3 where k1 = ? and k2 = ? and k3 = ?");
            execute(prepared, List.of(intLit(1), intLit(2), intLit(3)));
            ExecPlan second = execute(prepared, List.of(intLit(4), intLit(5), intLit(6)));

            Assertions.assertAll(
                    () -> Assertions.assertFalse(prepared.context().isCached()),
                    () -> Assertions.assertEquals(Map.of("k1", "4", "k2", "5", "k3", "6"), scanBindings(second)));
        } finally {
            connectContext.getSessionVariable().setSqlSelectLimit(oldLimit);
        }
    }

    @Test
    public void testWindowFunctionKeepsThePlanOutOfTheCache() throws Exception {
        PreparedQuery prepared =
                prepare("select v, row_number() over () from pq_dup3 where k1 = ? and k2 = ? and k3 = ?");
        execute(prepared, List.of(intLit(1), intLit(2), intLit(3)));
        ExecPlan second = execute(prepared, List.of(intLit(4), intLit(5), intLit(6)));

        Assertions.assertAll(
                () -> Assertions.assertFalse(prepared.context().isCached()),
                () -> Assertions.assertEquals(Map.of("k1", "4", "k2", "5", "k3", "6"), scanBindings(second)));
    }

    @Test
    public void testAlwaysEmptyResultKeepsThePlanOutOfTheCache() throws Exception {
        PreparedQuery prepared = prepare("select v from pq_varchar where k1 = ?");
        ExecPlan first = execute(prepared, new NullLiteral());

        Assertions.assertAll(
                () -> Assertions.assertFalse(first.getPhysicalPlan().getOp() instanceof PhysicalOlapScanOperator),
                () -> Assertions.assertFalse(prepared.context().isCached()));
    }

    @Test
    public void testRejectedPlanIsReportedInTheDebugLog() throws Exception {
        ReasonAppender appender = new ReasonAppender();
        Logger planner = (Logger) LogManager.getLogger(PrepareStmtPlanner.class);
        Level oldLevel = planner.getLevel();
        appender.start();
        planner.addAppender(appender);
        Configurator.setLevel(PrepareStmtPlanner.class.getName(), Level.DEBUG);
        try {
            PreparedQuery prepared = prepare("select ?, v from pq_dup3 where k1 = ? and k2 = ? and k3 = ?");
            execute(prepared, List.of(intLit(7), intLit(1), intLit(2), intLit(3)));
        } finally {
            Configurator.setLevel(PrepareStmtPlanner.class.getName(), oldLevel);
            planner.removeAppender(appender);
            appender.stop();
        }

        Assertions.assertTrue(appender.messages.stream().anyMatch(m -> m.contains("parameters appear in the predicate")),
                "expected a debug line naming the reason, got " + appender.messages);
    }

    @Test
    public void testParameterInSelectListKeepsThePlanOutOfTheCache() throws Exception {
        PreparedQuery prepared = prepare("select ?, v from pq_dup3 where k1 = ? and k2 = ? and k3 = ?");
        execute(prepared, List.of(intLit(7), intLit(1), intLit(2), intLit(3)));
        ExecPlan second = execute(prepared, List.of(intLit(8), intLit(4), intLit(5), intLit(6)));

        Assertions.assertAll(
                () -> Assertions.assertFalse(prepared.context().isCached()),
                () -> Assertions.assertTrue(planConstants(second).contains("8"),
                        "select list parameter stuck at its first value: " + planConstants(second)),
                () -> Assertions.assertEquals(Map.of("k1", "4", "k2", "5", "k3", "6"), scanBindings(second)));
    }

    @Test
    public void testRepeatedEqualityOnOneColumnIsNotAPointQuery() throws Exception {
        PreparedQuery prepared = prepare("select v from pq_dup3 where k1 = ? and k1 = ? and k2 = ? and k3 = ?");
        execute(prepared, List.of(intLit(1), intLit(1), intLit(2), intLit(3)));
        execute(prepared, List.of(intLit(4), intLit(4), intLit(5), intLit(6)));

        Assertions.assertFalse(prepared.context().isCached());
    }

    @Test
    public void testLimitInAndOrPredicatesAreNotPointQueries() throws Exception {
        List<String> queries = List.of(
                "select v from pq_dup3 where k1 = ? and k2 = ? and k3 = ? limit 1",
                "select v from pq_dup3 where k1 in (?) and k2 = ? and k3 = ?",
                "select v from pq_dup3 where (k1 = ? or k2 = ?) and k3 = ?");
        for (String query : queries) {
            PreparedQuery prepared = prepare(query);
            execute(prepared, List.of(intLit(1), intLit(2), intLit(3)));
            execute(prepared, List.of(intLit(4), intLit(5), intLit(6)));
            Assertions.assertFalse(prepared.context().isCached(), query);
        }
    }

    @Test
    public void testSchemaChangeToAnUncacheablePlanClearsTheStaleCache() throws Exception {
        PreparedQuery prepared = prepare("select v from pq_stale where k1 = ? and k2 = ? and k3 = ?");
        execute(prepared, List.of(intLit(1), intLit(2), intLit(3)));
        Assertions.assertTrue(prepared.context().isCached());

        OlapTable table = getOlapTable("pq_stale");
        table.lastSchemaUpdateTime.set(System.currentTimeMillis() + 3600_000L);
        long oldLimit = connectContext.getSessionVariable().getSqlSelectLimit();
        connectContext.getSessionVariable().setSqlSelectLimit(100);
        try {
            execute(prepared, List.of(intLit(4), intLit(5), intLit(6)));
        } finally {
            connectContext.getSessionVariable().setSqlSelectLimit(oldLimit);
        }

        Assertions.assertAll(
                () -> Assertions.assertFalse(prepared.context().isCached()),
                () -> Assertions.assertNull(prepared.context().getExecPlan()));
    }

    @Test
    public void testOneAndTwoKeyPointQueriesKeepUsingTheCache() throws Exception {
        assertStaysCached("select v1 from tprimary where pk = ?",
                List.of(bigint(1)), List.of(bigint(2)));
        assertStaysCached("select v from pq_dup2 where k1 = ? and k2 = ?",
                List.of(intLit(1), intLit(2)), List.of(intLit(4), intLit(5)));
    }

    @Test
    public void testWiderIntegerParameterStillRebinds() throws Exception {
        PreparedQuery prepared = prepare("select v1 from tprimary where pk = ?");
        ExecPlan first = execute(prepared, bigint(1));
        ExecPlan second = execute(prepared, intLit(2));

        Assertions.assertAll(
                () -> Assertions.assertTrue(prepared.context().isCached()),
                () -> Assertions.assertSame(first.getPhysicalPlan(), second.getPhysicalPlan()),
                () -> Assertions.assertEquals(2, scanPredicateValue(second)));
    }

    @Test
    public void testPartitionedPointQueryRebindsAcrossPartitions() throws Exception {
        PreparedQuery prepared = prepare("select v from pq_part where k1 = ? and k2 = ?");
        ExecPlan first = execute(prepared, List.of(intLit(5), intLit(1)));
        ExecPlan second = execute(prepared, List.of(intLit(15), intLit(1)));
        ExecPlan fresh = execute(prepare("select v from pq_part where k1 = ? and k2 = ?"),
                List.of(intLit(15), intLit(1)));

        Assertions.assertAll(
                () -> Assertions.assertTrue(prepared.context().isCached()),
                () -> Assertions.assertSame(first.getPhysicalPlan(), second.getPhysicalPlan()),
                () -> Assertions.assertEquals(Map.of("k1", "15", "k2", "1"), scanBindings(second)),
                () -> Assertions.assertEquals(fresh.getExplainString(TExplainLevel.NORMAL),
                        second.getExplainString(TExplainLevel.NORMAL)));
    }

    @Test
    public void testFloatingPointParameterFallsBackToFullPlanning() throws Exception {
        PreparedQuery prepared = prepare("select v from pq_dup3 where k1 = ? and k2 = ? and k3 = ?");
        execute(prepared, List.of(intLit(1), intLit(2), intLit(3)));

        ExecPlan withFloat = execute(prepared, List.of(floatLit(2.0), intLit(2), intLit(3)));
        ExecPlan fresh = execute(prepare("select v from pq_dup3 where k1 = ? and k2 = ? and k3 = ?"),
                List.of(floatLit(2.0), intLit(2), intLit(3)));
        ExecPlan backToInts = execute(prepared, List.of(intLit(4), intLit(5), intLit(6)));

        Assertions.assertAll(
                () -> Assertions.assertEquals(scanBindings(fresh), scanBindings(withFloat)),
                () -> Assertions.assertEquals(Map.of("k1", "4", "k2", "5", "k3", "6"), scanBindings(backToInts)),
                () -> Assertions.assertTrue(prepared.context().isCached()));
    }

    private static PreparedQuery prepare(String query) throws Exception {
        String name = "prepared_" + NEXT_STMT_ID.incrementAndGet();
        boolean oldEnablePrepare = connectContext.getSessionVariable().isEnablePrepareStmt();
        connectContext.getSessionVariable().setEnablePrepareStmt(true);
        try {
            PrepareStmt stmt = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(
                    "prepare " + name + " from " + query, connectContext);
            PrepareStmtContext context = new PrepareStmtContext(stmt, connectContext, null);
            connectContext.putPreparedStmt(name, context);
            return new PreparedQuery(name, stmt, context);
        } finally {
            connectContext.getSessionVariable().setEnablePrepareStmt(oldEnablePrepare);
        }
    }

    private static ExecPlan execute(PreparedQuery prepared, Expr... values) {
        return execute(prepared, List.of(values));
    }

    private static ExecPlan execute(PreparedQuery prepared, List<Expr> params) {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setThreadLocalInfo();

        ExecuteStmt executeStmt = new ExecuteStmt(prepared.name(), params);
        Analyzer.analyze(executeStmt, connectContext);
        StatementBase assigned = prepared.statement().assignValues(params);
        return PrepareStmtPlanner.plan(executeStmt, assigned, connectContext);
    }

    private static IntLiteral bigint(long value) {
        return new IntLiteral(value, IntegerType.BIGINT);
    }

    private static IntLiteral intLit(long value) {
        return new IntLiteral(value, IntegerType.INT);
    }

    private static FloatLiteral doubleLit(double value) {
        return new FloatLiteral(value, FloatType.DOUBLE);
    }

    private static FloatLiteral floatLit(double value) {
        return new FloatLiteral(value, FloatType.FLOAT);
    }

    private static long scanPredicateValue(ExecPlan plan) {
        PhysicalOlapScanOperator scan =
                findPhysicalOperator(plan.getPhysicalPlan(), PhysicalOlapScanOperator.class);
        ConstantOperator constant = scan.getPredicate().getChild(1).cast();
        return constant.getBigint();
    }

    private static String aiPrompt(ExecPlan plan) {
        PhysicalAIProjectOperator aiProject =
                findPhysicalOperator(plan.getPhysicalPlan(), PhysicalAIProjectOperator.class);
        CallOperator call = aiProject.getColumnRefMap().values().stream()
                .flatMap(ScalarOperator::asStream)
                .filter(CallOperator.class::isInstance)
                .map(CallOperator.class::cast)
                .filter(candidate -> candidate.getFnName().equalsIgnoreCase("ai_complete"))
                .findFirst()
                .orElseThrow();
        return call.getChild(0).asStream()
                .filter(ConstantOperator.class::isInstance)
                .map(ConstantOperator.class::cast)
                .findFirst()
                .orElseThrow()
                .getVarchar();
    }

    private static <T> T findPhysicalOperator(OptExpression root, Class<T> type) {
        if (type.isInstance(root.getOp())) {
            return type.cast(root.getOp());
        }
        for (OptExpression input : root.getInputs()) {
            T match = findPhysicalOperatorOrNull(input, type);
            if (match != null) {
                return match;
            }
        }
        throw new AssertionError("Missing physical operator " + type.getSimpleName());
    }

    private static <T> T findPhysicalOperatorOrNull(OptExpression root, Class<T> type) {
        if (type.isInstance(root.getOp())) {
            return type.cast(root.getOp());
        }
        for (OptExpression input : root.getInputs()) {
            T match = findPhysicalOperatorOrNull(input, type);
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    private static void assertStaysCached(String sql, List<Expr> firstParams, List<Expr> secondParams)
            throws Exception {
        PreparedQuery prepared = prepare(sql);
        ExecPlan first = execute(prepared, firstParams);
        ExecPlan second = execute(prepared, secondParams);
        ExecPlan freshPlan = execute(prepare(sql), secondParams);
        Map<String, String> fresh = scanBindings(freshPlan);
        Map<String, String> freshTypes = scanBindingTypes(freshPlan);

        Assertions.assertAll(
                () -> Assertions.assertTrue(prepared.context().isCached(), sql),
                () -> Assertions.assertSame(first.getPhysicalPlan(), second.getPhysicalPlan(), sql),
                () -> Assertions.assertEquals(fresh, scanBindings(second), sql),
                () -> Assertions.assertEquals(freshTypes, scanBindingTypes(second), sql));
    }

    private static Map<String, String> logicalFilterBindings(ExecPlan plan) {
        ScalarOperator predicate = plan.getLogicalPlan().getRoot().inputAt(0).getOp().getPredicate();
        Map<String, String> bindings = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            bindings.put(((ColumnRefOperator) conjunct.getChild(0)).getName(), conjunct.getChild(1).toString());
        }
        return bindings;
    }


    // every constant reachable from the physical tree, so a select-list parameter frozen at its first value shows up
    private static Set<String> planConstants(ExecPlan plan) {
        Set<String> constants = new LinkedHashSet<>();
        List<OptExpression> pending = new ArrayList<>(List.of(plan.getPhysicalPlan()));
        while (!pending.isEmpty()) {
            OptExpression node = pending.remove(pending.size() - 1);
            List<ScalarOperator> roots = new ArrayList<>();
            if (node.getOp().getPredicate() != null) {
                roots.add(node.getOp().getPredicate());
            }
            if (node.getOp().getProjection() != null) {
                roots.addAll(node.getOp().getProjection().getColumnRefMap().values());
            }
            roots.stream().flatMap(ScalarOperator::asStream)
                    .filter(ConstantOperator.class::isInstance)
                    .forEach(constant -> constants.add(constant.toString()));
            pending.addAll(node.getInputs());
        }
        return constants;
    }

    @SafeVarargs
    private static void assertCachedPlanMatchesFreshPlan(String sql, Supplier<List<Expr>>... paramSets) throws Exception {
        PreparedQuery cached = prepare(sql);
        for (Supplier<List<Expr>> params : paramSets) {
            ExecPlan fromCache = execute(cached, params.get());
            ExecPlan fromScratch = execute(prepare(sql), params.get());
            Assertions.assertAll(
                    () -> Assertions.assertEquals(scanBindings(fromScratch), scanBindings(fromCache),
                            sql + " bound with " + params.get()),
                    () -> Assertions.assertEquals(scanBindingTypes(fromScratch), scanBindingTypes(fromCache),
                            sql + " bound with " + params.get()));
        }
    }

    // the invariant is that a rebound constant is the one full planning would build, so its type has to match too:
    // 1 reads the same whether it is varchar(10) or varchar(1048576), and 1.00 whether decimal(4,2) or decimal(10,2)
    private static Map<String, String> scanBindingTypes(ExecPlan plan) {
        PhysicalOlapScanOperator scan =
                findPhysicalOperator(plan.getPhysicalPlan(), PhysicalOlapScanOperator.class);
        Map<String, String> types = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ScalarOperator conjunct : Utils.extractConjuncts(scan.getPredicate())) {
            Type type = conjunct.getChild(1).getType();
            types.put(bindingKey(conjunct), type.getPrimitiveType() + "/" + type.toSql());
        }
        return types;
    }

    // the scan predicate as column name -> literal, so cached and freshly planned trees compare across differing ref ids
    private static Map<String, String> scanBindings(ExecPlan plan) {
        PhysicalOlapScanOperator scan =
                findPhysicalOperator(plan.getPhysicalPlan(), PhysicalOlapScanOperator.class);
        Map<String, String> bindings = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ScalarOperator conjunct : Utils.extractConjuncts(scan.getPredicate())) {
            bindings.put(bindingKey(conjunct), conjunct.getChild(1).toString());
        }
        return bindings;
    }

    private static String bindingKey(ScalarOperator conjunct) {
        return conjunct.getChild(0) instanceof ColumnRefOperator
                ? ((ColumnRefOperator) conjunct.getChild(0)).getName() : conjunct.getChild(0).toString();
    }

    private static class ReasonAppender extends AbstractAppender {
        private final List<String> messages = new ArrayList<>();

        ReasonAppender() {
            super("prepare-stmt-planner-reasons", null, null);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }

    private record PreparedQuery(String name, PrepareStmt statement, PrepareStmtContext context) {
    }
}
