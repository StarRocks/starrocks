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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JDBCTopNPushDownTest extends ConnectorPlanTestBase {
    private static final String TABLE = "jdbc_postgres.partitioned_db0.tbl0";
    private boolean previousTopNPushDown;

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
        boolean runningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        try {
            starRocksAssert.withResource("CREATE EXTERNAL RESOURCE jdbc_topn_pg_resource PROPERTIES ("
                    + "\"type\"=\"jdbc\", \"user\"=\"test\", \"password\"=\"test\", "
                    + "\"driver_url\"=\"test_driver_url\", \"driver_class\"=\"org.postgresql.Driver\", "
                    + "\"jdbc_uri\"=\"jdbc:postgresql://127.0.0.1:5432/testdb\")")
                    .withTable("CREATE EXTERNAL TABLE test.jdbc_topn_pg_resource_table "
                            + "(id BIGINT, d DATE, ts DATETIME) ENGINE=jdbc PROPERTIES ("
                            + "\"resource\"=\"jdbc_topn_pg_resource\", \"table\"=\"public.topn_probe\")");
        } finally {
            FeConstants.runningUnitTest = runningUnitTest;
        }
    }

    @BeforeEach
    public void setUp() {
        previousTopNPushDown = connectContext.getSessionVariable().isEnableJdbcTopNPushDown();
        connectContext.getSessionVariable().setEnableJdbcTopNPushDown(new SessionVariable().isEnableJdbcTopNPushDown());
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(true);
    }

    @AfterEach
    public void tearDown() {
        connectContext.getSessionVariable().setEnableJdbcTopNPushDown(previousTopNPushDown);
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(true);
    }

    @Test
    public void testDefaultEnabledAndExplicitlyDisabled() throws Exception {
        Assertions.assertTrue(connectContext.getSessionVariable().isEnableJdbcTopNPushDown());
        String sql = "select a from " + TABLE + " where c > 1 order by c desc nulls last, d asc nulls first limit 10";
        String plan = getFragmentPlan(sql);
        // The remote ORDER BY replaces the local TopN rather than feeding it.
        assertContains(plan, "ORDER BY \"c\" DESC NULLS LAST, \"d\" ASC NULLS FIRST LIMIT 10");
        Assertions.assertFalse(plan.contains("TOP-N"), plan);
        connectContext.getSessionVariable().setEnableJdbcTopNPushDown(false);
        String localPlan = getFragmentPlan(sql);
        Assertions.assertFalse(localPlan.contains("ORDER BY"), localPlan);
        assertContains(localPlan, "TOP-N");
    }

    @Test
    public void testGroupBySumAndHaving() throws Exception {
        String plan = getFragmentPlan("select a, sum(c) total from " + TABLE
                + " group by a having sum(c) > 2 order by total desc nulls last limit 10");
        assertContains(plan, "GROUP BY \"a\" HAVING (sum(\"c\") > 2)",
                "ORDER BY \"jdbc_agg_", "DESC NULLS LAST LIMIT 10");
        Assertions.assertFalse(plan.contains("AGGREGATE ("), plan);
        Assertions.assertFalse(plan.contains("TOP-N"), plan);
    }

    @Test
    public void testCountAndUnprojectedAggregate() throws Exception {
        String plan = getFragmentPlan("select a from " + TABLE
                + " group by a having count(*) > 1 order by count(*) desc nulls first limit 10");
        assertContains(plan, "GROUP BY \"a\" HAVING (count(*) > 1)",
                "ORDER BY \"jdbc_agg_", "DESC NULLS FIRST LIMIT 10");
        Assertions.assertFalse(plan.contains("TOP-N"), plan);
    }

    @Test
    public void testTemporalMetadataAndDerivedTimeFallback() throws Exception {
        JDBCTable table = (JDBCTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(
                connectContext, "jdbc_postgres", "partitioned_db0", "tbl0");
        List<Column> originalSchema = table.getFullSchema();
        Map<String, String> originalTypeNames = table.getOriginalJdbcColumnTypeNames();
        List<Column> schema = new ArrayList<>(originalSchema);
        schema.add(new Column("created_at", DateType.DATETIME, true));
        schema.add(new Column("created_date", DateType.DATE, true));
        schema.add(new Column("created_z", DateType.DATETIME, true));
        try {
            table.setNewFullSchema(schema);
            table.setOriginalJdbcColumnTypeNames(Map.of(
                    "created_at", "timestamp", "created_date", "date", "created_z", "timestamptz"));
            for (String column : List.of("created_at", "created_date")) {
                assertContains(getFragmentPlan("select a from " + TABLE + " order by " + column
                                + " desc nulls last, c asc nulls first limit 10"),
                        "ORDER BY \"" + column + "\" DESC NULLS LAST, \"c\" ASC NULLS FIRST LIMIT 10");
            }
            Assertions.assertFalse(getFragmentPlan("select a from " + TABLE
                    + " order by created_z limit 10").contains("ORDER BY"));
            Assertions.assertFalse(getFragmentPlan("select a, max(created_at) m from " + TABLE
                    + " group by a order by m limit 10").contains("ORDER BY"));
            Assertions.assertFalse(table.isInlineTable());
            Assertions.assertEquals("timestamp", table.getOriginalJdbcColumnTypeNames().get("created_at"));
        } finally {
            table.setNewFullSchema(originalSchema);
            table.setOriginalJdbcColumnTypeNames(originalTypeNames);
        }
    }

    @Test
    public void testStringBooleanAndDecimalSortKeys() throws Exception {
        JDBCTable table = (JDBCTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(
                connectContext, "jdbc_postgres", "partitioned_db0", "tbl0");
        List<Column> originalSchema = table.getFullSchema();
        Map<String, String> originalTypeNames = table.getOriginalJdbcColumnTypeNames();
        List<Column> schema = new ArrayList<>(originalSchema);
        schema.add(new Column("name", TypeFactory.createVarcharType(100), true));
        schema.add(new Column("label", TypeFactory.createVarcharType(65533), true));
        schema.add(new Column("active", BooleanType.BOOLEAN, true));
        schema.add(new Column("amount", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 12, 2), true));
        schema.add(new Column("uid", TypeFactory.createVarcharType(36), true));
        try {
            table.setNewFullSchema(schema);
            table.setOriginalJdbcColumnTypeNames(Map.of(
                    "name", "varchar", "label", "text", "active", "bool", "amount", "numeric", "uid", "uuid"));
            // PostgreSQL compares text under the column's collation, so the remote sort has to ask
            // for COLLATE "C" to match the byte order the local TopN would apply.
            String namePlan = getFragmentPlan("select a from " + TABLE + " order by name desc nulls last limit 10");
            assertContains(namePlan, "ORDER BY \"name\" COLLATE \"C\" DESC NULLS LAST LIMIT 10");
            Assertions.assertFalse(namePlan.contains("TOP-N"), namePlan);
            assertContains(getFragmentPlan("select a from " + TABLE + " order by label asc nulls first limit 10"),
                    "ORDER BY \"label\" COLLATE \"C\" ASC NULLS FIRST LIMIT 10");
            assertContains(getFragmentPlan("select a from " + TABLE + " order by active desc nulls last limit 10"),
                    "ORDER BY \"active\" DESC NULLS LAST LIMIT 10");
            assertContains(getFragmentPlan("select a from " + TABLE + " order by amount asc nulls last limit 10"),
                    "ORDER BY \"amount\" ASC NULLS LAST LIMIT 10");
            // A mixed key list collates only the string column.
            assertContains(getFragmentPlan("select a from " + TABLE
                            + " order by name asc nulls last, amount desc nulls first limit 10"),
                    "ORDER BY \"name\" COLLATE \"C\" ASC NULLS LAST, \"amount\" DESC NULLS FIRST LIMIT 10");
            // A uuid key is pushed like any other sort key, and bare: it maps to VARCHAR too, but
            // PostgreSQL rejects a collation on the type outright, and its values already sort the
            // way their canonical text does byte by byte.
            String uuidPlan = getFragmentPlan("select a from " + TABLE + " order by uid asc nulls last limit 10");
            assertContains(uuidPlan, "ORDER BY \"uid\" ASC NULLS LAST LIMIT 10");
            Assertions.assertFalse(uuidPlan.contains("COLLATE"), uuidPlan);
            Assertions.assertFalse(uuidPlan.contains("TOP-N"), uuidPlan);
            // Mixed with a collatable key, only the collatable one carries the collation.
            assertContains(getFragmentPlan("select a from " + TABLE
                            + " order by uid asc nulls last, name desc nulls first limit 10"),
                    "ORDER BY \"uid\" ASC NULLS LAST, \"name\" COLLATE \"C\" DESC NULLS FIRST LIMIT 10");
        } finally {
            table.setNewFullSchema(originalSchema);
            table.setOriginalJdbcColumnTypeNames(originalTypeNames);
        }
    }

    @Test
    public void testLossySourceTypesStayLocal() throws Exception {
        JDBCTable table = (JDBCTable) GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(
                connectContext, "jdbc_postgres", "partitioned_db0", "tbl0");
        List<Column> originalSchema = table.getFullSchema();
        Map<String, String> originalTypeNames = table.getOriginalJdbcColumnTypeNames();
        List<Column> schema = new ArrayList<>(originalSchema);
        // An unconstrained numeric maps to VARCHAR until it is read as DECIMAL(38,18). Ordering it
        // as a string would push down -1, 1, 10, 2 while StarRocks keeps -1, 1, 2, 10.
        schema.add(new Column("unbounded_num", TypeFactory.createVarcharType(65533), true));
        schema.add(new Column("padded", TypeFactory.createCharType(10), true));
        schema.add(new Column("wide_amount", TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL256, 50, 2), true));
        schema.add(new Column("ratio", TypeFactory.createVarcharType(100), true));
        try {
            table.setNewFullSchema(schema);
            table.setOriginalJdbcColumnTypeNames(Map.of(
                    "unbounded_num", "numeric", "padded", "bpchar", "wide_amount", "numeric"));
            for (String column : List.of("unbounded_num", "padded", "wide_amount", "ratio")) {
                String plan = getFragmentPlan("select a from " + TABLE + " order by " + column + " limit 10");
                Assertions.assertFalse(plan.contains("ORDER BY"), column + ": " + plan);
            }
        } finally {
            table.setNewFullSchema(originalSchema);
            table.setOriginalJdbcColumnTypeNames(originalTypeNames);
        }
    }

    @Test
    public void testLocalAggregateAndHavingBlockTopN() throws Exception {
        String sql = "select a, sum(c) total from " + TABLE + " group by a order by total desc limit 10";
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(false);
        String plan = getFragmentPlan(sql);
        assertContains(plan, "AGGREGATE (");
        Assertions.assertFalse(plan.contains("ORDER BY"), plan);
        connectContext.getSessionVariable().setEnableJdbcAggPushDown(true);
        plan = getFragmentPlan("select a, sum(c) total from " + TABLE
                + " group by a having abs(sum(c)) > 2 order by total desc limit 10");
        Assertions.assertFalse(plan.contains("ORDER BY"), plan);
    }

    @Test
    public void testNestedTopNComposesWithProjectPushDown() throws Exception {
        // The rule consumes the TopN it rewrites, so nothing marks the scan as already pushed. A
        // second TopN can still reach it, because PushDownProjectToJDBCScanRule rebuilds the scan
        // with DEFAULT_LIMIT once it folds an expression into the remote SQL. That second push is
        // correct: it wraps the first one, which keeps its own ORDER BY/LIMIT.
        String plan = getFragmentPlan("select c+1 from (select c from " + TABLE
                + " order by c limit 10) x order by c+1 limit 5");
        assertContains(plan, "SELECT (\"c\" + 1) AS \"jdbc_proj_",
                "ORDER BY \"c\" ASC NULLS FIRST LIMIT 10",
                "ORDER BY \"jdbc_proj_", "ASC NULLS FIRST LIMIT 5");
        Assertions.assertFalse(plan.contains("TOP-N"), plan);
        // Without an expression to fold, the inner push leaves its LIMIT on the scan operator and
        // the outer TopN stays local rather than appending a second ORDER BY to that LIMIT.
        String limitedPlan = getFragmentPlan("select c from (select c from " + TABLE
                + " order by c desc limit 10) x order by c asc limit 5");
        assertContains(limitedPlan, "TOP-N", "ORDER BY \"c\" DESC NULLS LAST LIMIT 10");
        // An aggregate over an already-pushed TopN folds around it instead of losing the bound.
        assertContains(getFragmentPlan("select count(*) from (select c from " + TABLE
                        + " order by c limit 10) x"),
                "SELECT count(*) AS \"jdbc_agg_", "ORDER BY \"c\" ASC NULLS FIRST LIMIT 10");
    }

    @Test
    public void testResidualFilterAndUnsupportedOrderStayLocal() throws Exception {
        for (String sql : new String[] {
                "select a from " + TABLE + " where md5(a) = 'x' order by c limit 10",
                "select a from " + TABLE + " order by a limit 10",
                "select a from " + TABLE + " order by c",
                "select a from jdbc0.partitioned_db0.tbl0 order by c limit 10"}) {
            String plan = getFragmentPlan(sql);
            Assertions.assertFalse(plan.contains("ORDER BY"), plan);
        }
    }

    @Test
    public void testOffsetIsPushedAndScanLimitIsNot() throws Exception {
        // PostgreSQL applies OFFSET between ORDER BY and LIMIT, so the remote returns exactly the
        // rows the dropped TopN would have kept and nothing re-applies the offset locally.
        String offsetPlan = getFragmentPlan("select a from " + TABLE + " order by c limit 2, 10");
        assertContains(offsetPlan, "ORDER BY \"c\" ASC NULLS FIRST LIMIT 10 OFFSET 2");
        Assertions.assertFalse(offsetPlan.contains("TOP-N"), offsetPlan);
        // An inner LIMIT leaves a global LIMIT operator between the TopN and the scan, so the rule's
        // pattern does not match it at all and the TopN stays local.
        String limitedPlan = getFragmentPlan(
                "select c from (select c from " + TABLE + " limit 100) t order by c limit 10");
        assertContains(limitedPlan, "TOP-N", "QUERY: SELECT \"c\" FROM \"tbl0\" LIMIT 100");
        Assertions.assertFalse(limitedPlan.contains("ORDER BY"), limitedPlan);
    }

    @Test
    public void testResourceBackedTableKeepsTopNLocal() throws Exception {
        JDBCTable table = (JDBCTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable("test", "jdbc_topn_pg_resource_table");
        Assertions.assertEquals("jdbc_topn_pg_resource", table.getResourceName());
        Assertions.assertEquals(JDBCTable.ProtocolType.POSTGRES, table.getProtocolType());
        Assertions.assertTrue(table.getOriginalJdbcColumnTypeNames().isEmpty());
        for (String column : List.of("d", "ts", "id")) {
            String plan = getFragmentPlan("SELECT id FROM test.jdbc_topn_pg_resource_table ORDER BY "
                    + column + " DESC NULLS LAST LIMIT 10");
            assertContains(plan, "TOP-N", "SCAN JDBC");
            String remote = plan.lines().filter(line -> line.contains("QUERY:")).findFirst().orElseThrow();
            Assertions.assertFalse(remote.contains("ORDER BY"), plan);
            Assertions.assertFalse(remote.contains("LIMIT"), plan);
        }
        Assertions.assertFalse(table.isInlineTable());
    }
}
