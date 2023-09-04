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
import com.starrocks.catalog.Table;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.EmptyStatisticStorage;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CTEPlanTest extends PlanTestBase {
    private static class TestStorage extends EmptyStatisticStorage {
        @Override
        public ColumnStatistic getColumnStatistic(Table table, String column) {
            return new ColumnStatistic(0, 2000000, 0, 8, 2000000);
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        globalStateMgr.setStatisticStorage(new TestStorage());

        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("t0");
        setTableStatistics(t0, 20000000);

        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("t1");
        setTableStatistics(t1, 2000000);
    }

    @Before
    public void alwaysCTEReuse() {
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
    }

    @After
    public void defaultCTEReuse() {
        connectContext.getSessionVariable().setCboCTERuseRatio(1.5);
    }

    @Test
    public void testMultiFlatCTE() throws Exception {
        String sql = "with x0 as (select * from t0), x1 as (select * from t1) " +
                "select * from (select * from x0 union all select * from x1 union all select * from x0) tt;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    RANDOM"));
        Assert.assertTrue(plan.contains("  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    RANDOM\n" +
                "\n" +
                "  5:OlapScanNode\n" +
                "     TABLE: t1"));
    }

    @Test
    public void testMultiContainsCTE() throws Exception {
        String sql = "with x0 as (select * from t0), x1 as (select * from x0) " +
                "select * from (select * from x0 union all select * from x1 union all select * from x0) tt;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 08\n" +
                "    RANDOM"));
    }

    @Test
    public void testFromUseCte() throws Exception {
        String sql = "with x0 as (select * from t0) " +
                "select * from (with x1 as (select * from t1) select * from x1 join x0 on x1.v4 = x0.v1) tt";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 7: v4 = 10: v1"));
        Assert.assertFalse(plan.contains("MultiCastDataSinks"));
    }

    @Test
    public void testSubqueryUserSameCTE() throws Exception {
        String sql = "with x0 as (select * from t0) " +
                "select * from x0 x,t1 y where v1 in (select v2 from x0 z where z.v1 = x.v1)";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    RANDOM");

        sql = "with x0 as (select * from t0) " +
                "select * from x0 t,t1 where v1 in (select v2 from x0 where t.v1 = v1)";
        plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    RANDOM"));
    }

    @Test
    public void testCTEJoinReorderLoseStatistics() throws Exception {
        connectContext.getSessionVariable().setMaxTransformReorderJoins(1);

        String sql = "with xx as (select * from t0) select * from xx as x0 join xx as x1 on x0.v1 = x1.v1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 04\n" +
                "    RANDOM");

        connectContext.getSessionVariable().setMaxTransformReorderJoins(4);
    }

    @Test
    public void testOneCTEInline() throws Exception {
        String sql = "with x0 as (select * from t0), x1 as (select * from t1) " +
                "select * from (select * from x0 union all select * from x1 union all select * from x0) tt;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 07\n" +
                "    RANDOM");
    }

    @Test
    public void testOneCTEInlineComplex() throws Exception {
        String sql = "with x0 as (select * from t0), x1 as (select * from x0) " +
                "select * from x1;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  RESULT SINK\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));
    }

    @Test
    public void testOneCTEInlineComplex2() throws Exception {
        String sql = "with x0 as (select * from t0), x1 as (select * from x0), x2 as (select * from x1), " +
                "x3 as (select * from x2) " +
                "select * from x3;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("  RESULT SINK\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     TABLE: t0"));
    }

    @Test
    public void testCTEPredicate() throws Exception {
        String sql = "with xx as (select * from t0) " +
                "select x1.v1 from xx x1 join xx x2 on x1.v2=x2.v3 where x1.v3 = 4 and x2.v2=3;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (2: v2 = 3) OR (3: v3 = 4)");
    }

    @Test
    public void testCTELimit() throws Exception {
        String sql = "with xx as (select * from t0) " +
                "select x1.v1 from (select * from xx limit 1) x1 " +
                "join (select * from xx limit 3) x2 on x1.v2=x2.v3;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks"));
        Assert.assertTrue(plan.contains("cardinality=1\n" +
                "     avgRowSize=24.0\n" +
                "     limit: 3"));
    }

    @Test
    public void testCTEPredicateLimit() throws Exception {
        String sql = "with xx as (select * from t0) " +
                "select x1.v1 from " +
                "(select * from xx where xx.v2 = 2 limit 1) x1 join " +
                "(select * from xx where xx.v3 = 4 limit 3) x2 on x1.v2=x2.v3;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "0:OlapScanNode\n" +
                "     TABLE: t0\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: (3: v3 = 4) OR (2: v2 = 2)\n" +
                "     partitions=0/1\n" +
                "     rollup: t0\n" +
                "     tabletRatio=0/0\n" +
                "     tabletList=\n" +
                "     cardinality=1\n" +
                "     avgRowSize=24.0\n" +
                "     limit: 3");
    }

    @Test
    public void testCTEPruneColumns() throws Exception {
        String sql = "with xx as (select * from t0) select v1 from xx union all select v2 from xx;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    RANDOM");
    }

    @Test
    public void testComplexCTE() throws Exception {
        String sql = "WITH " +
                "  s AS (select * from t0), \n" +
                "  a AS (select * from s), \n" +
                "  a2 AS (select * from s), \n" +
                "  b AS (" +
                "    select v3, v1, v2 from s\n" +
                "    UNION\n" +
                "    select v3 + 1, v1 + 2, v2 + 3 from s" +
                "  )\n" +
                "  select * from b;";
        String plan = getFragmentPlan(sql);
        Assert.assertTrue(plan.contains("MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 05\n" +
                "    RANDOM"));
    }

    @Test
    public void testComplexCTEAllCostInline() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(Double.MAX_VALUE);

        String sql = "WITH x1 AS (select * from t0), \n" +
                " x2 AS (select * from x1) \n" +
                " select * " +
                " from (select x2.* from x1 join x2 on x1.v2 = x2.v2) as s1" +
                " join (select x1.* from x1 join x2 on x1.v3 = x2.v3) as s2 on s1.v2 = s2.v2;";
        String plan = getFragmentPlan(sql);
        defaultCTEReuse();
        Assert.assertFalse(plan.contains("MultiCastDataSinks"));
    }

    @Test
    public void testSubqueryWithPushPredicate() throws Exception {
        String sql = "select * from " +
                "(with xx as (select * from t0) select x1.* from xx x1 join xx x2 on x1.v2 = x2.v2) s " +
                "where s.v1 = 2;";

        String plan = getFragmentPlan(sql);
        defaultCTEReuse();
        assertContains(plan, "3:SELECT\n" +
                "  |  predicates: 4: v1 = 2\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 4> : 1: v1\n" +
                "  |  <slot 5> : 2: v2\n" +
                "  |  <slot 6> : 3: v3");
    }

    @Test
    public void testSubqueryWithPushLimit() throws Exception {
        String sql = "select * from " +
                "(with xx as (select * from t0) " +
                "select x1.* from xx x1 left outer join[broadcast] xx x2 on x1.v2 = x2.v2) s " +
                "where s.v1 = 2 limit 10;";

        String plan = getFragmentPlan(sql);
        defaultCTEReuse();
        Assert.assertTrue(plan.contains("  3:SELECT\n" +
                "  |  predicates: 4: v1 = 2\n" +
                "  |  limit: 10"));
    }

    @Test
    public void testLeftJoinCTEWithConstOnPredicates() throws Exception {
        String sql1 = "WITH \n" +
                "    w_t0 as (SELECT * FROM t0) \n" +
                "SELECT * \n" +
                "FROM t1 LEFT JOIN w_t0 \n" +
                "ON t1.v4 = w_t0.v1 \n" +
                "AND false;\n";

        getFragmentPlan(sql1);

        String sql2 = "WITH \n" +
                "    w_t0 as (SELECT * FROM t0) \n" +
                "SELECT * \n" +
                "FROM w_t0 LEFT JOIN t1 \n" +
                "ON t1.v4 = w_t0.v1 \n" +
                "AND true;\n";

        getFragmentPlan(sql2);
    }

    @Test
    public void testCTEConsumeTuple() throws Exception {
        String sql = "WITH w_t0 as (SELECT * FROM t0) \n" +
                "SELECT x0.v1, x1.v2 FROM  w_t0 x0, w_t0 x1";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 01\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    RANDOM");

        String thrift = getThriftPlan(sql);
        assertNotContains(thrift, "tuple_id:3");
    }

    @Test
    public void testCTEAnchorOperatorOutputColumns() throws Exception {
        String sql = "SELECT \n" +
                "  CAST(\n" +
                "    (CAST(t1.v4 AS FLOAT) IN ((SELECT subt0.v1 FROM t0 AS subt0 WHERE NULL))) \n" +
                "    AND CAST(\n" +
                "      CAST(t1.v4 AS FLOAT) IN ((SELECT subt0.v1 FROM t0 AS subt0 WHERE NULL)) AS BOOLEAN\n" +
                "    ) AS INT\n" +
                "  ) as count \n" +
                "FROM (SELECT t1.v4 FROM t1) t1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "33:Project\n" +
                "  |  <slot 12> : CAST((7: expr) AND (CASE WHEN (16: countRows IS NULL) OR (16: countRows = 0) " +
                "THEN FALSE WHEN CAST(CAST(1: v4 AS FLOAT) AS DOUBLE) IS NULL THEN NULL WHEN 14: cast IS NOT NULL " +
                "THEN TRUE WHEN 17: countNotNulls < 16: countRows THEN NULL ELSE FALSE END) AS INT)\n");
    }

    @Test
    public void testCTEAnchorOperatorOutputColumns1() throws Exception {
        String sql = "SELECT (t1.v4 IN (SELECT subt0.v1 FROM t0 AS subt0 WHERE NULL)) IS NULL\n" +
                "FROM t1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "16:Project\n" +
                "  |  <slot 8> : CASE WHEN (11: countRows IS NULL) OR (11: countRows = 0) " +
                "THEN FALSE WHEN 1: v4 IS NULL THEN NULL WHEN 9: v1 IS NOT NULL " +
                "THEN TRUE WHEN 12: countNotNulls < 11: countRows THEN NULL ELSE FALSE END IS NULL\n");
    }

    @Test
    public void testEmptyPredicate() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(2);
        String sql = "WITH w_t0 as (SELECT * FROM t0) \n" +
                "SELECT v1, v2, v3 FROM  w_t0 x0 where false union select v1, v2, v3 from w_t0 x1 where abs(1) = 2";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  2:AGGREGATE (update finalize)");
    }

    @Test
    public void testEmptyCTE() throws Exception {
        String sql = "WITH w_t0 as (SELECT * FROM t0), " +
                "          w_t1 as (select * from t1)\n" +
                "SELECT v1, v2, v3 FROM  w_t0 x0 where false " +
                "union " +
                "select v1, v2, v3 from w_t0 x1 where abs(1) = 2 " +
                "union " +
                "select v4, v5, v6 from w_t1 x2 where 1 > 2 " +
                "union " +
                "select v4, v5, v6 from w_t1 x2 where not null ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  PARTITION: RANDOM\n" +
                "\n" +
                "  RESULT SINK\n" +
                "\n" +
                "  2:AGGREGATE (update finalize)\n");
    }

    @Test
    public void testCTEExchangePruneColumn() throws Exception {
        String sql = "WITH w_t0 as (SELECT * FROM t0) \n" +
                "SELECT x0.v1, x1.v2 FROM  w_t0 x0, w_t0 x1";

        String thrift = getThriftPlan(sql);
        assertContains(thrift, "TMultiCastDataStreamSink");
        assertContains(thrift, "dest_dop:0, output_columns:[1]");
        assertContains(thrift, "dest_dop:0, output_columns:[2]");
    }

    @Test
    public void testMultiNestCTE() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(10000);
        String sql = "WITH x1 as (" +
                "   WITH x2 as (SELECT * FROM t0)" +
                "   SELECT * from x2 " +
                "   UNION ALL " +
                "   SELECT * from x2 " +
                ") \n" +
                "SELECT * from x1 " +
                "UNION ALL " +
                "SELECT * from x1 ";
        defaultCTEReuse();
        String plan = getFragmentPlan(sql);
        Assert.assertEquals(4, StringUtils.countMatches(plan, "TABLE: t0"));
        Assert.assertEquals(0, StringUtils.countMatches(plan, "MultiCastDataSinks"));

        alwaysCTEReuse();
        plan = getFragmentPlan(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan, "TABLE: t0"));
        Assert.assertEquals(2, StringUtils.countMatches(plan, "MultiCastDataSinks"));
    }

    @Test
    public void testMultiNestCTE2() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(10000);
        String sql = "WITH x1 as (" +
                "   WITH x2 as (" +
                "       WITH x3 as (SELECT * FROM t0)" +
                "       SELECT * FROM x3 " +
                "       UNION ALL " +
                "       SELECT * FROM x3 " +
                "   )" +
                "   SELECT * from x2 " +
                "   UNION ALL " +
                "   SELECT * from x2 " +
                ") \n" +
                "SELECT * from x1 " +
                "UNION ALL " +
                "SELECT * from x1 ";
        defaultCTEReuse();
        String plan = getFragmentPlan(sql);
        Assert.assertEquals(8, StringUtils.countMatches(plan, "TABLE: t0"));
        Assert.assertEquals(0, StringUtils.countMatches(plan, "MultiCastDataSinks"));

        alwaysCTEReuse();
        plan = getFragmentPlan(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan, "TABLE: t0"));
        Assert.assertEquals(3, StringUtils.countMatches(plan, "MultiCastDataSinks"));
    }

    @Test
    public void testMultiNestCTE3() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(10000000);
        String sql = "WITH x1 as (" +
                "   WITH x2 as (SELECT * FROM t0)" +
                "   SELECT * from x2 " +
                "   UNION ALL " +
                "   SELECT * from x2 " +
                ") \n" +
                "SELECT * from (" +
                "   with x3 as (" +
                "       SELECT * from x1 " +
                "       UNION ALL " +
                "       SELECT * from x1 " +
                "   )" +
                "   select * from x3" +
                "   union all " +
                "   select * from x3" +
                ") x4 ";
        String plan = getFragmentPlan(sql);
        defaultCTEReuse();
        Assert.assertEquals(8, StringUtils.countMatches(plan, "TABLE: t0"));
        Assert.assertEquals(0, StringUtils.countMatches(plan, "MultiCastDataSinks"));

        alwaysCTEReuse();
        plan = getFragmentPlan(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan, "TABLE: t0"));
        Assert.assertEquals(3, StringUtils.countMatches(plan, "MultiCastDataSinks"));
    }

    @Test
    public void testMultiNestCTE4() throws Exception {
        connectContext.getSessionVariable().setCboCTERuseRatio(Double.MAX_VALUE);
        String sql = "WITH x1 as (" +
                "   WITH x2 as (SELECT * FROM t0)" +
                "   SELECT * from x2 " +
                "   UNION ALL " +
                "   SELECT * from x2 " +
                ") \n" +
                "SELECT * from (" +
                "   with x3 as (" +
                "       SELECT * from x1 " +
                "       UNION ALL " +
                "       SELECT * from x1 " +
                "   )" +
                "   select * from x3" +
                "   union all " +
                "   select * from x3" +
                ") x4 join (" +
                "   with x5 as (" +
                "       SELECT * from x1 " +
                "       UNION ALL " +
                "       SELECT * from x1 " +
                "   )" +
                "   select * from x5" +
                "   union all " +
                "   select * from x5" +
                ") x7";
        String plan = getFragmentPlan(sql);
        defaultCTEReuse();
        Assert.assertEquals(16, StringUtils.countMatches(plan, "TABLE: t0"));
        Assert.assertEquals(0, StringUtils.countMatches(plan, "MultiCastDataSinks"));

        alwaysCTEReuse();
        plan = getFragmentPlan(sql);
        Assert.assertEquals(1, StringUtils.countMatches(plan, "TABLE: t0"));
        Assert.assertEquals(4, StringUtils.countMatches(plan, "MultiCastDataSinks"));
    }

    @Test
    public void testMultiRefCTE() throws Exception {
        String sql = "WITH x1 as (" +
                " select * from t0" +
                "), " +
                " x2 as (" +
                " select * from x1" +
                " union all" +
                " select * from x1" +
                ")" +
                "SELECT * from x1 " +
                "UNION ALL " +
                "SELECT * from x2 ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 06\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 09\n" +
                "    RANDOM\n" +
                "\n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testCTELimitNumInline() throws Exception {
        connectContext.getSessionVariable().setCboCTEMaxLimit(4);
        defaultCTEReuse();
        String sql = "with x1 as (select * from t0),\n" +
                "     x2 as (select * from t0),\n" +
                "     x3 as (select * from t0),\n" +
                "     x4 as (select * from t0),\n" +
                "     x5 as (select * from t0)\n" +
                "select * from x1 union all\n" +
                "select * from x1 union all\n" +
                "select * from x2 union all\n" +
                "select * from x2 union all\n" +
                "select * from x3 union all\n" +
                "select * from x3 union all\n" +
                "select * from x4 union all\n" +
                "select * from x4 union all\n" +
                "select * from x5 union all\n" +
                "select * from x5;";
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setCboCTEMaxLimit(10);
        System.out.println(plan);
        Assert.assertFalse(plan.contains("MultiCastDataSinks"));
    }

    @Test
    public void testCTELimitNumReuse() throws Exception {
        connectContext.getSessionVariable().setCboCTEMaxLimit(4);
        connectContext.getSessionVariable().setCboCTERuseRatio(100000);
        String sql = "with x1 as (select * from t0),\n" +
                "     x2 as (select * from t0),\n" +
                "     x3 as (select * from t0),\n" +
                "     x4 as (select * from t0),\n" +
                "     x5 as (select * from t0),\n" +
                "     x6 as (select * from t0)\n" +
                "select * from x1 union all\n" +
                "select * from x1 union all\n" +
                "select * from x1 union all\n" +
                "select * from x2 union all\n" +
                "select * from x2 union all\n" +
                "select * from x2 union all\n" +
                "select * from x3 union all\n" +
                "select * from x3 union all\n" +
                "select * from x3 union all\n" +
                "select * from x4 union all\n" +
                "select * from x4 union all\n" +
                "select * from x4 union all\n" +
                "select * from x5 union all\n" +
                "select * from x5 union all\n" +
                "select * from x5 union all\n" +
                "select * from x6 union all\n" +
                "select * from x6;";
        String plan = getFragmentPlan(sql);
        connectContext.getSessionVariable().setCboCTEMaxLimit(10);
        Assert.assertEquals(5, StringUtils.countMatches(plan, "MultiCastDataSinks"));
    }

    @Test
    public void testAllCTEConsumePruned() throws Exception {
        String sql = "select * from t0 where (abs(2) = 1 or v1 in (select v4 from t1)) and v1 = 2 and v1 = 5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:EMPTYSET\n" +
                "\n" +
                "PLAN FRAGMENT 1\n" +
                " OUTPUT EXPRS:\n" +
                "  PARTITION: UNPARTITIONED\n" +
                "\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 02\n" +
                "    UNPARTITIONED\n" +
                "\n" +
                "  1:EMPTYSET");
    }

    @Test
    public void testCTEColumnPruned() throws Exception {
        String sql = "WITH x1 as (" +
                " select * from t0" +
                ") " +
                "SELECT t1.* from t1, x1 " +
                "UNION ALL " +
                "SELECT t2.* from t2, x1 ";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 03\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 10\n" +
                "    RANDOM");
    }

    @Test
    public void testMultiDistinctWithLimit() throws Exception {
        {
            String sql = "select sum(distinct(v1)), avg(distinct(v2)) from t0 limit 1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "2:Project\n" +
                    "  |  <slot 4> : 4: sum\n" +
                    "  |  <slot 5> : CAST(7: multi_distinct_sum AS DOUBLE) / CAST(6: multi_distinct_count AS DOUBLE)\n" +
                    "  |  limit: 1\n" +
                    "  |  \n" +
                    "  1:AGGREGATE (update finalize)\n" +
                    "  |  output: multi_distinct_sum(1: v1), multi_distinct_count(2: v2), multi_distinct_sum(2: v2)\n" +
                    "  |  group by: \n" +
                    "  |  limit: 1");
        }
        {
            String sql = "select sum(distinct(v1)), avg(distinct(v2)) from t0 group by v3 limit 1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "2:Project\n" +
                    "  |  <slot 4> : 4: sum\n" +
                    "  |  <slot 5> : CAST(7: multi_distinct_sum AS DOUBLE) / CAST(6: multi_distinct_count AS DOUBLE)\n" +
                    "  |  limit: 1\n" +
                    "  |  \n" +
                    "  1:AGGREGATE (update finalize)\n" +
                    "  |  output: multi_distinct_sum(1: v1), multi_distinct_count(2: v2), multi_distinct_sum(2: v2)\n" +
                    "  |  group by: 3: v3\n" +
                    "  |  limit: 1");
        }
        {
            String sql = "select count(distinct v1, v2), avg(distinct(v2)) from t0 limit 1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "29:Project\n" +
                    "  |  <slot 4> : 4: count\n" +
                    "  |  <slot 5> : CAST(8: sum AS DOUBLE) / CAST(10: count AS DOUBLE)\n" +
                    "  |  limit: 1\n" +
                    "  |  \n" +
                    "  28:NESTLOOP JOIN\n" +
                    "  |  join op: CROSS JOIN\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  limit: 1");
        }
        {
            String sql = "select count(distinct v1, v2), avg(distinct(v2)) from t0 group by v3 limit 1";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "20:HASH JOIN\n" +
                    "  |  join op: INNER JOIN (BUCKET_SHUFFLE(S))\n" +
                    "  |  colocate: false, reason: \n" +
                    "  |  equal join conjunct: 8: v3 <=> 14: v3\n" +
                    "  |  limit: 1");
        }
        {
            String sql = "select count(distinct v2, v3) from t0 limit 5;\n";
            String plan = getFragmentPlan(sql);
            assertContains(plan, "6:AGGREGATE (merge finalize)\n" +
                    "  |  output: count(4: count)\n" +
                    "  |  group by: \n" +
                    "  |  limit: 5\n" +
                    "  |  \n" +
                    "  5:EXCHANGE");
        }
    }

    @Test
    public void testNestCte() throws Exception {
        String sql = "select /*+SET_VAR(cbo_max_reorder_node_use_exhaustive=1)*/* " +
                "from t0 " +
                "where (t0.v1 in (with c1 as (select 1 as v2) select x1.v2 from c1 x1 join c1 x2 join c1 x3)) is null;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  MultiCastDataSinks\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 15\n" +
                "    RANDOM\n" +
                "  STREAM DATA SINK\n" +
                "    EXCHANGE ID: 21\n" +
                "    RANDOM");
    }

    @Test
    public void testGatherWindowCTE() throws Exception {
        String sql = " WITH with_t_0 as (\n" +
                "  SELECT v3 FROM t0 \n" +
                ")\n" +
                "SELECT\n" +
                "  subt0.v3,\n" +
                "  ROW_NUMBER() OVER (PARTITION BY subt0.v3, subt0.v2),\n" +
                "  LAST_VALUE(subt0.v1) OVER (ORDER BY subt0.v3)\n" +
                "FROM t0 subt0, with_t_0\n" +
                "UNION ALL\n" +
                "SELECT\n" +
                "  subt0.v3,\n" +
                "  ROW_NUMBER() OVER (PARTITION BY subt0.v3, subt0.v2),\n" +
                "  LAST_VALUE(subt0.v1) OVER (ORDER BY subt0.v3)\n" +
                "FROM t0 subt0, with_t_0";

        String plan = getFragmentPlan(sql);
        assertContains(plan, "  24:ANALYTIC\n" +
                "  |  functions: [, row_number(), ]\n" +
                "  |  partition by: 14: v3, 13: v2\n" +
                "  |  window: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n" +
                "  |  \n" +
                "  23:SORT\n" +
                "  |  order by: <slot 14> 14: v3 ASC, <slot 13> 13: v2 ASC\n" +
                "  |  offset: 0");
    }

    @Test
    public void testNullTypeHack() throws Exception {
        String sql = "WITH cte_1 AS (\n" +
                "  SELECT null v1\n" +
                ")\n" +
                "SELECT  \n" +
                "  CASE \n" +
                "    WHEN a.v1 = b.v1 THEN 1 \n" +
                "    ELSE -1 \n" +
                "  END IS_OK\n" +
                "FROM cte_1 a, cte_1 b";

        String plan = getThriftPlan(sql);
        assertNotContains(plan, "NULL_TYPE");
    }

    @Test
    public void testMergePushdownPredicate() throws Exception {
        String sql = "with with_t_0 as (select v1, v2, v4 from t0 join t1),\n" +
                "with_t_1 as (select v1, v2, v5 from t0 join t1)\n" +
                "select v5, 1 from with_t_1 join with_t_0 left semi join\n" +
                "(select v2 from with_t_0 where v4 = 123) subwith_t_0\n" +
                "on with_t_0.v1 = subwith_t_0.v2 and with_t_0.v1 > 0\n" +
                "where with_t_0.v4 < 100;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "6:SELECT\n" +
                "  |  predicates: 19: v1 > 0, 22: v4 < 100");
        assertContains(plan, "9:SELECT\n" +
                "  |  predicates: 26: v2 > 0, 28: v4 = 123");
    }
}
