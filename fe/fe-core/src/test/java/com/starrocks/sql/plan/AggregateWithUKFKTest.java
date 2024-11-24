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

import org.junit.BeforeClass;
import org.junit.Test;

public class AggregateWithUKFKTest extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        starRocksAssert.withTable("CREATE TABLE `tt2` (\n" +
                "  `c21`  int NULL,\n" +
                "  `c22`  int NULL,\n" +
                "  `c23`  int NULL,\n" +
                "  `c24`  int NULL,\n" +
                "  `c25`  int NULL,\n" +
                "  `c26`  int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c21`)\n" +
                "DISTRIBUTED BY HASH(`c21`) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"unique_constraints\" = \"c21;c22;c23;c24;c25\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `tt1` (\n" +
                "  `c11`  int NULL,\n" +
                "  `c12`  int NULL,\n" +
                "  `c13`  int NULL,\n" +
                "  `c14`  int NULL,\n" +
                "  `c15`  int NULL,\n" +
                "  `c16`  int NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c11`)\n" +
                "DISTRIBUTED BY HASH(`c11`) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"unique_constraints\" = \"c11;c12;c13,c14;c11,c12,c13,c14\",\n" +
                "\"foreign_key_constraints\" = \"(c13) REFERENCES tt2(c23);(c15) REFERENCES tt2(c25);\"\n" +
                ");");

        connectContext.getSessionVariable().setEnableUKFKOpt(true);
    }

    @Test
    public void testEliminateAgg1() throws Exception {
        String sql = "SELECT \n" +
                "    id, \n" +
                "    SUM(big_value) AS sum_big_value\n" +
                "FROM \n" +
                "    test_agg_group_single_unique_key\n" +
                "GROUP BY \n" +
                "    id\n" +
                "ORDER BY \n" +
                "    id;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: id, INT, false]\n" +
                "  |  6 <-> [2: big_value, BIGINT, true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: test_agg_group_single_unique_key, rollup: test_agg_group_single_unique_key\n" +
                "     preAggregation: off. Reason: None aggregate function\n");

        sql = "SELECT \n" +
                "    id, \n" +
                "    COUNT(varchar_value) AS count_varchar_value\n" +
                "FROM \n" +
                "    test_agg_group_single_unique_key\n" +
                "GROUP BY \n" +
                "    id\n" +
                "ORDER BY \n" +
                "    id;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: id, INT, false]\n" +
                "  |  6 <-> if[(5: varchar_value IS NULL, cast(0 as BIGINT), cast(1 as BIGINT)); " +
                "args: BOOLEAN,BIGINT,BIGINT; result: BIGINT; args nullable: false; result nullable: true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "SELECT\n" +
                "    id,\n" +
                "    big_value,\n" +
                "    AVG(decimal_value) AS avg_decimal_value\n" +
                "FROM\n" +
                "    test_agg_group_multi_unique_key\n" +
                "GROUP BY\n" +
                "    id, big_value\n" +
                "ORDER BY\n" +
                "    id;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: id, INT, false]\n" +
                "  |  2 <-> [2: big_value, BIGINT, true]\n" +
                "  |  6 <-> cast([4: decimal_value, DECIMAL64(10,5), true] as DECIMAL128(38,11))\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: test_agg_group_multi_unique_key, rollup: test_agg_group_multi_unique_key\n" +
                "     preAggregation: off. Reason: None aggregate function\n");

        sql = "SELECT\n" +
                "    id,\n" +
                "    big_value,\n" +
                "    COUNT(varchar_value) AS count_varchar_value\n" +
                "FROM\n" +
                "    test_agg_group_multi_unique_key\n" +
                "GROUP BY\n" +
                "    id, big_value\n" +
                "ORDER BY\n" +
                "    id;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: id, INT, false]\n" +
                "  |  2 <-> [2: big_value, BIGINT, true]\n" +
                "  |  6 <-> if[(5: varchar_value IS NULL, cast(0 as BIGINT), cast(1 as BIGINT)); " +
                "args: BOOLEAN,BIGINT,BIGINT; result: BIGINT; args nullable: false; result nullable: true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode");
        sql = "SELECT\n" +
                "    id,\n" +
                "    big_value,\n" +
                "    COUNT(varchar_value) AS count_varchar_value\n" +
                "FROM test_agg_group_multi_unique_key\n" +
                "GROUP BY id, big_value\n" +
                "HAVING COUNT(varchar_value) > 0;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  PREDICATES: if(5: varchar_value IS NULL, 0, 1) > 0");
        sql = "SELECT\n" +
                "    id,\n" +
                "    big_value,\n" +
                "    COUNT(varchar_value) AS count_varchar_value\n" +
                "FROM test_agg_group_multi_unique_key\n" +
                "GROUP BY id, big_value\n" +
                "HAVING SUM(varchar_value) > 0 and id + 2 > 5;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "CAST(5: varchar_value AS DOUBLE) > 0.0, 1: id > 3");
    }

    @Test
    public void testEliminateAgg2() throws Exception {
        String sql;
        String plan;

        sql = "select c11, c12, c13, c14, c15, c16, sum(c11) from tt1 group by c11, c12, c13, c14, c15, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "\n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: c11\n" +
                "  |  <slot 2> : 2: c12\n" +
                "  |  <slot 3> : 3: c13\n" +
                "  |  <slot 4> : 4: c14\n" +
                "  |  <slot 5> : 5: c15\n" +
                "  |  <slot 6> : 6: c16\n" +
                "  |  <slot 7> : CAST(1: c11 AS BIGINT)\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select c13, c14, c15, c16, sum(c11) from tt1 group by c13, c14, c15, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "\n" +
                "  1:Project\n" +
                "  |  <slot 3> : 3: c13\n" +
                "  |  <slot 4> : 4: c14\n" +
                "  |  <slot 5> : 5: c15\n" +
                "  |  <slot 6> : 6: c16\n" +
                "  |  <slot 7> : CAST(1: c11 AS BIGINT)\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        // UK (c13, c14) is not satisified.
        sql = "select c14, c15, c16, sum(c11) from tt1 group by c14, c15, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: c11)\n" +
                "  |  group by: 4: c14, 5: c15, 6: c16\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testEliminateAggAfterPruneGroupBys() throws Exception {
        String sql;
        String plan;

        sql = "select sum(c11) from tt1 group by c11, c12, c13, c14, c15, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "\n" +
                "  1:Project\n" +
                "  |  <slot 7> : CAST(1: c11 AS BIGINT)\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select c11, c12, c13, sum(c11) from tt1 group by c11, c12, c13, c14, c15, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "\n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: c11\n" +
                "  |  <slot 2> : 2: c12\n" +
                "  |  <slot 3> : 3: c13\n" +
                "  |  <slot 7> : CAST(1: c11 AS BIGINT)\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select c11, c12, c13, c14, sum(c11) from tt1 group by c11, c12, c13, c14, c15, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "\n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: c11\n" +
                "  |  <slot 2> : 2: c12\n" +
                "  |  <slot 3> : 3: c13\n" +
                "  |  <slot 4> : 4: c14\n" +
                "  |  <slot 7> : CAST(1: c11 AS BIGINT)\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testEliminateAggAfterJoinFromUKChild() throws Exception {
        String sql;
        String plan;

        sql = "select c14, c21, sum(c11) from tt1 join tt2 on c13=c23 group by c14, c21";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  4:Project\n" +
                "  |  <slot 4> : 4: c14\n" +
                "  |  <slot 7> : 7: c21\n" +
                "  |  <slot 13> : CAST(1: c11 AS BIGINT)\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 3: c13 = 9: c23\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode");

        sql = "select sum(c11) from tt1 join tt2 on c13=c23 group by c14, c21";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  4:Project\n" +
                "  |  <slot 13> : CAST(1: c11 AS BIGINT)\n" +
                "  |  \n" +
                "  3:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)\n" +
                "  |  colocate: false, reason: \n" +
                "  |  equal join conjunct: 3: c13 = 9: c23\n" +
                "  |  \n" +
                "  |----2:EXCHANGE\n" +
                "  |    \n" +
                "  0:OlapScanNode");

        sql = "with w1 as (select c15, c14, sum(c11) as c11 from tt1 group by c15, c14)" +
                " select c14, c21, sum(c11) from w1 join tt2 on c15=c25 group by c14, c21";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:Project\n" +
                "  |  <slot 4> : 4: c14\n" +
                "  |  <slot 8> : 8: c21\n" +
                "  |  <slot 14> : 7: sum\n" +
                "  |  \n" +
                "  4:HASH JOIN");

        sql = "with w1 as (select c15, c14, sum(c11) as c11 from tt1 group by c15, c14)" +
                " select sum(c11) from w1 join tt2 on c15=c25 group by c14, c21";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  6:Project\n" +
                "  |  <slot 14> : 7: sum\n" +
                "  |  \n" +
                "  5:HASH JOIN");
    }


    @Test
    public void testCannotEliminateAggAfterJoinFromUKChild() throws Exception {
        String sql;
        String plan;

        sql = "with w1 as (select c15, c14, sum(c11) as c11 from tt1 group by c15, c14)" +
                " select c14, c21, sum(c11) from w1 left join tt2 on c15=c25 group by c14, c21";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  6:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(7: sum)\n" +
                "  |  group by: 4: c14, 8: c21\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 4> : 4: c14\n" +
                "  |  <slot 7> : 7: sum\n" +
                "  |  <slot 8> : 8: c21\n" +
                "  |  \n" +
                "  4:HASH JOIN");

        sql = "with w1 as (select c15, c14, sum(c11) as c11 from tt1 group by c15, c14)" +
                " select c14, c21, sum(c11) from w1 full join tt2 on c15=c25 group by c14, c21";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  7:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(7: sum)\n" +
                "  |  group by: 4: c14, 8: c21\n" +
                "  |  \n" +
                "  6:Project\n" +
                "  |  <slot 4> : 4: c14\n" +
                "  |  <slot 7> : 7: sum\n" +
                "  |  <slot 8> : 8: c21\n" +
                "  |  \n" +
                "  5:HASH JOIN");
    }


    @Test
    public void testEliminateAggAfterJoinFromFKChild() throws Exception {
        String sql;
        String plan;

        sql = "select c11, c12, c14, c15, c16, sum(c11) from tt1 join tt2 on c13=c23 group by c11, c12, c14, c15, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 1> : 1: c11\n" +
                "  |  <slot 2> : 2: c12\n" +
                "  |  <slot 4> : 4: c14\n" +
                "  |  <slot 5> : 5: c15\n" +
                "  |  <slot 6> : 6: c16\n" +
                "  |  <slot 13> : CAST(1: c11 AS BIGINT)\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select sum(c11) from tt1 join tt2 on c13=c23 group by c11, c12, c13, c14, c15, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 13> : CAST(1: c11 AS BIGINT)\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }


    @Test
    public void testCannotEliminateAggAfterJoinFromFKChild() throws Exception {
        String sql;
        String plan;

        sql = "select c11, c12, c14, c15, c16, sum(c11) from tt1 right join tt2 on c13=c23 group by c11, c12, c14, c15, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  6:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(1: c11)\n" +
                "  |  group by: 1: c11, 2: c12, 4: c14, 5: c15, 6: c16\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 1> : 1: c11\n" +
                "  |  <slot 2> : 2: c12\n" +
                "  |  <slot 4> : 4: c14\n" +
                "  |  <slot 5> : 5: c15\n" +
                "  |  <slot 6> : 6: c16\n" +
                "  |  \n" +
                "  4:HASH JOIN");


        sql = "select c11, c12, c14, c15, c16, sum(c11) from tt1 full join tt2 on c13=c23 group by c11, c12, c14, c15, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "\n" +
                "  6:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: sum(1: c11)\n" +
                "  |  group by: 1: c11, 2: c12, 4: c14, 5: c15, 6: c16\n" +
                "  |  \n" +
                "  5:Project\n" +
                "  |  <slot 1> : 1: c11\n" +
                "  |  <slot 2> : 2: c12\n" +
                "  |  <slot 4> : 4: c14\n" +
                "  |  <slot 5> : 5: c15\n" +
                "  |  <slot 6> : 6: c16\n" +
                "  |  \n" +
                "  4:HASH JOIN");
    }

    @Test
    public void testEliminateAggAfterAgg() throws Exception {
        String sql;
        String plan;

        sql = "select c15, c16 from (" +
                "select c15, c16 from tt1 group by c15, c16" +
                ")t group by c15, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  RESULT SINK\n" +
                "\n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 5: c15, 6: c16\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select c15, c15+1 from (" +
                "select c15 from tt1 group by c15" +
                ")t group by 1, 2";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  RESULT SINK\n" +
                "\n" +
                "  2:Project\n" +
                "  |  <slot 5> : 5: c15\n" +
                "  |  <slot 7> : CAST(5: c15 AS BIGINT) + 1\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  group by: 5: c15\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select c16, sum_c11, count(sum_c11) from (" +
                "select c16, sum(c11) as sum_c11 from tt1 group by c16" +
                ")t group by c16, sum_c11";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  RESULT SINK\n" +
                "\n" +
                "  2:Project\n" +
                "  |  <slot 6> : 6: c16\n" +
                "  |  <slot 7> : 7: sum\n" +
                "  |  <slot 8> : if(7: sum IS NULL, CAST(0 AS BIGINT), CAST(1 AS BIGINT))\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: c11)\n" +
                "  |  group by: 6: c16\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select c16, count(sum_c11) from (" +
                "select c15, c16, sum(c11) as sum_c11 from tt1 group by c15, c16 " +
                ")t group by c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  3:AGGREGATE (update serialize)\n" +
                "  |  STREAMING\n" +
                "  |  output: count(7: sum)\n" +
                "  |  group by: 6: c16\n" +
                "  |  \n" +
                "  2:Project\n" +
                "  |  <slot 6> : 6: c16\n" +
                "  |  <slot 7> : 7: sum\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: sum(1: c11)\n" +
                "  |  group by: 5: c15, 6: c16\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select c15, c16, count(sum_c11) from (" +
                "select c11, c15, c16, sum(c11) as sum_c11 from tt1 group by c11, c15, c16 " +
                ")t group by c15, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  RESULT SINK\n" +
                "\n" +
                "  2:AGGREGATE (update finalize)\n" +
                "  |  output: count(7: sum)\n" +
                "  |  group by: 5: c15, 6: c16\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  <slot 5> : 5: c15\n" +
                "  |  <slot 6> : 6: c16\n" +
                "  |  <slot 7> : CAST(1: c11 AS BIGINT)\n" +
                "  |  \n" +
                "  0:OlapScanNode");

        sql = "select c11, c16, count(sum_c11) from (" +
                "select c11, c15, c16, sum(c11) as sum_c11 from tt1 group by c11, c15, c16 " +
                ")t group by c11, c16";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  RESULT SINK\n" +
                "\n" +
                "  1:Project\n" +
                "  |  <slot 1> : 1: c11\n" +
                "  |  <slot 6> : 6: c16\n" +
                "  |  <slot 8> : if(CAST(1: c11 AS BIGINT) IS NULL, CAST(0 AS BIGINT), CAST(1 AS BIGINT))\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testEliminateAggForCountReturnType() throws Exception {
        String sql;
        String plan;

        sql = "select c21, count(c22) from tt2 group by c21";
        plan = getVerboseExplain(sql);
        assertContains(plan, "if[(2: c22 IS NULL, cast(0 as BIGINT), cast(1 as BIGINT)); " +
                "args: BOOLEAN,BIGINT,BIGINT; result: BIGINT; args nullable: false; result nullable: true]");

        sql = "select c21, count(1) as cnt from tt2 group by c21 order by cnt";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  2:SORT\n" +
                "  |  order by: [7, BIGINT, false] ASC\n" +
                "  |  offset: 0\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: c21, INT, true]\n" +
                "  |  7 <-> 1\n" +
                "  |  cardinality: 1");

        sql = "select c21, count(*) as cnt from tt2 group by c21 order by cnt";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  2:SORT\n" +
                "  |  order by: [7, BIGINT, false] ASC\n" +
                "  |  offset: 0\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: c21, INT, true]\n" +
                "  |  7 <-> 1\n" +
                "  |  cardinality: 1");

        sql = "select c21, count() as cnt from tt2 group by c21 order by cnt";
        plan = getVerboseExplain(sql);
        assertContains(plan, "  2:SORT\n" +
                "  |  order by: [7, BIGINT, false] ASC\n" +
                "  |  offset: 0\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: c21, INT, true]\n" +
                "  |  7 <-> 1\n" +
                "  |  cardinality: 1");
    }

}
