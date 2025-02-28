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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SkewJoinV2Test extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        connectContext.getSessionVariable().setEnableOptimizerSkewJoinByQueryRewrite(false);
        connectContext.getSessionVariable().setEnableOptimizerSkewJoinByBroadCastSkewValues(true);
    }

    @AfterClass
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableOptimizerSkewJoinByQueryRewrite(true);
        connectContext.getSessionVariable().setEnableOptimizerSkewJoinByBroadCastSkewValues(false);
        PlanTestBase.afterClass();
    }

    @Test
    public void testSkewJoinV2() throws Exception {
        String sql = "select v2, v5 from t0 join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        String sqlPlan = getVerboseExplain(sql);
        assertCContains(sqlPlan, "Input Partition: HYBRID_HASH_PARTITIONED\n" +
                "  RESULT SINK");
        // this is a normal union, which means its local exchanger is PASS_THROUGH
        assertCContains(sqlPlan, "10:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [2: v2, BIGINT, true] | [5: v5, BIGINT, true]\n" +
                "  |      [2: v2, BIGINT, true] | [5: v5, BIGINT, true]\n" +
                "  |  pass-through-operands: all");

        // shuffle join is UNION's left child, with global runtime filter
        assertCContains(sqlPlan, "  4:HASH JOIN\n" +
                "  |  join op: INNER JOIN (PARTITIONED)\n" +
                "  |  equal join conjunct: [1: v1, BIGINT, true] = [4: v4, BIGINT, true]\n");
        // shuffle join's both child is shuffle exchange
        assertCContains(sqlPlan, " 2:EXCHANGE\n" +
                "     distribution type: SHUFFLE\n" +
                "     partition exprs: [1: v1, BIGINT, true]");

        assertCContains(sqlPlan, " |----3:EXCHANGE\n" +
                "  |       distribution type: SHUFFLE\n" +
                "  |       partition exprs: [4: v4, BIGINT, true]");

        // broadcast join is UNION's right child, with local runtime filter
        assertCContains(sqlPlan, "8:HASH JOIN\n" +
                "  |    |  join op: INNER JOIN (BROADCAST)\n" +
                "  |    |  equal join conjunct: [1: v1, BIGINT, true] = [4: v4, BIGINT, true]\n");
        // broadcast's left child is ROUND_ROBIN, right child is BROADCAST
        assertCContains(sqlPlan, "  |    6:EXCHANGE\n" +
                "  |       distribution type: ROUND_ROBIN");
        assertCContains(sqlPlan, "|    |----7:EXCHANGE\n" +
                "  |    |       distribution type: BROADCAST");

        // left table's scan node has its own fragment with split data sink
        // split data sink will split data with split expr
        // if v1 NOT IN (1, 2), use HASH_PARTITIONED
        // else use RANDOM
        assertCContains(sqlPlan, "PLAN FRAGMENT 2(F00)\n" +
                "\n" +
                "  Input Partition: RANDOM\n" +
                "  SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 1: v1\n" +
                "  OutPut Exchange Id: 02\n" +
                "  Split expr: (1: v1 NOT IN (1, 2)) OR (1: v1 IS NULL)\n" +
                "  OutPut Partition: RANDOM\n" +
                "  OutPut Exchange Id: 06\n" +
                "  Split expr: 1: v1 IN (1, 2)\n" +
                "\n" +
                "  0:OlapScanNode\n" +
                "     table: t0, rollup: t0");

        // right table's scan node has its own fragment with split data sink
        // split data sink will split data with split expr
        // if v4 NOT IN (1, 2), use HASH_PARTITIONED
        // else use UNPARTITIONED
        assertCContains(sqlPlan, "PLAN FRAGMENT 1(F01)\n" +
                "\n" +
                "  Input Partition: RANDOM\n" +
                "  SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 4: v4\n" +
                "  OutPut Exchange Id: 03\n" +
                "  Split expr: (4: v4 NOT IN (1, 2)) OR (4: v4 IS NULL)\n" +
                "  OutPut Partition: UNPARTITIONED\n" +
                "  OutPut Exchange Id: 07\n" +
                "  Split expr: 4: v4 IN (1, 2)\n" +
                "\n" +
                "  1:OlapScanNode\n" +
                "     table: t1, rollup: t1");
    }

    @Test
    public void testSkewJoinV2WithComplexPredicate() throws Exception {
        String sql = "select v2, v5 from t0 join[skew|t0.v1(1,2)] t1 on abs(v1) = abs(v4) ";
        String sqlPlan = getVerboseExplain(sql);
        // if on predicate's input is not column from table, then split expr should use Project's output column like "7: abs"
        assertCContains(sqlPlan, "SplitCastDataSink:\n" +
                "  OutPut Partition: HASH_PARTITIONED: 7: abs\n" +
                "  OutPut Exchange Id: 04\n" +
                "  Split expr: (7: abs NOT IN (1, 2)) OR (7: abs IS NULL)\n" +
                "  OutPut Partition: RANDOM\n" +
                "  OutPut Exchange Id: 08\n" +
                "  Split expr: 7: abs IN (1, 2)\n" +
                "\n" +
                "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  2 <-> [2: v2, BIGINT, true]\n" +
                "  |  7 <-> abs[([1: v1, BIGINT, true]); " +
                "args: BIGINT; result: LARGEINT; args nullable: true; result nullable: true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode\n" +
                "     table: t0, rollup: t0");
    }

    @Test
    public void testSkewJoinV2WithLeftJoin() throws Exception {
        String sql = "select v2, v5 from t0 left join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        String sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, " join op: LEFT OUTER JOIN (PARTITIONED)");

        sql = "select v2 from t0 left semi join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "LEFT SEMI JOIN (PARTITIONED)");

        sql = "select v2 from t0 left anti join[skew|t0.v1(1,2)] t1 on v1 = v4 ";
        sqlPlan = getFragmentPlan(sql);
        assertCContains(sqlPlan, "LEFT ANTI JOIN (PARTITIONED)");
    }

    @Test
    public void testSkewJoinV2WithOneAgg() throws Exception {
        // one phase agg need skew join's output is the same as original shuffle join
        String sql = "select v4 from t0 join[skew|t0.v1(1,2)] t1 on v1 = v4 group by v4";
        String sqlPlan = getVerboseExplain(sql);
        assertCContains(sqlPlan, "12:AGGREGATE (update finalize)\n" +
                "  |  group by: [4: v4, BIGINT, true]");

        // union's local exchange type is DIRECT
        assertCContains(sqlPlan, " 11:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [4: v4, BIGINT, true]\n" +
                "  |      [4: v4, BIGINT, true]\n" +
                "  |  pass-through-operands: all\n" +
                "  |  local exchange type: DIRECT");

        // union's right child is exchange instead of broadcast join, exchange is used to force data output
        assertCContains(sqlPlan, "|----10:EXCHANGE\n" +
                "  |       distribution type: SHUFFLE\n" +
                "  |       partition exprs: [4: v4, BIGINT, true]");

        // broadcast join itself is in one fragment
        assertCContains(sqlPlan, " 9:Project\n" +
                "  |  output columns:\n" +
                "  |  4 <-> [4: v4, BIGINT, true]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  8:HASH JOIN\n" +
                "  |  join op: INNER JOIN (BROADCAST)");
    }

    @Test
    public void testSkewJoinV2WithTwoAgg() throws Exception {
        // 2 phase agg has no requirement for its child
        String sql = "select v3 from t0 join[skew|t0.v1(1,2)] t1 on v1 = v4 group by t0.v3";
        String sqlPlan = getVerboseExplain(sql);

        // this is 2 phase agg
        assertCContains(sqlPlan, "11:AGGREGATE (update serialize)");
        // union's local exchange type is PassThrough
        assertNotContains(sqlPlan, "local exchange type");

        // union's right child is broadcast join instead of exchange node
        assertCContains(sqlPlan, "10:UNION\n" +
                "  |  child exprs:\n" +
                "  |      [3: v3, BIGINT, true]\n" +
                "  |      [3: v3, BIGINT, true]\n" +
                "  |  pass-through-operands: all");
        assertCContains(sqlPlan, "9:Project\n" +
                "  |    |  output columns:\n" +
                "  |    |  3 <-> [3: v3, BIGINT, true]\n" +
                "  |    |  cardinality: 1\n" +
                "  |    |  \n" +
                "  |    8:HASH JOIN\n" +
                "  |    |  join op: INNER JOIN (BROADCAST)");
    }

    @Test
    public void testSkewJoinV2WithCountStar() throws Exception {
        String sql = "select count(1) from t0 join[skew|t0.v1(1,2)] t1 on v1 = v4";
        String sqlPlan = getVerboseExplain(sql);
        assertCContains(sqlPlan, "9:Project\n" +
                "  |    |  output columns:\n" +
                "  |    |  9 <-> 1\n" +
                "  |    |  cardinality: 1\n" +
                "  |    |  \n" +
                "  |    8:HASH JOIN\n" +
                "  |    |  join op: INNER JOIN (BROADCAST)");
        assertCContains(sqlPlan, "5:Project\n" +
                "  |  output columns:\n" +
                "  |  9 <-> 1\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  4:HASH JOIN");
    }
}
