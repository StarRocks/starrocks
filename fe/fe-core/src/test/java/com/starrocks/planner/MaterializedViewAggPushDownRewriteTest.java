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

package com.starrocks.planner;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.rule.transformation.materialization.AggregatedMaterializedViewPushDownRewriter;
import com.starrocks.thrift.TExplainLevel;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase.getAggFunction;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils.REWRITE_ROLLUP_FUNCTION_MAP;
import static com.starrocks.sql.optimizer.rule.transformation.materialization.common.AggregateFunctionRollupUtils.SAFE_REWRITE_ROLLUP_FUNCTION_MAP;

public class MaterializedViewAggPushDownRewriteTest extends MaterializedViewTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        MaterializedViewTestBase.beforeClass();
        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);
        connectContext.getSessionVariable().setEnableMaterializedViewPushDownRewrite(true);
        createTables("sql/ssb/",
                Lists.newArrayList("customer", "dates", "supplier", "part", "lineorder", "lineorder0"));
    }

    @Test
    public void testAggPushDown_RollupFunctions_Simple() {
        String mvAggArg = "LO_REVENUE";
        String queryAggArg = "LO_REVENUE";
        for (Map.Entry<String, String> e : REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String queryAggFunc = getAggFunction(funcName, queryAggArg);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, %s as revenue_sum\n" +
                    "from lineorder l group by LO_ORDERDATE", mvAggFunc);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, %s as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE", queryAggFunc);
                sql(query).contains("mv0");
            });
        }
    }

    @Test
    public void testAggPushDown_RollupFunctions_MVWithExpr1() {
        String mvAggArg = "(LO_REVENUE + 1) * 2";
        String queryAggArg = "(LO_REVENUE + 1) * 2";
        for (Map.Entry<String, String> e : REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String queryAggFunc = getAggFunction(funcName, queryAggArg);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, %s as revenue_sum\n" +
                    "from lineorder l group by LO_ORDERDATE", mvAggFunc);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, %s as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE", queryAggFunc);
                sql(query).contains("mv0");
            });
        }
    }

    @Test
    public void testAggPushDown_RollupFunctions_MVWithExpr2() {
        String mvAggArg = "LO_REVENUE";
        String queryAggArg = "LO_REVENUE";
        Set<String> numberAggFunctions = ImmutableSet.of("sum", "avg", "count", "min", "max");
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            if (!numberAggFunctions.contains(funcName)) {
                continue;
            }
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String queryAggFunc = getAggFunction(funcName, queryAggArg);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, %s as revenue_sum\n" +
                    "from lineorder l group by LO_ORDERDATE", mvAggFunc);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, " +
                        "   %s + 1 as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE", queryAggFunc, queryAggFunc);
                sql(query).contains("mv0");
            });
        }
    }

    @Test
    public void testAggPushDown_RollupFunctions_MVWithExpr3() {
        String mvAggArg = "LO_REVENUE";
        String queryAggArg = "LO_REVENUE";
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, sum(%s + 1) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE", mvAggArg);
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = String.format("select LO_ORDERDATE, " +
                    "   sum(%s + 1) as revenue_sum\n" +
                    "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                    "   group by LO_ORDERDATE having sum(%s + 1) > 10", queryAggArg, queryAggArg);
            sql(query).contains("mv0");
        });
    }

    @Test
    public void testAggPushDown_RollupFunctions_QueryMV_NoMatch() {
        // query and mv's agg function is not the same, cannot rewrite.
        String mvAggArg = "LO_REVENUE";
        String queryAggArg = "(LO_REVENUE + 1) * 2";
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String queryAggFunc = getAggFunction(funcName, queryAggArg);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, %s as revenue_sum\n" +
                    "from lineorder l group by LO_ORDERDATE", mvAggFunc);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, %s as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE", queryAggFunc);
                sql(query).nonMatch("mv0");
            });
        }
    }

    @Test
    public void testAggPushDown_RollupFunctions_MultiAggs1() {
        // one query contains multi agg functions, all can be rewritten.
        String aggArg = "LO_REVENUE";
        List<String> aggFuncs = Lists.newArrayList();
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, aggArg);
            aggFuncs.add(mvAggFunc);
        }
        String agg = Joiner.on(", ").join(aggFuncs);
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, %s \n" +
                "from lineorder l group by LO_ORDERDATE", agg);
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = String.format("select LO_ORDERDATE, %s \n" +
                    "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                    "   group by LO_ORDERDATE", agg);
            sql(query).match("mv0");
        });
    }

    @Test
    public void testAggPushDown_RollupFunctions_MultiAggs2() {
        // one query contains multi agg functions, all can be rewritten.
        String aggArg = "LO_REVENUE";

        // add both rollup functions and equivalent agg functions
        Map<String, String> mvFunToQueryFuncMap = Maps.newHashMap();
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, aggArg);
            mvFunToQueryFuncMap.put(mvAggFunc, mvAggFunc);
        }
        List<Pair<String, String>> equivalentFuncs = ImmutableList.of(
                Pair.create(String.format("bitmap_union(to_bitmap(%s))", aggArg), String.format("count(distinct(%s))", aggArg)),
                Pair.create(String.format("bitmap_union(to_bitmap(%s))", aggArg),
                        String.format("bitmap_union_count(to_bitmap(%s))", aggArg)),
                Pair.create(String.format("array_agg_distinct(%s)", aggArg), String.format("count(distinct(%s))", aggArg)),
                Pair.create(String.format("array_agg_distinct(%s)", aggArg), String.format("sum(distinct(%s))", aggArg))
        );
        for (Pair<String, String> pair : equivalentFuncs) {
            mvFunToQueryFuncMap.put(pair.first, pair.second);
        }

        String queryAgg = Joiner.on(", ").join(mvFunToQueryFuncMap.keySet());
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, %s \n" +
                "from lineorder l group by LO_ORDERDATE", queryAgg);

        String mvAgg = Joiner.on(", ").join(mvFunToQueryFuncMap.values());
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = String.format("select LO_ORDERDATE, %s \n" +
                    "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                    "   group by LO_ORDERDATE", mvAgg);
            sql(query).match("mv0");
        });
    }

    @Test
    public void testAggPushDown_RollupFunctions_MultiAggs3() {
        // one query contains multi agg functions, all can be rewritten.
        String aggArg = "LO_REVENUE";
        List<String> aggFuncs = Lists.newArrayList();
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, aggArg);
            aggFuncs.add(mvAggFunc);
        }
        String agg = Joiner.on(", ").join(aggFuncs);
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, %s \n" +
                "from lineorder l group by LO_ORDERDATE", agg);
        starRocksAssert.withMaterializedView(mv, () -> {
            {

                String query = String.format("select LO_ORDERDATE, %s \n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE having sum(%s) > 1", agg, aggArg);
                sql(query).match("mv0");
            }
            {
                String query = "select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1), min(LO_REVENUE + 1), " +
                        "bitmap_union_count(to_bitmap(LO_REVENUE + 1)), approx_count_distinct(LO_REVENUE + 1), " +
                        "PERCENTILE_APPROX(LO_REVENUE + 1, 0.5), PERCENTILE_APPROX(LO_REVENUE + 1, 0.7), " +
                        "sum(distinct LO_REVENUE + 1), count(distinct LO_REVENUE + 1) " +
                        "from lineorder l join dates d " +
                        "on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE HAVING sum(LO_REVENUE + 1) > 1 ;";
                sql(query).nonMatch("mv0");
            }
        });
    }

    @Test
    public void testAggPushDown_RollupFunctions_MultiAggs4() {
        // one query contains multi agg functions, all can be rewritten.
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL AS " +
                "select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1), min(LO_REVENUE + 1), " +
                "bitmap_union(to_bitmap(LO_REVENUE + 1)), hll_union(hll_hash(LO_REVENUE + 1)), " +
                "percentile_union(percentile_hash(LO_REVENUE + 1)), any_value(LO_REVENUE + 1), " +
                "bitmap_agg(LO_REVENUE + 1), array_agg_distinct(LO_REVENUE + 1) \n" +
                "from lineorder l group by LO_ORDERDATE;");
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = "select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1), min(LO_REVENUE + 1), " +
                    "bitmap_union_count(to_bitmap(LO_REVENUE + 1)), approx_count_distinct(LO_REVENUE + 1), " +
                    "PERCENTILE_APPROX(LO_REVENUE + 1, 0.5), PERCENTILE_APPROX(LO_REVENUE + 1, 0.7), " +
                    "sum(distinct LO_REVENUE + 1), count(distinct LO_REVENUE + 1) " +
                    "from lineorder l join dates d " +
                    "on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE HAVING sum(LO_REVENUE + 1) > 1 ;";
            sql(query).match("mv0");
        });
    }

    @Test
    public void testAggPushDown_RollupFunctions_MultiSameFunctions_BadCase() {
        connectContext.getSessionVariable().setEnableMaterializedViewPushDownRewrite(false);
        int repeatTimes = 1;
        String aggArg = "LO_REVENUE";
        String aggFunc = String.format("bitmap_union(to_bitmap(%s))", aggArg);
        List<String> repeatAggs = Lists.newArrayList();
        for (int i = 0; i < repeatTimes; i++) {
            repeatAggs.add(String.format("%s as agg%s", aggFunc, i));
        }
        String agg = Joiner.on(", ").join(repeatAggs);
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, %s \n" +
                "from lineorder l group by LO_ORDERDATE", agg);
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = String.format("select LO_ORDERDATE, %s \n" +
                    "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                    "   group by LO_ORDERDATE", agg);
            sql(query);
        });
        connectContext.getSessionVariable().setEnableMaterializedViewPushDownRewrite(true);
    }

    @Test
    public void testAggPushDown_RollupFunctions_MultiSameFunctions_BaseCase() {
        int repeatTimes = 1;
        String aggArg = "LO_REVENUE";
        String aggFunc = String.format("sum(%s)", aggArg);
        List<String> repeatAggs = Lists.newArrayList();
        for (int i = 0; i < repeatTimes; i++) {
            repeatAggs.add(String.format("%s as agg%s", aggFunc, i));
        }
        String agg = Joiner.on(", ").join(repeatAggs);
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, %s \n" +
                "from lineorder l group by LO_ORDERDATE", agg);

        // push down rule should return a non rewrite plan even if push down is failed.
        new MockUp<AggregatedMaterializedViewPushDownRewriter>() {
            @Mock
            public boolean checkAggOpt(OptExpression optExpression) {
                return false;
            }
        };
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = String.format("select LO_ORDERDATE, %s \n" +
                    "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                    "   group by LO_ORDERDATE", agg);
            sql(query).nonMatch("mv0").contains("lineorder");
        });
    }

    @Test
    public void testAggPushDown_RollupFunctions_MultiSameAggs1() {
        // one query contains multi same agg functions, all can be rewritten.
        String aggArg = "LO_REVENUE";
        List<String> aggFuncs = Lists.newArrayList();
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, aggArg);
            aggFuncs.add(mvAggFunc);
        }

        int repeatTimes = 4;
        for (String aggFunc : aggFuncs) {
            if (aggFunc.contains("bitmap_union")) {
                continue;
            }
            List<String> repeatAggs = Lists.newArrayList();
            for (int i = 0; i < repeatTimes; i++) {
                repeatAggs.add(String.format("%s as agg%s", aggFunc, i));
            }
            String agg = Joiner.on(", ").join(repeatAggs);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, %s \n" +
                    "from lineorder l group by LO_ORDERDATE", agg);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, %s \n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE", agg);
                sql(query).match("mv0");
            });
        }
    }

    @Test
    public void testAggPushDown_RollupFunctions_MultiSameAggs2() {
        // one query contains multi same agg functions, all can be rewritten.
        String aggArg = "LO_REVENUE";
        List<Pair<String, String>> aggFuncs = ImmutableList.of(
                Pair.create(String.format("bitmap_union(to_bitmap(%s))", aggArg), String.format("count(distinct(%s))", aggArg)),
                Pair.create(String.format("bitmap_union(to_bitmap(%s))", aggArg),
                        String.format("bitmap_union_count(to_bitmap(%s))", aggArg)),
                Pair.create(String.format("array_agg_distinct(%s)", aggArg), String.format("count(distinct(%s))", aggArg)),
                Pair.create(String.format("array_agg_distinct(%s)", aggArg), String.format("sum(distinct(%s))", aggArg))
        );


        int repeatTimes = 4;
        for (Pair<String, String> pair : aggFuncs) {
            List<String> repeatAggs = Lists.newArrayList();
            for (int i = 0; i < repeatTimes; i++) {
                repeatAggs.add(String.format("%s as agg%s", pair.second, i));
            }
            String agg = Joiner.on(", ").join(repeatAggs);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, %s \n" +
                    "from lineorder l group by LO_ORDERDATE", pair.first);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, %s \n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE", agg);
                sql(query).match("mv0");
            });
        }
    }

    private static final Set<String> IGNORE_AGG_FUNCTIONS = Sets.newHashSet("bitmap", "array", "hll", "percentile");
    private boolean isAggFunctionSupportHaving(String funcName) {
        for (String ignore : IGNORE_AGG_FUNCTIONS) {
            if (StringUtils.containsIgnoreCase(funcName, ignore)) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void testAggPushDown_RollupFunctions_WithHaving() {
        String mvAggArg = "(LO_REVENUE + 1) * 2";
        String queryAggArg = "(LO_REVENUE + 1) * 2";
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {

            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            if (!isAggFunctionSupportHaving(mvAggFunc)) {
                continue;
            }

            // HLL, BITMAP, PERCENTILE and ARRAY, MAP, STRUCT type couldn't as Predicate
            String queryAggFunc = getAggFunction(funcName, queryAggArg);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, %s as revenue_sum\n" +
                    "from lineorder l group by LO_ORDERDATE", mvAggFunc);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, %s as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE having revenue_sum is not null", queryAggFunc);
                sql(query).contains("mv0");
            });
        }
    }

    @Test
    public void testJoinWithAggPushDown_MultiJoins_GroupByKeysNoSatisfy() {
        String mvAggArg = "(LO_REVENUE + 1) * 2";
        String queryAggArg = "(LO_REVENUE + 1) * 2";
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String queryAggFunc = getAggFunction(funcName, queryAggArg);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, %s as revenue_sum\n" +
                    "from lineorder l group by LO_ORDERDATE", mvAggFunc);

            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, %s as revenue_sum\n" +
                        "     FROM lineorder AS l\n" +
                        "            INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY\n" +
                        "            INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY\n" +
                        "            INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY\n" +
                        "            INNER JOIN dates AS d ON l.lo_orderdate = d.d_datekey" +
                        " group by LO_ORDERDATE;", queryAggFunc);
                sql(query).nonMatch("mv0");
            });
        }
    }

    @Test
    public void testJoinWithAggPushDown_MultiJoins_GroupByKeysSatisfy1() {
        String mvAggArg = "(LO_REVENUE + 1) * 2";
        String queryAggArg = "(LO_REVENUE + 1) * 2";
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String queryAggFunc = getAggFunction(funcName, queryAggArg);

            // If you want to push down agg, agg mv should contain all join keys.
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, lo_custkey, lo_partkey, lo_suppkey, %s as revenue_sum\n" +
                    "from lineorder l group by LO_ORDERDATE, lo_custkey, lo_partkey, lo_suppkey", mvAggFunc);

            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, %s as revenue_sum\n" +
                        "     FROM lineorder AS l\n" +
                        "            INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY\n" +
                        "            INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY\n" +
                        "            INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY\n" +
                        "            INNER JOIN dates AS d ON l.lo_orderdate = d.d_datekey" +
                        " group by LO_ORDERDATE;", queryAggFunc);
                sql(query).contains("mv0");
            });
        }
    }

    @Test
    public void testJoinWithAggPushDown_MultiJoins_GroupByKeysSatisfy2() {
        // If you want to push down agg, agg mv should contain all join keys.
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL AS\n" +
                "select LO_ORDERDATE, lo_custkey, lo_partkey, lo_suppkey, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), " +
                "   bitmap_union(to_bitmap(LO_REVENUE)), hll_union(hll_hash(LO_REVENUE)), " +
                "   percentile_union(percentile_hash(LO_REVENUE)), any_value(LO_REVENUE), " +
                "   bitmap_agg(LO_REVENUE), array_agg_distinct(LO_REVENUE) \n" +
                "from lineorder l GROUP BY LO_ORDERDATE, lo_custkey, lo_partkey, lo_suppkey;");
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = String.format("select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE) " +
                    "FROM lineorder AS l INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY " +
                    "INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY " +
                    "INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY " +
                    "INNER JOIN dates AS d ON l.lo_orderdate = d.d_date " +
                    "group by LO_ORDERDATE order by LO_ORDERDATE;");
            sql(query).contains("mv0");
        });
    }

    @Test
    public void testJoinWithAggPushDown_MultiJoins_GroupByKeysSatisfy3() {
        // If you want to push down agg, agg mv should contain all join keys.
        String mv2 = String.format("CREATE MATERIALIZED VIEW mv2 REFRESH MANUAL AS\n" +
                "select LO_ORDERDATE, lo_custkey, lo_partkey, lo_suppkey, sum(LO_REVENUE), max(LO_REVENUE), " +
                "min(LO_REVENUE), bitmap_union(to_bitmap(LO_REVENUE)), hll_union(hll_hash(LO_REVENUE)), " +
                "percentile_union(percentile_hash(LO_REVENUE)), any_value(LO_REVENUE), bitmap_agg(LO_REVENUE), " +
                "array_agg_distinct(LO_REVENUE) \n" +
                "from lineorder l GROUP BY LO_ORDERDATE, lo_custkey, lo_partkey, lo_suppkey;");
        String mv0 = "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL AS\n" +
                "select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union(to_bitmap(LO_REVENUE)), " +
                "hll_union(hll_hash(LO_REVENUE)), percentile_union(percentile_hash(LO_REVENUE)), " +
                "any_value(LO_REVENUE), bitmap_agg(LO_REVENUE), array_agg_distinct(LO_REVENUE) \n" +
                "from lineorder l group by LO_ORDERDATE;";
        String mv1 = "\n" +
                "CREATE MATERIALIZED VIEW mv1 REFRESH MANUAL AS\n" +
                "select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1), min(LO_REVENUE + 1), " +
                "bitmap_union(to_bitmap(LO_REVENUE + 1)), hll_union(hll_hash(LO_REVENUE + 1)), " +
                "percentile_union(percentile_hash(LO_REVENUE + 1)), any_value(LO_REVENUE + 1), bitmap_agg(LO_REVENUE + 1), " +
                "array_agg_distinct(LO_REVENUE + 1) \n" +
                "from lineorder l group by LO_ORDERDATE;";
        starRocksAssert.withMaterializedViews(ImmutableList.of(mv0, mv1, mv2), (obj) -> {
            {
                String query = String.format("select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), " +
                        "bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), " +
                        "PERCENTILE_APPROX(LO_REVENUE, 0.5), " +
                        "PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE)  " +
                        "FROM lineorder AS l INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY " +
                        "INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY " +
                        "INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY " +
                        "INNER JOIN dates AS d ON l.lo_orderdate = d.d_date " +
                        "group by LO_ORDERDATE order by LO_ORDERDATE;");
                sql(query).contains("mv2");
            }

            {
                String query = "select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count" +
                        "(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), " +
                        "PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) " +
                        "from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE " +
                        "HAVING sum(LO_REVENUE) > 1 order by LO_ORDERDATE;";
                sql(query).contains("mv0");
            }
        });
        connectContext.getSessionVariable().setQueryIncludingMVNames("");
    }

    @Test
    public void testAggPushDown_RollupFunctions_GroupByAggIntersect1() {
        String mvAggArg = "LO_ORDERDATE";
        String queryAggArg = "LO_ORDERDATE";
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String queryAggFunc = getAggFunction(funcName, queryAggArg);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, %s as revenue_sum\n" +
                    "from lineorder l group by LO_ORDERDATE", mvAggFunc);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, %s as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE", queryAggFunc);
                sql(query).contains("mv0");
            });
        }
    }

    @Test
    public void testAggPushDown_RollupFunctions_GroupByAggIntersect2() {
        String mvAggArg = "LO_ORDERDATE";
        String queryAggArg = "LO_ORDERDATE";
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String queryAggFunc = getAggFunction(funcName, queryAggArg);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select lo_orderdate, %s as revenue_sum\n" +
                    "from lineorder l group by lo_orderdate", mvAggFunc);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select lo_orderkey + d_datekey, %s as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by lo_orderkey + d_datekey", queryAggFunc);
                sql(query).nonMatch("mv0");
            });
        }
    }

    @Test
    public void testAggPushDown_RollupFunctions_GroupBy_Agg_Intersect3() {
        String mvAggArg = "LO_ORDERDATE";
        String queryAggArg = "LO_ORDERDATE";
        for (Map.Entry<String, String> e : SAFE_REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String queryAggFunc = getAggFunction(funcName, queryAggArg);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select lo_orderkey, %s as revenue_sum\n" +
                    "from lineorder l group by lo_orderkey", mvAggFunc);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select lo_orderkey + d_datekey, %s as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by lo_orderkey + d_datekey", queryAggFunc);
                sql(query).nonMatch("mv0");
            });
        }
    }

    @Test
    public void testJoinWithAggPushDown_CountDistinct() {
        String mv = "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, count(distinct LO_REVENUE) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE";
        starRocksAssert.withMaterializedView(mv, () -> {
            {
                String query = "select LO_ORDERDATE, count(distinct LO_REVENUE) as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE";
                // TODO: It's safe to push down count(distinct) to mv only when join keys are uniqe constraint in this case.
                // TODO: support this if group by keys are equals to join keys
                sql(query).nonMatch("mv0");
            }
        });
    }

    @Test
    public void testJoinWithAggPushDown_RollupFunctions_CountDistinct() {
        // one query contains count(distinct) agg function, it can be rewritten.
        List<String> mvs = ImmutableList.of(
                "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL AS " +
                        "select LO_ORDERDATE, array_agg_distinct(LO_REVENUE) \n" +
                        "from lineorder l group by LO_ORDERDATE;",
                "CREATE MATERIALIZED VIEW mv2 REFRESH MANUAL as " +
                        "select LO_ORDERDATE, LO_REVENUE, sum(LO_QUANTITY) as quantity_sum\n" +
                        "from lineorder l group by LO_ORDERDATE,LO_REVENUE"
        );
        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            {
                String query = "select LO_ORDERDATE, count(distinct LO_REVENUE) " +
                        "from lineorder l join dates d " +
                        "on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;";
                sql(query).match("mv0");
            }
        });
    }

    @Test
    public void testJoinWithAggPushDown_NotSPJG() {
        String mv = "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE,lo_orderkey, sum(LO_REVENUE) as c\n" +
                "from lineorder l group by LO_ORDERDATE,lo_orderkey";
        starRocksAssert.withMaterializedView(mv, () -> {
            {
                String query = "select LO_ORDERDATE,lo_orderkey,sum(LO_REVENUE)\n" +
                        "from lineorder l " +
                        "where LO_ORDERDATE='2020-05-01' " +
                        "and lo_orderkey in (" +
                        "  select lo_orderkey from lineorder0 group by lo_orderkey" +
                        ") " +
                        "group by LO_ORDERDATE,lo_orderkey";
                sql(query).match("mv0");
            }
        });
    }

    @Test
    public void testJoinWithAggPushDown_NoGroupBy() {
        String mv = "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, LO_SUPPKEY, sum(LO_REVENUE) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE,LO_SUPPKEY";
        starRocksAssert.withMaterializedView(mv, () -> {
            {
                String query = "select sum(LO_REVENUE) as revenue_sum\n" +
                        "   from lineorder l join supplier s on l.lo_suppkey = s.s_suppkey";
                // TODO: It's safe to push down count(distinct) to mv only when join keys are unique constraint in this case.
                // TODO: support this if group by keys are equals to join keys
                sql(query).match("mv0");
            }
        });
    }

    @Test
    public void testJoinWithAggPushDown_BitmapUnion1() {
        String aggArg = "LO_REVENUE";
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, bitmap_union(to_bitmap(%s)) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE", aggArg);
        starRocksAssert.withMaterializedView(mv, () -> {
            String[] queries = new String[]{
                    // count distinct
                    String.format("select LO_ORDERDATE, count(distinct %s) as revenue_sum\n" +
                            "   from lineorder l group by LO_ORDERDATE", aggArg),
                    // mutli_distinct_count
                    String.format("select LO_ORDERDATE, multi_distinct_count(%s) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    // bitmap_union_count
                    String.format("select LO_ORDERDATE, bitmap_union_count(to_bitmap(%s)) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    // bitmap_union
                    String.format("select LO_ORDERDATE, bitmap_union(to_bitmap(%s)) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    // bitmap_agg
                    String.format("select LO_ORDERDATE, bitmap_agg(%s) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    // no need to push down
                    String.format("select LO_ORDERDATE, bitmap_union_count(revenue_sum) from (" +
                            "   select LO_ORDERDATE, bitmap_union(to_bitmap(%s)) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE) a group by LO_ORDERDATE", aggArg),
            };

            for (int i = 0; i < queries.length; i++) {
                String query = queries[i];
                sql(query).contains("mv0");
            }
        });
    }

    @Test
    public void testJoinWithAggPushDown_BitmapUnion2() {
        String aggArg = "(LO_REVENUE + 10) * 10";
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, bitmap_union(to_bitmap(%s)) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE", aggArg);
        starRocksAssert.withMaterializedView(mv, () -> {
            String[] queries = new String[]{
                    // count distinct
                    String.format("select LO_ORDERDATE, count(distinct %s) as revenue_sum\n" +
                            "   from lineorder l group by LO_ORDERDATE", aggArg),
                    // mutli_distinct_count
                    String.format("select LO_ORDERDATE, multi_distinct_count(%s) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    // bitmap_union_count
                    String.format("select LO_ORDERDATE, bitmap_union_count(to_bitmap(%s)) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    // bitmap_union
                    String.format("select LO_ORDERDATE, bitmap_union(to_bitmap(%s)) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    // bitmap_agg
                    String.format("select LO_ORDERDATE, bitmap_agg(%s) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    // no need to push down
                    String.format("select LO_ORDERDATE, bitmap_union_count(revenue_sum) from (" +
                            "   select LO_ORDERDATE, bitmap_union(to_bitmap(%s)) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE) a group by LO_ORDERDATE", aggArg),
            };

            for (int i = 0; i < queries.length; i++) {
                String query = queries[i];
                sql(query).contains("mv0");
            }
        });
    }

    @Test
    public void testAggPushDown_HLLUnion1() {
        String aggArg = "LO_REVENUE";
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, hll_union(hll_hash(%s)) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE", aggArg);

        starRocksAssert.withMaterializedView(mv, () -> {
            String[] queries = {
                    String.format("select LO_ORDERDATE, hll_union(hll_hash(%s)) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    String.format("select LO_ORDERDATE, approx_count_distinct(%s) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    String.format("select LO_ORDERDATE, ndv(%s) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    String.format("select LO_ORDERDATE, hll_union_agg(hll_hash(%s)) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
            };
            for (int i = 0; i < queries.length; i++) {
                String query = queries[i];
                sql(query).contains("mv0");
            }
        });
    }

    @Test
    public void testAggPushDown_HLLUnion2() {
        String aggArg = "case when (LO_REVENUE > 10) then 0 else LO_REVENUE end";
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, hll_union(hll_hash(%s)) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE", aggArg);

        starRocksAssert.withMaterializedView(mv, () -> {
            String[] queries = {
                    String.format("select LO_ORDERDATE, hll_union(hll_hash(%s)) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    String.format("select LO_ORDERDATE, approx_count_distinct(%s) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    String.format("select LO_ORDERDATE, ndv(%s) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    String.format("select LO_ORDERDATE, hll_union_agg(hll_hash(%s)) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
            };
            for (int i = 0; i < queries.length; i++) {
                String query = queries[i];
                sql(query).contains("mv0");
            }
        });
    }

    @Test
    public void testAggPushDown_ArrayAggDistinct1() {
        String aggArg = "LO_REVENUE";
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, array_agg_distinct(%s) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE", aggArg);

        starRocksAssert.withMaterializedView(mv, () -> {
            String[] queries = {
                    String.format("select LO_ORDERDATE, array_agg_distinct(%s) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    String.format("select LO_ORDERDATE, count(distinct %s) " +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    String.format("select LO_ORDERDATE, sum(distinct %s) " +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    String.format("select LO_ORDERDATE, multi_distinct_count(%s) " +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
            };
            for (int i = 0; i < queries.length; i++) {
                String query = queries[i];
                sql(query).contains("mv0");
            }
        });
    }

    @Test
    public void testAggPushDown_ArrayAggDistinct2() {
        String aggArg = "(LO_REVENUE * 2) + 1";
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, array_agg_distinct(%s) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE", aggArg);

        starRocksAssert.withMaterializedView(mv, () -> {
            String[] queries = {
                    String.format("select LO_ORDERDATE, array_agg_distinct(%s) as revenue_sum\n" +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    String.format("select LO_ORDERDATE, count(distinct %s) " +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    String.format("select LO_ORDERDATE, sum(distinct %s) " +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
                    String.format("select LO_ORDERDATE, multi_distinct_count(%s) " +
                            "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                            "   group by LO_ORDERDATE", aggArg),
            };
            for (int i = 0; i < queries.length; i++) {
                String query = queries[i];
                sql(query).contains("mv0");
            }
        });
    }

    @Test
    public void testAggPushDown_ArrayAgg() {
        // array_agg has no rollup function but array_agg(distinct) has.
        String funcName = "array_agg(LO_REVENUE)";
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, %s as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE", funcName);
        String aggFuncName = funcName;
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = String.format("select LO_ORDERDATE, %s as revenue_sum\n" +
                    "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                    "   group by LO_ORDERDATE", aggFuncName);
            sql(query).nonMatch("mv0");
        });
    }

    @Test
    public void testJoinWithAggPushDown_PercentileUnion() {
        String mv = "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, percentile_union(percentile_hash(LO_REVENUE)) as revenue_sum\n" +
                "from lineorder l group by LO_ORDERDATE";
        starRocksAssert.withMaterializedView(mv, () -> {
            {
                String[] queries = {
                        "select LO_ORDERDATE, percentile_union(percentile_hash(LO_REVENUE)) as revenue_sum\n" +
                                "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                                "   group by LO_ORDERDATE",
                        "select LO_ORDERDATE, PERCENTILE_APPROX(LO_REVENUE, 0.11) as revenue_sum\n" +
                                "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                                "   group by LO_ORDERDATE",
                };
                for (int i = 0; i < queries.length; i++) {
                    String query = queries[i];
                    sql(query).contains("mv0");
                }
            }
        });
    }

    @Test
    public void testAggPushDown_ComplexCase1() {
        String mv = "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL AS\n" +
                "select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union(to_bitmap(LO_REVENUE)), " +
                "   hll_union(hll_hash(LO_REVENUE)), percentile_union(percentile_hash(LO_REVENUE)), any_value(LO_REVENUE), " +
                "   bitmap_agg(LO_REVENUE), array_agg_distinct(LO_REVENUE) \n" +
                "from lineorder l group by LO_ORDERDATE;";
        starRocksAssert.withMaterializedView(mv, () -> {
            String[] queries = {"select LO_ORDERDATE, \n" +
                    "    sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), \n" +
                    "    bitmap_union_count(to_bitmap(LO_REVENUE)), \n" +
                    "    approx_count_distinct(LO_REVENUE), \n" +
                    "    PERCENTILE_APPROX(LO_REVENUE, 0.5), \n" +
                    "    PERCENTILE_APPROX(LO_REVENUE, 0.7), \n" +
                    "    sum(distinct LO_REVENUE) ,\n" +
                    "    count(distinct LO_REVENUE) \n" +
                    "from lineorder l join dates d on l.LO_ORDERDATE = d.D_DATE\n" +
                    "group by LO_ORDERDATE",
            };
            for (int i = 0; i < queries.length; i++) {
                String query = queries[i];
                sql(query).contains("mv0");
            }
        });
    }

    @Test
    public void testAggPushDown_ComplexCase2() {
        String mv = "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL AS\n" +
                "select LO_ORDERDATE, array_agg_distinct(LO_REVENUE) \n" +
                "from lineorder l group by LO_ORDERDATE;";
        starRocksAssert.withMaterializedView(mv, () -> {
            String[] queries = {"select LO_ORDERDATE, \n" +
                    "    sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), \n" +
                    "    bitmap_union_count(to_bitmap(LO_REVENUE)), \n" +
                    "    approx_count_distinct(LO_REVENUE), \n" +
                    "    PERCENTILE_APPROX(LO_REVENUE, 0.5), \n" +
                    "    PERCENTILE_APPROX(LO_REVENUE, 0.7), \n" +
                    "    sum(distinct LO_REVENUE) ,\n" +
                    "    count(distinct LO_REVENUE) \n" +
                    "from lineorder l join dates d on l.LO_ORDERDATE = d.D_DATE\n" +
                    "group by LO_ORDERDATE",
            };
            for (int i = 0; i < queries.length; i++) {
                String query = queries[i];
                sql(query).nonMatch("mv0");
            }
        });
    }

    @Test
    public void testAggPushDown_ComplexCase3() {
        List<String> mvs = ImmutableList.of(
                "CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL AS\n" +
                        "select LO_ORDERDATE, array_agg_distinct(LO_REVENUE) \n" +
                        "from lineorder l group by LO_ORDERDATE;",
                "CREATE MATERIALIZED VIEW mv1 REFRESH MANUAL AS\n" +
                        "select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union(to_bitmap(LO_REVENUE)), " +
                        "   hll_union(hll_hash(LO_REVENUE)), percentile_union(percentile_hash(LO_REVENUE)), any_value(LO_REVENUE), " +
                        "   bitmap_agg(LO_REVENUE), array_agg_distinct(LO_REVENUE) \n" +
                        "from lineorder l group by LO_ORDERDATE;"
        );
        starRocksAssert.withMaterializedViews(mvs, (obj) -> {
            try {
                String[] queries = {"select LO_ORDERDATE, \n" +
                        "    sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), \n" +
                        "    bitmap_union_count(to_bitmap(LO_REVENUE)), \n" +
                        "    approx_count_distinct(LO_REVENUE), \n" +
                        "    PERCENTILE_APPROX(LO_REVENUE, 0.5), \n" +
                        "    PERCENTILE_APPROX(LO_REVENUE, 0.7), \n" +
                        "    sum(distinct LO_REVENUE) ,\n" +
                        "    count(distinct LO_REVENUE) \n" +
                        "from lineorder l join dates d on l.LO_ORDERDATE = d.D_DATE\n" +
                        "group by LO_ORDERDATE",
                };
                for (int i = 0; i < queries.length; i++) {
                    String query = queries[i];
                    sql(query).contains("mv1");
                }
            } catch (Exception e) {
                Assert.fail();
            }
        });
    }

    @Test
    public void testAggPushDown_RollupFunctions_WithMultiGroupByKeys() {
        String mvAggArg = "LO_REVENUE";
        String queryAggArg = "LO_REVENUE";
        for (Map.Entry<String, String> e : REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String queryAggFunc = getAggFunction(funcName, queryAggArg);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, %s as revenue_sum\n" +
                    "from lineorder l group by LO_ORDERDATE", mvAggFunc);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, d.d_date, %s as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE, d.d_date", queryAggFunc);
                sql(query).contains("mv0");
            });
        }
    }

    @Test
    public void testAggPushDown_RollupFunctions_WithPredicates() {
        String mvAggArg = "LO_REVENUE";
        String queryAggArg = "LO_REVENUE";
        for (Map.Entry<String, String> e : REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String queryAggFunc = getAggFunction(funcName, queryAggArg);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, %s as revenue_sum\n" +
                    "from lineorder l group by LO_ORDERDATE", mvAggFunc);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, d.d_date, %s as revenue_sum\n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   where d.d_date in ('2024-05-27') group by LO_ORDERDATE, d.d_date", queryAggFunc);
                sql(query).contains("mv0");
            });
        }
    }

    @Test
    public void testAggPushDown_WithoutFunctions_WithMultiGroupByKeys() {
        String mvAggArg = "LO_REVENUE";
        String queryAggArg = "LO_REVENUE";
        for (Map.Entry<String, String> e : REWRITE_ROLLUP_FUNCTION_MAP.entrySet()) {
            String funcName = e.getKey();
            String mvAggFunc = getAggFunction(funcName, mvAggArg);
            String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                    "select LO_ORDERDATE, %s as revenue_sum\n" +
                    "from lineorder l group by LO_ORDERDATE", mvAggFunc);
            starRocksAssert.withMaterializedView(mv, () -> {
                String query = String.format("select LO_ORDERDATE, d.d_date \n" +
                        "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                        "   group by LO_ORDERDATE, d.d_date");
                sql(query).contains("mv0");
            });
        }
    }

    @Test
    public void testAggPushDown_RollupFunctions_Avg() {
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, sum(LO_REVENUE) as revenue_sum, count(LO_REVENUE) as revenue_cnt \n" +
                "from lineorder l group by LO_ORDERDATE");
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = String.format("select LO_ORDERDATE, avg(LO_REVENUE) as revenue_sum\n" +
                    "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                    "   group by LO_ORDERDATE");
            sql(query).contains("mv0");
        });
    }

    @Test
    public void testAggPushDown_RollupFunctions_MultiAvgs1() {
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, sum(LO_REVENUE) as revenue_sum, count(LO_REVENUE) as revenue_cnt \n" +
                "from lineorder l group by LO_ORDERDATE");
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = String.format("select LO_ORDERDATE, avg(LO_REVENUE) as avg1, avg(LO_REVENUE) as avg2\n" +
                    "   from lineorder l join dates d on l.LO_ORDERDATE = d.d_datekey\n" +
                    "   group by LO_ORDERDATE");
            sql(query).contains("mv0");
        });
    }

    @Test
    public void testAggPushDown_RollupFunctions_MultiAvgs2() {
        String mv = String.format("CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL as " +
                "select LO_ORDERDATE, sum(LO_REVENUE) as revenue_sum, count(LO_REVENUE) as revenue_cnt,\n" +
                " sum(lo_custkey) as sum_lo_custkey, count(lo_custkey) as count_lo_custkey\n" +
                "from lineorder l group by LO_ORDERDATE");
        starRocksAssert.withMaterializedView(mv, () -> {
            String query = String.format("select LO_ORDERDATE, sum(LO_REVENUE), avg(LO_REVENUE) as avg1, " +
                    "avg(LO_REVENUE) as avg2, avg(lo_custkey) as avg3 from lineorder l \n" +
                    "join dates d on l.LO_ORDERDATE = d.d_datekey group by LO_ORDERDATE");
            sql(query).contains("mv0");
        });
    }

    @Test
    public void testAggPushDownWithDecimalTypes() throws Exception {
        String tbl1 = "CREATE TABLE `test_pt8` (\n" +
                "  `id` bigint(20) NULL,\n" +
                "  `pt` date NOT NULL,\n" +
                "  `gmv` bigint(20) NULL,\n" +
                "  `gmv2` bigint(20) NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY date_trunc('day', pt)\n" +
                "DISTRIBUTED BY HASH(`pt`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        String tbl2 = "CREATE TABLE `test_pt9` (\n" +
                "  `id` bigint(20) NULL COMMENT \"id\",\n" +
                "  `pt` date NOT NULL,\n" +
                "  `name` varchar(20) NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "PARTITION BY date_trunc('day', pt)\n" +
                "DISTRIBUTED BY HASH(`pt`)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");\n" ;
        starRocksAssert.withTable(tbl1);
        starRocksAssert.withTable(tbl2);
        String mv = "CREATE MATERIALIZED VIEW `test_pt8_mv` \n" +
                "PARTITION BY (`pt`)\n" +
                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                "REFRESH ASYNC START(\"2024-11-22 17:34:45\") EVERY(INTERVAL 1 MINUTE)\n" +
                "PROPERTIES (\n" +
                "\"query_rewrite_consistency\" = \"LOOSE\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n" +
                "AS\n" +
                "SELECT  `id`,`pt`,SUM((`gmv` + `gmv2`) * 0.01) AS `sum_channel_direct_indirect_gmv`\n" +
                "FROM `test_pt8` GROUP BY  `id`,`pt`;\n";
        starRocksAssert.withRefreshedMaterializedView(mv);
        String query = "SELECT SUM((gmv+gmv2)*0.01)\n" +
                "FROM test_pt8 WHERE pt = '20241126' AND id IN ( SELECT id FROM test_pt9 WHERE id = '1' )";
        String plan = getQueryPlan(query, TExplainLevel.VERBOSE);
        System.out.println(plan);
        starRocksAssert.dropTable("test_pt8");
        starRocksAssert.dropTable("test_pt9");
        starRocksAssert.dropMaterializedView("test_pt8_mv");
    }
}
