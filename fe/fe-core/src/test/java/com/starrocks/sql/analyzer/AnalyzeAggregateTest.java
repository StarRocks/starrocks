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

package com.starrocks.sql.analyzer;

import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeAggregateTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testAggregate() {
        analyzeFail("select v1 from t0 where abs(sum(v2)) = 2;",
                "WHERE clause cannot contain aggregations");
        analyzeFail("select sum(v1) from t0 order by sum(max(v2) over ())",
                "Unsupported nest window function inside aggregation.");
        analyzeFail("select sum(v1) from t0 order by sum(abs(max(v2) over ()))",
                "Unsupported nest window function inside aggregation.");
        analyzeFail("select sum(v1) from t0 order by sum(max(v2))",
                "Unsupported nest aggregation function inside aggregation.");
        analyzeFail("select sum(v1) from t0 order by sum(abs(max(v2)))",
                "Unsupported nest aggregation function inside aggregation.");
        analyzeFail("select sum(max(v2)) from t0",
                "Unsupported nest aggregation function inside aggregation.");
        analyzeFail("select sum(1 + max(v2)) from t0",
                "Unsupported nest aggregation function inside aggregation.");
        analyzeFail("select min(v1) col from t0 order by min(col) + 1,  min(col)",
                "Column 'col' cannot be resolved");

        analyzeFail("select v1 from t0 group by v1,cast(v2 as int) having cast(v2 as boolean)",
                "must be an aggregate expression or appear in GROUP BY clause");
        analyzeSuccess("select abs(count(distinct b1)) from test_object;");
        analyzeSuccess("select date_add(ti, INTERVAL 2 DAY) from tall group by date_add(ti, INTERVAL 2 DAY)");

        analyzeSuccess("select v1,count(*) FROM t0 group by v1 having array_position([1,2,3,4],v1) = 1");
        analyzeSuccess("select v1,count(*) FROM t0 group by v1 having array_position([1,2,3,v1],4) = 1");
        analyzeFail("select v1,count(*) FROM t0 group by v1 having array_position([1,2,3,4],v2) = 1");
        analyzeFail("select v1,count(*) FROM t0 group by v1 having array_position([v1,v2,v3],v1) = 1");
        analyzeSuccess("select v1,count(*) FROM t0 group by v1 having array_position([1,2,3,4],sum(v2)) = 1");
        analyzeSuccess("select v1 from t0 group by v1 having parse_json('{\"a\": 1}')->'a'=1");
        analyzeSuccess("select ta,tc from tall group by ta,tc having ta = @@sql_mode");
        analyzeSuccess("select ta,tc from tall group by ta,tc having ta = user()");

        analyzeSuccess("select count() from t0");

        analyzeSuccess("select max_by(v1,v2) from t0");
        analyzeFail("select max_by(v1) from t0", "No matching function with signature: max_by(bigint(20)).");
        analyzeFail("select max_by(v1,v2,v3) from t0",
                "No matching function with signature: max_by(bigint(20), bigint(20), bigint(20)).");
        analyzeFail("select max_by(v1,1) from t0", "max_by function args must be column");
        analyzeFail("select max_by(1,v1) from t0", "max_by function args must be column");

        analyzeSuccess("select min_by(v1,v2) from t0");
        analyzeFail("select min_by(v1) from t0", "No matching function with signature: min_by(bigint(20)).");
        analyzeFail("select min_by(v1,v2,v3) from t0",
                "No matching function with signature: min_by(bigint(20), bigint(20), bigint(20)).");
        analyzeFail("select min_by(v1,1) from t0", "min_by function args must be column");
        analyzeFail("select min_by(1,v1) from t0", "min_by function args must be column");
    }

    @Test
    public void testGrouping() {
        analyzeFail("select grouping(foo) from t0 group by grouping sets((v1), (v2))",
                "cannot be resolved");

        //The arguments to GROUPING must be expressions referenced by GROUP BY
        analyzeFail("select grouping(v3) from t0 group by grouping sets((v1), (v2))",
                "The arguments of GROUPING must be expressions referenced by GROUP BY");

        //Grouping operations are not allowed in order by
        analyzeFail("select v1 from t0 group by v1 order by grouping(v1)",
                "ORDER BY clause cannot contain grouping");

        //cannot use GROUPING functions without [grouping sets|rollup|cube] clause
        analyzeFail("select grouping(v1) from t0",
                "cannot use GROUPING functions without [grouping sets|rollup|cube] clause");
        analyzeFail("select grouping(v1), sum(v2) from t0",
                "cannot use GROUPING functions without [grouping sets|rollup|cube] clause");
        analyzeFail("select grouping(v1) from t0 group by v1",
                "cannot use GROUPING functions without [grouping sets|rollup|cube] clause");

        //grouping functions only support column.
        analyzeFail("select v1, grouping(v1+1) from t0 group by grouping sets((v1))",
                "grouping functions only support column.");

        //cannot contain grouping
        analyzeFail("select v1 from t0 inner join t1 on grouping(v1)= v4", "JOIN clause cannot contain grouping");
        analyzeFail("select v1 from t0 where grouping(v1) = 1", "WHERE clause cannot contain grouping");
        analyzeFail("select v1 from t0 where abs(grouping(v1)) = 1", "WHERE clause cannot contain grouping");
        analyzeFail("select v1 from t0 group by grouping(v1)", "GROUP BY clause cannot contain grouping");
        analyzeFail(" select sum(v1) from t0 group by grouping sets((v1),(v2)) having grouping(v2) = 1",
                "HAVING clause cannot contain grouping");
    }

    @Test
    public void testAggInSort() {
        analyzeSuccess("SELECT max(v1) FROM t0 WHERE true ORDER BY sum(1)");
        analyzeSuccess("SELECT v1 FROM t0 group by v1 ORDER BY sum(1)");
        analyzeFail("SELECT 1 FROM t0 WHERE true ORDER BY sum(1)",
                "ORDER BY contains aggregate function and applies to the result of a non-aggregated query");
        analyzeFail("SELECT v1 FROM t0 WHERE true ORDER BY sum(1)",
                "must be an aggregate expression or appear in GROUP BY clause");
    }

    @Test
    public void testDistinct() {
        analyzeSuccess("select distinct v1, v2 from t0 order by v1");
        analyzeSuccess("select distinct v1, v2 as v from t0 order by v");
        analyzeSuccess("select distinct abs(v1) as v from t0 order by v");
        analyzeFail("select distinct v1 from t0 order by v2",
                "must be an aggregate expression or appear in GROUP BY clause");
        analyzeFail("select distinct v1 as v from t0 order by v2",
                " must be an aggregate expression or appear in GROUP BY clause");
        analyzeFail("select * from t0 order by max(v2)",
                "column must appear in the GROUP BY clause or be used in an aggregate function.");
        analyzeFail("select distinct max(v1) from t0",
                "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
        analyzeFail("select distinct abs(v1) from t0 order by max(v1)",
                "for SELECT DISTINCT, ORDER BY expressions must appear in select list");
        analyzeFail("select distinct abs(v1) from t0 order by max(v2)",
                "for SELECT DISTINCT, ORDER BY expressions must appear in select list");

        analyzeSuccess("select distinct v1 as v from t0 having v = 1");
        analyzeFail("select distinct v1 as v from t0 having v2 = 2",
                "must be an aggregate expression or appear in GROUP BY clause");

        analyzeFail("select distinct v1,sum(v2) from t0",
                "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");
        analyzeFail("select distinct v2 from t0 group by v1,v2",
                "cannot combine SELECT DISTINCT with aggregate functions or GROUP BY");

        analyzeSuccess("select distinct v1, v2 from t0");
        analyzeFail("select v2, distinct v1 from t0");
        analyzeSuccess("select distinct abs(v1) from t0");
        analyzeSuccess("select distinct v1 from t0 order by v1");
        analyzeSuccess("select distinct v1, v2 from t0 order by v2");
        analyzeSuccess("select distinct v1 from t0 where v1 = 1");
        analyzeSuccess("select distinct v1 from t0 having v1 = 1");
        analyzeSuccess("select distinct v1 from t0 where v2 = 1");
        analyzeFail("select distinct v1,v2 from t0 order by v3");
        analyzeFail("select distinct v1 from t0 order by sum(v2)");

        analyzeSuccess("select count(distinct v1), count(distinct v3) from tarray");

        analyzeFail("select abs(distinct v1) from t0");
        analyzeFail("SELECT VAR_SAMP ( DISTINCT v2 ) FROM v0");
        analyzeFail("select distinct v1 from t0 having sum(v1) > 2");
    }
    @Test
    public void testDistinctAggOnComplexTypes() {
        analyzeSuccess("select count(distinct va) from ttypes group by v1");
        analyzeSuccess("select count(*) from ttypes group by va");

        // more than one count distinct
        analyzeSuccess("select count(distinct va), count(distinct va1) from ttypes group by v1");
        analyzeSuccess("select count(distinct va), count(distinct va1) from ttypes");
        analyzeSuccess("select count(distinct vm), count(distinct vm1) from ttypes group by v1");
        analyzeSuccess("select count(distinct vm), count(distinct vm1) from ttypes");
        analyzeSuccess("select count(distinct vs), count(distinct vs1) from ttypes group by v1");
        analyzeSuccess("select count(distinct vs), count(distinct vs1) from ttypes");
        analyzeFail("select count(distinct vj), count(distinct vj1) from ttypes group by v1");
        analyzeFail("select count(distinct vj), count(distinct vj1) from ttypes");

        // single count distinct
        analyzeSuccess("select count(distinct vm) from ttypes");
        analyzeSuccess("select count(distinct vs) from ttypes");
        analyzeFail("select count(distinct vj) from ttypes");
        analyzeSuccess("select count(distinct vm) from ttypes group by v1");
        analyzeSuccess("select count(distinct vs) from ttypes group by v1");
        analyzeFail("select count(distinct vj) from ttypes group by v1");
        analyzeSuccess("select count(distinct va),count(distinct vm) from ttypes group by v1");

        // group by complex types
        analyzeSuccess("select count(*) from ttypes group by vm");
        analyzeSuccess("select count(*) from ttypes group by vs");
        analyzeFail("select count(*) from ttypes group by vj");
    }

    @Test
    public void testGroupByUseOutput() {
        analyzeSuccess("select v1 + 1 as v from t0 group by v");
        analyzeSuccess("select v1 + 1 as v from t0 group by grouping sets((v))");
        analyzeSuccess("select v1 + 1 as v from t0 group by cube(v)");
        analyzeSuccess("select v1 + 1 as v from t0 group by rollup(v)");
    }

    @Test
    public void testForQualifiedName() {
        QueryRelation query = ((QueryStatement) analyzeSuccess("select grouping_id(t0.v1, t0.v3), " +
                "grouping(t0.v2) from t0 group by cube(t0.v1, t0.v2, t0.v3);"))
                .getQueryRelation();
        Assert.assertEquals("grouping(t0.v1, t0.v3), grouping(t0.v2)",
                String.join(", ", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess(
                "select grouping_id(test.t0.v1, test.t0.v3), grouping(test.t0.v2) from t0 " +
                        "group by cube(test.t0.v1, test.t0.v2, test.t0.v3);"))
                .getQueryRelation();
        Assert.assertEquals("grouping(test.t0.v1, test.t0.v3), grouping(test.t0.v2)",
                String.join(", ", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select grouping(t0.v1), grouping(t0.v2), grouping_id(t0.v1,t0.v2), " +
                "v1,v2 from t0 group by grouping sets((t0.v1,t0.v2),(t0.v1),(t0.v2))"))
                .getQueryRelation();
        Assert.assertEquals("grouping(t0.v1), grouping(t0.v2), grouping(t0.v1, t0.v2), v1, v2",
                String.join(", ", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select grouping(test.t0.v1), grouping(test.t0.v2), " +
                "grouping_id(test.t0.v1,test.t0.v2), v1,v2 from t0 " +
                "group by grouping sets((test.t0.v1,test.t0.v2),(test.t0.v1),(test.t0.v2))"))
                .getQueryRelation();
        Assert.assertEquals("grouping(test.t0.v1), grouping(test.t0.v2), grouping(test.t0.v1, test.t0.v2), v1, v2",
                String.join(", ", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select t0.v1, t0.v2, grouping_id(t0.v1, t0.v2), " +
                "SUM(t0.v3) from t0 group by cube(t0.v1, t0.v2)"))
                .getQueryRelation();
        Assert.assertEquals("v1, v2, grouping(t0.v1, t0.v2), sum(t0.v3)",
                String.join(", ", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess(
                "select test.t0.v1, test.t0.v2, grouping_id(test.t0.v1, test.t0.v2), " +
                        "SUM(test.t0.v3) from t0 group by cube(test.t0.v1, test.t0.v2)"))
                .getQueryRelation();
        Assert.assertEquals("v1, v2, grouping(test.t0.v1, test.t0.v2), sum(test.t0.v3)",
                String.join(", ", query.getColumnOutputNames()));

        query = ((QueryStatement) analyzeSuccess("select grouping(v1), grouping(v2), grouping_id(v1,v2), " +
                "v1,v2 from t0 group by grouping sets((v1,v2),(v1),(v2))"))
                .getQueryRelation();
        Assert.assertEquals("grouping(v1), grouping(v2), grouping(v1, v2), v1, v2",
                String.join(", ", query.getColumnOutputNames()));
    }

    @Test
    public void testAnyValueFunction() {
        analyzeSuccess("select v1, any_value(v2) from t0 group by v1");
    }

    @Test
    public void testPercentileFunction() {
        analyzeFail("select percentile_approx(0.5) from tall group by tb");
        analyzeFail("select percentile_approx('c',0.5) from tall group by tb");
        analyzeFail("select percentile_approx(1,'c') from tall group by tb");
        analyzeFail("select percentile_approx(1,1,'c') from tall group by tb");
        analyzeFail("select percentile_approx(1,1,tc) from tall group by tb");
        analyzeFail("select percentile_approx(1,1,0.5,tc) from tall group by tb");
        analyzeSuccess("select percentile_approx(1,5) from tall group by tb");
        analyzeSuccess("select percentile_approx(1,0.5,1047) from tall group by tb");
        analyzeSuccess("select percentile_disc(tj,0.5) from tall group by tb");
    }

    @Test
    public void testWindowFunnelFunction() {
        // For the argument `window_size`.
        analyzeFail("SELECT window_funnel(-1, ti, 0, [ta='a', ta='b']) FROM tall",
                "window argument must >= 0");
        analyzeFail("SELECT window_funnel('VARCHAR', ti, 0, [ta='a', ta='b']) FROM tall",
                "window argument must be numerical type");
        analyzeFail("SELECT window_funnel(tc, ti, 0, [ta='a', ta='b']) FROM tall",
                "window argument must be numerical type");

        // For the argument `mode`.
        analyzeFail("SELECT window_funnel(1, ti, -1, [ta='a', ta='b']) FROM tall",
                "mode argument's range must be [0-7]");
        analyzeFail("SELECT window_funnel(1, ti, 8, [ta='a', ta='b']) FROM tall",
                "mode argument's range must be [0-7]");
        analyzeFail("SELECT window_funnel(1, ti, 'VARCHAR', [ta='a', ta='b']) FROM tall",
                "mode argument must be numerical type");
        analyzeFail("SELECT window_funnel(1, ti, ta, [ta='a', ta='b']) FROM tall",
                "mode argument must be numerical type");

        // For the argument `time`.
        analyzeFail("SELECT window_funnel(1, '2022-01-01', 0, [ta='a', ta='b']) FROM tall",
                "time arg must be column");

        // For the argument `condition`.
        analyzeFail("SELECT window_funnel(1, ti, 0, ti) FROM tall",
                "No matching function with signature");

        // Successful statements.
        analyzeSuccess("SELECT window_funnel(0, ti, 0, [ta='a', ta='b']) FROM tall");
        analyzeSuccess("SELECT window_funnel(1, ti, 0, [ta='a', ta='b']) FROM tall");
        analyzeSuccess("SELECT window_funnel(1, ta, 0, [ta='a', ta='b']) FROM tall");
        analyzeSuccess("SELECT window_funnel(1, ta, 0, [true, true, false]) FROM tall");
    }
}
