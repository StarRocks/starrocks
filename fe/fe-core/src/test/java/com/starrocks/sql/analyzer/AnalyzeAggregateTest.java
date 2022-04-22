// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeAggregateTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void TestAggregate() {
        analyzeFail("select v1 from t0 where abs(sum(v2)) = 2;",
                "WHERE clause cannot contain aggregations");
        analyzeFail("select sum(v1) from t0 order by sum(max(v2) over ())",
                "Cannot nest window function inside aggregation");
        analyzeFail("select sum(v1) from t0 order by sum(abs(max(v2) over ()))",
                "Cannot nest window function inside aggregation");
        analyzeFail("select sum(v1) from t0 order by sum(max(v2))",
                "Cannot nest aggregations inside aggregation");
        analyzeFail("select sum(v1) from t0 order by sum(abs(max(v2)))",
                "Cannot nest aggregations inside aggregation");
        analyzeFail("select sum(max(v2)) from t0",
                "Cannot nest aggregations inside aggregation");
        analyzeFail("select sum(1 + max(v2)) from t0",
                "Cannot nest aggregations inside aggregation");

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
    }

    @Test
    public void TestGrouping() {
        analyzeFail("select grouping(foo) from t0 group by grouping sets((v1), (v2))",
                "cannot be resolved");

        //The arguments to GROUPING must be expressions referenced by GROUP BY
        analyzeFail("select grouping(v3) from t0 group by grouping sets((v1), (v2))",
                "The arguments to GROUPING must be expressions referenced by GROUP BY");

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
    public void TestAggInSort() {
        analyzeSuccess("SELECT max(v1) FROM t0 WHERE true ORDER BY sum(1)");
        analyzeSuccess("SELECT v1 FROM t0 group by v1 ORDER BY sum(1)");
        analyzeFail("SELECT 1 FROM t0 WHERE true ORDER BY sum(1)",
                "ORDER BY contains aggregate function and applies to the result of a non-aggregated query");
        analyzeFail("SELECT v1 FROM t0 WHERE true ORDER BY sum(1)",
                "must be an aggregate expression or appear in GROUP BY clause");
    }

    @Test
    public void TestDistinct() {
        analyzeSuccess("select distinct v1, v2 from t0 order by v1");
        analyzeSuccess("select distinct v1, v2 as v from t0 order by v");
        analyzeSuccess("select distinct abs(v1) as v from t0 order by v");
        analyzeFail("select distinct v1 from t0 order by v2",
                "must be an aggregate expression or appear in GROUP BY clause");
        analyzeFail("select distinct v1 as v from t0 order by v2",
                "must be an aggregate expression or appear in GROUP BY clause");

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
        analyzeSuccess("select distinct v1 from t0 order by sum(v2)");

        analyzeFail("select count(distinct v1), count(distinct v3) from tarray",
                "No matching function with signature: multi_distinct_count(ARRAY)");

        analyzeFail("select abs(distinct v1) from t0");
        analyzeFail("SELECT VAR_SAMP ( DISTINCT v2 ) FROM v0");
    }

    @Test
    public void TestGroupByUseOutput() {
        analyzeSuccess("select v1 + 1 as v from t0 group by v");
        analyzeSuccess("select v1 + 1 as v from t0 group by grouping sets((v))");
        analyzeSuccess("select v1 + 1 as v from t0 group by cube(v)");
        analyzeSuccess("select v1 + 1 as v from t0 group by rollup(v)");
    }
}
