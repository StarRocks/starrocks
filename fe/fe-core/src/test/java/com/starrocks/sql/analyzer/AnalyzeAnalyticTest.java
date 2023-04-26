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

import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeAnalyticTest {    // use a unique dir so that it won't be conflict with other unit test which

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testSingle() {
        analyzeFail("select sum(v1) over(partition by v2 rows between 1 preceding and 2 following) from t0",
                "Windowing clause requires ORDER BY clause");
        analyzeFail("select count(distinct v1) over() from t0", "DISTINCT not allowed in analytic function");

        analyzeFail("select abs(v1) over() from t0", "not supported with OVER clause");

        analyzeFail(
                "select sum(v1) over(partition by v2 order by v2 rows between v2+1 preceding and 2 following) from t0",
                "must be a constant positive integer");

        analyzeFail("select sum(v1) from t0 having sum(v2) over() > 0", "HAVING clause cannot contain window function");

        analyzeFail("select sum(v2) over() from t0 group by v3",
                "must be an aggregate expression or appear in GROUP BY clause");
        analyzeFail("select sum(v3) over(partition by v2) from t0 group by v3",
                "must be an aggregate expression or appear in GROUP BY clause");
        analyzeFail("select sum(v3) over(order by v2) from t0 group by v3",
                "must be an aggregate expression or appear in GROUP BY clause");
    }

    @Test
    public void testRange() {
        analyzeFail("select sum(v1) " +
                        "over(partition by v2 order by v3 range between 1 preceding and unbounded following) from t0",
                "RANGE is only supported with both the lower " +
                        "and upper bounds UNBOUNDED or one UNBOUNDED and the other CURRENT ROW");

        analyzeFail("select sum(v1) " +
                        "over(partition by v2 order by v3 range between unbounded preceding and 1 following) from t0",
                "RANGE is only supported with both the lower " +
                        "and upper bounds UNBOUNDED or one UNBOUNDED and the other CURRENT ROW");

        analyzeFail("select sum(v1) " +
                        "over(partition by v2 order by v3 range between current row and current row) from t0",
                "RANGE is only supported with both the lower " +
                        "and upper bounds UNBOUNDED or one UNBOUNDED and the other CURRENT ROW");

        analyzeFail("select sum(v1) over(partition by v2 order by v3 range unbounded following) from t0",
                "UNBOUNDED FOLLOWING is only allowed for upper bound of BETWEEN");

        analyzeSuccess("select sum(v1) " +
                "over(partition by v2 order by v3 range between unbounded preceding and unbounded following) from t0");
        analyzeSuccess("select sum(v1) " +
                "over(partition by v2 order by v3 range between current row and unbounded following) from t0");
        analyzeSuccess("select sum(v1) " +
                "over(partition by v2 order by v3 range between unbounded preceding and current row) from t0");
        analyzeSuccess("select sum(v1) over(partition by v2 order by v3 range unbounded preceding) from t0");
    }

    @Test
    public void testWindowCannotContain() {
        analyzeFail("select v1 from t0 inner join t1 on sum(v1) over() = v4",
                "JOIN clause cannot contain window function");
        analyzeFail("select v1 from t0 where sum(v1) over() = 1", "WHERE clause cannot contain window function");
        analyzeFail("select v1 from t0 group by sum(v1) over()", "GROUP BY clause cannot contain window function");
    }
}
