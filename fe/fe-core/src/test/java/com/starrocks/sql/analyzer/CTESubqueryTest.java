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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

/**
 * Test CTE used in subqueries (IN, EXISTS, scalar subquery)
 */
public class CTESubqueryTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @Test
    public void testCTEInWhereSubquery() {
        // Basic CTE with IN subquery - this should work
        analyzeSuccess("with a1 as (select v1 from t0) select v1 from t0 where v1 in (select v1 from a1)");

        // CTE with IN subquery referencing CTE
        analyzeSuccess("with a1 as (select v1 as cust_no from t0) " +
                "select v1 from t0 where v1 in (select cust_no from a1)");

        // Multiple CTEs with IN subquery
        analyzeSuccess("with a1 as (select v1 from t0), a2 as (select v2 from t0) " +
                "select v1 from t0 where v1 in (select v1 from a1) and v2 in (select v2 from a2)");

        // CTE used in both main query and subquery
        analyzeSuccess("with a1 as (select v1, v2 from t0) " +
                "select * from t0 join t1 on t0.v2=t1.v4 and t0.v1 in (select v1 from a1)");
    }

    @Test
    public void testCTEInExistsSubquery() {
        // CTE with EXISTS subquery
        analyzeSuccess("with a1 as (select v1 from t0) " +
                "select v1 from t0 where exists (select 1 from a1 where a1.v1 = t0.v1)");

        // Multiple CTEs with correlated EXISTS
        analyzeSuccess("with a1 as (select v1, v2 from t0), a2 as (select v1 from t0) " +
                "select * from a1 where exists (select 1 from a2 where a2.v1 = a1.v1)");
    }

    @Test
    public void testCTEInScalarSubquery() {
        // CTE with scalar subquery
        analyzeSuccess("with a1 as (select max(v1) as max_v1 from t0) " +
                "select v1 from t0 where v1 = (select max_v1 from a1)");
    }

    @Test
    public void testCTEInJoinOnClause() {
        // CTE used in ON clause subquery
        analyzeSuccess("with a1 as (select v1 from t0) " +
                "select t0.v1 from t0 join t1 on t0.v1 = t1.v4 " +
                "and t0.v1 in (select v1 from a1)");
    }

    @Test
    public void testNestedCTEInSubquery() {
        // Nested subquery with CTE
        analyzeSuccess("with a1 as (select v1 from t0) " +
                "select v1 from t0 where v1 in (select v1 from (select v1 from a1) t)");

        // CTE with nested IN subqueries
        analyzeSuccess("with a1 as (select v1 from t0), a2 as (select v1 from t0) " +
                "select v1 from t0 where v1 in (select v1 from a1 where v1 in (select v1 from a2))");
    }

    @Test
    public void testCTEWithJoinInSubquery() {
        // Simulate the original issue case - CTE with JOIN and IN subquery
        analyzeSuccess("with a1 as (select t0.v1 as cust_no from t0 join t1 on t0.v1 = t1.v4) " +
                "select t0.v1 from t0 join t1 on t0.v1 = t1.v4 " +
                "where t0.v1 in (select cust_no from a1)");
    }
}
