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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ADBCScanPlanTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();
    }

    @Test
    public void testADBCSelectAll() throws Exception {
        String sql = "select a, b, c, d from adbc0.test_db0.tbl0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "TABLE: \"test_db0\".\"tbl0\"");
        assertContains(plan, "QUERY: SELECT \"a\", \"b\", \"c\", \"d\"");
    }

    @Test
    public void testADBCColumnPruning() throws Exception {
        String sql = "select a, c from adbc0.test_db0.tbl0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "QUERY: SELECT \"a\", \"c\"");
    }

    @Test
    public void testADBCPredicatePushdown() throws Exception {
        String sql = "select a from adbc0.test_db0.tbl0 where c > 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "WHERE");
    }

    @Test
    public void testADBCLimitPushdown() throws Exception {
        String sql = "select a from adbc0.test_db0.tbl0 limit 5";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "LIMIT 5");
    }

    @Test
    public void testADBCCombinedPushdown() throws Exception {
        String sql = "select a from adbc0.test_db0.tbl0 where b = 'x' limit 10";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "WHERE");
        assertContains(plan, "LIMIT 10");
    }

    @Test
    public void testADBCCrossCatalogJoin() throws Exception {
        String sql = "select t1.a, t2.v1 from adbc0.test_db0.tbl0 t1 join test.t0 t2 on t1.c = t2.v1";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
        assertContains(plan, "OlapScanNode");
    }

    @Test
    public void testADBCTlsExplain() throws Exception {
        // Verify that Thrift serialization works with TLS fields present (but null)
        String sql = "select a from adbc0.test_db0.tbl0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "SCAN ADBC");
    }
}
