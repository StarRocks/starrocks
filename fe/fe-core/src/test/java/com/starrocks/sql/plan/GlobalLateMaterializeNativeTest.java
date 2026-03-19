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

import com.starrocks.common.FeConstants;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GlobalLateMaterializeNativeTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();

        FeConstants.runningUnitTest = true;
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(true);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);

        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable("""
                CREATE TABLE supplier_nullable ( S_SUPPKEY     INTEGER NOT NULL,
                                             S_NAME        CHAR(25) NOT NULL,
                                             S_ADDRESS     VARCHAR(40),\s
                                             S_NATIONKEY   INTEGER NOT NULL,
                                             S_PHONE       CHAR(15) NOT NULL,
                                             S_ACCTBAL     double NOT NULL,
                                             S_COMMENT     VARCHAR(101) NOT NULL,
                                             PAD char(1) NOT NULL)
                ENGINE=OLAP
                DUPLICATE KEY(`s_suppkey`)
                COMMENT "OLAP"
                DISTRIBUTED BY RANDOM BUCKETS 4
                PROPERTIES (
                "replication_num" = "1",
                "in_memory" = "false"
                );""");

        starRocksAssert.withTable("""
                CREATE TABLE test_array (
                                             test_key    INTEGER NOT NULL,
                                             test_a1     array<int>,\s
                                             PAD char(1) NOT NULL)
                ENGINE=OLAP
                DUPLICATE KEY(`test_key`)
                COMMENT "OLAP"
                DISTRIBUTED BY RANDOM BUCKETS 4
                PROPERTIES (
                "replication_num" = "1",
                "in_memory" = "false"
                );""");
        starRocksAssert.withTable("""
                CREATE TABLE test_struct (
                                             test_key    INTEGER NOT NULL,
                                             test_struct struct<name int, value string>,\s
                                             PAD char(1) NOT NULL)
                ENGINE=OLAP
                DUPLICATE KEY(`test_key`)
                COMMENT "OLAP"
                DISTRIBUTED BY RANDOM BUCKETS 4
                PROPERTIES (
                "replication_num" = "1",
                "in_memory" = "false"
                );""");
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(false);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
    }

    @Test
    public void testLazyMaterialize() throws Exception {
        String sql;
        String plan;
        sql = "select *,upper(S_ADDRESS) from supplier_nullable limit 10";
        plan = getFragmentPlan(sql);
        assertCContains(plan, """
                  6:Decode
                  |  <dict id 10> : <string id 3>
                  |  <dict id 11> : <string id 7>
                  |  <dict id 12> : <string id 9>\
                """);

        sql = "select distinct S_SUPPKEY, S_ADDRESS from ( select S_ADDRESS, S_SUPPKEY " +
                "from supplier_nullable limit 10) t";
        plan = getFragmentPlan(sql);
        assertCContains(plan, "  6:Decode\n" +
                "  |  <dict id 9> : <string id 3>");

        sql = "select * from supplier where S_SUPPKEY < 10 order by 1,2 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    @Test
    public void testProjection() throws Exception {
        String sql;
        String plan;

        sql = "select *,upper(S_ADDRESS) from supplier_nullable limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
        assertContains(plan, "Decode");
        assertContains(plan, "Project");
    }

    @Test
    public void testArraySubFieldPrune() throws Exception {
        String sql;
        String plan;
        sql = "select array_length(test_a1) from test_array limit 1";
        plan = getCostExplain(sql);
        assertNotContains(plan, "FETCH");

        sql = "select array_length(test_a1),* from test_array limit 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  4:FETCH
                  |  lookup node: 03
                  |  table: test_array
                  |    <slot 1> => test_key
                  |    <slot 3> => PAD
                  |  limit: 1\
                """);
    }

    @Test
    public void testStructSubFieldPrune() throws Exception {
        String sql;
        String plan;
        sql = "select test_struct.name from test_struct limit 1";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");
    }

    @Test
    public void testLeftJoin() throws Exception {
        String sql;
        String plan;

        sql = "select * from test_struct l join test_array r on l.test_key=r.test_key limit 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    @Test
    public void testWithOffset() throws Exception {
        String sql;
        String plan;

        sql = "select * from test_struct order by 1 limit 100000000,10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }

    @Test
    public void testJson() throws Exception {
        String sql;
        String plan;
        connectContext.getSessionVariable().setEnableDeferProjectAfterTopN(true);
        connectContext.getSessionVariable().setCboPruneJsonSubfield(false);

        sql = "select v_int, get_json_string(v_json, '$.a') from tjson order by v_int limit 1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "FETCH");
    }
}
