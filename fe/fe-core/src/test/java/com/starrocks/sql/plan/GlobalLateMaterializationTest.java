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
import com.starrocks.qe.SessionVariable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GlobalLateMaterializationTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();

        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(true);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(false);
    }

    @Test
    public void testLazyMaterialize() throws Exception {
        String sql;
        String plan;
        // test add fetch at top
        sql = "select * from iceberg0.unpartitioned_db.t0 order by id limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  4:FETCH
                  |  lookup node: 03
                  |  table: t0
                  |    <slot 2> => data
                  |    <slot 3> => date
                  |  limit: 10
                """);
        sql = "select * from iceberg0.unpartitioned_db.t0 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  3:FETCH
                  |  lookup node: 02
                  |  table: t0
                  |    <slot 1> => id
                  |    <slot 2> => data
                  |    <slot 3> => date
                  |  limit: 10\
                """);

        sql = "select * from iceberg0.unpartitioned_db.t0 where id < 10 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  3:FETCH
                  |  lookup node: 02
                  |  table: t0
                  |    <slot 2> => data
                  |    <slot 3> => date
                  |  limit: 10\
                """);
    }

    @Test
    public void testProjection() throws Exception {
        String sql;
        String plan;
        // test with projection
        sql = "select * from iceberg0.unpartitioned_db.t0 where id < 10 order by upper(id) limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  5:FETCH
                  |  lookup node: 04
                  |  table: t0
                  |    <slot 2> => data
                  |    <slot 3> => date
                  |  limit: 10\
                """);

        sql = "select *,lower(id) from iceberg0.unpartitioned_db.t0 order by upper(id) limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  5:FETCH
                  |  lookup node: 04
                  |  table: t0
                  |    <slot 2> => data
                  |    <slot 3> => date
                  |  limit: 10\
                """);
    }

    @Test
    public void testCTE() throws Exception {
        String sql;
        String plan;
        // test cte support
        sql = "with cte as (select id as kid,* from iceberg0.unpartitioned_db.t0 limit 10) select * from cte l " +
                "join cte r on l.kid=r.kid limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  16:FETCH
                  |  lookup node: 15
                  |  table: t0
                  |    <slot 7> => data
                  |    <slot 8> => date
                  |  table: t0
                  |    <slot 12> => data
                  |    <slot 13> => date
                  |  limit: 10\
                """);
        assertContains(plan, """
                  3:FETCH
                  |  lookup node: 02
                  |  table: t0
                  |    <slot 1> => id
                  |  limit: 10\
                """);

        sql = "with cte as (select id as kid,* from iceberg0.unpartitioned_db.t0 limit 10) " +
                "select * from cte where data < 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  3:FETCH
                  |  lookup node: 02
                  |  table: t0
                  |    <slot 1> => id
                  |    <slot 2> => data
                  |    <slot 3> => date
                  |  limit: 10\
                """);
        // two fetch ops
        sql = "with cte as (select id as kid,* from iceberg0.unpartitioned_db.t0 limit 10) " +
                "select * from cte where data < 10 limit 5";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  3:FETCH
                  |  lookup node: 02
                  |  table: t0
                  |    <slot 2> => data
                  |  limit: 10\
                """);
        assertContains(plan, """
                  7:FETCH
                  |  lookup node: 06
                  |  table: t0
                  |    <slot 1> => id
                  |    <slot 3> => date
                  |  limit: 5\
                """);
        // fetch at cte producer
        sql = "with cte as (select id as kid,* from iceberg0.unpartitioned_db.t0 limit 10) " +
                "select * from cte l join cte r on l.kid = r.kid";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  3:FETCH
                  |  lookup node: 02
                  |  table: t0
                  |    <slot 1> => id
                  |    <slot 2> => data
                  |    <slot 3> => date
                  |  limit: 10\
                """);

        // cte push down to one side
        sql = "with cte as (select id as kid,* from iceberg0.unpartitioned_db.t0) " +
                "(select * from cte order by id ) union all (select * from cte limit 10) ";

        plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");

        // push down two sides
        sql = "with cte as (select id as kid,* from iceberg0.unpartitioned_db.t0) " +
                "(select * from cte order by id limit 10) union all (select * from cte limit 10) ";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  14:FETCH
                  |  lookup node: 13
                  |  table: t0
                  |    <slot 12> => data
                  |    <slot 13> => date
                  |  limit: 10\
                """);

        // two fetch ops under the same fragment
        sql = "with cte as (select id as kid,* from iceberg0.unpartitioned_db.t0 order by id limit 100) " +
                "select * from cte order by date limit 10 ";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  8:FETCH
                  |  lookup node: 07
                  |  table: t0
                  |    <slot 2> => data
                  |  limit: 10\
                """);
    }

    @Test
    public void testJoin() throws Exception {
        String sql;
        String plan;
        // basic join
        sql = "select * from iceberg0.unpartitioned_db.t0 t0 join iceberg0.partitioned_db.t1 t1 on t0.id = t1.id " +
                " limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  6:FETCH
                  |  lookup node: 05
                  |  table: t0
                  |    <slot 2> => data
                  |    <slot 3> => date
                  |  table: t1
                  |    <slot 7> => data
                  |    <slot 8> => date
                  |  limit: 10\
                """);

        sql = "select * from iceberg0.unpartitioned_db.t0 t0 join iceberg0.partitioned_db.t1 t1 on t0.id = t1.id " +
                " order by t0.date limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  7:FETCH
                  |  lookup node: 06
                  |  table: t0
                  |    <slot 2> => data
                  |  table: t1
                  |    <slot 7> => data
                  |    <slot 8> => date
                  |  limit: 10\
                """);

        sql = "select * from iceberg0.unpartitioned_db.t0 t0 join iceberg0.partitioned_db.t1 t1 on t0.id = t1.id " +
                " order by t0.date, t0.data limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  7:FETCH
                  |  lookup node: 06
                  |  table: t1
                  |    <slot 7> => data
                  |    <slot 8> => date
                  |  limit: 10\
                """);

        // shuffle join
        sql = "select * from iceberg0.unpartitioned_db.t0 t0 join [shuffle] iceberg0.partitioned_db.t1 t1 on t0.id = t1.id " +
                " order by t0.date, t0.data limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  8:FETCH
                  |  lookup node: 07
                  |  table: t1
                  |    <slot 7> => data
                  |    <slot 8> => date
                  |  limit: 10\
                """);
    }

    @Test
    public void testWindow() throws Exception {
        String sql;
        String plan;
        // test with window function
        sql = "select *, row_number() over (partition by id) from iceberg0.unpartitioned_db.t0 t0";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");

        sql = "select *, row_number() over (partition by id) from iceberg0.unpartitioned_db.t0 t0 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  6:FETCH
                  |  lookup node: 05
                  |  table: t0
                  |    <slot 2> => data
                  |    <slot 3> => date
                  |  limit: 10\
                """);
    }

    @Test
    public void testLocalPartitionTopN() throws Exception {
        String sql;
        String plan;
        // test local partition top n
        sql = "with cte as (select *, row_number() over (partition by id) rn from iceberg0.unpartitioned_db.t0 t0) " +
                " select * from cte where rn = 1";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");

        sql = "with cte as (select *, row_number() over (partition by id) rn from iceberg0.unpartitioned_db.t0 t0) " +
                " select * from cte where rn = 1 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  8:FETCH
                  |  lookup node: 07
                  |  table: t0
                  |    <slot 2> => data
                  |    <slot 3> => date
                  |  limit: 10\
                """);

        sql = "with cte as (select *, max(data) over (partition by id) rn from iceberg0.unpartitioned_db.t0 t0) " +
                " select * from cte where rn = 1 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  7:FETCH
                  |  lookup node: 06
                  |  table: t0
                  |    <slot 3> => date
                  |  limit: 10\
                """);
    }

    @Test
    public void testGroupBy() throws Exception {
        String plan;
        String sql;

        // group by
        sql = "select count(*) from iceberg0.unpartitioned_db.t0 group by id limit 10";
        plan = getFragmentPlan(sql);
        assertNotContains(plan, "FETCH");

        sql = "with cte as (select id,date from iceberg0.unpartitioned_db.t0 limit 10) " +
                "select distinct id,date from cte";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  3:FETCH
                  |  lookup node: 02
                  |  table: t0
                  |    <slot 1> => id
                  |    <slot 3> => date
                  |  limit: 10\
                """);
    }

    @Test
    public void testSessionVariableControl() throws Exception {
        final SessionVariable sv = connectContext.getSessionVariable();
        final int prevMaxLimit = sv.getGlobalLateMaterializeMaxLimit();
        final int prevMaxFetchOps = sv.getGlobalLateMaterializeMaxFetchOps();

        try {
            sv.setGlobalLateMaterializeMaxFetchOps(1);
            sv.setGlobalLateMaterializeMaxLimit(1024);

            String sql;
            String plan;

            sql = "select * from iceberg0.unpartitioned_db.t0 order by id limit 1025";
            plan = getFragmentPlan(sql);
            assertNotContains(plan, "FETCH");

            sql = "with cte as (select * from iceberg0.unpartitioned_db.t0 order by id limit 10) " +
                    "select * from cte order by date limit 5";
            plan = getFragmentPlan(sql);
            assertContains(plan, """
                      5:FETCH
                      |  lookup node: 04
                      |  table: t0
                      |    <slot 2> => data
                      |  limit: 5\
                    """);

        } finally {
            sv.setGlobalLateMaterializeMaxLimit(prevMaxLimit);
            sv.setGlobalLateMaterializeMaxFetchOps(prevMaxFetchOps);
        }
    }

    @Test
    public void testUnsupportedTables() throws Exception {
        String sql;
        String plan;
        // iceberg v3 table join mysql external table
        sql = "select * from iceberg0.unpartitioned_db.t0 t0 join mysql_table on t0.id = mysql_table.k1 limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                6:FETCH
                  |  lookup node: 05
                  |  table: t0
                  |    <slot 2> => data
                  |    <slot 3> => date
                  |  limit: 10
                  | \s
                  4:EXCHANGE
                     limit: 10""");

        // iceberg v3 table join iceberg v1 table
        sql = "select t0.date, t1.b from iceberg0.unpartitioned_db.t0  t0 " +
                "join iceberg0.partitioned_db.part_tbl1 t1 on t0.id = t1.c limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, """
                  7:FETCH
                  |  lookup node: 06
                  |  table: t0
                  |    <slot 3> => date
                  |  limit: 10\
                """);
    }

}
