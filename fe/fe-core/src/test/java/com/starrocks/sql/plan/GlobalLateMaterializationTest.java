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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GlobalLateMaterializationTest extends ConnectorPlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        ConnectorPlanTestBase.beforeClass();

        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(true);
    }

    @AfterAll
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableGlobalLateMaterialization(false);
    }

    @Test
    public void testIcebergV3TableJoin() throws Exception {
        String sql;
        String plan;

        sql = "select * from iceberg0.unpartitioned_db.t0 t0 inner join iceberg0.partitioned_db.t1 t1 on t0.id = t1.id";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => data\n" +
                "  |    <slot 3> => date\n" +
                "  |  table: t1\n" +
                "  |    <slot 5> => data\n" +
                "  |    <slot 6> => date\n" +
                "  |  \n" +
                "  3:HASH JOIN");

        sql = "select t0.* from iceberg0.unpartitioned_db.t0 t0 inner join iceberg0.partitioned_db.t1 t1 on t0.id = t1.id";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => data\n" +
                "  |    <slot 3> => date\n" +
                "  |  \n" +
                "  3:HASH JOIN");

        sql = "select t0.* from iceberg0.unpartitioned_db.t0 t0 " +
                "inner join iceberg0.partitioned_db.t1 t1 on t0.id = t1.id limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => data\n" +
                "  |    <slot 3> => date\n" +
                "  |  limit: 10");

        // join with projection
        sql = "select length(t0.data) + length(t1.date) from iceberg0.unpartitioned_db.t0 t0 " +
                "inner join iceberg0.partitioned_db.t1 t1 on t0.id = t1.id limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  6:Project\n" +
                "  |  <slot 7> : CAST(length(2: data) AS BIGINT) + CAST(length(6: date) AS BIGINT)\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => data\n" +
                "  |  table: t1\n" +
                "  |    <slot 6> => date\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  3:HASH JOIN");
        // join with aggregation
        sql = "select count(t0.data), count(t1.date) from iceberg0.unpartitioned_db.t0 t0 " +
                "inner join iceberg0.partitioned_db.t1 t1 on t0.id = t1.id limit 10";

        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => data\n" +
                "  |  table: t1\n" +
                "  |    <slot 6> => date\n" +
                "  |  \n" +
                "  3:HASH JOIN");
    }

    @Test
    public void testTopN() throws Exception {
        String sql;
        String plan;
        sql = "select * from iceberg0.unpartitioned_db.t0 order by id limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  4:FETCH\n" +
                "  |  lookup node: 03\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => data\n" +
                "  |    <slot 3> => date\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  2:MERGING-EXCHANGE\n" +
                "     limit: 10");

        sql = "select * from iceberg0.unpartitioned_db.t0 order by id limit 1000, 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  4:FETCH\n" +
                "  |  lookup node: 03\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => data\n" +
                "  |    <slot 3> => date\n" +
                "  |  limit: 10\n" +
                "  |  \n" +
                "  2:MERGING-EXCHANGE\n" +
                "     offset: 1000\n" +
                "     limit: 10");

        sql = "select * from iceberg0.unpartitioned_db.t0 t0 join iceberg0.partitioned_db.t1 t1 on t0.id = t1.id " +
                "order by t0.date limit 10";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t0\n" +
                "  |    <slot 3> => date");
        assertContains(plan, "  9:FETCH\n" +
                "  |  lookup node: 08\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => data\n" +
                "  |  table: t1\n" +
                "  |    <slot 5> => data\n" +
                "  |    <slot 6> => date\n" +
                "  |  limit: 10");
    }

    @Test
    public void testUnsupportedTables() throws Exception {
        String sql;
        String plan;
        // iceberg v3 table join mysql external table
        sql = "select * from iceberg0.unpartitioned_db.t0 t0 join mysql_table on t0.id = mysql_table.k1";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t0\n" +
                "  |    <slot 2> => data\n" +
                "  |    <slot 3> => date");

        // iceberg v3 table join iceberg v1 table
        sql = "select t0.date, t1.b from iceberg0.unpartitioned_db.t0  t0 " +
                "join iceberg0.partitioned_db.part_tbl1 t1 on t0.id = t1.c";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  6:Project\n" +
                "  |  <slot 3> : 3: date\n" +
                "  |  <slot 5> : 5: b\n" +
                "  |  \n" +
                "  5:FETCH\n" +
                "  |  lookup node: 04\n" +
                "  |  table: t0\n" +
                "  |    <slot 3> => date\n" +
                "  |  \n" +
                "  3:HASH JOIN");
    }

}
