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

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class HiveComplexTypePrunePlanTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
    }

    @After
    public void resetSessionVariable() {
        connectContext.getSessionVariable().setEnablePruneComplexTypes(false);
    }

    @Test
    public void testAggCTE() throws Exception {
        String sql = "with stream as (select array_agg(col_struct.c0) as t1 from hive0.subfield_db.subfield group by " +
                "col_int) select t1 from stream";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "[/col_struct/c0]");

        connectContext.getSessionVariable().setEnablePruneComplexTypes(true);
        plan = getVerboseExplain(sql);
        assertContains(plan, "Pruned type: 9 [col_struct] <-> [struct<c0 int(11)>]");
    }

    @Test
    public void testNoProjectOnTF() throws Exception {
        connectContext.getSessionVariable().setEnablePruneComplexTypes(true);
        // no project on input, and no project on tf, no prune
        String sql = "select c2_struct from hive0.subfield_db.subfield, unnest(c2) as t(c2_struct);";
        assertVerbosePlanContains(sql, "[ARRAY<struct<c2_sub1 int(11), c2_sub2 int(11)>>]");
        sql = "select c2 from hive0.subfield_db.subfield, unnest(c2) as t(c2_struct);";
        assertVerbosePlanContains(sql, "[ARRAY<struct<c2_sub1 int(11), c2_sub2 int(11)>>]");
        sql = "select c2, c2_struct from hive0.subfield_db.subfield, unnest(c2) as t(c2_struct);";
        assertVerbosePlanContains(sql, "[ARRAY<struct<c2_sub1 int(11), c2_sub2 int(11)>>]");
        // project on input, no project on tf, prune
        sql = "select c3_struct from hive0.subfield_db.subfield, unnest(c3.c3_sub1) as t(c3_struct);";
        assertVerbosePlanContains(sql, "[struct<c3_sub1 array<struct<c3_sub1_sub1 int(11), c3_sub1_sub2 int(11)>>>]");
        // output contains the complete column, no prune
        sql = "select c3 from hive0.subfield_db.subfield, unnest(c3.c3_sub1) as t(c3_struct);";
        assertVerbosePlanContains(sql, "[struct<c3_sub1 array<struct<c3_sub1_sub1 int(11), " +
                "c3_sub1_sub2 int(11)>>, c3_sub2 int(11)>]");
        sql = "select c3, c3_struct from hive0.subfield_db.subfield, unnest(c3.c3_sub1) as t(c3_struct);";
        assertVerbosePlanContains(sql, "[struct<c3_sub1 array<struct<c3_sub1_sub1 int(11), " +
                "c3_sub1_sub2 int(11)>>, c3_sub2 int(11)>]");
    }

    @Test
    public void testProjectOnTF() throws Exception {
        connectContext.getSessionVariable().setEnablePruneComplexTypes(true);
        // no project on input, and project on tf, prune
        String sql = "select c2_struct.c2_sub1 from hive0.subfield_db.subfield, unnest(c2) as t(c2_struct);";
        assertVerbosePlanContains(sql, "[ARRAY<struct<c2_sub1 int(11)>>]");
        // no project on input, and project on tf all subfiled, no prune
        sql =
                "select c2_struct.c2_sub1, c2_struct.c2_sub2 from hive0.subfield_db.subfield, unnest(c2) as t(c2_struct);";
        assertVerbosePlanContains(sql, "[ARRAY<struct<c2_sub1 int(11), c2_sub2 int(11)>>]");
        sql = "select c2_struct, c2_struct.c2_sub2 from hive0.subfield_db.subfield, unnest(c2) as t(c2_struct);";
        assertVerbosePlanContains(sql, "[ARRAY<struct<c2_sub1 int(11), c2_sub2 int(11)>>]");
        // project on input, project on tf, prune
        sql = "select c3_struct.c3_sub1_sub1 from hive0.subfield_db.subfield, unnest(c3.c3_sub1) as t(c3_struct);";
        assertVerbosePlanContains(sql, "[struct<c3_sub1 array<struct<c3_sub1_sub1 int(11)>>>]");
        // project on input, project on tf but all subfiled, prune
        sql = "select c3_struct.c3_sub1_sub1, c3_struct.c3_sub1_sub2 from hive0.subfield_db.subfield, " +
                "unnest(c3.c3_sub1) as t(c3_struct);";
        assertVerbosePlanContains(sql, "[struct<c3_sub1 array<struct<c3_sub1_sub1 int(11), c3_sub1_sub2 int(11)>>>]");
        sql = "select c3_struct.c3_sub1_sub1, c3_struct from hive0.subfield_db.subfield, " +
                "unnest(c3.c3_sub1) as t(c3_struct);";
        assertVerbosePlanContains(sql, "[struct<c3_sub1 array<struct<c3_sub1_sub1 int(11), c3_sub1_sub2 int(11)>>>]");
    }

    @Test
    public void testNoOutputOnTF() throws Exception {
        connectContext.getSessionVariable().setEnablePruneComplexTypes(true);
        String sql =
                "select c1 from hive0.subfield_db.subfield, unnest(c2) as t(c2_struct) where c2_struct.c2_sub1 > 0;";
        assertVerbosePlanContains(sql, "[ARRAY<struct<c2_sub1 int(11)>>]");
        sql =
                "select c1 from hive0.subfield_db.subfield, unnest(c3.c3_sub1) as t(c3_struct) where c3_struct.c3_sub1_sub1 > 0;";
        assertVerbosePlanContains(sql, "[struct<c3_sub1 array<struct<c3_sub1_sub1 int(11)>>>]");
    }

    @Test
    public void testUnnestComplex() throws Exception {
        connectContext.getSessionVariable().setEnablePruneComplexTypes(true);
        String sql = "select c3_struct.c3_sub1_sub1 from hive0.subfield_db.subfield, unnest(c2) as t(c_struct), " +
                "unnest(c3.c3_sub1) as tt(c3_struct) where c_struct.c2_sub1 > 10";
        assertVerbosePlanContains(sql, "[ARRAY<struct<c2_sub1 int(11)>>]");
        assertVerbosePlanContains(sql, "[struct<c3_sub1 array<struct<c3_sub1_sub1 int(11)>>>]");
    }

    @Test
    public void testUnnestCrossJoin() throws Exception {
        connectContext.getSessionVariable().setEnablePruneComplexTypes(true);
        String sql = "select c3_struct.c3_sub1_sub1 from hive0.subfield_db.subfield cross join " +
                "unnest(c3.c3_sub1) as t(c3_struct) where c1 > 0;";
        assertVerbosePlanContains(sql, "[struct<c3_sub1 array<struct<c3_sub1_sub1 int(11)>>>]");
    }
}
