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

import com.starrocks.server.GlobalStateMgr;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PruneComplexSubfieldTest extends PlanTestNoneDBBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestNoneDBBase.beforeClass();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        String dbName = "prune_column_test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        starRocksAssert.withTable("CREATE TABLE `pc0` (\n" +
                "  `v1` bigint NULL, \n" +
                "  `map1` MAP<INT, INT> NULL, \n" +
                "  `map2` MAP<INT, MAP<INT, INT>> NULL, " +
                "  `map3` MAP<INT, MAP<INT, MAP<INT, INT>>> NULL, " +
                "  `map4` MAP<INT, MAP<INT, MAP<INT, MAP<INT, INT>>>> NULL, " +
                "  `map5` MAP<INT, STRUCT<s1 INT, m2 MAP<INT, STRUCT<s2 int, s3 int>>>>" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `sc0` (\n" +
                "  `v1` bigint NULL, \n" +
                "  `st1` struct<s1 INT, s2 INT> NULL, \n" +
                "  `st2` struct<s1 INT, s2 INT, sm3 MAP<INT, INT>> NULL, \n" +
                "  `st3` struct<s1 INT, s2 INT, sa3 ARRAY<INT>> NULL, \n" +
                "  `st4` struct<s1 INT, s2 INT, ss3 struct<s31 INT, s32 INT>> NULL, \n" +
                "  `st5` struct<s1 INT, s2 INT, ss3 struct<s31 INT, s32 INT>, " +
                " ss4 struct<s41 INT, s52 struct<s421 INT, s423 INT>>> NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL, \n" +
                "  `v2` bigint NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");
    }

    @Before
    public void setUp() {
        connectContext.getSessionVariable().setCboPruneSubfield(true);
        connectContext.getSessionVariable().setEnablePruneComplexTypes(false);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(-1);
        connectContext.getSessionVariable().setCboCteReuse(true);
        connectContext.getSessionVariable().setCboCTERuseRatio(0);
    }

    @After
    public void tearDown() {
        connectContext.getSessionVariable().setCboCteReuse(false);
        connectContext.getSessionVariable().setCboPruneSubfield(false);
        connectContext.getSessionVariable().setEnablePruneComplexTypes(true);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000);
        connectContext.getSessionVariable().setCboCTERuseRatio(1.5);
    }

    @Test
    public void testJoinPruneColumn() throws Exception {
        String sql = "select sc0.st1.s1, st1.s2 from t0 join sc0 on sc0.v1 = t0.v1";
        String plan = getVerboseExplain(sql);
        System.out.println(plan);
        assertContains(plan, "ColumnAccessPath: [/st1/s2, /st1/s1]");
    }

    @Test
    public void testUnionAllPruneColumn() throws Exception {
        String sql = "select st1.s1 from (" +
                " select v1, st1, st2, st3 from sc0 x1 " +
                " union all " +
                " select v1, st1, st2, st3 from sc0 x2) x3";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "[/st1/s1]");
    }

    @Test
    public void testCTEPruneColumn() throws Exception {
        String sql =
                "with t1 as (select * from sc0) select x1.st1.s1, x2.st2.s2 from t1 x1 join t1 x2 on x1.v1 = x2.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/st1/s1, /st2/s2]");
    }

    @Test
    public void testPruneMapColumn() throws Exception {
        String sql = "select map_keys(map1) from pc0";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "/map1/KEY");

        sql = "select map_values(map2) from pc0";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");

        sql = "select map_keys(map3[1][2]) from pc0";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");

        sql = "select map_keys(map4[1][2]) from pc0";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");

        sql = "select map1, " +
                "     map2, " +
                "     map_values(map1), " +
                "     map_keys(map1)," +
                "     map_values(map2), " +
                "     map_keys(map2)" +
                " from pc0";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");
    }

    @Test
    public void testPruneMapStructNest() throws Exception {
        String sql = "select map5[1].m2 from pc0";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");

    }

    @Test
    public void testIsNull() throws Exception {
        String sql = "select 1 from pc0 where map1 is null";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "[/map1/OFFSET]");

        sql = "select 1 from sc0 where st1 is null";
        plan = getVerboseExplain(sql);
        System.out.println(plan);
        assertContains(plan, "[/st1/s1]");
    }

    @Test
    public void testIsNullStruct() throws Exception {
        String sql = "select 1 from sc0 where st1.s2 is null";
        String plan = getVerboseExplain(sql);
        System.out.println(plan);
        assertContains(plan, "[/st1/s2]");
    }

    @Test
    public void testPruneStructColumn() throws Exception {
        String sql = "select st1 from sc0";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");

        sql = "select st1.s1, st1.s2 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st1/s2, /st1/s1]");

        sql = "select st2.s1, st2.sm3 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st2/s1, /st2/sm3]");

        sql = "select st2.s1, map_keys(st2.sm3), st3.sa3 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st2/sm3/KEY, /st2/s1, /st3/sa3]");

        sql = "select st4.ss3, st4.s1 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st4/ss3, /st4/s1]");

        sql = "select st4.ss3, st4.ss3.s31 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st4/ss3]");

        sql = "select st4.ss3, st4.ss3.s31, st4 from sc0";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");

        sql = "select st5.ss4.s52.s421, st5.ss3.s32 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st5/ss4/s52/s421, /st5/ss3/s32]");
    }

    @Test
    public void testPruneGroupStructColumn() throws Exception {
        String sql = "select st1.s1, st1.s2 from sc0 group by st1";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");

        sql = "select st1.s1, st1.s2 from sc0 group by st1.s1, st1.s2";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st1/s2, /st1/s1]");
    }

    @Test
    public void testPruneMapValues() throws Exception {
        String sql = "select map_keys(map1), map_values(map1) from pc0";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");

        sql = "select map_keys(map1), map_size(map1) from pc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/map1/KEY, /map1/OFFSET]");
    }

    @Test
    public void testStructUpperCase() throws Exception {
        String sql = "select map5[1].S1, map5[2].M2[4].S3 from pc0;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "map5[1].s1");
        assertContains(plan, "map5[2].m2[4].s3");

        sql = "select st1.S2, st2.SM3[1], ST3.SA3, ST5.SS3.S32 from sc0;";
        plan = getVerboseExplain(sql);
        assertContains(plan, "st1.s2"); 
        assertContains(plan, "st2.sm3[1]"); 
        assertContains(plan, "st5.ss3.s32"); 
        assertContains(plan, "st3.sa3"); 
    }
}
