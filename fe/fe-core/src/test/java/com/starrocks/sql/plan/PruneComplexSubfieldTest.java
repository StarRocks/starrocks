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
                "  `map5` MAP<INT, STRUCT<s1 INT, m2 MAP<INT, STRUCT<s2 int, s3 int>>>>," +
                "  `a1` ARRAY<INT> NULL" +
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
                " ss4 struct<s41 INT, s52 struct<s421 INT, s423 INT>>> NULL," +
                "  `st6` struct<s1 INT, m2 MAP<int, STRUCT<s3 int, s4 int>>, " +
                "a3 ARRAY<STRUCT<s5 int, s6 int>>> NULL\n" +
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

        starRocksAssert.withTable("CREATE TABLE `tt` (\n" +
                "  `v1` bigint NULL, \n" +
                "  `ass` ARRAY<STRUCT<a int, b int, c int>> NULL " +
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
        assertContains(plan, "ColumnAccessPath: [/st1/s1, /st1/s2]");
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  3 <-> [3: v1, BIGINT, true]\n" +
                "  |  12 <-> 4: st1.s1\n" +
                "  |  13 <-> 4: st1.s2\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testUnionAllPruneColumn() throws Exception {
        String sql = "select st1.s1 from (" +
                " select v1, st1, st2, st3 from sc0 x1 " +
                " union all " +
                " select v1, st1, st2, st3 from sc0 x2) x3";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "[/st1/s1]");
        assertContains(plan, "  5:Project\n" +
                "  |  output columns:\n" +
                "  |  22 <-> 9: st1.s1\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  4:OlapScanNode");
        assertContains(plan, "  2:Project\n" +
                "  |  output columns:\n" +
                "  |  21 <-> 2: st1.s1\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:OlapScanNode");
    }

    @Test
    public void testCTEPruneColumn() throws Exception {
        String sql =
                "with t1 as (select * from sc0) select x1.st1.s1, x2.st2.s2 from t1 x1 join t1 x2 on x1.v1 = x2.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: v1, BIGINT, true]\n" +
                "  |  26 <-> 2: st1.s1\n" +
                "  |  27 <-> 3: st2.s2\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode");
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
        assertContains(plan, "ColumnAccessPath: [/map3/INDEX/INDEX/KEY]");

        sql = "select map_keys(map4[1][2]) from pc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/map4/INDEX/INDEX/KEY]");

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
        assertContains(plan, "ColumnAccessPath: [/map5/INDEX/m2]");
    }

    @Test
    public void testIsNull() throws Exception {
        String sql = "select 1 from pc0 where map1 is null";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "[/map1/OFFSET]");
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
        assertContains(plan, "[/st1/s1, /st1/s2]");

        sql = "select st2.s1, st2.sm3 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st2/s1, /st2/sm3]");

        sql = "select st2.s1, map_keys(st2.sm3), st3.sa3 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st2/s1, /st2/sm3/KEY, /st3/sa3]");

        sql = "select st4.ss3, st4.s1 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st4/s1, /st4/ss3]");

        sql = "select st4.ss3, st4.ss3.s31 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st4/ss3]");

        sql = "select st4.ss3, st4.ss3.s31, st4 from sc0";
        plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");

        sql = "select st5.ss4.s52.s421, st5.ss3.s32 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st5/ss3/s32, /st5/ss4/s52/s421]");
    }

    @Test
    public void testPruneGroupStructColumn() throws Exception {
        String sql = "select st1.s1, st1.s2 from sc0 group by st1";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");

        sql = "select st1.s1, st1.s2 from sc0 group by st1.s1, st1.s2";
        plan = getVerboseExplain(sql);
        assertContains(plan, "[/st1/s1, /st1/s2]");
    }

    @Test
    public void testPruneMapValues() throws Exception {
        String sql = "select map_keys(map1), map_values(map1) from pc0";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");

        sql = "select map_keys(map1), map_size(map1) from pc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/map1/KEY]");
    }

    @Test
    public void testPruneComplexFunction() throws Exception {
        String sql = "select st6.m2[1].s3, st6.a3[2].s6 from sc0";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/st6/a3/INDEX/s6, /st6/m2/INDEX/s3]");

        sql = "select st6.a3[1].s5, st6.a3[2].s6 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "/st6/a3/INDEX/s5");
        assertContains(plan, "/st6/a3/INDEX/s6");

        sql = "select st6.a3[1].s5, array_length(st6.a3) from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/st6/a3/ALL]");

        sql = "select st6.m2[1].s3, map_keys(st6.m2) from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/st6/m2/ALL]");

        sql = "select st6.m2[1].s3, map_size(st6.m2) from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/st6/m2/ALL]");

        sql = "select st6.m2[1].s3, st6.m2[3].s4 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/st6/m2/INDEX/s3, /st6/m2/INDEX/s4]");

        sql = "select array_length(a1) from pc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/a1/OFFSET]");

        sql = "select array_length(a1), a1[1] from pc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/a1/ALL]");

        sql = "select a1[map1[1]] from pc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/a1/ALL, /map1/INDEX]");

        sql = "select a1[map_size(map1)] from pc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/a1/ALL, /map1/OFFSET]");

        sql = "select st6.m2[st1.s1].s3 from sc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/st1/s1, /st6/m2/ALL/s3]");

        sql = "select a1[a1[1]] from pc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/a1/ALL]");

        sql = "select a1[a1[a1[a1[a1[2]]]]] from pc0";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/a1/ALL]");
    }

    @Test
    public void testPredicate() throws Exception {
        String sql = "select st6.m2[1].s3, st6.a3[2].s6 from sc0 where st6.m2[1].s3 = 1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/st6/a3/INDEX/s6, /st6/m2/INDEX/s3]");
        assertContains(plan, "PredicateAccessPath: [/st6/m2/INDEX/s3]");

        sql = "select st6.m2[1].s3, st6.a3[2].s6 from sc0 where map_size(st6.m2) = 1";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/st6/a3/INDEX/s6, /st6/m2/ALL]");
        assertContains(plan, "PredicateAccessPath: [/st6/m2/OFFSET]");

        sql = "select st6.m2[1].s3, st6.a3[2].s6 from sc0 where st6.m2 = map{1:row(1,1)}";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/st6/a3/INDEX/s6, /st6/m2]");
        assertContains(plan, "PredicateAccessPath: [/st6/m2]");

        sql = "select map_keys(st6.m2), st6.a3[2].s6 from sc0 where map_size(st6.m2) = 1";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/st6/a3/INDEX/s6, /st6/m2/KEY]");
        assertContains(plan, "PredicateAccessPath: [/st6/m2/OFFSET]");

        sql = "select array_length(a1), a1[1] from pc0 where a1[2] = 3";
        plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/a1/ALL]");
        assertContains(plan, "PredicateAccessPath: [/a1/INDEX]");
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
    
    @Test
    public void testCTEInlinePruneColumn() throws Exception {
        String sql =
                "with t1 as (select * from sc0) select x1.st1.s1, x2.st2.s2 from t1 x1 join t1 x2 on x1.v1 = x2.v1";
        String plan;
        try {
            connectContext.getSessionVariable().setCboCTERuseRatio(10000000);
            plan = getVerboseExplain(sql);
        } finally {
            connectContext.getSessionVariable().setCboCTERuseRatio(1.15);
        }
        assertContains(plan, "  4:Project\n" +
                "  |  output columns:\n" +
                "  |  15 <-> [15: v1, BIGINT, true]\n" +
                "  |  25 <-> 17: st2.s2\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  3:OlapScanNode");
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  8 <-> [8: v1, BIGINT, true]\n" +
                "  |  24 <-> 9: st1.s1\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode");
        assertContains(plan, "ColumnAccessPath: [/st1/s1]");
        assertContains(plan, "ColumnAccessPath: [/st2/s2]");
    }

    @Test
    public void testUnionJoinPruneColumn() throws Exception {
        String sql = "select x1.st3.sa3, x2.st2.sm3[1], x1.st4.ss3 from (" +
                "select * from sc0 " +
                "union all " +
                "select * from sc0) x1 join sc0 x2 on x1.v1 = x2.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  9:Project\n" +
                "  |  output columns:\n" +
                "  |  22 <-> [22: v1, BIGINT, true]\n" +
                "  |  33 <-> 24: st2.sm3[1]\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  8:OlapScanNode");
        assertContains(plan, "ColumnAccessPath: [/st2/sm3/INDEX]");
        assertContains(plan, "  0:UNION\n" +
                "  |  output exprs:\n" +
                "  |      [15, BIGINT, true] | [34, struct<s31 int(11), s32 int(11)>, true] | " +
                "[32, ARRAY<INT>, true]\n" +
                "  |  child exprs:\n" +
                "  |      [1: v1, BIGINT, true] | [35: expr, struct<s31 int(11), s32 int(11)>, true] | " +
                "[36: expr, ARRAY<INT>, true]\n" +
                "  |      [8: v1, BIGINT, true] | [37: expr, struct<s31 int(11), s32 int(11)>, true] | " +
                "[38: expr, ARRAY<INT>, true]");
        assertContains(plan, "  5:Project\n" +
                "  |  output columns:\n" +
                "  |  8 <-> [8: v1, BIGINT, true]\n" +
                "  |  37 <-> 12: st4.ss3\n" +
                "  |  38 <-> 11: st3.sa3\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  4:OlapScanNode");
        assertContains(plan, "ColumnAccessPath: [/st3/sa3, /st4/ss3]");
        assertContains(plan, "  2:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: v1, BIGINT, true]\n" +
                "  |  35 <-> 5: st4.ss3\n" +
                "  |  36 <-> 4: st3.sa3\n" +
                "  |  cardinality: 1");
        assertContains(plan, "ColumnAccessPath: [/st3/sa3, /st4/ss3]");
    }

    @Test
    public void testUnionJoinPruneColumn2() throws Exception {
        String sql = "select x1.st3.sa3, x2.st2.sm3[1], x1.st4.ss3 from (" +
                "select v1, st1, st3, st4 from sc0 " +
                "union all " +
                "select v1, st1, st3, st4 from sc0 group by v1, st1, st3, st4) x1 join sc0 x2 on x1.v1 = x2.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "30 <-> 21: st2.sm3[1]");
        assertContains(plan, "ColumnAccessPath: [/st2/sm3/INDEX]");
        assertContains(plan, "  0:UNION\n" +
                "  |  output exprs:\n" +
                "  |      [15, BIGINT, true] | [29, ARRAY<INT>, true] | " +
                "[31, struct<s31 int(11), s32 int(11)>, true]\n" +
                "  |  child exprs:\n" +
                "  |      [1: v1, BIGINT, true] | [32: expr, ARRAY<INT>, true] | " +
                "[33: expr, struct<s31 int(11), s32 int(11)>, true]\n" +
                "  |      [8: v1, BIGINT, true] | [34: expr, ARRAY<INT>, true] | " +
                "[35: expr, struct<s31 int(11), s32 int(11)>, true]");
        assertContains(plan, "  6:Project\n" +
                "  |  output columns:\n" +
                "  |  8 <-> [8: v1, BIGINT, true]\n" +
                "  |  34 <-> 11: st3.sa3\n" +
                "  |  35 <-> 12: st4.ss3\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  5:AGGREGATE");
        assertContains(plan, "  2:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: v1, BIGINT, true]\n" +
                "  |  32 <-> 4: st3.sa3\n" +
                "  |  33 <-> 5: st4.ss3\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  1:OlapScanNode");
        assertContains(plan, "ColumnAccessPath: [/st3/sa3, /st4/ss3]");
    }

    @Test
    public void testJoinPruneColumn2() throws Exception {
        String sql = "select x.st1.s1, st1.s2 from t0 join " +
                "(select v1, st1, st2, st3, st4 from sc0 group by v1, st1, st2, st3, st4) " +
                "x on x.v1 = t0.v1";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");
        assertContains(plan, "  2:Project\n" +
                "  |  output columns:\n" +
                "  |  3 <-> [3: v1, BIGINT, true]\n" +
                "  |  12 <-> 4: st1.s1\n" +
                "  |  13 <-> 4: st1.s2");
    }

    @Test
    public void testJoinPruneColumn3() throws Exception {
        String sql = "select st1.s1, st1.s2 from t0 join " +
                "(select v1, st1 from sc0 group by v1, st1.s1, st1) " +
                "x on x.v1 = t0.v1";
        String plan = getVerboseExplain(sql);
        assertNotContains(plan, "ColumnAccessPath");
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  3 <-> [3: v1, BIGINT, true]\n" +
                "  |  4 <-> [4: st1, struct<s1 int(11), s2 int(11)>, true]\n" +
                "  |  10 <-> 4: st1.s1");
        assertContains(plan, "  3:Project\n" +
                "  |  output columns:\n" +
                "  |  3 <-> [3: v1, BIGINT, true]\n" +
                "  |  13 <-> 4: st1.s1\n" +
                "  |  14 <-> 4: st1.s2");
    }

    @Test
    public void testPredicateScan() throws Exception {
        String sql = "select st3.sa3 from sc0 where st4.ss3.s31 = 1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "ColumnAccessPath: [/st3/sa3, /st4/ss3/s31]");
        assertContains(plan, "PredicateAccessPath: [/st4/ss3/s31]");
    }

    @Test
    public void testCTEInlinePruneColumn2() throws Exception {
        String sql = "with t1 as (select * from sc0) " +
                "select x1.st1.s1, x2.st2.s2 from t1 x1 join " +
                "(select v1, st1, st2 from t1 group by v1, st1,st2) x2 on x1.v1 = x2.v1";
        String plan;
        try {
            connectContext.getSessionVariable().setCboCTERuseRatio(10000000);
            plan = getVerboseExplain(sql);
        } finally {
            connectContext.getSessionVariable().setCboCTERuseRatio(1.15);
        }
        assertContains(plan, "  7:Project\n" +
                "  |  output columns:\n" +
                "  |  15 <-> [15: v1, BIGINT, true]\n" +
                "  |  25 <-> 17: st2.s2\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  6:AGGREGATE");
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  8 <-> [8: v1, BIGINT, true]\n" +
                "  |  24 <-> 9: st1.s1\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode");
        assertContains(plan, "ColumnAccessPath: [/st1/s1]");
        assertNotContains(plan, "ColumnAccessPath: [/st2/s2]");
    }

    @Test
    public void testCTEInlinePruneColumn3() throws Exception {
        String sql = "with t1 as (select * from sc0) " +
                "select x1.st1.s1, x2.st2.s2 from t1 x1 join " +
                "(select v1, st1, st2 from t1 group by v1, st1,st2) x2 on x1.v1 = x2.v1";
        String plan;
        plan = getVerboseExplain(sql);
        assertContains(plan, "  12:Project\n" +
                "  |  output columns:\n" +
                "  |  15 <-> [15: v1, BIGINT, true]\n" +
                "  |  25 <-> 17: st2.s2\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  11:AGGREGATE");
        assertContains(plan, "  Output Exprs:1: v1 | 2: st1 | 3: st2 | 26: expr\n" +
                "  Input Partition: RANDOM\n" +
                "  MultiCastDataSinks:");
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  1 <-> [1: v1, BIGINT, true]\n" +
                "  |  2 <-> [2: st1, struct<s1 int(11), s2 int(11)>, true]\n" +
                "  |  3 <-> [3: st2, struct<s1 int(11), s2 int(11), sm3 map<int(11),int(11)>>, true]\n" +
                "  |  26 <-> 2: st1.s1\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode");
        assertNotContains(plan, "ColumnAccessPath:");
    }

    @Test
    public void testNullPredicateOnOuterJoin() throws Exception {
        String sql = "select st1.s1, st3 is null from t0" +
                " left join " +
                "sc0 x on x.v1 = t0.v1";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  6:Project\n" +
                "  |  output columns:\n" +
                "  |  10 <-> [12: expr, INT, true]\n" +
                "  |  11 <-> 6: st3 IS NULL\n" +
                "  |  hasNullableGenerateChild: true\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  5:HASH JOIN");
        assertContains(plan, "  1:Project\n" +
                "  |  output columns:\n" +
                "  |  3 <-> [3: v1, BIGINT, true]\n" +
                "  |  6 <-> [6: st3, struct<s1 int(11), s2 int(11), sa3 array<int(11)>>, true]\n" +
                "  |  12 <-> 4: st1.s1\n" +
                "  |  cardinality: 1\n" +
                "  |  \n" +
                "  0:OlapScanNode");
        assertContains(plan, "ColumnAccessPath: [/st1/s1]");
    }

    @Test
    public void testNoEqualsJoin() throws Exception {
        String sql = "select st2.s2, st4.ss3.s31 from t0" +
                " left join " +
                "sc0 x on x.st3.s1 + 1 >= t0.v1 + 2";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  5:NESTLOOP JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN\n" +
                "  |  other join predicates: cast([14: expr, INT, true] as BIGINT) + 1 >= [1: v1, BIGINT, true] + 2");
        assertContains(plan, "ColumnAccessPath: [/st2/s2, /st3/s1, /st4/ss3/s31]");
    }

    @Test
    public void testNoEqualsJoinHaving() throws Exception {
        String sql = "select st2.s2, st4.ss3.s31 from t0" +
                " left join " +
                "sc0 x on x.st3.s1 + 1 >= t0.v1 + 2 where x.st1.s2 = 2";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "  5:NESTLOOP JOIN\n" +
                "  |  join op: RIGHT OUTER JOIN\n" +
                "  |  other join predicates: cast([14: expr, INT, true] as BIGINT) + 1 >= [1: v1, BIGINT, true] + 2\n" +
                "  |  other predicates: [15: expr, INT, true] = 2");
        assertContains(plan, "ColumnAccessPath: [/st1/s2, /st2/s2, /st3/s1, /st4/ss3/s31]");
    }

    @Test
    public void testExprRefMultipleTableCols() throws Exception {
        String sql = "select t.c1, t.c2.s1 from (select array_map(x -> (x + t.v1), t.a1) c1, t.st1 c2 from " +
                "(select pc0.a1, sc0.* from pc0 join sc0) t) t join pc0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "1:Project\n" +
                "  |  <slot 8> : 8: v1\n" +
                "  |  <slot 27> : 9: st1.s1",
                "5:Project\n" +
                        "  |  <slot 16> : array_map(<slot 15> -> CAST(<slot 15> AS BIGINT) + 8: v1, 7: a1)\n" +
                        "  |  <slot 27> : 27: expr");
    }

    @Test
    public void testLiteralArrayPredicates() throws Exception {
        {
            String sql = "select pc0.a1 from pc0 where (([]) is not NULL)";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     table: pc0, rollup: pc0\n" +
                    "     preAggregation: on\n" +
                    "     Predicates: array_length([]) IS NOT NULL\n" +
                    "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                    "     tabletList=\n" +
                    "     actualRows=0, avgRowSize=1.0\n" +
                    "     Pruned type: 7 <-> [ARRAY<INT>]\n" +
                    "     cardinality: 1");

        }
        {
            String sql = "select st3.sa3, array_length(st3.sa3) from sc0 where (([1,2,3]) is NOT NULL)";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     table: sc0, rollup: sc0\n" +
                    "     preAggregation: on\n" +
                    "     Predicates: array_length([1,2,3]) IS NOT NULL\n" +
                    "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                    "     tabletList=\n" +
                    "     actualRows=0, avgRowSize=3.0\n" +
                    "     Pruned type: 4 <-> [struct<s1 int(11), s2 int(11), sa3 array<int(11)>>]\n" +
                    "     ColumnAccessPath: [/st3/sa3]\n" +
                    "     cardinality: 1\n");
        }
    }

    @Test
    public void testCommonPathMerge() throws Exception {
        {
            String sql = "select pc0.a1[0],pc0.a1[1] from pc0 where (([]) is not NULL)";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     table: pc0, rollup: pc0\n" +
                    "     preAggregation: on\n" +
                    "     Predicates: array_length([]) IS NOT NULL\n" +
                    "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                    "     tabletList=\n" +
                    "     actualRows=0, avgRowSize=3.0\n" +
                    "     Pruned type: 7 <-> [ARRAY<INT>]\n" +
                    "     ColumnAccessPath: [/a1/INDEX]\n" +
                    "     cardinality: 1");
        }
        {
            String sql = "select st3.sa3[0], array_length(st3.sa3) from sc0 where (([1,2,3]) is NOT NULL)";
            String plan = getVerboseExplain(sql);
            assertContains(plan, "  0:OlapScanNode\n" +
                    "     table: sc0, rollup: sc0\n" +
                    "     preAggregation: on\n" +
                    "     Predicates: array_length([1,2,3]) IS NOT NULL\n" +
                    "     partitionsRatio=0/1, tabletsRatio=0/0\n" +
                    "     tabletList=\n" +
                    "     actualRows=0, avgRowSize=3.0\n" +
                    "     Pruned type: 4 <-> [struct<s1 int(11), s2 int(11), sa3 array<int(11)>>]\n" +
                    "     ColumnAccessPath: [/st3/sa3/ALL]\n" +
                    "     cardinality: 1\n");
        }
    }

    @Test
    public void testSubfieldWithoutCols() throws Exception {
        String sql = "select [1, 2, 3] is null from pc0 t1 right join sc0 t2 on t1.v1 = t2.v1;";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "5:Project\n" +
                "  |  <slot 15> : array_length([1,2,3]) IS NULL");

        sql = "select [1, 2, 3][1] is null from pc0 t1 right join sc0 t2 on t1.v1 = t2.v1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "5:Project\n" +
                "  |  <slot 15> : [1,2,3][1] IS NULL");

        sql = "select map_keys(map{'a':1,'b':2}) is null from pc0 t1 right join sc0 t2 on t1.v1 = t2.v1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "5:Project\n" +
                "  |  <slot 15> : array_length(map_keys(map{'a':1,'b':2})) IS NULL");

        sql = "select row(1,2,3).col2 is null from pc0 t1 right join sc0 t2 on t1.v1 = t2.v1;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "5:Project\n" +
                "  |  <slot 15> : row(1, 2, 3).col2 IS NULL");
    }



    @Test
    public void testForceReuseCTE1() throws Exception {
        String sql = "with cte1 as (select array_map((x -> uuid()), t.a1) c1, t.map1 c2 from pc0 t) " +
                "select * from " +
                "(select * from cte1) t1 join " +
                "(select * from cte1) t2 on t1.c1=t2.c1 ";
        assertContainsCTEReuse(sql);
    }

    @Test
    public void testForceReuseCTE2() throws Exception {
        String sql = "with cte1 as (select rand() as c1, t.map1 c2 from pc0 t) " +
                "select * from " +
                "(select * from cte1) t1 join " +
                "(select * from cte1) t2 on t1.c1=t2.c1 ";
        assertContainsCTEReuse(sql);
    }

    @Test
    public void testForceReuseCTE3() throws Exception {
        String sql = "with cte1 as (select random() as c1, t.map1 c2 from pc0 t) " +
                "select * from " +
                "(select * from cte1) t1 join " +
                "(select * from cte1) t2 on t1.c1=t2.c1 ";
        assertContainsCTEReuse(sql);
    }

    @Test
    public void testForceReuseCTE4() throws Exception {
        String sql = "with cte1 as (select v1 as c1, t.map1 c2 from pc0 t where rand() < 0.5) " +
                "select * from " +
                "(select * from cte1) t1 join " +
                "(select * from cte1) t2 on t1.c1=t2.c1 ";
        assertContainsCTEReuse(sql);
    }

    @Test
    public void testForceReuseCTE5() throws Exception {
        String sql = "with cte1 as (select rand() as c1, t.map1 c2 from pc0 t), " +
                "cte2 as (select c1, count(1) as c11 from cte1 group by c1)" +
                "select * from " +
                "(select * from cte1) t1 join " +
                "(select * from cte2) t2 on t1.c1=t2.c1 ";
        assertContainsCTEReuse(sql);
    }

    @Test
    public void testForceReuseCTE6() throws Exception {
        String sql = "with cte1 as (select c1 + 1 as c1, c2 from " +
                "   (select v1 as c1, t.map1 c2 from pc0 t where rand() < 0.5) t2) " +
                "select * from " +
                "(select * from cte1) t1 join " +
                "(select * from cte1) t2 on t1.c1=t2.c1 ";
        assertContainsCTEReuse(sql);
    }

    @Test
    public void testForceReuseCTE7() throws Exception {
        String sql = "with cte1 as (select c1 + 1 as c1, c2 from " +
                "   (select rand() as c1, t.map1 c2 from pc0 t) t2) " +
                "select * from " +
                "(select * from cte1) t1 join " +
                "(select * from cte1) t2 on t1.c1=t2.c1 ";
        assertContainsCTEReuse(sql);
    }

    @Test
    public void testForceReuseCTE8() throws Exception {
        String sql = "with cte1 as (select rank() over(order by c1) as c1, c2 from " +
                "   (select rand() as c1, t.map1 c2 from pc0 t) t2) " +
                "select * from " +
                "(select * from cte1) t1 join " +
                "(select * from cte1) t2 on t1.c1=t2.c1 ";
        assertContainsCTEReuse(sql);
    }

    @Test
    public void testForceReuseCTE9() throws Exception {
        String sql = "with cte1 as (select rand() as c1, t.map1 c2 from pc0 t), " +
                "cte2 as (select a.c1, b.c2 from cte1 as a join cte1 as b on a.c1 = b.c1)" +
                "select * from " +
                "(select * from cte2) t1 join " +
                "(select * from cte2) t2 on t1.c1=t2.c1 ";
        assertContainsCTEReuse(sql);
    }

    @Test
    public void testForceReuseCTE10() throws Exception {
        String sql = "with cte1 as (select rand() as c1, t.map1 c2 from pc0 t) " +
                "select * from cte1 union all " +
                "select * from cte1";
        assertContainsCTEReuse(sql);
    }

    @Test
    public void testForceReuseCTE11() throws Exception {
        String sql = "with cte1 as (select t.v1, sum(t.v1) from pc0 t group by t.v1 having rand() > 0.5) " +
                "select * from cte1 union all " +
                "select * from cte1";
        assertContainsCTEReuse(sql);
    }

    @Test
    public void testArrayIndexStruct() throws Exception {
        String sql = "select ass[1].a, ass[1].b from tt;";
        String plan = getVerboseExplain(sql);
        assertContains(plan, "[/ass/INDEX/a, /ass/INDEX/b]");
    }
}
