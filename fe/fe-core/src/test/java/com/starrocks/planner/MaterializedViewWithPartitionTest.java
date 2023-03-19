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

package com.starrocks.planner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

public class MaterializedViewWithPartitionTest extends MaterializedViewTestBase {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewWithPartitionTest.class);

    @BeforeClass
    public static void setUp() throws Exception {
        MaterializedViewTestBase.setUp();

        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        starRocksAssert.withTable("create table test_base_part(c1 int, c2 bigint, c3 bigint, c4 bigint)" +
                " partition by range(c3) (" +
                " partition p1 values less than (\"100\")," +
                " partition p2 values less than (\"200\")," +
                " partition p3 values less than (\"1000\")," +
                " PARTITION p4 values less than (\"2000\")," +
                " PARTITION p5 values less than (\"3000\"))" +
                " distributed by hash(c1)" +
                " properties (\"replication_num\"=\"1\");");

        starRocksAssert.withTable("create table test_base_part2(c1 int, c2 bigint, c3 bigint, c4 bigint)" +
                " partition by range(c3) (" +
                " partition p1 values less than (\"100\")," +
                " partition p2 values less than (\"200\")," +
                " partition p3 values less than (\"1000\")," +
                " PARTITION p4 values less than (\"2000\")," +
                " PARTITION p5 values less than (\"3000\"))" +
                " distributed by hash(c1)" +
                " properties (\"replication_num\"=\"1\");");
    }

    // MV's partition columns is the same with the base table, and mv has partition filter predicates.
    @Test
    public void testPartitionPrune_SingleTable1() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);

        String partial_mv_6 = "create materialized view partial_mv_6" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, c2 from test_base_part where c3 < 2000;";
        starRocksAssert.withMaterializedView(partial_mv_6);
        refreshMaterializedView(MATERIALIZED_DB_NAME, "partial_mv_6");

        // test match
        testRewriteOK("select c1, c3, c2 from test_base_part where c3 < 2000")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=4/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=8/8");

        // test union all
        testRewriteOK("select c1, c3, c2 from test_base_part")
                .contains("UNION")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=4/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=8/8")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/5\n" +
                        "     rollup: test_base_part\n" +
                        "     tabletRatio=2/2");

        // test union all
        testRewriteOK("select c1, c3, c2 from test_base_part where c3 < 3000")
                .contains("UNION")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=4/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=8/8")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/5\n" +
                        "     rollup: test_base_part\n" +
                        "     tabletRatio=2/2");

        // test query delta
        testRewriteOK("select c1, c3, c2 from test_base_part where c3 < 1000")
                .contains("partial_mv_6")
                .contains("PREDICATES: 6: c3 <= 999\n" +
                        "     partitions=3/5");

        starRocksAssert.dropMaterializedView("partial_mv_6");
    }

    // MV has no partitions and base table has partitions.
    @Test
    public void testPartitionPrune_SingleTable2() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);

        String partial_mv_6 = "create materialized view partial_mv_6" +
                " distributed by hash(c1) as" +
                " select c1, c3, c2 from test_base_part where c3 < 2000;";
        starRocksAssert.withMaterializedView(partial_mv_6);
        refreshMaterializedView(MATERIALIZED_DB_NAME, "partial_mv_6");

        // test match all
        testRewriteOK("select c1, c3, c2 from test_base_part where c3 < 2000")
                .contains("partial_mv_6")
                .contains("partitions=1/1");

        // test union
        testRewriteOK("select c1, c3, c2 from test_base_part")
                .contains("UNION")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 6: c3 < 2000\n" +
                        "     partitions=1/1\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=2/2")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/5\n" +
                        "     rollup: test_base_part\n" +
                        "     tabletRatio=2/2");

        // test query predicate
        // TODO: add more post actions after MV Rewrite:
        testRewriteOK("select c1, c3, c2 from test_base_part where c3 < 1000")
                .contains("partial_mv_6")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 6: c3 <= 999, 6: c3 < 2000\n" +
                        "     partitions=1/1\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=2/2");

        starRocksAssert.dropMaterializedView("partial_mv_6");
    }

    // mv's partition columns is the same with the base table, mv table has no partition filter.
    @Test
    public void testPartitionPrune_SingleTable3() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);

        String partial_mv_6 = "create materialized view partial_mv_6" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, c2 from test_base_part where c2 < 2000;";
        starRocksAssert.withMaterializedView(partial_mv_6);
        refreshMaterializedView(MATERIALIZED_DB_NAME, "partial_mv_6");

        // test match all
        testRewriteOK("select c1, c3, c2 from test_base_part where c2 < 2000")
                .contains("partial_mv_6")
                .contains("partitions=5/5");

        // test query delta: with more predicates
        testRewriteOK("select c1, c3, c2 from test_base_part where c2 < 2000 and c3 < 2000")
                .contains("partial_mv_6")
                .contains("partitions=4/5");

        // test union all
        testRewriteOK("select c1, c3, c2 from test_base_part")
                .contains("UNION")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=5/5")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 9: c2 >= 2000\n" +
                        "     partitions=5/5\n" +
                        "     rollup: test_base_part");

        // test union all
        testRewriteOK("select c1, c3, c2 from test_base_part where c2 < 3000 and c3 < 3000")
                .contains("UNION")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=5/5\n" +
                        "     rollup: partial_mv_6")
                .contains("PREDICATES: 9: c2 < 3000, 9: c2 > 1999\n" +
                        "     partitions=5/5\n" +
                        "     rollup: test_base_part");

        // test query delta
        // TODO: support remove redundant predicates.
        testRewriteOK("select c1, c3, c2 from test_base_part where c2 < 1000 and c3 < 1000")
                .contains("partial_mv_6")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 7: c2 <= 999, 6: c3 <= 999\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=6/6");

        starRocksAssert.dropMaterializedView("partial_mv_6");
    }

    // MV has multi tables and partition columns.
    @Test
    public void testPartitionPrune_MultiTables1() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);

        // test partition prune
        String partial_mv_6 = "create materialized view partial_mv_6" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select t1.c1, t2.c2, t1.c3, t2.c1 as c21, t2.c2 as c22, t2.c3 as c23 from test_base_part t1 " +
                " inner join test_base_part2 t2" +
                " on t1.c1 = t2.c1 and t1.c3=t2.c3" +
                " where t1.c3 < 2000;";
        starRocksAssert.withMaterializedView(partial_mv_6);
        refreshMaterializedView(MATERIALIZED_DB_NAME, "partial_mv_6");

        // exactly match
        testRewriteOK(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 2000")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=4/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=8/8");

        // test query delta
        testRewriteOK(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                " inner join test_base_part2 t2 \n" +
                " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                " where t1.c3 < 2000 and t2.c3 > 100;")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 11: c3 <= 1999, 11: c3 >= 101\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=6/6");

        // test query delta
        testRewriteOK(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 1000;")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 11: c3 <= 999\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=6/6");

        // test query delta, agg + join
        testRewriteOK(
                " select count(1) from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 1000;")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 12: c3 <= 999\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=6/6");

        // test union all
        // TODO: MV can be rewritten but cannot bingo(because cost?)!
        testRewriteOK(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 3000")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=5/5\n" +
                        "     rollup: test_base_part\n" +
                        "     tabletRatio=10/10")
                .contains("TABLE: test_base_part2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=5/5\n" +
                        "     rollup: test_base_part2\n" +
                        "     tabletRatio=10/10");

        testRewriteOK(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1=t2.c1 and t1.c3=t2.c3 ")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 1: c1 IS NOT NULL, 3: c3 IS NOT NULL\n" +
                        "     partitions=5/5\n" +
                        "     rollup: test_base_part\n" +
                        "     tabletRatio=10/10")
                .contains("TABLE: test_base_part2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: c1 IS NOT NULL, 7: c3 IS NOT NULL\n" +
                        "     partitions=5/5\n" +
                        "     rollup: test_base_part2\n" +
                        "     tabletRatio=10/10");

        starRocksAssert.dropMaterializedView("partial_mv_6");
    }

    // MV has no partitions and has joins.
    @Test
    public void testPartitionPrune_MultiTables2() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);

        // test partition prune
        String partial_mv_6 = "create materialized view partial_mv_6" +
                " distributed by hash(c1) as" +
                " select t1.c1, t2.c2, t1.c3, t2.c1 as c21, t2.c2 as c22, t2.c3 as c23 from test_base_part t1 " +
                " inner join test_base_part2 t2" +
                " on t1.c1 = t2.c1 and t1.c3=t2.c3" +
                " where t1.c3 < 2000;";
        starRocksAssert.withMaterializedView(partial_mv_6);
        refreshMaterializedView(MATERIALIZED_DB_NAME, "partial_mv_6");

        // exactly match
        testRewriteOK(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 2000")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 11: c3 < 2000\n" +
                        "     partitions=1/1\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=2/2");

        // test query delta
        testRewriteOK(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 2000 and t2.c3 > 100;")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 11: c3 <= 1999, 11: c3 >= 100, 11: c3 >= 101, 11: c3 < 2000\n" +
                        "     partitions=1/1\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=2/2");

        // test query delta
        testRewriteOK(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 1000;")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 11: c3 <= 999, 11: c3 < 2000\n" +
                        "     partitions=1/1\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=2/2");

        // test query delta, agg + join
        testRewriteOK(
                " select count(1) from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 1000;")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 12: c3 <= 999, 12: c3 < 2000\n" +
                        "     partitions=1/1\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=2/2");

        // test union all
        // TODO: MV can be rewritten but cannot bingo(because cost?)!
        testRewriteOK(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 3000")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=5/5\n" +
                        "     rollup: test_base_part\n" +
                        "     tabletRatio=10/10")
                .contains("TABLE: test_base_part2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=5/5\n" +
                        "     rollup: test_base_part2\n" +
                        "     tabletRatio=10/10");

        testRewriteOK(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1=t2.c1 and t1.c3=t2.c3 ")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 1: c1 IS NOT NULL, 3: c3 IS NOT NULL\n" +
                        "     partitions=5/5\n" +
                        "     rollup: test_base_part\n" +
                        "     tabletRatio=10/10")
                .contains("TABLE: test_base_part2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: c1 IS NOT NULL, 7: c3 IS NOT NULL\n" +
                        "     partitions=5/5\n" +
                        "     rollup: test_base_part2\n" +
                        "     tabletRatio=10/10");

        starRocksAssert.dropMaterializedView("partial_mv_6");
    }

    // MV definition has partition/distribution predicates.
    @Test
    public void testBucketPrune_SingleTable1() throws Exception {
        // test bucket prune
        starRocksAssert.withMaterializedView("create materialized view partial_mv_7" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, c2 from test_base_part where c3 < 2000 and c1 = 1;");
        refreshMaterializedView(MATERIALIZED_DB_NAME, "partial_mv_7");

        // Union: MV table should be partition/distribution pruned.
        testRewriteOK("select c1, c3, c2 from test_base_part")
                .contains("UNION")
                .contains("TABLE: partial_mv_7\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: c1 = 1\n" +
                        "     partitions=4/5\n" +
                        "     rollup: partial_mv_7\n" +
                        "     tabletRatio=4/8")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: (8: c1 != 1) OR (10: c3 >= 2000)\n" +
                        "     partitions=5/5\n" +
                        "     rollup: test_base_part");

        // match all
        testRewriteOK("select c1, c3, c2 from test_base_part where c3 < 2000 and c1 = 1")
                .contains("TABLE: partial_mv_7\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: c1 = 1\n" +
                        "     partitions=4/5\n" +
                        "     rollup: partial_mv_7\n" +
                        "     tabletRatio=4/8");

        // query delta: with more partition predicates
        testRewriteOK("select c1, c3, c2 from test_base_part where c3 < 1000 and c1 = 1")
                .contains("TABLE: partial_mv_7\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 6: c3 <= 999, 5: c1 = 1\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_7\n" +
                        "     tabletRatio=3/6");

        // no match
        testRewriteOK("select c1, c3, c2 from test_base_part where c3 < 1000")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=3/5\n" +
                        "     rollup: test_base_part\n" +
                        "     tabletRatio=6/6");

        starRocksAssert.dropMaterializedView("partial_mv_7");
    }

    // MV definition has no partition/distribution predicates but query has.
    @Test
    public void testBucketPrune_SingleTable2() throws Exception {
        // c2 is not partition/distribution key column.
        starRocksAssert.withMaterializedView("create materialized view partial_mv_8" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, c2 from test_base_part where c2 > 10;");
        refreshMaterializedView(MATERIALIZED_DB_NAME, "partial_mv_8");

        // test match all
        testRewriteOK("select c1, c3, c2 from test_base_part where c2 > 10")
                .contains("TABLE: partial_mv_8\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=5/5\n" +
                        "     rollup: partial_mv_8\n" +
                        "     tabletRatio=10/10");

        // test partition prune
        testRewriteOK("select c1, c3, c2 from test_base_part where c2 > 10 and c3 < 1000")
                .contains("TABLE: partial_mv_8\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 6: c3 <= 999\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_8\n" +
                        "     tabletRatio=6/6");

        // test distribution prune
        testRewriteOK("select c1, c3, c2 from test_base_part where c2 > 10 and c1 = 1")
                .contains("TABLE: partial_mv_8\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: c1 = 1\n" +
                        "     partitions=5/5\n" +
                        "     rollup: partial_mv_8\n" +
                        "     tabletRatio=5/10");

        // test partition and distribution prune
        testRewriteOK("select c1, c3, c2 from test_base_part where c2 > 10 and c3 < 1000 and c1 = 1")
                .contains("TABLE: partial_mv_8\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: c1 = 1, 6: c3 <= 999\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_8\n" +
                        "     tabletRatio=3/6");

        // test query delta
        testRewriteOK("select c1, c3, c2 from test_base_part where c2 > 100 and c3 < 1000 and c1 = 1")
                .contains("TABLE: partial_mv_8\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 7: c2 >= 101, 5: c1 = 1, 6: c3 <= 999\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_8\n" +
                        "     tabletRatio=3/6");

        // test non match
        testRewriteOK("select c1, c3, c2 from test_base_part")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 9: c2 <= 10\n" +
                        "     partitions=5/5\n" +
                        "     rollup: test_base_part\n" +
                        "     tabletRatio=10/10");

        starRocksAssert.dropMaterializedView("partial_mv_8");
    }
}
