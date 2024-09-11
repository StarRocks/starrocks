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

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class MaterializedViewWithPartitionTest extends MaterializedViewTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MaterializedViewTestBase.beforeClass();

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

        starRocksAssert.withTable("create table test_base_part3(c1 int, c2 bigint, c3 date, c4 bigint)" +
                " partition by range(c3) (" +
                " partition p1 values less than (\"20240603\")," +
                " partition p2 values less than (\"20240604\")," +
                " partition p3 values less than (\"20240605\")," +
                " PARTITION p4 values less than (\"20240606\")," +
                " PARTITION p5 values less than (\"20240607\"))" +
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
        sql("select c1, c3, c2 from test_base_part where c3 < 2000")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=4/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=8/8");

        // test union all
        sql("select c1, c3, c2 from test_base_part")
                .contains("UNION")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=4/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=8/8")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: (10: c3 >= 2000) OR (10: c3 IS NULL)\n" +
                        "     partitions=2/5");

        // test union all
        sql("select c1, c3, c2 from test_base_part where c3 < 3000")
                .contains("UNION")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=4/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=8/8")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/5");

        // test query delta
        sql("select c1, c3, c2 from test_base_part where c3 < 1000")
                .contains("partial_mv_6")
                .contains("PREDICATES: 6: c3 < 1000\n" +
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
        sql("select c1, c3, c2 from test_base_part where c3 < 2000")
                .contains("partial_mv_6")
                .contains("partitions=1/1");

        // test union
        sql("select c1, c3, c2 from test_base_part")
                .contains("UNION")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: partial_mv_6")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/5\n" +
                        "     rollup: test_base_part\n" +
                        "     tabletRatio=2/2");

        // test query predicate
        // TODO: add more post actions after MV Rewrite:
        sql("select c1, c3, c2 from test_base_part where c3 < 1000")
                .contains("partial_mv_6")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 6: c3 < 1000");

        starRocksAssert.dropMaterializedView("partial_mv_6");
    }

    // mv's partition columns is the same with the base table, mv table has no partition filter.
    @Test
    public void testPartitionPrune_SingleTable3() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        connectContext.getSessionVariable().setMaterializedViewUnionRewriteMode(1);

        String partial_mv_6 = "create materialized view partial_mv_6" +
                " partition by c3" +
                " distributed by hash(c1) as" +
                " select c1, c3, c2 from test_base_part where c2 < 2000;";
        starRocksAssert.withMaterializedView(partial_mv_6);
        refreshMaterializedView(MATERIALIZED_DB_NAME, "partial_mv_6");

        // test match all
        sql("select c1, c3, c2 from test_base_part where c2 < 2000")
                .contains("partial_mv_6")
                .contains("partitions=5/5");

        // test query delta: with more predicates
        sql("select c1, c3, c2 from test_base_part where c2 < 2000 and c3 < 2000")
                .contains("partial_mv_6")
                .contains("partitions=4/5");

        // test union all
        sql("select c1, c3, c2 from test_base_part")
                .contains("UNION")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=5/5")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 9: c2 >= 2000\n" +
                        "     partitions=5/5");

        sql("select c1, c3, c2 from test_base_part where c2 < 3000 and c3 < 3000")
                .contains("partial_mv_6")
                .contains("     TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 10: c3 < 3000, 9: c2 < 3000, 9: c2 >= 2000\n" +
                        "     partitions=5/5");
        // test query delta
        sql("select c1, c3, c2 from test_base_part where c2 < 1000 and c3 < 1000")
                .contains("partial_mv_6")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 7: c2 < 1000\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=6/6");

        starRocksAssert.dropMaterializedView("partial_mv_6");
        connectContext.getSessionVariable().setMaterializedViewUnionRewriteMode(0);
    }

    // MV has multi tables and partition columns.
    @Test
    public void testPartitionPrune_MultiTables1() throws Exception {
        connectContext.getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        connectContext.getSessionVariable().setMaterializedViewUnionRewriteMode(1);

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
        sql(
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
        sql(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 2000 and t2.c3 > 100;")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 11: c3 > 100\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=6/6");

        // test query delta
        sql(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 1000;")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 11: c3 < 1000\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=6/6");

        // test query delta, agg + join
        sql(
                " select count(1) from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 1000;")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 12: c3 < 1000\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_6\n" +
                        "     tabletRatio=6/6");

        // TODO: MV can be rewritten but cannot bingo(because cost?)!
        // test union all
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        sql(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 3000")
                .contains("UNION")
                .contains("7:OlapScanNode\n" +
                        "     TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=4/5");
        sql(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1=t2.c1 and t1.c3=t2.c3 ")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 15: c1 IS NOT NULL, 16: c3 IS NOT NULL\n" +
                        "     partitions=1/5")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 15: c1 IS NOT NULL, 16: c3 IS NOT NULL\n" +
                        "     partitions=1/5");

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        connectContext.getSessionVariable().setMaterializedViewUnionRewriteMode(0);
        starRocksAssert.dropMaterializedView("partial_mv_6");
    }

    // MV has no partitions and has joins.
    @Test
    public void testPartitionPrune_MultiTables2() throws Exception {
        starRocksAssert.getCtx().getSessionVariable().setEnableMaterializedViewUnionRewrite(true);
        connectContext.getSessionVariable().setMaterializedViewUnionRewriteMode(1);
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
        sql(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 2000")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1\n" +
                        "     rollup: partial_mv_6");

        // test query delta
        sql(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 2000 and t2.c3 > 100;")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 11: c3 > 100, 11: c3 < 2000\n" +
                        "     partitions=1/1");

        // test query delta
        sql(
                " select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 1000;")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 11: c3 < 1000\n" +
                        "     partitions=1/1");

        // test query delta, agg + join
        sql(
                " select count(1) from test_base_part t1 \n" +
                        " inner join test_base_part2 t2 \n" +
                        " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                        " where t1.c3 < 1000;")
                .contains("TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 12: c3 < 1000\n" +
                        "     partitions=1/1");

        // test union all
        connectContext.getSessionVariable().setMaterializedViewRewriteMode("force");
        sql(" select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                " inner join test_base_part2 t2 \n" +
                " on t1.c1 = t2.c1 and t1.c3=t2.c3 \n" +
                " where t1.c3 < 3000")
                .contains("UNION")
                .contains("7:OlapScanNode\n" +
                        "     TABLE: partial_mv_6\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=1/1");

        sql(" select t1.c1, t1.c3, t2.c2 from test_base_part t1 \n" +
                " inner join test_base_part2 t2 \n" +
                " on t1.c1=t2.c1 and t1.c3=t2.c3 ")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 15: c1 IS NOT NULL, 16: c3 IS NOT NULL\n" +
                        "     partitions=1/5\n" +
                        "     rollup: test_base_part")
                .contains("TABLE: test_base_part2\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 19: c1 IS NOT NULL, 21: c3 IS NOT NULL\n" +
                        "     partitions=1/5");

        connectContext.getSessionVariable().setMaterializedViewRewriteMode("default");
        connectContext.getSessionVariable().setMaterializedViewUnionRewriteMode(0);
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
        sql("select c1, c3, c2 from test_base_part")
                .contains("UNION")
                .contains("     TABLE: partial_mv_7\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=4/5\n" +
                        "     rollup: partial_mv_7\n" +
                        "     tabletRatio=8/8")
                .contains("TABLE: test_base_part");

        // match all
        sql("select c1, c3, c2 from test_base_part where c3 < 2000 and c1 = 1")
                .contains("TABLE: partial_mv_7\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=4/5\n" +
                        "     rollup: partial_mv_7\n" +
                        "     tabletRatio=4/8");

        // query delta: with more partition predicates
        sql("select c1, c3, c2 from test_base_part where c3 < 1000 and c1 = 1")
                .contains("TABLE: partial_mv_7\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_7\n" +
                        "     tabletRatio=3/6");

        // no match
        sql("select c1, c3, c2 from test_base_part where c3 < 1000")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 3: c3 < 1000\n" +
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
        sql("select c1, c3, c2 from test_base_part where c2 > 10")
                .contains("TABLE: partial_mv_8\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     partitions=5/5\n" +
                        "     rollup: partial_mv_8\n" +
                        "     tabletRatio=10/10");

        // test partition prune
        sql("select c1, c3, c2 from test_base_part where c2 > 10 and c3 < 1000")
                .contains("TABLE: partial_mv_8\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 6: c3 < 1000\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_8\n" +
                        "     tabletRatio=6/6");

        // test distribution prune
        sql("select c1, c3, c2 from test_base_part where c2 > 10 and c1 = 1")
                .contains("TABLE: partial_mv_8\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: c1 = 1\n" +
                        "     partitions=5/5\n" +
                        "     rollup: partial_mv_8\n" +
                        "     tabletRatio=5/10");

        // test partition and distribution prune
        sql("select c1, c3, c2 from test_base_part where c2 > 10 and c3 < 1000 and c1 = 1")
                .contains("TABLE: partial_mv_8\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: c1 = 1\n" +
                        "     partitions=3/5\n" +
                        "     rollup: partial_mv_8\n" +
                        "     tabletRatio=3/6");

        // test query delta
        sql("select c1, c3, c2 from test_base_part where c2 > 100 and c3 < 1000 and c1 = 1")
                .contains("TABLE: partial_mv_8\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 5: c1 = 1, 7: c2 > 100\n" +
                        "     partitions=3/5");

        // test non match
        sql("select c1, c3, c2 from test_base_part")
                .contains("TABLE: test_base_part\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: 9: c2 <= 10\n" +
                        "     partitions=5/5");

        starRocksAssert.dropMaterializedView("partial_mv_8");
    }

    @Test
    public void testPartitionPrune_WithFunc() throws Exception {
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `partial_mv_14`\n" +
                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                "PARTITION BY (`c3`)\n" +
                "REFRESH MANUAL\n" +
                "AS SELECT `c3`, sum(`c4`) AS `total`\n" +
                "FROM `test_mv`.`test_base_part3`\n" +
                "GROUP BY `c3`;");
        refreshMaterializedView(MATERIALIZED_DB_NAME, "partial_mv_14");

        sql("select c3, sum(c4) from test_base_part3 where date_format(c3,'%Y%m%d')='20240602' group by c3")
                .contains("TABLE: partial_mv_14\n" +
                        "     PREAGGREGATION: ON\n" +
                        "     PREDICATES: col$: c3 = '2024-06-02'\n" +
                        "     partitions=1/5");
        starRocksAssert.dropMaterializedView("partial_mv_14");
    }

    @Test
    public void testPartitionWithNull() throws Exception {
        {
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `partial_mv_9`\n" +
                    "COMMENT \"MATERIALIZED_VIEW\"\n" +
                    "PARTITION BY (`c3`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 6\n" +
                    "REFRESH MANUAL\n" +
                    "AS SELECT `test_base_part`.`c1`, `test_base_part`.`c3`, sum(`test_base_part`.`c4`) AS `total`\n" +
                    "FROM `test_mv`.`test_base_part`\n" +
                    "WHERE `test_base_part`.`c3` < 1000\n" +
                    "GROUP BY `test_base_part`.`c3`, `test_base_part`.`c1`;");

            String query = "select c1, c3, sum(c4) from test_base_part group by c1, c3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "partial_mv_9", "PREDICATES: (10: c3 >= 1000) OR (10: c3 IS NULL)");
            starRocksAssert.dropMaterializedView("partial_mv_9");
        }

        {
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `partial_mv_11`\n" +
                    "COMMENT \"MATERIALIZED_VIEW\"\n" +
                    "PARTITION BY (`c3`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 6\n" +
                    "REFRESH MANUAL\n" +
                    "AS SELECT `test_base_part`.`c1`, `test_base_part`.`c3`, sum(`test_base_part`.`c4`) AS `total`\n" +
                    "FROM `test_mv`.`test_base_part`\n" +
                    "WHERE `test_base_part`.`c3` < 1000\n" +
                    "GROUP BY `test_base_part`.`c3`, `test_base_part`.`c1`;");

            String query = "select c1, c3, sum(c4) from test_base_part where c3 < 2000 group by c1, c3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "partial_mv_11", "TABLE: test_base_part\n" +
                    "     PREAGGREGATION: ON\n" +
                    "     partitions=1/5");
            PlanTestBase.assertNotContains(plan, "10: c3 IS NULL");
            starRocksAssert.dropMaterializedView("partial_mv_11");
        }

        {
            starRocksAssert.withTable("create table test_base_part_not_null(c1 int, c2 bigint, c3 bigint not null, c4 bigint)" +
                    " partition by range(c3) (" +
                    " partition p1 values less than (\"100\")," +
                    " partition p2 values less than (\"200\")," +
                    " partition p3 values less than (\"1000\")," +
                    " PARTITION p4 values less than (\"2000\")," +
                    " PARTITION p5 values less than (\"3000\"))" +
                    " distributed by hash(c1)" +
                    " properties (\"replication_num\"=\"1\");");

            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `partial_mv_10`\n" +
                    "COMMENT \"MATERIALIZED_VIEW\"\n" +
                    "PARTITION BY (`c3`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 6\n" +
                    "REFRESH MANUAL\n" +
                    "AS SELECT `c1`, `c3`, sum(`c4`) AS `total`\n" +
                    "FROM `test_mv`.`test_base_part_not_null`\n" +
                    "WHERE `c3` < 1000\n" +
                    "GROUP BY `c3`, `c1`;");

            String query = "select c1, c3, sum(c4) from test_base_part_not_null group by c1, c3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "partial_mv_10", "partitions=2/5");
            starRocksAssert.dropMaterializedView("partial_mv_10");
        }

        {
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `partial_mv_12`\n" +
                    "COMMENT \"MATERIALIZED_VIEW\"\n" +
                    "PARTITION BY (`c3`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 6\n" +
                    "REFRESH MANUAL\n" +
                    "AS SELECT `test_base_part`.`c1`, `test_base_part`.`c3`, sum(`test_base_part`.`c4`) AS `total`\n" +
                    "FROM `test_mv`.`test_base_part`\n" +
                    "WHERE `test_base_part`.`c3` is null\n" +
                    "GROUP BY `test_base_part`.`c3`, `test_base_part`.`c1`;");

            String query = "select c1, c3, sum(c4) from test_base_part group by c1, c3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "partial_mv_12", "PREDICATES: 10: c3 IS NOT NULL");
            starRocksAssert.dropMaterializedView("partial_mv_12");
        }

        {
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW `partial_mv_13`\n" +
                    "COMMENT \"MATERIALIZED_VIEW\"\n" +
                    "PARTITION BY (`c3`)\n" +
                    "DISTRIBUTED BY HASH(`c1`) BUCKETS 6\n" +
                    "REFRESH MANUAL\n" +
                    "AS SELECT `test_base_part`.`c1`, `test_base_part`.`c3`, sum(`test_base_part`.`c4`) AS `total`\n" +
                    "FROM `test_mv`.`test_base_part`\n" +
                    "WHERE `test_base_part`.`c3` is not null\n" +
                    "GROUP BY `test_base_part`.`c3`, `test_base_part`.`c1`;");

            String query = "select c1, c3, sum(c4) from test_base_part group by c1, c3;";
            String plan = getFragmentPlan(query);
            PlanTestBase.assertContains(plan, "partial_mv_13", "PREDICATES: 10: c3 IS NULL");
            starRocksAssert.dropMaterializedView("partial_mv_13");
        }
    }
}
