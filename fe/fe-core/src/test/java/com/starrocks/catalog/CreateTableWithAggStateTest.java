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

package com.starrocks.catalog;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class CreateTableWithAggStateTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        Config.enable_strict_storage_medium_check = true;
        Config.enable_auto_tablet_distribution = true;
        Config.enable_experimental_rowstore = true;
        Config.default_replication_num = 1;
        FeConstants.enablePruneEmptyOutputScan = false;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
        starRocksAssert.useDatabase("test");
        UtFrameUtils.setUpForPersistTest();
    }

    @Test
    public void testCreateTableWithAggStateAvg() {
        starRocksAssert.withTable("CREATE TABLE test_agg_tbl1(\n" +
                        "  k1 VARCHAR(10),\n" +
                        "  k6 avg(tinyint),\n" +
                        "  k7 avg(smallint),\n" +
                        "  k8 avg(int),\n" +
                        "  k9 avg(bigint),\n" +
                        "  k10 avg(largeint),\n" +
                        "  k11 avg(float),\n" +
                        "  k12 avg(double),\n" +
                        "  k13 avg(decimal(10, 0))\n" +
                        ")\n" +
                        "AGGREGATE KEY(k1)\n" +
                        "PARTITION BY (k1) \n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3;",
                () -> {
                    final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                            .getDb(connectContext.getDatabase())
                            .getTable("test_agg_tbl1");
                    String columns = table.getColumns().toString();
                    String expect = "[`k1` varchar(10) NULL COMMENT \"\", " +
                            "`k10` varbinary avg(largeint(40)) NULL COMMENT \"\", " +
                            "`k11` varbinary avg(float) NULL COMMENT \"\", " +
                            "`k12` varbinary avg(double) NULL COMMENT \"\", " +
                            "`k13` varbinary avg(decimal(10, 0)) NULL COMMENT \"\", " +
                            "`k6` varbinary avg(tinyint(4)) NULL COMMENT \"\", " +
                            "`k7` varbinary avg(smallint(6)) NULL COMMENT \"\", " +
                            "`k8` varbinary avg(int(11)) NULL COMMENT \"\", " +
                            "`k9` varbinary avg(bigint(20)) NULL COMMENT \"\"]";
                    Assert.assertEquals(expect, columns);
                    // avg_union
                    {
                        String sql = "select k1, " +
                                "avg_union(k6), avg_union(k7), avg_union(k8), avg_union(k9), avg_union(k10), avg_union(k11)," +
                                "avg_union(k12), avg_union(k13) from test_agg_tbl1 group by k1;";
                        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
                        System.out.println(plan);
                        PlanTestBase.assertContains(plan, "|  aggregate: avg_union[([9: k13, VARBINARY, true]); " +
                                "args: VARBINARY; result: VARBINARY; args nullable: true; result nullable: true], " +
                                "avg_union[([2: k6, VARBINARY, true]); args: VARBINARY; result: VARBINARY; " +
                                "args nullable: true; result nullable: true], " +
                                "avg_union[([3: k7, VARBINARY, true]); args: VARBINARY; result: " +
                                "VARBINARY; args nullable: true; result nullable: true], " +
                                "avg_union[([4: k8, VARBINARY, true]); args: VARBINARY; result: " +
                                "VARBINARY; args nullable: true; result nullable: true], " +
                                "avg_union[([5: k9, VARBINARY, true]); args: VARBINARY; result: " +
                                "VARBINARY; args nullable: true; result nullable: true], " +
                                "avg_union[([6: k10, VARBINARY, true]); args: VARBINARY; result: " +
                                "VARBINARY; args nullable: true; result nullable: true], " +
                                "avg_union[([7: k11, VARBINARY, true]); args: VARBINARY; result: " +
                                "VARBINARY; args nullable: true; result nullable: true], " +
                                "avg_union[([8: k12, VARBINARY, true]); args: VARBINARY; result: " +
                                "VARBINARY; args nullable: true; result nullable: true]");
                    }
                    // avg_merge
                    {

                        String sql = "select k1, " +
                                "avg_merge(k6), avg_merge(k7), avg_merge(k8), avg_merge(k9), avg_merge(k10), avg_merge(k11)," +
                                "avg_merge(k12), avg_merge(k13) from test_agg_tbl1 group by k1;";
                        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
                        PlanTestBase.assertContains(plan, "|  aggregate: avg_merge[([9: k13, VARBINARY, true]); " +
                                "args: VARBINARY; result: DECIMAL128(38,6); args nullable: true; result nullable: true], " +
                                "avg_merge[([2: k6, VARBINARY, true]); args: VARBINARY; result: DOUBLE; " +
                                "args nullable: true; result nullable: true], " +
                                "avg_merge[([3: k7, VARBINARY, true]); args: VARBINARY; result: DOUBLE; " +
                                "args nullable: true; result nullable: true], " +
                                "avg_merge[([4: k8, VARBINARY, true]); args: VARBINARY; result: DOUBLE; " +
                                "args nullable: true; result nullable: true], " +
                                "avg_merge[([5: k9, VARBINARY, true]); args: VARBINARY; result: DOUBLE; " +
                                "args nullable: true; result nullable: true], " +
                                "avg_merge[([6: k10, VARBINARY, true]); args: VARBINARY; result: DOUBLE; " +
                                "args nullable: true; result nullable: true], " +
                                "avg_merge[([7: k11, VARBINARY, true]); args: VARBINARY; result: DOUBLE; " +
                                "args nullable: true; result nullable: true], " +
                                "avg_merge[([8: k12, VARBINARY, true]); args: VARBINARY; result: DOUBLE; " +
                                "args nullable: true; result nullable: true]");
                    }
                });
    }

    @Test
    public void testCreateTableWithAggStateSum() {
        starRocksAssert.withTable("\n" +
                        "CREATE TABLE test_agg_tbl1(\n" +
                        "  k1 VARCHAR(10),\n" +
                        "  k2 datetime,\n" +
                        "  k6 sum(tinyint),\n" +
                        "  k7 sum(smallint),\n" +
                        "  k8 sum(int),\n" +
                        "  k9 sum(bigint),\n" +
                        "  k10 sum(largeint),\n" +
                        "  k11 sum(float),\n" +
                        "  k12 sum(double),\n" +
                        "  k13 sum(decimal(27,9))\n" +
                        ")\n" +
                        "AGGREGATE KEY(k1, k2)\n" +
                        "PARTITION BY (k1) \n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3;",
                () -> {
                    final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                            .getDb(connectContext.getDatabase())
                            .getTable("test_agg_tbl1");
                    String columns = table.getColumns().toString();
                    String expect = "[`k1` varchar(10) NULL COMMENT \"\", " +
                            "`k10` largeint(40) sum(largeint(40)) NULL COMMENT \"\", " +
                            "`k11` double sum(float) NULL COMMENT \"\", " +
                            "`k12` double sum(double) NULL COMMENT \"\", " +
                            "`k13` decimal(38, 9) sum(decimal(27, 9)) NULL COMMENT \"\", " +
                            "`k2` datetime NULL COMMENT \"\", " +
                            "`k6` bigint(20) sum(tinyint(4)) NULL COMMENT \"\", " +
                            "`k7` bigint(20) sum(smallint(6)) NULL COMMENT \"\", " +
                            "`k8` bigint(20) sum(int(11)) NULL COMMENT \"\", " +
                            "`k9` bigint(20) sum(bigint(20)) NULL COMMENT \"\"]";
                    Assert.assertEquals(expect, columns);
                });
    }

    @Test
    public void testCreateTableWithAggStateHllSketch() {
        starRocksAssert.withTable("\n" +
                        "CREATE TABLE test_agg_tbl1 (\n" +
                        "  dt VARCHAR(10),\n" +
                        "  hll_id ds_hll_count_distinct(varchar not null),\n" +
                        "  hll_province ds_hll_count_distinct(varchar),\n" +
                        "  hll_age ds_hll_count_distinct(varchar, int),\n" +
                        "  hll_dt ds_hll_count_distinct(varchar not null, int, varchar)\n" +
                        ")\n" +
                        "AGGREGATE KEY(dt)\n" +
                        "PARTITION BY (dt) \n" +
                        "DISTRIBUTED BY HASH(dt) BUCKETS 4;",
                () -> {
                    final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                            .getDb(connectContext.getDatabase())
                            .getTable("test_agg_tbl1");
                    String columns = table.getColumns().toString();
                    String expect = "[`dt` varchar(10) NULL COMMENT \"\", " +
                            "`hll_age` varbinary ds_hll_count_distinct(varchar, int(11)) NULL COMMENT \"\", " +
                            "`hll_dt` varbinary ds_hll_count_distinct(varchar, int(11), varchar) NULL COMMENT \"\", " +
                            "`hll_id` varbinary ds_hll_count_distinct(varchar) NULL COMMENT \"\", " +
                            "`hll_province` varbinary ds_hll_count_distinct(varchar) NULL COMMENT \"\"]";
                    Assert.assertEquals(expect, columns);
                });
    }

    @Test
    public void testCreateTableWithAggStateMinBy() {
        starRocksAssert.withTable("\n" +
                        "CREATE TABLE test_agg_tbl1(\n" +
                        "  k1 VARCHAR(10),\n" +
                        "  k2 min_by(datetime, date),\n" +
                        "  k6 min_by(tinyint, date),\n" +
                        "  k7 min_by(smallint, date),\n" +
                        "  k8 min_by(int, date),\n" +
                        "  k9 min_by(bigint, date),\n" +
                        "  k10 min_by(largeint, date),\n" +
                        "  k11 min_by(float, date),\n" +
                        "  k12 min_by(double, date),\n" +
                        "  k13 min_by(decimal(10, 0), date)\n" +
                        ")\n" +
                        "AGGREGATE KEY(k1)\n" +
                        "PARTITION BY (k1) \n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3;",
                () -> {
                    final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                            .getDb(connectContext.getDatabase())
                            .getTable("test_agg_tbl1");
                    String columns = table.getColumns().toString();
                    String expect = "[`k1` varchar(10) NULL COMMENT \"\", " +
                            "`k10` varbinary min_by(largeint(40), date) NULL COMMENT \"\", " +
                            "`k11` varbinary min_by(float, date) NULL COMMENT \"\", " +
                            "`k12` varbinary min_by(double, date) NULL COMMENT \"\", " +
                            "`k13` varbinary min_by(decimal(10, 0), date) NULL COMMENT \"\", " +
                            "`k2` varbinary min_by(datetime, date) NULL COMMENT \"\", " +
                            "`k6` varbinary min_by(tinyint(4), date) NULL COMMENT \"\", " +
                            "`k7` varbinary min_by(smallint(6), date) NULL COMMENT \"\", " +
                            "`k8` varbinary min_by(int(11), date) NULL COMMENT \"\", " +
                            "`k9` varbinary min_by(bigint(20), date) NULL COMMENT \"\"]";
                    Assert.assertEquals(expect, columns);
                });
    }

    @Test
    public void testCreateTableWithAggStateArrayAgg() {
        starRocksAssert.withTable("\n" +
                        "CREATE TABLE test_agg_tbl1(\n" +
                        "  k1 VARCHAR(10),\n" +
                        "  k2 array_agg(datetime),\n" +
                        "  k6 array_agg(tinyint),\n" +
                        "  k7 array_agg(smallint),\n" +
                        "  k8 array_agg(int),\n" +
                        "  k9 array_agg(bigint),\n" +
                        "  k10 array_agg(largeint),\n" +
                        "  k11 array_agg(float),\n" +
                        "  k12 array_agg(double),\n" +
                        "  k13 array_agg(decimal(38,1))\n" +
                        ")\n" +
                        "AGGREGATE KEY(k1)\n" +
                        "PARTITION BY (k1) \n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3;",
                () -> {
                    final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                            .getDb(connectContext.getDatabase())
                            .getTable("test_agg_tbl1");
                    String columns = table.getColumns().toString();
                    String expect = "[`k1` varchar(10) NULL COMMENT \"\", " +
                            "`k10` struct<col1 array<largeint(40)>> array_agg(largeint(40)) NULL COMMENT \"\", " +
                            "`k11` struct<col1 array<float>> array_agg(float) NULL COMMENT \"\", " +
                            "`k12` struct<col1 array<double>> array_agg(double) NULL COMMENT \"\", " +
                            "`k13` struct<col1 array<DECIMAL128(38,1)>> array_agg(decimal(38, 1)) NULL COMMENT \"\", " +
                            "`k2` struct<col1 array<datetime>> array_agg(datetime) NULL COMMENT \"\", " +
                            "`k6` struct<col1 array<tinyint(4)>> array_agg(tinyint(4)) NULL COMMENT \"\", " +
                            "`k7` struct<col1 array<smallint(6)>> array_agg(smallint(6)) NULL COMMENT \"\", " +
                            "`k8` struct<col1 array<int(11)>> array_agg(int(11)) NULL COMMENT \"\", " +
                            "`k9` struct<col1 array<bigint(20)>> array_agg(bigint(20)) NULL COMMENT \"\"]";
                    Assert.assertEquals(expect, columns);
                });
    }

    @Ignore
    public void testCreateTableWithAggStateGroupConcat() {
        starRocksAssert.withTable("\n" +
                        "CREATE TABLE test_agg_tbl1(\n" +
                        "  k1 VARCHAR(10),\n" +
                        "  k2 group_concat(datetime),\n" +
                        "  k6 group_concat(tinyint),\n" +
                        "  k7 group_concat(smallint),\n" +
                        "  k8 group_concat(int),\n" +
                        "  k9 group_concat(bigint),\n" +
                        "  k10 group_concat(largeint),\n" +
                        "  k11 group_concat(float),\n" +
                        "  k12 group_concat(double),\n" +
                        "  k13 group_concat(decimal(21, 10))\n" +
                        ")\n" +
                        "AGGREGATE KEY(k1)\n" +
                        "PARTITION BY (k1) \n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3;",
                () -> {
                    final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                            .getDb(connectContext.getDatabase())
                            .getTable("test_agg_tbl1");
                    String columns = table.getColumns().toString();
                    String expect = "[`k1` varchar(10) NULL COMMENT \"\", " +
                            "`k10` struct<col1 array<varchar(1048576)>> group_concat(largeint(40)) NULL COMMENT \"\", " +
                            "`k11` struct<col1 array<varchar(1048576)>> group_concat(float) NULL COMMENT \"\", " +
                            "`k12` struct<col1 array<varchar(1048576)>> group_concat(double) NULL COMMENT \"\", " +
                            "`k13` struct<col1 array<varchar(1048576)>> group_concat(decimal(21, 10)) NULL COMMENT \"\", " +
                            "`k2` struct<col1 array<varchar(1048576)>> group_concat(datetime) NULL COMMENT \"\", " +
                            "`k6` struct<col1 array<varchar(1048576)>> group_concat(tinyint(4)) NULL COMMENT \"\", " +
                            "`k7` struct<col1 array<varchar(1048576)>> group_concat(smallint(6)) NULL COMMENT \"\", " +
                            "`k8` struct<col1 array<varchar(1048576)>> group_concat(int(11)) NULL COMMENT \"\", " +
                            "`k9` struct<col1 array<varchar(1048576)>> group_concat(bigint(20)) NULL COMMENT \"\"]";
                    Assert.assertEquals(expect, columns);
                });
    }

    @Test
    public void testCreateTableWithAggStateBadCase1() {
        try {
            starRocksAssert.withTable("\n" +
                    "CREATE TABLE test_agg_tbl1(\n" +
                    "  k1 VARCHAR(10),\n" +
                    "  k2 sum(datetime),\n" +
                    "  k13 sum(decimal)\n" +
                    ")\n" +
                    "AGGREGATE KEY(k1)\n" +
                    "PARTITION BY (k1) \n" +
                    "DISTRIBUTED BY HASH(k1) BUCKETS 3;");
            final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                    .getDb(connectContext.getDatabase())
                    .getTable("test_agg_tbl1");
            Assert.assertEquals(null, table);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e.getMessage().contains("AggStateType function sum with input DECIMAL64(10,0) has " +
                    "wildcard decimal."));
        }
    }

    @Test
    public void testCreateTableWithAggStateBadCase2() {
        try {
            starRocksAssert.withTable("\n" +
                    "CREATE TABLE test_agg_tbl1(\n" +
                    "  k1 VARCHAR(10),\n" +
                    "  k2 sum(datetime) not null,\n" +
                    "  k13 sum(decimal(10, 2))\n" +
                    ")\n" +
                    "AGGREGATE KEY(k1)\n" +
                    "PARTITION BY (k1) \n" +
                    "DISTRIBUTED BY HASH(k1) BUCKETS 3;");
            final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getLocalMetastore()
                    .getDb(connectContext.getDatabase())
                    .getTable("test_agg_tbl1");
            Assert.assertEquals(null, table);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(e.getMessage().contains("Agg state column k2 must be nullable column."));
        }
    }

    @Test
    public void testAggStateFuncExpr() {
        starRocksAssert.withTable("\n" +
                        " CREATE TABLE `t1` ( \n" +
                        "    `k1`  date, \n" +
                        "    `k2`  datetime not null,\n" +
                        "    `k3`  char(20), \n" +
                        "    `k4`  varchar(20) not null, \n" +
                        "    `k5`  boolean, \n" +
                        "    `k6`  tinyint not null, \n" +
                        "    `k7`  smallint, \n" +
                        "    `k8`  int not null, \n" +
                        "    `k9`  bigint, \n" +
                        "    `k10` largeint not null, \n" +
                        "    `k11` float, \n" +
                        "    `k12` double not null, \n" +
                        "    `k13` decimal(27,9)\n" +
                        ") DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) \n" +
                        "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) \n" +
                        "PROPERTIES (  \"replication_num\" = \"1\");",
                () -> {
                    {
                        String sql = "select k1, k2, " +
                                "sum_state(k9), sum_state(k10), sum_state(k11), sum_state(k12), sum_state(k13) from t1;";
                        String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
                        PlanTestBase.assertContains(plan, "  |  14 <-> sum_state[([9: k9, BIGINT, true]); " +
                                "args: BIGINT; result: BIGINT; args nullable: true; result nullable: true]\n" +
                                "  |  15 <-> sum_state[([10: k10, LARGEINT, false]); args: LARGEINT; " +
                                "result: LARGEINT; args nullable: false; result nullable: true]\n" +
                                "  |  16 <-> sum_state[([11: k11, FLOAT, true]); args: FLOAT; result: DOUBLE; " +
                                "args nullable: true; result nullable: true]\n" +
                                "  |  17 <-> sum_state[([12: k12, DOUBLE, false]); args: DOUBLE; result: DOUBLE; " +
                                "args nullable: false; result nullable: true]\n" +
                                "  |  18 <-> sum_state[([13: k13, DECIMAL128(27,9), true]); args: DECIMAL128; " +
                                "result: DECIMAL128(38,9); args nullable: true; result nullable: true]");
                    }
                });
    }
}