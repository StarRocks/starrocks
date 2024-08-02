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
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
        starRocksAssert.useDatabase("test");
        UtFrameUtils.setUpForPersistTest();
    }

    @Test
    public void testCreateTableWithAggStateAvg() {
        starRocksAssert.withTable("CREATE TABLE test_agg_tbl1(\n" +
                        "  k1 VARCHAR(10),\n" +
                        "  k6 agg_state<avg(tinyint)> agg_state_union,\n" +
                        "  k7 agg_state<avg(smallint)> agg_state_union,\n" +
                        "  k8 agg_state<avg(int)> agg_state_union,\n" +
                        "  k9 agg_state<avg(bigint)> agg_state_union,\n" +
                        "  k10 agg_state<avg(largeint)> agg_state_union,\n" +
                        "  k11 agg_state<avg(float)> agg_state_union,\n" +
                        "  k12 agg_state<avg(double)> agg_state_union,\n" +
                        "  k13 agg_state<avg(decimal(10, 0))> agg_state_union\n" +
                        ")\n" +
                        "AGGREGATE KEY(k1)\n" +
                        "PARTITION BY (k1) \n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3;",
                () -> {
                    final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getDb(connectContext.getDatabase())
                            .getTable("test_agg_tbl1");
                    String columns = table.getColumns().toString();
                    System.out.println("columns = " + columns);
                    String[] expects = {
                            "`k1` varchar(10) NULL",
                            "`k6` AGG_STATE<avg(tinyint(4))> AGG_STATE_UNION NULL",
                            "`k7` AGG_STATE<avg(smallint(6))> AGG_STATE_UNION NULL",
                            "`k8` AGG_STATE<avg(int(11))> AGG_STATE_UNION NULL",
                            "`k9` AGG_STATE<avg(bigint(20))> AGG_STATE_UNION NULL",
                            "`k10` AGG_STATE<avg(largeint(40))> AGG_STATE_UNION NULL",
                            "`k11` AGG_STATE<avg(float)> AGG_STATE_UNION NULL",
                            "`k12` AGG_STATE<avg(double)> AGG_STATE_UNION NULL",
                            "`k13` AGG_STATE<avg(decimal(10, 0))> AGG_STATE_UNION NULL",
                    };
                    for (String exp : expects) {
                        Assert.assertTrue(columns.contains(exp));
                    }
                });
    }

    @Test
    public void testCreateTableWithAggStateSum() {
        starRocksAssert.withTable("\n" +
                "CREATE TABLE test_agg_tbl1(\n" +
                "  k1 VARCHAR(10),\n" +
                "  k2 datetime,\n" +
                "  k6 agg_state<sum(tinyint)> agg_state_union,\n" +
                "  k7 agg_state<sum(smallint)> agg_state_union,\n" +
                "  k8 agg_state<sum(int)> agg_state_union,\n" +
                "  k9 agg_state<sum(bigint)> agg_state_union,\n" +
                "  k10 agg_state<sum(largeint)> agg_state_union,\n" +
                "  k11 agg_state<sum(float)> agg_state_union,\n" +
                "  k12 agg_state<sum(double)> agg_state_union,\n" +
                "  k13 agg_state<sum(decimal(27,9))> agg_state_union\n" +
                ")\n" +
                "AGGREGATE KEY(k1, k2)\n" +
                "PARTITION BY (k1) \n" +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3;",
                () -> {
                    final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getDb(connectContext.getDatabase())
                            .getTable("test_agg_tbl1");
                    String columns = table.getColumns().toString();
                    System.out.println("columns = " + columns);
                    String[] expects = {
                            "`k1` varchar(10) NULL",
                            "`k2` datetime NULL",
                            "`k6` AGG_STATE<sum(tinyint(4))> AGG_STATE_UNION NULL",
                            "`k7` AGG_STATE<sum(smallint(6))> AGG_STATE_UNION NULL",
                            "`k8` AGG_STATE<sum(int(11))> AGG_STATE_UNION NULL",
                            "`k9` AGG_STATE<sum(bigint(20))> AGG_STATE_UNION NULL",
                            "`k10` AGG_STATE<sum(largeint(40))> AGG_STATE_UNION NULL ",
                            "`k11` AGG_STATE<sum(float)> AGG_STATE_UNION NULL",
                            "`k12` AGG_STATE<sum(double)> AGG_STATE_UNION NULL",
                            "`k13` AGG_STATE<sum(decimal(27", "9))> AGG_STATE_UNION NULL",
                    };
                    for (String exp : expects) {
                        Assert.assertTrue(columns.contains(exp));
                    }
                });
    }

    @Test
    public void testCreateTableWithAggStateHllSketch() {
        starRocksAssert.withTable("\n" +
                        "CREATE TABLE test_agg_tbl1 (\n" +
                        "  dt VARCHAR(10),\n" +
                        "  hll_id agg_state<ds_hll_count_distinct(varchar not null)> agg_state_union,\n" +
                        "  hll_province agg_state<ds_hll_count_distinct(varchar)> agg_state_union,\n" +
                        "  hll_age agg_state<ds_hll_count_distinct(varchar, int)> agg_state_union,\n" +
                        "  hll_dt agg_state<ds_hll_count_distinct(varchar not null, int, varchar)> agg_state_union\n" +
                        ")\n" +
                        "AGGREGATE KEY(dt)\n" +
                        "PARTITION BY (dt) \n" +
                        "DISTRIBUTED BY HASH(dt) BUCKETS 4;",
                () -> {
                    final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getDb(connectContext.getDatabase())
                            .getTable("test_agg_tbl1");
                    String columns = table.getColumns().toString();
                    System.out.println("columns = " + columns);
                    String[] expects = {
                            "`dt` varchar(10) NULL",
                            "`hll_province` AGG_STATE<ds_hll_count_distinct(varchar)> AGG_STATE_UNION NULL",
                            "`hll_age` AGG_STATE<ds_hll_count_distinct(varchar, int(11))> AGG_STATE_UNION NULL",
                            "`hll_dt` AGG_STATE<ds_hll_count_distinct(varchar, int(11), varchar)> AGG_STATE_UNION NULL",
                            "`hll_id` AGG_STATE<ds_hll_count_distinct(varchar)> AGG_STATE_UNION NULL",
                    };
                    for (String exp : expects) {
                        Assert.assertTrue(columns.contains(exp));
                    }
                });
    }

    @Test
    public void testCreateTableWithAggStateMinBy() {
        starRocksAssert.withTable("\n" +
                        "CREATE TABLE test_agg_tbl1(\n" +
                        "  k1 VARCHAR(10),\n" +
                        "  k2 agg_state<min_by(datetime, date)> agg_state_union,\n" +
                        "  k6 agg_state<min_by(tinyint, date)> agg_state_union,\n" +
                        "  k7 agg_state<min_by(smallint, date)> agg_state_union,\n" +
                        "  k8 agg_state<min_by(int, date)> agg_state_union,\n" +
                        "  k9 agg_state<min_by(bigint, date)> agg_state_union,\n" +
                        "  k10 agg_state<min_by(largeint, date)> agg_state_union,\n" +
                        "  k11 agg_state<min_by(float, date)> agg_state_union,\n" +
                        "  k12 agg_state<min_by(double, date)> agg_state_union,\n" +
                        "  k13 agg_state<min_by(decimal(10, 0), date)> agg_state_union\n" +
                        ")\n" +
                        "AGGREGATE KEY(k1)\n" +
                        "PARTITION BY (k1) \n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3;",
                () -> {
                    final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getDb(connectContext.getDatabase())
                            .getTable("test_agg_tbl1");
                    String columns = table.getColumns().toString();
                    System.out.println("columns = " + columns);
                    String[] expects = {
                            "`k1` varchar(10) NULL",
                            "`k2` AGG_STATE<min_by(datetime, date)> AGG_STATE_UNION NULL",
                            "`k6` AGG_STATE<min_by(tinyint(4), date)> AGG_STATE_UNION NULL",
                            "`k7` AGG_STATE<min_by(smallint(6), date)> AGG_STATE_UNION NULL",
                            "`k8` AGG_STATE<min_by(int(11), date)> AGG_STATE_UNION NULL",
                            "`k9` AGG_STATE<min_by(bigint(20), date)> AGG_STATE_UNION NULL COMMENT",
                            "`k10` AGG_STATE<min_by(largeint(40), date)> AGG_STATE_UNION NULL",
                            "`k11` AGG_STATE<min_by(float, date)> AGG_STATE_UNION NULL",
                            "`k12` AGG_STATE<min_by(double, date)> AGG_STATE_UNION NULL",
                            "`k13` AGG_STATE<min_by(decimal(10, 0), date)> AGG_STATE_UNION NULL",
                    };
                    for (String exp : expects) {
                        Assert.assertTrue(columns.contains(exp));
                    }
                });
    }

    @Test
    public void testCreateTableWithAggStateArrayAgg() {
        starRocksAssert.withTable("\n" +
                        "CREATE TABLE test_agg_tbl1(\n" +
                        "  k1 VARCHAR(10),\n" +
                        "  k2 agg_state<array_agg(datetime)> agg_state_union,\n" +
                        "  k6 agg_state<array_agg(tinyint)> agg_state_union,\n" +
                        "  k7 agg_state<array_agg(smallint)> agg_state_union,\n" +
                        "  k8 agg_state<array_agg(int)> agg_state_union,\n" +
                        "  k9 agg_state<array_agg(bigint)> agg_state_union,\n" +
                        "  k10 agg_state<array_agg(largeint)> agg_state_union,\n" +
                        "  k11 agg_state<array_agg(float)> agg_state_union,\n" +
                        "  k12 agg_state<array_agg(double)> agg_state_union,\n" +
                        "  k13 agg_state<array_agg(decimal(38,1))> agg_state_union\n" +
                        ")\n" +
                        "AGGREGATE KEY(k1)\n" +
                        "PARTITION BY (k1) \n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3;",
                () -> {
                    final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getDb(connectContext.getDatabase())
                            .getTable("test_agg_tbl1");
                    String columns = table.getColumns().toString();
                    System.out.println("columns = " + columns);
                    String[] expects = {
                            "`k1` varchar(10) NULL",
                            "`k2` AGG_STATE<array_agg(datetime)> AGG_STATE_UNION NULL",
                            "`k6` AGG_STATE<array_agg(tinyint(4))> AGG_STATE_UNION NULL",
                            "`k7` AGG_STATE<array_agg(smallint(6))> AGG_STATE_UNION NULL",
                            "`k8` AGG_STATE<array_agg(int(11))> AGG_STATE_UNION NULL",
                            "`k9` AGG_STATE<array_agg(bigint(20))> AGG_STATE_UNION NULL",
                            "`k10` AGG_STATE<array_agg(largeint(40))> AGG_STATE_UNION NULL",
                            "`k11` AGG_STATE<array_agg(float)> AGG_STATE_UNION NULL",
                            "`k12` AGG_STATE<array_agg(double)> AGG_STATE_UNION NULL",
                            "`k13` AGG_STATE<array_agg(decimal(38", "1))> AGG_STATE_UNION NULL",
                    };
                    for (String exp : expects) {
                        Assert.assertTrue(columns.contains(exp));
                    }
                });
    }

    @Test
    public void testCreateTableWithAggStateGroupConcat() {
        starRocksAssert.withTable("\n" +
                        "CREATE TABLE test_agg_tbl1(\n" +
                        "  k1 VARCHAR(10),\n" +
                        "  k2 agg_state<group_concat(datetime)> agg_state_union,\n" +
                        "  k6 agg_state<group_concat(tinyint)> agg_state_union,\n" +
                        "  k7 agg_state<group_concat(smallint)> agg_state_union,\n" +
                        "  k8 agg_state<group_concat(int)> agg_state_union,\n" +
                        "  k9 agg_state<group_concat(bigint)> agg_state_union,\n" +
                        "  k10 agg_state<group_concat(largeint)> agg_state_union,\n" +
                        "  k11 agg_state<group_concat(float)> agg_state_union,\n" +
                        "  k12 agg_state<group_concat(double)> agg_state_union,\n" +
                        "  k13 agg_state<group_concat(decimal(21, 10))> agg_state_union\n" +
                        ")\n" +
                        "AGGREGATE KEY(k1)\n" +
                        "PARTITION BY (k1) \n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3;",
                () -> {
                    final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getDb(connectContext.getDatabase())
                            .getTable("test_agg_tbl1");
                    String columns = table.getColumns().toString();
                    System.out.println("columns = " + columns);
                    String[] expects = {
                            "`k1` varchar(10) NULL",
                            "`k2` AGG_STATE<group_concat(datetime)> AGG_STATE_UNION NULL",
                            "`k6` AGG_STATE<group_concat(tinyint(4))> AGG_STATE_UNION NULL",
                            "`k7` AGG_STATE<group_concat(smallint(6))> AGG_STATE_UNION NULL",
                            "`k8` AGG_STATE<group_concat(int(11))> AGG_STATE_UNION NULL",
                            "`k9` AGG_STATE<group_concat(bigint(20))> AGG_STATE_UNION NULL",
                            "`k10` AGG_STATE<group_concat(largeint(40))> AGG_STATE_UNION NULL",
                            "`k11` AGG_STATE<group_concat(float)> AGG_STATE_UNION NULL",
                            "`k12` AGG_STATE<group_concat(double)> AGG_STATE_UNION NULL",
                            "`k13` AGG_STATE<group_concat(decimal(21, 10))> AGG_STATE_UNION NULL",
                    };
                    for (String exp : expects) {
                        Assert.assertTrue(columns.contains(exp));
                    }
                });
    }

    @Test
    public void testCreateTableWithAggStateBadCase1() {
        try {
            starRocksAssert.withTable("\n" +
                    "CREATE TABLE test_agg_tbl1(\n" +
                    "  k1 VARCHAR(10),\n" +
                    "  k2 agg_state<group_concat(datetime)> agg_state_union,\n" +
                    "  k13 agg_state<group_concat(decimal)> agg_state_union\n" +
                    ")\n" +
                    "AGGREGATE KEY(k1)\n" +
                    "PARTITION BY (k1) \n" +
                    "DISTRIBUTED BY HASH(k1) BUCKETS 3;");
            final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getDb(connectContext.getDatabase())
                    .getTable("test_agg_tbl1");
            Assert.assertEquals(null, table);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("AggStateType function group_concat with input DECIMAL64(10,0) has " +
                    "wildcard decimal."));
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
                    String sql = "select k1, k2, sum_state(k13) from t1;";
                    String plan = UtFrameUtils.getVerboseFragmentPlan(starRocksAssert.getCtx(), sql);
                    PlanTestBase.assertContains(plan, "result: DECIMAL128(38,9)");
                });
    }
}