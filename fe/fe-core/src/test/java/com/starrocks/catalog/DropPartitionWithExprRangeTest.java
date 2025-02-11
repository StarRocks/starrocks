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

import com.google.common.collect.ImmutableList;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.scheduler.PartitionBasedMvRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class DropPartitionWithExprRangeTest extends MVTestBase {
    private static String R1;
    private static String R2;
    private static List<String> RANGE_TABLES;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        starRocksAssert.withDatabase("test");
        starRocksAssert.useDatabase("test");
        // range partition table
        R1 = "CREATE TABLE r1 \n" +
                "(\n" +
                "    dt date,\n" +
                "    k2 int,\n" +
                "    v1 int \n" +
                ")\n" +
                "PARTITION BY RANGE(dt)\n" +
                "(\n" +
                "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                "    PARTITION p3 values [('2022-03-01'),('2022-04-01'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');";
        R2 = "CREATE TABLE r2 \n" +
                "(\n" +
                "    dt date,\n" +
                "    k2 int,\n" +
                "    v1 int \n" +
                ")\n" +
                "PARTITION BY date_trunc('day', dt)\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES('replication_num' = '1');";
        RANGE_TABLES = ImmutableList.of(R1, R2);
    }

    @AfterClass
    public static void afterClass() throws Exception {
    }

    private void withTablePartitions(String tableName) {
        if (tableName.equalsIgnoreCase("r1")) {
            return;
        }

        addRangePartition(tableName, "p1", "2024-01-01", "2024-01-02");
        addRangePartition(tableName, "p2", "2024-01-02", "2024-01-03");
        addRangePartition(tableName, "p3", "2024-01-03", "2024-01-04");
        addRangePartition(tableName, "p4", "2024-01-04", "2024-01-05");
    }

    private void withTablePartitionsV2(String tableName) {
        addRangePartition(tableName, "p1", "2024-01-29", "2024-01-30");
        addRangePartition(tableName, "p2", "2024-01-30", "2024-01-31");
        addRangePartition(tableName, "p3", "2024-01-31", "2024-02-01");
        addRangePartition(tableName, "p4", "2024-02-01", "2024-02-02");
    }

    @Test
    public void testDropPartitionsWithRangeTable1() {
        starRocksAssert.withTable(R1, (obj) -> {
            String tableName = (String) obj;
            OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
            Assert.assertEquals(4, olapTable.getVisiblePartitions().size());

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt >= current_date();", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(4, olapTable.getVisiblePartitions().size());
            }
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt >= '2022-03-01';", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(3, olapTable.getVisiblePartitions().size());
            }
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt > '2022-02-02';", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(3, olapTable.getVisiblePartitions().size());
            }
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt >= '2022-01-02';", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(2, olapTable.getVisiblePartitions().size());
            }
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt <= current_date();", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(0, olapTable.getVisiblePartitions().size());
            }
        });
    }

    @Test
    public void testDropPartitionsWithRangeTable2() {
        starRocksAssert.withTable(R2, (obj) -> {
            String tableName = (String) obj;
            withTablePartitions(tableName);
            OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
            Assert.assertEquals(4, olapTable.getVisiblePartitions().size());

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt >= current_date();", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(4, olapTable.getVisiblePartitions().size());
            }
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt >= '2024-01-04';", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(3, olapTable.getVisiblePartitions().size());
            }
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt > '2024-01-04';", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(3, olapTable.getVisiblePartitions().size());
            }
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt >= '2024-01-03';", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(2, olapTable.getVisiblePartitions().size());
            }
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt <= current_date();", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(0, olapTable.getVisiblePartitions().size());
            }
        });
    }

    @Test
    public void testDropPartitionsWithRangeTable3() {
        starRocksAssert.withTable(R2, (obj) -> {
            String tableName = (String) obj;
            withTablePartitionsV2(tableName);
            OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
            Assert.assertEquals(4, olapTable.getVisiblePartitions().size());

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE date_trunc('day', dt) <= date_sub(current_date(), 2) and " +
                        "date_trunc('day', dt) != (date_trunc(\'month\', date_trunc('day', dt)) + interval 1 month - " +
                        "interval 1 day)", tableName);
                System.out.println(dropPartitionSql);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(4, olapTable.getVisiblePartitions().size());
            }

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE date_trunc('day', dt) <= date_sub('2024-02-01', 2)", tableName);
                System.out.println(dropPartitionSql);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(3, olapTable.getVisiblePartitions().size());
            }

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt <= date_sub(current_date(), 2) and " +
                        "dt != (date_trunc(\'month\', dt) + interval 1 month - interval 1 day)", tableName);
                System.out.println(dropPartitionSql);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(3, olapTable.getVisiblePartitions().size());
            }

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt <= date_sub(current_date(), 2) and " +
                        "dt <= (date_trunc(\'month\', dt) + interval 1 month - interval 1 day)", tableName);
                System.out.println(dropPartitionSql);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(3, olapTable.getVisiblePartitions().size());
            }

            {
                // bingo!
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE dt <= date_sub(current_date(), 2) and dt >= '2024-01-01' and dt <= '2024-01-31' and " +
                        "dt != '2024-01-31'", tableName);
                System.out.println(dropPartitionSql);
                starRocksAssert.alterTable(dropPartitionSql);
                Assert.assertEquals(2, olapTable.getVisiblePartitions().size());
            }
        });
    }

    @Test
    public void testMVRefreshWithTTLCondition1() {
        for (String table : RANGE_TABLES) {
            starRocksAssert.withTable(table,
                    (obj) -> {
                        String tableName = (String) obj;
                        withTablePartitions(tableName);
                        String mvCreateDdl = String.format("create materialized view test_mv1\n" +
                                "partition by (dt) \n" +
                                "distributed by random \n" +
                                "REFRESH DEFERRED MANUAL \n" +
                                "PROPERTIES ('partition_retention_condition' = 'dt >= current_date() - interval 1 month')\n as" +
                                " select * from %s;", tableName);
                        starRocksAssert.withMaterializedView(mvCreateDdl,
                                () -> {
                                    String mvName = "test_mv1";
                                    MaterializedView mv = starRocksAssert.getMv("test", mvName);
                                    {
                                        // all partitions are expired, no need to create partitions for mv
                                        PartitionBasedMvRefreshProcessor processor = refreshMV("test", mv);
                                        Assert.assertEquals(0, mv.getVisiblePartitions().size());
                                        Assert.assertTrue(processor.getNextTaskRun() == null);
                                        ExecPlan execPlan = processor.getMvContext().getExecPlan();
                                        Assert.assertTrue(execPlan == null);
                                    }

                                    {
                                        // add new partitions
                                        LocalDateTime now = LocalDateTime.now();
                                        addRangePartition(tableName, "p5",
                                                now.minusMonths(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                                now.minusMonths(1).plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                                true);
                                        addRangePartition(tableName, "p6",
                                                now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                                now.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                                true);
                                        PartitionBasedMvRefreshProcessor processor = refreshMV("test", mv);
                                        Assert.assertTrue(processor != null);
                                        Assert.assertTrue(processor.getNextTaskRun() == null);
                                        Assert.assertEquals(2, mv.getVisiblePartitions().size());
                                        ExecPlan execPlan = processor.getMvContext().getExecPlan();
                                        Assert.assertTrue(execPlan != null);
                                        String plan = execPlan.getExplainString(StatementBase.ExplainLevel.NORMAL);
                                        PlanTestBase.assertContains(plan, "     PREAGGREGATION: ON\n" +
                                                "     partitions=2/6");
                                    }
                                });
                    });
        }
    }

    @Test
    public void testMVRefreshWithTTLCondition2() {
        for (String table : RANGE_TABLES) {
            starRocksAssert.withTable(table,
                    (obj) -> {
                        String tableName = (String) obj;
                        withTablePartitions(tableName);
                        String mvCreateDdl = String.format("create materialized view test_mv1\n" +
                                "partition by (dt) \n" +
                                "distributed by random \n" +
                                "REFRESH DEFERRED MANUAL \n" +
                                "AS select * from %s;", tableName);
                        starRocksAssert.withMaterializedView(mvCreateDdl,
                                () -> {
                                    String mvName = "test_mv1";
                                    MaterializedView mv = starRocksAssert.getMv("test", mvName);
                                    {
                                        // all partitions are expired, no need to create partitions for mv
                                        PartitionBasedMvRefreshProcessor processor = refreshMV("test", mv);
                                        Assert.assertEquals(4, mv.getVisiblePartitions().size());
                                        Assert.assertTrue(processor.getNextTaskRun() == null);
                                        ExecPlan execPlan = processor.getMvContext().getExecPlan();
                                        Assert.assertTrue(execPlan == null);
                                    }

                                    // alter mv ttl condition
                                    String alterMVSql = String.format("alter materialized view %s set (" +
                                            "'partition_retention_condition' = 'dt >= current_date() " +
                                            "- interval 1 month')", mvName);
                                    starRocksAssert.alterMvProperties(alterMVSql);

                                    {
                                        // all partitions are expired, no need to create partitions for mv
                                        PartitionBasedMvRefreshProcessor processor = refreshMV("test", mv);
                                        Assert.assertEquals(4, mv.getVisiblePartitions().size());
                                        Assert.assertTrue(processor.getNextTaskRun() == null);
                                        ExecPlan execPlan = processor.getMvContext().getExecPlan();
                                        Assert.assertTrue(execPlan == null);
                                    }

                                    {
                                        // add new partitions
                                        LocalDateTime now = LocalDateTime.now();
                                        addRangePartition(tableName, "p5",
                                                now.minusMonths(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                                now.minusMonths(1).plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                                true);
                                        addRangePartition(tableName, "p6",
                                                now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                                now.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                                                true);
                                        PartitionBasedMvRefreshProcessor processor = refreshMV("test", mv);
                                        Assert.assertTrue(processor != null);
                                        Assert.assertTrue(processor.getNextTaskRun() == null);
                                        Assert.assertEquals(6, mv.getVisiblePartitions().size());
                                        ExecPlan execPlan = processor.getMvContext().getExecPlan();
                                        Assert.assertTrue(execPlan != null);
                                        String plan = execPlan.getExplainString(StatementBase.ExplainLevel.NORMAL);
                                        PlanTestBase.assertContains(plan, "     PREAGGREGATION: ON\n" +
                                                "     partitions=2/6");
                                    }

                                    // run partition ttl scheduler
                                    {
                                        DynamicPartitionScheduler scheduler = GlobalStateMgr.getCurrentState()
                                                .getDynamicPartitionScheduler();
                                        scheduler.runOnceForTest();
                                        Assert.assertEquals(2, mv.getVisiblePartitions().size());
                                    }
                                });
                    });
        }
    }
}