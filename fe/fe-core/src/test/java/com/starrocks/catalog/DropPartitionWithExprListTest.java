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

import com.google.common.collect.Lists;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.scheduler.mv.pct.MVPCTBasedRefreshProcessor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class DropPartitionWithExprListTest extends MVTestBase {
    private static String T1;
    private static String T2;
    private static String T3;
    private static String T4;
    private static String T5;
    private static String T6;
    private static List<String> TABLES_WITH_DATE_DT_TYPES;
    private static List<String> TABLES_WITH_DATETIME_DT_TYPES;

    @BeforeAll
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

        // table with partition expression whose partitions have multi columns
        T1 = "CREATE TABLE t1 (\n" +
                " id BIGINT,\n" +
                " age SMALLINT,\n" +
                " dt date not null,\n" +
                " province VARCHAR(64) not null\n" +
                ")\n" +
                "PARTITION BY (province, dt) \n" +
                "DISTRIBUTED BY RANDOM\n";
        // table whose partitions have multi columns
        T2 = "CREATE TABLE t2 (\n" +
                " id BIGINT,\n" +
                " age SMALLINT,\n" +
                " dt date not null,\n" +
                " province VARCHAR(64) not null\n" +
                ")\n" +
                "PARTITION BY LIST (province, dt) (\n" +
                "     PARTITION p1 VALUES IN ((\"beijing\", \"2024-01-01\")),\n" +
                "     PARTITION p2 VALUES IN ((\"guangdong\", \"2024-01-01\")), \n" +
                "     PARTITION p3 VALUES IN ((\"beijing\", \"2024-01-02\")),\n" +
                "     PARTITION p4 VALUES IN ((\"guangdong\", \"2024-01-02\")) \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n";
        T3 = "CREATE TABLE t3 (\n" +
                "      id BIGINT,\n" +
                "      age SMALLINT,\n" +
                "      dt VARCHAR(10),\n" +
                "      province VARCHAR(64) not null\n" +
                ")\n" +
                "DUPLICATE KEY(id)\n" +
                "PARTITION BY LIST (province) (\n" +
                "     PARTITION p1 VALUES IN (\"beijing\",\"chongqing\") ,\n" +
                "     PARTITION p2 VALUES IN (\"guangdong\", \"shenzhen\"), \n" +
                "     PARTITION p3 VALUES IN (\"shanghai\"), \n" +
                "     PARTITION p4 VALUES IN (\"chengdu\") \n" +
                ")\n" +
                "DISTRIBUTED BY RANDOM\n";
        // table with partition expression whose partitions have multiple values
        T4 = "CREATE TABLE t4 (\n" +
                " id BIGINT,\n" +
                " age SMALLINT,\n" +
                " dt VARCHAR(10) not null,\n" +
                " province VARCHAR(64) not null\n" +
                ")\n" +
                "PARTITION BY province, str2date(dt, '%Y-%m-%d') \n" +
                "DISTRIBUTED BY RANDOM\n";
        // table with partition expression whose partitions have multi columns(nullable partition columns)
        T5 = "CREATE TABLE t5 (\n" +
                " id BIGINT,\n" +
                " age SMALLINT,\n" +
                " dt datetime,\n" +
                " province VARCHAR(64)\n" +
                ")\n" +
                "PARTITION BY province, date_trunc('day', dt) \n" +
                "DISTRIBUTED BY RANDOM\n";
        T6 = "CREATE TABLE t6 (\n" +
                " id BIGINT,\n" +
                " age SMALLINT,\n" +
                " dt datetime,\n" +
                " province VARCHAR(64)\n" +
                ")\n" +
                "PARTITION BY str2date(dt, '%Y-%m-%d'), date_trunc('day', dt) \n" +
                "DISTRIBUTED BY RANDOM\n";
        TABLES_WITH_DATE_DT_TYPES = Lists.newArrayList(T1, T2);
        TABLES_WITH_DATETIME_DT_TYPES = Lists.newArrayList(T5, T6);
    }

    @AfterAll
    public static void afterClass() throws Exception {
    }

    private void withTablePartitions(String tableName) {
        if (tableName.equalsIgnoreCase("t6")) {
            addListPartition(tableName, "p1", "2024-01-01", "2024-01-01");
            addListPartition(tableName, "p2", "2024-01-02", "2024-01-01");
            addListPartition(tableName, "p3", "2024-01-01", "2024-01-02");
            addListPartition(tableName, "p4", "2024-01-02", "2024-01-02");
        } else {
            addListPartition(tableName, "p1", "beijing", "2024-01-01");
            addListPartition(tableName, "p2", "guangdong", "2024-01-01");
            addListPartition(tableName, "p3", "beijing", "2024-01-02");
            addListPartition(tableName, "p4", "guangdong", "2024-01-02");
        }
    }

    private void withTablePartitionsV2(String tableName) {
        if (tableName.equalsIgnoreCase("t6")) {
            addListPartition(tableName, "p1", "2024-01-29", "2024-01-30");
            addListPartition(tableName, "p2", "2024-01-30", "2024-01-31");
            addListPartition(tableName, "p3", "2024-01-31", "2024-02-01");
            addListPartition(tableName, "p4", "2024-02-01", "2024-02-02");
        } else {
            addListPartition(tableName, "p1", "beijing", "2024-01-29");
            addListPartition(tableName, "p2", "guangdong", "2024-01-30");
            addListPartition(tableName, "p3", "beijing", "2024-01-31");
            addListPartition(tableName, "p4", "guangdong", "2024-02-01");
        }
    }

    private void withTablesWithStringDtTypes(StarRocksAssert.ExceptionConsumer<OlapTable> runner) {
        for (String t : TABLES_WITH_DATE_DT_TYPES) {
            System.out.println(t);
            starRocksAssert.withTable(t, (obj) -> {
                String tableName = (String) obj;

                // Automatic partition creation is not supported in FE UTs
                //String insertSql = String.format("insert into %s values " +
                //        "(1, 1, '2024-01-01', 'beijing'), (1, 1, '2024-01-01', 'guangdong')," +
                //        "(2, 1, '2024-01-02', 'beijing'), (2, 1, '2024-01-02', 'guangdong');", tableName);
                //executeInsertSql(insertSql);
                withTablePartitions(tableName);

                OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
                Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());

                runner.accept(olapTable);
            });
        }
    }

    @Test
    public void testDropPartitionsWithExprBasic() {
        starRocksAssert.withTable(T1, (obj) -> {
            String tableName = (String) obj;
            withTablePartitions(tableName);
            OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
            Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());

            {
                try {
                    String dropPartitionSql = String.format("alter table %s DROP TEMPORARY PARTITIONS " +
                                    "WHERE dt <= current_date();", tableName);
                    starRocksAssert.alterTable(dropPartitionSql);
                    Assertions.fail();
                } catch (Exception e) {
                    Assertions.assertTrue(e.getMessage().contains("Can't drop temp partitions with where expression " +
                            "and `TEMPORARY` keyword."));
                }
            }

            {
                try {
                    String dropPartitionSql = String.format("alter table %s DROP PARTITIONS IF EXISTS " +
                            "WHERE dt <= current_date();", tableName);
                    starRocksAssert.alterTable(dropPartitionSql);
                    Assertions.fail();
                } catch (Exception e) {
                    Assertions.assertTrue(e.getMessage().contains("Can't drop partitions with where expression and " +
                            "`IF EXISTS` keyword."));
                }
            }
        });
    }

    @Test
    public void testDropPartitionsWithExpr1() {
        withTablesWithStringDtTypes((olapTable) -> {
            String tableName = olapTable.getName();
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE dt = '2021-02-01';",
                        tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
            }

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE dt = '2024-01-01';",
                        tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(2, olapTable.getVisiblePartitions().size());
            }

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE dt = '2024-01-02';",
                        tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(0, olapTable.getVisiblePartitions().size());
            }
        });
    }

    @Test
    public void testDropPartitionsWithExpr2() {
        withTablesWithStringDtTypes((olapTable) -> {
            String tableName = olapTable.getName();
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE dt >= current_date();",
                        tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
            }
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE dt <= '2021-02-01';",
                        tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
            }

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE dt >= '2024-01-01';",
                        tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(0, olapTable.getVisiblePartitions().size());
            }
        });
    }

    @Test
    public void testDropPartitionsWithExpr3() {
        withTablesWithStringDtTypes((olapTable) -> {
            String tableName = olapTable.getName();
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE dt <= current_date();", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(0, olapTable.getVisiblePartitions().size());
            }
        });
    }

    @Test
    public void testDropPartitionsWithSt2dateExpr1() {
        starRocksAssert.withTable(T4,
                (obj) -> {
                    String tableName = (String) obj;

                    // mock partitions
                    withTablePartitions(tableName);
                    OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
                    Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());

                    {
                        String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                                "WHERE str2date(dt, '%%Y-%%m-%%d') >= current_date();", tableName);
                        starRocksAssert.alterTable(dropPartitionSql);
                        Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
                    }

                    {
                        String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                "str2date(dt, '%%Y-%%m-%%d') <= '2021-02-01';", tableName);
                        starRocksAssert.alterTable(dropPartitionSql);
                        Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
                    }

                    {
                        String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                "str2date(dt, '%%Y-%%m-%%d') >= '2021-02-01';", tableName);
                        starRocksAssert.alterTable(dropPartitionSql);
                        Assertions.assertEquals(0, olapTable.getVisiblePartitions().size());
                    }
                });
    }

    @Test
    public void testDropPartitionsWithSt2dateExpr2() {
        starRocksAssert.withTable(T4,
                (obj) -> {
                    String tableName = (String) obj;

                    // mock partitions
                    withTablePartitions(tableName);
                    OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
                    Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());

                    {
                        try {

                            String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE dt like '2024%%';",
                                    tableName);
                            starRocksAssert.alterTable(dropPartitionSql);
                            Assertions.fail();
                        } catch (Exception e) {
                            Assertions.assertTrue(e.getMessage().contains("Column `dt` in the partition condition is not " +
                                    "a table's partition expression, please use table's partition expressions: " +
                                    "`province`/`str2date(dt, '%Y-%m-%d')`"));
                        }
                        Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
                    }

                    {
                        try {

                            String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                    "str2date(dt, '%%Y-%%m-%%d') like '2024%%';", tableName);
                            starRocksAssert.alterTable(dropPartitionSql);
                            Assertions.fail();
                        } catch (Exception e) {
                            Assertions.assertTrue(e.getMessage().contains("left operand of LIKE must be of type STRING"));
                        }
                        Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
                    }

                    {
                        try {

                            String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE str2date(dt, " +
                                    "'%%Y-%%m-%%d') >= current_date() ;", tableName);
                            starRocksAssert.alterTable(dropPartitionSql);
                        } catch (Exception e) {
                            Assertions.fail();
                        }
                        Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
                    }


                    {
                        String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                "str2date(dt, '%%Y-%%m-%%d') >= current_date() and province='guangdong';", tableName);
                        starRocksAssert.alterTable(dropPartitionSql);
                        Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
                    }

                    {
                        String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                "str2date(dt, '%%Y-%%m-%%d') <= '2021-02-01' and province='guangdong';", tableName);
                        starRocksAssert.alterTable(dropPartitionSql);
                        Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
                    }

                    {
                        String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                "str2date(dt, '%%Y-%%m-%%d') >= '2021-02-01' and province='beijing';", tableName);
                        starRocksAssert.alterTable(dropPartitionSql);
                        Assertions.assertEquals(2, olapTable.getVisiblePartitions().size());
                    }

                    {
                        String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                "str2date(dt, '%%Y-%%m-%%d') >= '2021-02-01' and province='guangdong';", tableName);
                        starRocksAssert.alterTable(dropPartitionSql);
                        Assertions.assertEquals(0, olapTable.getVisiblePartitions().size());
                    }
                });
    }

    @Test
    public void testDropPartitionsWithDateTrucExpr1() {
        for (String sql : TABLES_WITH_DATETIME_DT_TYPES) {
            starRocksAssert.withTable(sql,
                    (obj) -> {
                        String tableName = (String) obj;

                        // mock partitions
                        withTablePartitions(tableName);
                        OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
                        Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());

                        {
                            String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                    "date_trunc('day', dt) >= current_date();", tableName);
                            starRocksAssert.alterTable(dropPartitionSql);
                            Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
                        }

                        {
                            String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                    "date_trunc('day', dt) <= '2021-02-01';", tableName);
                            starRocksAssert.alterTable(dropPartitionSql);
                            Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
                        }

                        {
                            String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                    "date_trunc('day', dt) >= '2021-02-01';", tableName);
                            starRocksAssert.alterTable(dropPartitionSql);
                            Assertions.assertEquals(0, olapTable.getVisiblePartitions().size());
                        }
                    });
        }
    }

    @Test
    public void testDropPartitionsWithDateTrucExpr2() {
        starRocksAssert.withTable(T5, (obj) -> {
            String tableName = (String) obj;
            // mock partitions
            withTablePartitions(tableName);
            OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
            Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE date_trunc('day', dt) >= " +
                        "current_date() and province='guangdong';", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
            }

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE date_trunc('day', dt) <= " +
                        "'2021-02-01' and province='guangdong';", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
            }

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE date_trunc('day', dt) >= " +
                        "'2021-02-01' and province='beijing';", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(2, olapTable.getVisiblePartitions().size());
            }

            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE date_trunc('day', dt) >= " +
                        "'2021-02-01' and province='guangdong';", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(0, olapTable.getVisiblePartitions().size());
            }
        });
    }

    @Test
    public void testDropPartitionsWithMultiGenerateColumns() {
        starRocksAssert.withTable(T6,
                (obj) -> {
                    String tableName = (String) obj;
                    // mock partitions
                    withTablePartitions(tableName);
                    OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
                    Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());

                    {
                        try {

                            String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                    "date_trunc('day', dt) >= current_date() and province='guangdong';", tableName);
                            starRocksAssert.alterTable(dropPartitionSql);
                            Assertions.fail();
                        } catch (Exception e) {
                            Assertions.assertTrue(e.getMessage().contains("Column `province` in the partition condition is not " +
                                    "a table's partition expression, please use table's partition expressions: `str2date(dt, " +
                                    "'%Y-%m-%d')`/`date_trunc('day', dt)`."));
                        }
                    }
                    {
                        try {

                            String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                    "date_trunc('day', dt2) >= current_date();", tableName);
                            starRocksAssert.alterTable(dropPartitionSql);
                            Assertions.fail();
                        } catch (Exception e) {
                            Assertions.assertTrue(e.getMessage().contains("Column 'dt2' cannot be resolve"));
                        }
                    }

                    {
                        String dropPartitionSql = String.format("alter table %s DROP PARTITIONS WHERE " +
                                "date_trunc('day', dt) = '2024-01-01';", tableName);
                        starRocksAssert.alterTable(dropPartitionSql);
                        Assertions.assertEquals(2, olapTable.getVisiblePartitions().size());
                    }
                });
    }

    @Test
    public void testDropPartitionsWithMultiValues1() {
        starRocksAssert.withTable(T3, (obj) -> {
            String tableName = (String) obj;
            withTablePartitions(tableName);
            OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
            Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE province = 'beijing';", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
            }
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS (p1);", tableName);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(3, olapTable.getVisiblePartitions().size());
            }
        });
    }

    @Test
    public void testDropPartitionsWithMultiValues2() {
        starRocksAssert.withTable(T5, (obj) -> {
            String tableName = (String) obj;
            withTablePartitionsV2(tableName);
            OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", tableName);
            Assertions.assertEquals(4, olapTable.getVisiblePartitions().size());
            {
                String dropPartitionSql = String.format("alter table %s DROP PARTITIONS " +
                        "WHERE date_trunc('day', dt) <= date_sub(current_date(), 2) and " +
                        "date_trunc('day', dt) != (date_trunc(\'month\', date_trunc('day', dt)) + interval 1 month - " +
                        "interval 1 day)", tableName);
                System.out.println(dropPartitionSql);
                starRocksAssert.alterTable(dropPartitionSql);
                Assertions.assertEquals(1, olapTable.getVisiblePartitions().size());
                Partition partition = olapTable.getVisiblePartitions().get(0);
                // 2024-01-31
                Assertions.assertEquals("p3", partition.getName());
            }
        });
    }

    @Test
    public void testMVRefreshWithTTLCondition1() {
        for (String table : TABLES_WITH_DATE_DT_TYPES) {
            starRocksAssert.withTable(table,
                    (obj) -> {
                        String tableName = (String) obj;
                        withTablePartitions(tableName);
                        String mvCreateDdl = String.format("create materialized view test_mv1\n" +
                                "partition by (dt) \n" +
                                "distributed by random \n" +
                                "REFRESH DEFERRED MANUAL \n" +
                                "PROPERTIES ('partition_retention_condition' = 'dt >= current_date() - interval 1 month')\n " +
                                "as select * from %s;", tableName);
                        starRocksAssert.withMaterializedView(mvCreateDdl,
                                () -> {
                                    String mvName = "test_mv1";
                                    MaterializedView mv = starRocksAssert.getMv("test", mvName);
                                    {
                                        // all partitions are expired, no need to create partitions for mv
                                        MVPCTBasedRefreshProcessor processor = refreshMV("test", mv);
                                        Assertions.assertEquals(0, mv.getVisiblePartitions().size());
                                        Assertions.assertTrue(processor.getNextTaskRun() == null);
                                        ExecPlan execPlan = processor.getMvContext().getExecPlan();
                                        Assertions.assertTrue(execPlan == null);
                                    }

                                    {
                                        // add new partitions
                                        LocalDateTime now = LocalDateTime.now();
                                        addListPartition(tableName, "p5", "guangdong",
                                                now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), true);
                                        addListPartition(tableName, "p6", "guangdong",
                                                now.minusMonths(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), true);

                                        MVPCTBasedRefreshProcessor processor = refreshMV("test", mv);
                                        Assertions.assertTrue(processor != null);
                                        Assertions.assertTrue(processor.getNextTaskRun() == null);
                                        Assertions.assertEquals(2, mv.getVisiblePartitions().size());
                                        ExecPlan execPlan = processor.getMvContext().getExecPlan();
                                        Assertions.assertTrue(execPlan != null);
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
        for (String table : TABLES_WITH_DATE_DT_TYPES) {
            starRocksAssert.withTable(table,
                    (obj) -> {
                        String tableName = (String) obj;
                        withTablePartitions(tableName);
                        String mvCreateDdl = String.format("create materialized view test_mv1\n" +
                                "partition by (dt) \n" +
                                "distributed by random \n" +
                                "REFRESH DEFERRED MANUAL \n" +
                                "as select * from %s;", tableName);
                        starRocksAssert.withMaterializedView(mvCreateDdl,
                                () -> {
                                    String mvName = "test_mv1";
                                    MaterializedView mv = starRocksAssert.getMv("test", mvName);
                                    {
                                        // all partitions are expired, no need to create partitions for mv
                                        MVPCTBasedRefreshProcessor processor = refreshMV("test", mv);
                                        Assertions.assertEquals(2, mv.getVisiblePartitions().size());
                                        Assertions.assertTrue(processor.getNextTaskRun() == null);
                                        ExecPlan execPlan = processor.getMvContext().getExecPlan();
                                        Assertions.assertTrue(execPlan == null);
                                    }

                                    // alter mv ttl condition
                                    String alterMVSql = String.format("alter materialized view %s set (" +
                                            "'partition_retention_condition' = 'dt >= current_date() - " +
                                            "interval 1 month')", mvName);
                                    starRocksAssert.alterMvProperties(alterMVSql);

                                    {
                                        // all partitions are expired, no need to create partitions for mv
                                        MVPCTBasedRefreshProcessor processor = refreshMV("test", mv);
                                        Assertions.assertEquals(2, mv.getVisiblePartitions().size());
                                        Assertions.assertTrue(processor.getNextTaskRun() == null);
                                        ExecPlan execPlan = processor.getMvContext().getExecPlan();
                                        Assertions.assertTrue(execPlan == null);
                                    }

                                    {
                                        // add new partitions
                                        LocalDateTime now = LocalDateTime.now();
                                        addListPartition(tableName, "p5", "guangdong",
                                                now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), true);
                                        addListPartition(tableName, "p6", "guangdong",
                                                now.minusMonths(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")), true);

                                        MVPCTBasedRefreshProcessor processor = refreshMV("test", mv);
                                        Assertions.assertTrue(processor != null);
                                        Assertions.assertTrue(processor.getNextTaskRun() == null);
                                        Assertions.assertEquals(4, mv.getVisiblePartitions().size());
                                        ExecPlan execPlan = processor.getMvContext().getExecPlan();
                                        Assertions.assertTrue(execPlan != null);
                                        String plan = execPlan.getExplainString(StatementBase.ExplainLevel.NORMAL);
                                        PlanTestBase.assertContains(plan, "     PREAGGREGATION: ON\n" +
                                                "     partitions=2/6");
                                    }

                                    // run partition ttl scheduler
                                    {
                                        DynamicPartitionScheduler scheduler = GlobalStateMgr.getCurrentState()
                                                .getDynamicPartitionScheduler();
                                        scheduler.runOnceForTest();
                                        Assertions.assertEquals(2, mv.getVisiblePartitions().size());
                                    }
                                });
                    });
        }
    }

    @Test
    public void testPartitionConditionTTL1() throws Exception {
        starRocksAssert.withTable("create table list_par_int(\n" +
                " k1 int,\n" +
                " k2 string)\n" +
                " partition by list(k1)\n" +
                " (partition p1 values in('1','2'),\n" +
                "  partition p2 values in('3','4'),\n" +
                "  partition p3 values in('5','6'),\n" +
                "  partition p4 values in('7','8'),\n" +
                "  partition p5 values in('9','10'),\n" +
                "  partition p6 values in('11','12'),\n" +
                "  partition p7 values in('13','14'),\n" +
                "  partition p8 values in('15','16'),\n" +
                "  partition p9 values in('17','18'),\n" +
                "  partition p10 values in('19','20'))\n" +
                " distributed by hash(k1)\n");

        OlapTable olapTable = (OlapTable) starRocksAssert.getTable("test", "list_par_int");
        Assertions.assertEquals(10, olapTable.getVisiblePartitions().size());

        String sql = "alter table list_par_int drop partitions where k1 > 5";
        starRocksAssert.alterTable(sql);
        Assertions.assertEquals(3, olapTable.getVisiblePartitions().size());
        String[] expectedPartitions = {"p1", "p2", "p3"};
        Assertions.assertArrayEquals(expectedPartitions, olapTable.getVisiblePartitionNames().toArray());
    }
}