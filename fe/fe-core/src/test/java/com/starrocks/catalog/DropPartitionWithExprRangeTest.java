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

import com.starrocks.common.util.UUIDUtil;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DropPartitionWithExprRangeTest {
    private static final Logger LOG = LogManager.getLogger(DropPartitionWithExprRangeTest.class);
    protected static ConnectContext connectContext;
    protected static PseudoCluster cluster;
    protected static StarRocksAssert starRocksAssert;
    private static String R1;
    private static String R2;

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
    }

    @AfterClass
    public static void afterClass() throws Exception {
    }

    @Before
    public void before() {
    }

    @After
    public void after() throws Exception {
    }

    public static void executeInsertSql(String sql) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        StatementBase statement = SqlParser.parseSingleStatement(sql, connectContext.getSessionVariable().getSqlMode());
        new StmtExecutor(connectContext, statement).execute();
    }

    private String toPartitionVal(String val) {
        return val == null ? "NULL" : String.format("'%s'", val);
    }

    private void addRangePartition(String tbl, String pName, String pVal1, String pVal2) {
        // mock the check to ensure test can run
        new MockUp<ExpressionRangePartitionInfo>() {
            @Mock
            public boolean isAutomaticPartition() {
                return false;
            }
        };
        new MockUp<ExpressionRangePartitionInfoV2>() {
            @Mock
            public boolean isAutomaticPartition() {
                return false;
            }
        };
        try {
            String addPartitionSql = String.format("ALTER TABLE %s ADD " +
                    "PARTITION %s VALUES [(%s),(%s))", tbl, pName, toPartitionVal(pVal1), toPartitionVal(pVal2));
            System.out.println(addPartitionSql);
            starRocksAssert.alterTable(addPartitionSql);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Failed to add partition", e);
        }
    }

    private void withTablePartitions(String tableName) {
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
}