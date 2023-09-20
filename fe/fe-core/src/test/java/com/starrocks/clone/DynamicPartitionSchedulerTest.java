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

package com.starrocks.clone;

import com.google.common.collect.Range;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.util.Map;

public class DynamicPartitionSchedulerTest {

    private static final Logger LOG = LogManager.getLogger(DynamicPartitionSchedulerTest.class);
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
    }

    @Test
    public void testPartitionTTLProperties() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    v1 int \n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01'),\n" +
                        "    PARTITION p3 values less than('2020-04-01'),\n" +
                        "    PARTITION p4 values less than('2020-05-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                        "PROPERTIES" +
                        "(" +
                        "    'replication_num' = '1'\n" +
                        ");");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("tbl1");
        // Now the table does not actually support partition ttl,
        // so in order to simplify the test, it is directly set like this
        tbl.getTableProperty().getProperties().put("partition_ttl_number", "3");
        tbl.getTableProperty().setPartitionTTLNumber(3);

        dynamicPartitionScheduler.registerTtlPartitionTable(db.getId(), tbl.getId());
        dynamicPartitionScheduler.runAfterCatalogReady();


        Assert.assertEquals(3, tbl.getPartitions().size());
    }

    @Test
    public void testPartitionTTLPropertiesZero() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.base\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");
        String sql = "create materialized view mv1 " +
                "partition by k1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"partition_ttl_number\" = \"0\"\n" +
                ") " +
                "as select k1, k2 from test.base;";
        Config.enable_experimental_mv = true;
        CreateMaterializedViewStatement createMaterializedViewStatement =
                (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().createMaterializedView(createMaterializedViewStatement);
            Assert.fail();
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("Illegal Partition TTL Number"));
        }
    }

    @Test
    public void testAutoPartitionPartitionLiveNumber() throws Exception {
        new MockUp<LocalDateTime>() {
            @Mock
            public LocalDateTime now() {
                return  LocalDateTime.of(2023, 3, 30, 1, 1, 1);
            }
        };

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE site_access(\n" +
                        "    event_day datetime,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('day', event_day)(\n" +
                        " START (\"2023-03-27\") END (\"2023-03-31\") EVERY (INTERVAL 1 day),\n" +
                        " START (\"9999-12-30\") END (\"9999-12-31\") EVERY (INTERVAL 1 day)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\n" +
                        "    \"partition_live_number\" = \"3\",\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("site_access");
        dynamicPartitionScheduler.registerTtlPartitionTable(db.getId(), tbl.getId());
        dynamicPartitionScheduler.runOnceForTest();

        Map<String, Range<PartitionKey>> rangePartitionMap = tbl.getRangePartitionMap();

        Assert.assertFalse(rangePartitionMap.containsKey("p20230327"));
        Assert.assertTrue(rangePartitionMap.containsKey("p20230328"));
        Assert.assertTrue(rangePartitionMap.containsKey("p20230329"));
        Assert.assertTrue(rangePartitionMap.containsKey("p20230330"));
        Assert.assertTrue(rangePartitionMap.containsKey("p99991230"));
    }

    @Test
    public void testAutoRandomPartitionFPartitionLiveNumber() throws Exception {
        new MockUp<LocalDateTime>() {
            @Mock
            public LocalDateTime now() {
                return  LocalDateTime.of(2023, 3, 30, 1, 1, 1);
            }
        };

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE site_access(\n" +
                        "    event_day datetime,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('day', event_day)(\n" +
                        " START (\"2023-03-27\") END (\"2023-03-31\") EVERY (INTERVAL 1 day),\n" +
                        " START (\"9999-12-30\") END (\"9999-12-31\") EVERY (INTERVAL 1 day)\n" +
                        ")\n" +
                        "DISTRIBUTED BY RANDOM BUCKETS 32\n" +
                        "PROPERTIES(\n" +
                        "    \"partition_live_number\" = \"3\",\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("site_access");
        dynamicPartitionScheduler.registerTtlPartitionTable(db.getId(), tbl.getId());
        dynamicPartitionScheduler.runOnceForTest();

        Map<String, Range<PartitionKey>> rangePartitionMap = tbl.getRangePartitionMap();

        Assert.assertFalse(rangePartitionMap.containsKey("p20230327"));
        Assert.assertTrue(rangePartitionMap.containsKey("p20230328"));
        Assert.assertTrue(rangePartitionMap.containsKey("p20230329"));
        Assert.assertTrue(rangePartitionMap.containsKey("p20230330"));
        Assert.assertTrue(rangePartitionMap.containsKey("p99991230"));
    }

    private void buildTimePartitions(String tableName, OlapTable tbl, int partitionCount) throws Exception {
        LocalDate currentDate = LocalDate.now();
        for (int i = 0; i < partitionCount; i++) {
            LocalDate lowerBound = currentDate.minus(Period.ofMonths(i + 1));
            LocalDate upperBound = currentDate.minus(Period.ofMonths(i));
            String partitionName = String.format("p_%d_%d", lowerBound.getYear(), lowerBound.getMonthValue());
            String addPartition = String.format("alter table %s add partition p%s values [('%s'), ('%s')) ",
                    tableName, partitionName, lowerBound.toString(), upperBound);
            starRocksAssert.getCtx().executeSql(addPartition);
        }
        LOG.info("table partitions: {}", tbl.getRangePartitionMap());
    }

    @Test
    public void testMaterializedViewPartitionTTL() throws Exception {
        String tableName = "test.tbl1";
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE " + tableName +
                        "(\n" +
                        "    k1 date,\n" +
                        "    v1 int \n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2022-06-01'),\n" +
                        "    PARTITION p2 values less than('2022-07-01'),\n" +
                        "    PARTITION p3 values less than('2022-08-01'),\n" +
                        "    PARTITION p4 values less than('2022-09-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                        "PROPERTIES\n" +
                        "(\n" +
                        "    'replication_num' = '1'\n" +
                        ");");
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW test.mv_ttl_mv1\n" +
                " REFRESH ASYNC " +
                " PARTITION BY k1\n" +
                " PROPERTIES('partition_ttl'='2 month')" +
                " AS SELECT k1, v1 FROM test.tbl1");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("mv_ttl_mv1");

        buildTimePartitions(tableName, tbl, 10);

        // normal ttl
        starRocksAssert.getCtx().executeSql("alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='2 month')");
        starRocksAssert.getCtx().executeSql("refresh materialized view test.mv_ttl_mv1 with sync mode");
        dynamicPartitionScheduler.runAfterCatalogReady();
        Assert.assertEquals(tbl.getRangePartitionMap().toString(), 2, tbl.getPartitions().size());

        // large ttl
        starRocksAssert.getCtx().executeSql("alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='10 year')");
        starRocksAssert.getCtx().executeSql("refresh materialized view test.mv_ttl_mv1 with sync mode");
        dynamicPartitionScheduler.runAfterCatalogReady();
        Assert.assertEquals(tbl.getRangePartitionMap().toString(), 14, tbl.getPartitions().size());

        // tiny ttl
        starRocksAssert.getCtx().executeSql("alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='1 day')");
        starRocksAssert.getCtx().executeSql("refresh materialized view test.mv_ttl_mv1 with sync mode");
        dynamicPartitionScheduler.runAfterCatalogReady();
        Assert.assertEquals(tbl.getRangePartitionMap().toString(), 1, tbl.getPartitions().size());

        // zero ttl
        starRocksAssert.getCtx().executeSql("alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='0 day')");
        starRocksAssert.getCtx().executeSql("refresh materialized view test.mv_ttl_mv1 with sync mode");
        dynamicPartitionScheduler.runAfterCatalogReady();
        Assert.assertEquals(tbl.getRangePartitionMap().toString(), 14, tbl.getPartitions().size());
        Assert.assertEquals("PT0S", tbl.getTableProperty().getPartitionTTL().toString());

        // tiny ttl
        starRocksAssert.getCtx().executeSql("alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='24 hour')");
        starRocksAssert.getCtx().executeSql("refresh materialized view test.mv_ttl_mv1 with sync mode");
        dynamicPartitionScheduler.runAfterCatalogReady();
        Assert.assertEquals(tbl.getRangePartitionMap().toString(), 1, tbl.getPartitions().size());

        // the ttl cross two partitions
        starRocksAssert.getCtx().executeSql("alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='32 day')");
        starRocksAssert.getCtx().executeSql("refresh materialized view test.mv_ttl_mv1 with sync mode");
        dynamicPartitionScheduler.runAfterCatalogReady();
        Assert.assertEquals(tbl.getRangePartitionMap().toString(), 2, tbl.getPartitions().size());

        // corner cases
        starRocksAssert.getCtx().executeSql("alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='error')");
        Assert.assertEquals("P32D", tbl.getTableProperty().getPartitionTTL().toString());
        starRocksAssert.getCtx().executeSql("alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='day')");
        Assert.assertEquals("P32D", tbl.getTableProperty().getPartitionTTL().toString());
        starRocksAssert.getCtx().executeSql("alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='0')");
        Assert.assertEquals("P32D", tbl.getTableProperty().getPartitionTTL().toString());
        starRocksAssert.getCtx().executeSql("alter materialized view test.mv_ttl_mv1 set ('partition_ttl'='0 day')");
        Assert.assertEquals("PT0S", tbl.getTableProperty().getPartitionTTL().toString());
    }
}
