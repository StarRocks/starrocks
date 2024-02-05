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
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.DynamicPartitionProperty;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;

public class DynamicPartitionSchedulerTest {

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

    @Test
    public void testRandomDynamicPartitionShouldMatchConfig() throws Exception {
        new MockUp<LocalDateTime>() {
            @Mock
            public LocalDateTime now() {
                return  LocalDateTime.of(2023, 3, 30, 1, 1, 1);
            }
        };

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test_random_bucket (\n" +
                        "    uid String,\n" +
                        "    tdbank_imp_date Date\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`uid`) \n" +
                        "PARTITION BY RANGE(`tdbank_imp_date`) ()\n" +
                        "DISTRIBUTED BY RANDOM BUCKETS 1\n" +
                        "PROPERTIES (\n" +
                        "     \"replication_num\" = \"1\", \n" +
                        "     \"dynamic_partition.enable\" = \"true\", \n" +
                        "     \"dynamic_partition.time_unit\" = \"DAY\", \n" +
                        "     \"dynamic_partition.time_zone\" = \"Asia/Shanghai\", \n" +
                        "     \"dynamic_partition.start\" = \"-180\", \n" +
                        "     \"dynamic_partition.end\" = \"3\", \n" +
                        "     \"dynamic_partition.prefix\" = \"p\", \n" +
                        "     \"dynamic_partition.buckets\" = \"4\", \n" +
                        "     \"dynamic_partition.history_partition_num\" = \"0\",\n" +
                        "     \"compression\" = \"LZ4\" );");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("test_random_bucket");
        dynamicPartitionScheduler.registerTtlPartitionTable(db.getId(), tbl.getId());
        dynamicPartitionScheduler.runOnceForTest();

        Collection<Partition> partitions = tbl.getPartitions();
        for (Partition partition : partitions) {
            DistributionInfo distributionInfo = partition.getDistributionInfo();
            Assert.assertEquals(4, distributionInfo.getBucketNum());
        }
    }

    @Test
    public void testPartitionColumnDateUseDynamicHour() throws Exception {
        new MockUp<LocalDateTime>() {
            @Mock
            public LocalDateTime now() {
                return  LocalDateTime.of(2023, 3, 30, 1, 1, 1);
            }
        };

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE `test_hour_partition2` (\n" +
                        "  `event_day` date NULL COMMENT \"\",\n" +
                        "  `site_id` int(11) NULL DEFAULT \"10\" COMMENT \"\",\n" +
                        "  `city_code` varchar(100) NULL COMMENT \"\",\n" +
                        "  `user_name` varchar(32) NULL DEFAULT \"\" COMMENT \"\",\n" +
                        "  `pv` bigint(20) NULL DEFAULT \"0\" COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`event_day`, `site_id`, `city_code`, `user_name`)\n" +
                        "PARTITION BY RANGE(`event_day`)\n" +
                        "()\n" +
                        "DISTRIBUTED BY HASH(`event_day`, `site_id`) BUCKETS 32 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"dynamic_partition.enable\" = \"true\",\n" +
                        "\"dynamic_partition.time_unit\" = \"DAY\",\n" +
                        "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n" +
                        "\"dynamic_partition.start\" = \"-1\",\n" +
                        "\"dynamic_partition.end\" = \"10\",\n" +
                        "\"dynamic_partition.prefix\" = \"p\",\n" +
                        "\"dynamic_partition.buckets\" = \"3\",\n" +
                        "\"dynamic_partition.history_partition_num\" = \"0\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\",\n" +
                        "\"enable_persistent_index\" = \"false\",\n" +
                        "\"compression\" = \"LZ4\"\n" +
                        ");");

        DynamicPartitionScheduler dynamicPartitionScheduler = GlobalStateMgr.getCurrentState()
                .getDynamicPartitionScheduler();
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl = (OlapTable) db.getTable("test_hour_partition2");
        DynamicPartitionProperty dynamicPartitionProperty = tbl.getTableProperty().getDynamicPartitionProperty();
        dynamicPartitionProperty.setTimeUnit("HOUR");
        boolean result = dynamicPartitionScheduler.executeDynamicPartitionForTable(db.getId(), tbl.getId());
        Assert.assertFalse(result);
    }

}
