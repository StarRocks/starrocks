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

package com.starrocks.externalcooldown;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.BaseTable;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;


public class ExternalCooldownPartitionSelectorTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_strict_storage_medium_check = false;
        Config.enable_experimental_rowstore = true;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @Test
    public void testGetPartition() throws Exception {
        Config.default_replication_num = 1;

        starRocksAssert.withDatabase("test");
        starRocksAssert.useDatabase("test");

        new MockUp<IcebergMetadata>() {
            @Mock
            public Table getTable(String dbName, String tblName) {
                return new IcebergTable(1, "iceberg_tbl", "iceberg_catalog",
                        "iceberg_catalog", "iceberg_db",
                        "table1", Lists.newArrayList(), new BaseTable(null, ""), Maps.newHashMap());
            }
        };
        new MockUp<IcebergTable>() {
            @Mock
            public String getTableIdentifier() {
                return "iceberg_catalog.iceberg_db.iceberg_tbl";
            }
        };

        ConnectorMgr connectorMgr = GlobalStateMgr.getCurrentState().getConnectorMgr();
        Map<String, String> properties = Maps.newHashMap();

        properties.put("type", "iceberg");
        properties.put("iceberg.catalog.type", "hive");
        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        connectorMgr.createConnector(new ConnectorContext("iceberg_catalog", "iceberg", properties));

        starRocksAssert.withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 1\n" +
                        "PROPERTIES(" +
                        "'replication_num' = '1',\n" +
                        "'external_cooldown_target' = 'iceberg_catalog.iceberg_db.iceberg_tbl',\n" +
                        "'external_cooldown_schedule' = 'START 01:00 END 07:59 EVERY INTERVAL 1m'\n," +
                        "'external_cooldown_wait_second' = '3600'\n" +
                        ");");

        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) testDb.getTable("tbl1");
        ExternalCooldownPartitionSelector selector = new ExternalCooldownPartitionSelector(testDb, table);
        Assert.assertTrue(selector.isTableSatisfied());
    }
}