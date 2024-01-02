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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/DynamicPartitionTableTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DynamicPartitionTableTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.alter_scheduler_interval_millisecond = 1000;
        FeConstants.runningUnitTest = true;

        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();

        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
    }

    @Test
    public void testNormal() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_normal` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");");
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("dynamic_partition_normal");
        Assert.assertEquals(table.getTableProperty().getDynamicPartitionProperty().getReplicationNum(),
                DynamicPartitionProperty.NOT_SET_REPLICATION_NUM);
    }

    @Test
    public void testMissTimeUnit() throws Exception {
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("Must assign dynamic_partition.time_unit properties");
        starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_time_unit` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");");
    }

    @Test
    public void testMissStart() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_start` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");");
    }

    @Test
    public void testMissEnd() throws Exception {
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("Must assign dynamic_partition.end properties");
        starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_end` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");");
    }

    @Test
    public void testMissBuckets() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_buckets` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\"\n" +
                ");");
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("dynamic_partition_buckets");
        Assert.assertEquals(table.getTableProperty().getDynamicPartitionProperty().getBuckets(), 0);
    }

    @Test
    public void testNotAllowed() throws Exception {
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("Only support dynamic partition properties on range partition table");
        starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_non_range` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");");
    }

    @Test
    public void testNotAllowedInMultiPartitions() throws Exception {
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("Dynamic partition only support single-column range partition");
        starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_normal` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1, k2)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\", \"100\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\", \"200\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\", \"300\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");");
    }

    @Test
    public void testMissTimeZone() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_miss_time_zone` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\"\n" +
                ");");
    }

    @Test
    public void testNormalTimeZone() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_time_zone` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.time_zone\" = \"Asia/Shanghai\",\n" +
                "\"dynamic_partition.prefix\" = \"p\"\n" +
                ");");
    }

    @Test
    public void testInvalidTimeZone() throws Exception {
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("Unknown or incorrect time zone: 'invalid'");
        starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_invalid_time_zone` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.buckets\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.time_zone\" = \"invalid\",\n" +
                "\"dynamic_partition.prefix\" = \"p\"\n" +
                ");");
    }

    @Test
    public void testSetDynamicPartitionReplicationNum() throws Exception {
        String tableName = "dynamic_partition_replication_num";
        starRocksAssert.withTable("CREATE TABLE test.`" + tableName + "` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE (k1)\n" +
                "(\n" +
                "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\",\n" +
                "\"dynamic_partition.replication_num\" = \"2\"\n" +
                ");");
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable(tableName);
        Assert.assertEquals(table.getTableProperty().getDynamicPartitionProperty().getReplicationNum(), 2);
    }

    @Test
    public void testEmptyDynamicPartition() throws Exception {
        String createOlapTblStmt = "CREATE TABLE test.`empty_dynamic_partition` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int NULL COMMENT \"\",\n" +
                "  `k3` smallint NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "()\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"dynamic_partition.enable\" = \"false\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.time_unit\" = \"day\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"dynamic_partition.buckets\" = \"1\"\n" +
                ");";
        String insertStmt =
                "insert into test.`empty_dynamic_partition` values ('2020-09-10', 1000, 100, 'test', '2020-09-10 23:59:59');";
        createTable(createOlapTblStmt);
        expectedException.expect(AnalysisException.class);
        expectedException.expectMessage("data cannot be inserted into table with empty partition." +
                "Use `SHOW PARTITIONS FROM empty_dynamic_partition` to see the currently partitions of this table. ");
        UtFrameUtils.parseStmtWithNewParser("explain " + insertStmt, connectContext);
    }
}
