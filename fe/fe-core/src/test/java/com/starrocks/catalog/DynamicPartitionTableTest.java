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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DynamicPartitionTableTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
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
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "dynamic_partition_normal");
        Assertions.assertEquals(table.getTableProperty().getDynamicPartitionProperty().getReplicationNum(),
                    DynamicPartitionProperty.NOT_SET_REPLICATION_NUM);
    }

    @Test
    public void testMissTimeUnit() {
        Throwable exception = assertThrows(DdlException.class,
                () -> starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_time_unit` (\n" +
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
                    ");"));
        assertThat(exception.getMessage(), containsString("Must assign dynamic_partition.time_unit properties"));
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
    public void testMissEnd() {
        Throwable exception =
                assertThrows(DdlException.class, () -> starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_end` (\n" +
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
                    ");"));
        assertThat(exception.getMessage(), containsString("Must assign dynamic_partition.end properties"));
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
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "dynamic_partition_buckets");
        Assertions.assertEquals(table.getTableProperty().getDynamicPartitionProperty().getBuckets(), 0);
    }

    @Test
    public void testNotAllowed() {
        Throwable exception = assertThrows(DdlException.class,
                () -> starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_non_range` (\n" +
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
                    ");"));
        assertThat(exception.getMessage(), containsString("Only support dynamic partition properties on range partition table"));
    }

    @Test
    public void testNotAllowedInMultiPartitions() {
        Throwable exception = assertThrows(DdlException.class,
                () -> starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_normal` (\n" +
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
                    ");"));
        assertThat(exception.getMessage(), containsString("Dynamic partition only support single-column range partition"));
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
    public void testInvalidTimeZone() {
        Throwable exception = assertThrows(DdlException.class,
                () -> starRocksAssert.withTable("CREATE TABLE test.`dynamic_partition_invalid_time_zone` (\n" +
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
                    ");"));
        assertThat(exception.getMessage(), containsString("Unknown or incorrect time zone: 'invalid'"));
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
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
        Assertions.assertEquals(table.getTableProperty().getDynamicPartitionProperty().getReplicationNum(), 2);
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
        Throwable exception = assertThrows(AnalysisException.class,
                () -> UtFrameUtils.parseStmtWithNewParser("explain " + insertStmt, connectContext));
        assertThat(exception.getMessage(), containsString("data cannot be inserted into table with empty partition." +
                "Use `SHOW PARTITIONS FROM empty_dynamic_partition` to see the currently partitions of this table. "));
    }
}
