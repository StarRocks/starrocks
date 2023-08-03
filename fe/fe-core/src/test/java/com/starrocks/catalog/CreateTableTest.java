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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/CreateTableTest.java

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
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.system.Backend;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class CreateTableTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        Backend be = UtFrameUtils.addMockBackend(10002);
        be.setIsDecommissioned(true);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addMockBackend(10004);
        Config.enable_strict_storage_medium_check = true;
        Config.enable_auto_tablet_distribution = true;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().createTable(createTableStmt);
    }

    private static void alterTableWithNewParser(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().alterTable(alterTableStmt);
    }

    @Test(expected = DdlException.class)
    public void testNotSpecifyReplicateNum() throws Exception {
        createTable(
                "CREATE TABLE test.`duplicate_table_with_null` ( `k1`  date, `k2`  datetime,`k3`  " +
                        "char(20), `k4`  varchar(20), `k5`  boolean, `k6`  tinyint, `k7`  smallint, " +
                        "`k8`  int, `k9`  bigint, `k10` largeint, `k11` float, `k12` double, " +
                        "`k13` decimal(27,9)) DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) PARTITION BY " +
                        "time_slice(k2, interval 1 hour) DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) " +
                        "PROPERTIES ( \"storage_format\" = \"v2\");"
        );
    }

    @Test(expected = SemanticException.class)
    public void testCreateUnsupportedType() throws Exception {
        createTable(
                "CREATE TABLE test.ods_warehoused (\n" +
                        " warehouse_id                                bigint(20)                 COMMENT        ''\n" +
                        ",company_id                                        bigint(20)                 COMMENT        ''\n" +
                        ",company_name                                string                        COMMENT        ''\n" +
                        ",is_sort_express_by_cost        tinyint(1)                COMMENT        ''\n" +
                        ",is_order_intercepted                tinyint(1)                COMMENT        ''\n" +
                        ",intercept_time_type                tinyint(3)                 COMMENT        ''\n" +
                        ",intercept_time                                time                        COMMENT        ''\n" +
                        ",intercept_begin_time                time                        COMMENT        ''\n" +
                        ",intercept_end_time                        time                        COMMENT        ''\n" +
                        ")\n" +
                        "PRIMARY KEY(warehouse_id)\n" +
                        "COMMENT \"\"\n" +
                        "DISTRIBUTED BY HASH(warehouse_id)\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");"
        );
    }

    @Test
    public void testNormal() throws DdlException {

        ExceptionChecker.expectThrowsNoException(
                () -> createTable(
                        "CREATE TABLE test.case_insensitive (\n" +
                                "    A1 TINYINT,\n" +
                                "    A2 DATE\n" +
                                ") ENGINE=OLAP\n" +
                                "DUPLICATE KEY(A1)\n" +
                                "COMMENT \"OLAP\"\n" +
                                "PARTITION BY RANGE (a2) (\n" +
                                "START (\"2021-01-01\") END (\"2022-01-01\") EVERY (INTERVAL 1 year)\n" +
                                ")\n" +
                                "DISTRIBUTED BY HASH(A1) BUCKETS 20\n" +
                                "PROPERTIES(\"replication_num\" = \"1\");"));

        ExceptionChecker.expectThrowsNoException(
                () -> createTable(
                        "create table test.lp_tbl0\n" + "(k1 bigint, k2 varchar(16) not null)\n" + "duplicate key(k1)\n"
                                + "partition by list(k2)\n" + "(partition p1 values in (\"shanghai\",\"beijing\"))\n"
                                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1');"));

        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.lp_tbl1\n" + "(k1 bigint, k2 varchar(16) not null," +
                        " dt varchar(10) not null)\n duplicate key(k1)\n"
                        + "partition by list(k2,dt)\n" + "(partition p1 values in ((\"2022-04-01\", \"shanghai\")) )\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1');"));

        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.lp_tbl2\n" + "(k1 bigint, k2 varchar(16), dt varchar(10))\n" +
                        "duplicate key(k1)\n"
                        + "partition by range(k1)\n" + "(partition p1 values [(\"1\"), (MAXVALUE)) )\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1');"));

        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.tbl1\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        ExceptionChecker.expectThrowsNoException(() -> createTable("create table test.tbl2\n" + "(k1 int, k2 int)\n"
                + "duplicate key(k1)\n" + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.tbl3\n" + "(k1 varchar(40), k2 int)\n" + "duplicate key(k1)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1');"));

        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.tbl4\n" + "(k1 varchar(40), k2 int, v1 int sum)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.tbl5\n" + "(k1 varchar(40), k2 int, v1 int sum)\n" + "aggregate key(k1,k2)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.tbl6\n" + "(k1 varchar(40), k2 int, k3 int)\n" + "duplicate key(k1, k2, k3)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tbl7\n" + "(k1 varchar(40), k2 int)\n"
                        + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1');"));

        ConfigBase.setMutableConfig("enable_strict_storage_medium_check", "false");
        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tb7(key1 int, key2 varchar(10)) \n"
                        +
                        "distributed by hash(key1) buckets 1 properties('replication_num' = '1', 'storage_medium' = 'ssd');"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tb8(key1 int, key2 varchar(10)) \n"
                        + "distributed by hash(key1) buckets 1 \n"
                        + "properties('replication_num' = '1', 'compression' = 'lz4_frame');"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tb9(key1 int, key2 varchar(10)) \n"
                        + "distributed by hash(key1) buckets 1 \n"
                        + "properties('replication_num' = '1', 'compression' = 'lz4');"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tb10(key1 int, key2 varchar(10)) \n"
                        + "distributed by hash(key1) buckets 1 \n"
                        + "properties('replication_num' = '1', 'compression' = 'zstd');"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tb11(key1 int, key2 varchar(10)) \n"
                        + "distributed by hash(key1) buckets 1 \n"
                        + "properties('replication_num' = '1', 'compression' = 'zlib');"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tb12(col1 bigint AUTO_INCREMENT, \n"
                        + "col2 varchar(10)) \n"
                        + "Primary KEY (col1) distributed by hash(col1) buckets 1 \n"
                        + "properties('replication_num' = '1', 'replicated_storage' = 'true');"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("create table test.tb13(col1 bigint, col2 bigint AUTO_INCREMENT) \n"
                        + "Primary KEY (col1) distributed by hash(col1) buckets 1 \n"
                        + "properties('replication_num' = '1', 'replicated_storage' = 'true');"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("CREATE TABLE test.full_width_space (\n" +
                        "    datekey DATE,\n" +
                        "    site_id INT,\n" +
                        "    city_code SMALLINT,\n" +
                        "    user_name VARCHAR(32),\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "ENGINE=olap\n" +
                        "DUPLICATE KEY(datekey, site_id, city_code, user_name)\n" +
                        "PARTITION BY RANGE (datekey) (\n" +
                        "　START (\"2019-01-01\") END (\"2021-01-01\") EVERY (INTERVAL 1 YEAR)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(site_id) BUCKETS 10\n" +
                        "PROPERTIES (\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");"));

        ExceptionChecker
                .expectThrowsNoException(() -> createTable("CREATE TABLE test.dynamic_partition_without_prefix (\n" +
                        "event_day DATE,\n" +
                        "site_id INT DEFAULT '10',\n" +
                        "city_code VARCHAR(\n" +
                        "100\n" +
                        "),\n" +
                        "user_name VARCHAR(\n" +
                        "32\n" +
                        ") DEFAULT '',\n" +
                        "pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY RANGE(event_day)(\n" +
                        "PARTITION p20200321 VALUES LESS THAN (\"2020-03-22\"),\n" +
                        "PARTITION p20200322 VALUES LESS THAN (\"2020-03-23\"),\n" +
                        "PARTITION p20200323 VALUES LESS THAN (\"2020-03-24\"),\n" +
                        "PARTITION p20200324 VALUES LESS THAN (\"2020-03-25\")\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                        "PROPERTIES(\n" +
                        "\t\"replication_num\" = \"1\",\n" +
                        "    \"dynamic_partition.enable\" = \"true\",\n" +
                        "    \"dynamic_partition.time_unit\" = \"DAY\",\n" +
                        "    \"dynamic_partition.start\" = \"-3\",\n" +
                        "    \"dynamic_partition.end\" = \"3\",\n" +
                        "    \"dynamic_partition.history_partition_num\" = \"0\"\n" +
                        ");"));

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable tbl6 = (OlapTable) db.getTable("tbl6");
        Assert.assertTrue(tbl6.getColumn("k1").isKey());
        Assert.assertTrue(tbl6.getColumn("k2").isKey());
        Assert.assertTrue(tbl6.getColumn("k3").isKey());

        OlapTable tbl7 = (OlapTable) db.getTable("tbl7");
        Assert.assertTrue(tbl7.getColumn("k1").isKey());
        Assert.assertFalse(tbl7.getColumn("k2").isKey());
        Assert.assertTrue(tbl7.getColumn("k2").getAggregationType() == AggregateType.NONE);
    }

    @Test
    public void testAbnormal() throws DdlException {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "FLOAT column can not be distribution column",
                () -> createTable("create table test.atbl1\n" + "(k1 int, k2 float)\n" + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Invalid partition column 'k3': invalid data type FLOAT",
                () -> createTable("create table test.atbl3\n" + "(k1 int, k2 int, k3 float)\n" + "duplicate key(k1)\n"
                        + "partition by range(k3)\n" + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Varchar should not in the middle of short keys",
                () -> createTable("create table test.atbl3\n" + "(k1 varchar(40), k2 int, k3 int)\n"
                        + "duplicate key(k1, k2, k3)\n" + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1', 'short_key' = '3');"));

        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Short key is too large. should less than: 3",
                () -> createTable("create table test.atbl4\n" + "(k1 int, k2 int, k3 int)\n"
                        + "duplicate key(k1, k2, k3)\n" + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1', 'short_key' = '4');"));

        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Failed to find enough hosts with storage " +
                                "medium HDD at all backends, number of replicas needed: 3",
                        () -> createTable("create table test.atbl5\n" + "(k1 int, k2 int, k3 int)\n"
                                + "duplicate key(k1, k2, k3)\n" + "distributed by hash(k1) buckets 1\n"
                                + "properties('replication_num' = '3');"));

        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.atbl6\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); "));

        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Table 'atbl6' already exists",
                        () -> createTable("create table test.atbl6\n" + "(k1 int, k2 int, k3 int)\n"
                                + "duplicate key(k1, k2, k3)\n" + "distributed by hash(k1) buckets 1\n"
                                + "properties('replication_num' = '1');"));

        ConfigBase.setMutableConfig("enable_strict_storage_medium_check", "true");
        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class,
                        "Failed to find enough hosts with storage " +
                                "medium SSD at all backends, number of replicas needed: 1",
                        () -> createTable(
                                "create table test.tb7(key1 int, key2 varchar(10)) distributed by hash(key1) \n"
                                        + "buckets 1 properties('replication_num' = '1', 'storage_medium' = 'ssd');"));

        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "unknown compression type: xxx",
                        () -> createTable("create table test.atbl8\n" + "(key1 int, key2 varchar(10))\n"
                                + "distributed by hash(key1) buckets 1\n"
                                + "properties('replication_num' = '1', 'compression' = 'xxx');"));

        ExceptionChecker
                .expectThrowsWithMsg(IllegalArgumentException.class, "The AUTO_INCREMENT column must be BIGINT",
                        () -> createTable("create table test.atbl9(col1 int AUTO_INCREMENT, col2 varchar(10)) \n"
                                + "Primary KEY (col1) distributed by hash(col1) buckets 1 \n"
                                + "properties('replication_num' = '1', 'replicated_storage' = 'true');"));

        ExceptionChecker
                .expectThrowsWithMsg(AnalysisException.class, "AUTO_INCREMENT column col1 must be NOT NULL",
                        () -> createTable("create table test.atbl10(col1 bigint NULL AUTO_INCREMENT, col2 varchar(10)) \n"
                                + "Primary KEY (col1) distributed by hash(col1) buckets 1 \n"
                                + "properties('replication_num' = '1', 'replicated_storage' = 'true');"));

        ExceptionChecker
                .expectThrowsWithMsg(IllegalArgumentException.class,
                        "More than one AUTO_INCREMENT column defined in CREATE TABLE Statement",
                        () -> createTable("create table test.atbl11(col1 bigint AUTO_INCREMENT, col2 bigint AUTO_INCREMENT) \n"
                                + "Primary KEY (col1) distributed by hash(col1) buckets 1 \n"
                                + "properties('replication_num' = '1', 'replicated_storage' = 'true');"));

        ExceptionChecker
                .expectThrowsWithMsg(DdlException.class, "Table with AUTO_INCREMENT column must use Replicated Storage",
                        () -> createTable("create table test.atbl12(col1 bigint AUTO_INCREMENT, col2 varchar(10)) \n"
                                + "Primary KEY (col1) distributed by hash(col1) buckets 1 \n"
                                + "properties('replication_num' = '1', 'replicated_storage' = 'FALSE');"));

        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown properties: {asd=true, enable_storage_cache=true, storage_cache_ttl=86400}",
                () -> createTable("CREATE TABLE test.demo (k0 tinyint NOT NULL, k1 date NOT NULL, k2 int NOT NULL," +
                        " k3 datetime not NULL, k4 bigint not NULL, k5 largeint not NULL) \n" +
                        "ENGINE = OLAP \n" +
                        "PRIMARY KEY( k0, k1, k2) \n" +
                        "PARTITION BY RANGE (k1) (START (\"1970-01-01\") END (\"2022-09-30\") " +
                        "EVERY (INTERVAL 60 day)) DISTRIBUTED BY HASH(k0) BUCKETS 1 " +
                        "PROPERTIES (\"replication_num\"=\"1\",\"enable_persistent_index\" = \"false\"," +
                        "\"enable_storage_cache\" = \"true\",\"storage_cache_ttl\" = \"86400\",\"asd\" = \"true\");"));

        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Unknown properties: {abc=def}",
                () -> createTable("CREATE TABLE test.lake_table\n" +
                        "(\n" +
                        "    k1 DATE,\n" +
                        "    k2 INT,\n" +
                        "    k3 SMALLINT,\n" +
                        "    v1 VARCHAR(2048),\n" +
                        "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                        ")\n" +
                        "DUPLICATE KEY(k1, k2, k3)\n" +
                        "PARTITION BY RANGE (k1, k2, k3)\n" +
                        "(\n" +
                        "    PARTITION p1 VALUES [(\"2014-01-01\", \"10\", \"200\"), (\"2014-01-01\", \"20\", \"300\")),\n" +
                        "    PARTITION p2 VALUES [(\"2014-06-01\", \"100\", \"200\"), (\"2014-07-01\", \"100\", \"300\"))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 32\n" +
                        "PROPERTIES ( \"replication_num\" = \"1\", \"abc\" = \"def\");"));
    }

    @Test
    public void testCreateJsonTable() {
        // success
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.json_tbl1\n" +
                        "(k1 int, j json)\n" +
                        "duplicate key(k1)\n" +
                        "partition by range(k1)\n" +
                        "(partition p1 values less than(\"10\"))\n" +
                        "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.json_tbl2\n" +
                        "(k1 int, j json, j1 json, j2 json)\n" +
                        "duplicate key(k1)\n" +
                        "partition by range(k1)\n" +
                        "(partition p1 values less than(\"10\"))\n" +
                        "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.json_tbl3\n"
                        + "(k1 int, k2 json)\n"
                        + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1');"));
        ExceptionChecker.expectThrowsNoException(() -> createTable("create table test.json_tbl4 \n" +
                "(k1 int(40), j json, j1 json, j2 json)\n" +
                "unique key(k1)\n" +
                "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));
        ExceptionChecker.expectThrowsNoException(() -> createTable("create table test.json_tbl5 \n" +
                "(k1 int(40), j json, j1 json, j2 json)\n" +
                "primary key(k1)\n" +
                "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));

        // failed
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Invalid data type of key column 'k2': 'JSON'",
                () -> createTable("create table test.json_tbl0\n"
                        + "(k1 int, k2 json)\n"
                        + "duplicate key(k1, k2)\n"
                        + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1');"));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "JSON column can not be distribution column",
                () -> createTable("create table test.json_tbl0\n"
                        + "(k1 int, k2 json)\n"
                        + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');"));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Column[j] type[JSON] cannot be a range partition key",
                () -> createTable("create table test.json_tbl0\n" +
                        "(k1 int(40), j json, j1 json, j2 json)\n" +
                        "duplicate key(k1)\n" +
                        "partition by range(k1, j)\n" +
                        "(partition p1 values less than(\"10\"))\n" +
                        "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));
    }

    /**
     * Disable json on unique/primary/aggregate key
     */
    @Test
    public void testAlterJsonTable() {
        // use json as bloomfilter
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.t_json_bloomfilter (\n" +
                        "k1 INT,\n" +
                        "k2 VARCHAR(20),\n" +
                        "k3 JSON\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ")"
        ));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Invalid bloom filter column 'k3': unsupported type JSON",
                () -> alterTableWithNewParser("ALTER TABLE test.t_json_bloomfilter set (\"bloom_filter_columns\"= \"k3\");"));

        // Modify column in unique key
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.t_json_unique_key (\n" +
                        "k1 INT,\n" +
                        "k2 VARCHAR(20)\n" +
                        ") ENGINE=OLAP\n" +
                        "UNIQUE KEY(k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ")"
        ));
        // Add column in unique key
        ExceptionChecker.expectThrowsNoException(
                () -> alterTableWithNewParser("ALTER TABLE test.t_json_unique_key ADD COLUMN k3 JSON"));

        // Add column in primary key
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.t_json_primary_key (\n" +
                        "k1 INT,\n" +
                        "k2 INT\n" +
                        ") ENGINE=OLAP\n" +
                        "PRIMARY KEY(k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");"
        ));
        ExceptionChecker.expectThrowsNoException(
                () -> alterTableWithNewParser("ALTER TABLE test.t_json_primary_key ADD COLUMN k3 JSON"));
    }

    @Test
    public void testCreateTableWithoutDistribution() {
        ConnectContext.get().getSessionVariable().setAllowDefaultPartition(true);

        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.tmp1\n" + "(k1 int, k2 int)\n"));
        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.tmp2\n" + "(k1 int, k2 float)\n"));
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Data type of first column cannot be HLL",
                () -> createTable("create table test.tmp3\n" + "(k1 hll, k2 float)\n"));
    }

    @Test
    public void testCreateSumAgg() throws Exception {
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.useDatabase("test");
        starRocksAssert.withTable("CREATE TABLE aggregate_table_sum\n" +
                "(\n" +
                "    id_int INT,\n" +
                "    sum_decimal decimal(5, 4) SUM DEFAULT '0',\n" +
                "    sum_bigint bigint SUM DEFAULT '0'\n" +
                ")\n" +
                "AGGREGATE KEY(id_int)\n" +
                "DISTRIBUTED BY HASH(id_int) BUCKETS 10\n" +
                "PROPERTIES(\"replication_num\" = \"1\");");
        final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getDb(connectContext.getDatabase())
                .getTable("aggregate_table_sum");
        String columns = table.getColumns().toString();
        System.out.println("columns = " + columns);
        Assert.assertTrue(columns.contains("`sum_decimal` decimal128(38, 4) SUM"));
        Assert.assertTrue(columns.contains("`sum_bigint` bigint(20) SUM "));
    }

    @Test
    public void testDecimal() throws Exception {
        String sql = "CREATE TABLE create_decimal_tbl\n" +
                "(\n" +
                "    c1 decimal(38, 1),\n" +
                "    c2 numeric(38, 1),\n" +
                "    c3 number(38, 1) \n" +
                ")\n" +
                "DUPLICATE KEY(c1)\n" +
                "DISTRIBUTED BY HASH(c1) BUCKETS 1\n" +
                "PROPERTIES(\"replication_num\" = \"1\");";
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.useDatabase("test");
        UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
    }

    @Test(expected = AnalysisException.class)
    public void testCreateSumSmallTypeAgg() throws Exception {
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.useDatabase("test");
        starRocksAssert.withTable("CREATE TABLE aggregate_table_sum\n" +
                "(\n" +
                "    id_int INT,\n" +
                "    sum_int int SUM DEFAULT '0',\n" +
                "    sum_smallint smallint SUM DEFAULT '0',\n" +
                "    sum_tinyint tinyint SUM DEFAULT '0'\n" +
                ")\n" +
                "AGGREGATE KEY(id_int)\n" +
                "DISTRIBUTED BY HASH(id_int) BUCKETS 10\n" +
                "PROPERTIES(\"replication_num\" = \"1\");");
    }

    @Test
    public void testLongColumnName() throws Exception {
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.useDatabase("test");
        String sql = "CREATE TABLE long_column_table (oh_my_gosh_this_is_a_long_column_name_look_at_it_it_has_more_" +
                "than_64_chars VARCHAR(100)) DISTRIBUTED BY HASH(oh_my_gosh_this_is_a_long_column_name_look_at_it_it_" +
                "has_more_than_64_chars) BUCKETS 8 PROPERTIES(\"replication_num\" = \"1\");";
        starRocksAssert.withTable(sql);
        final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getDb(connectContext.getDatabase())
                .getTable("long_column_table");
        Assert.assertEquals(1, table.getColumns().size());
        Assert.assertNotNull(
                table.getColumn("oh_my_gosh_this_is_a_long_column_name_look_at_it_it_has_more_than_64_chars"));
    }

    @Test
    public void testCreateTableDefaultCurrentTimestamp() throws Exception {
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.useDatabase("test");
        String sql = "CREATE TABLE `test_create_default_current_timestamp` (\n" +
                "    k1 int,\n" +
                "    ts datetime NOT NULL DEFAULT CURRENT_TIMESTAMP\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"in_memory\" = \"false\",\n" +
                "    \"storage_format\" = \"DEFAULT\"\n" +
                ");";
        starRocksAssert.withTable(sql);
        final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getDb(connectContext.getDatabase())
                .getTable("test_create_default_current_timestamp");
        Assert.assertEquals(2, table.getColumns().size());
    }
    @Test
    public void testCreateTableDefaultUUID() throws Exception {
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.useDatabase("test");
        String sql = "CREATE TABLE `test_create_default_uuid` (\n" +
                "    k1 int,\n" +
                "    uuid VARCHAR(36) NOT NULL DEFAULT (uuid())\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"in_memory\" = \"false\",\n" +
                "    \"storage_format\" = \"DEFAULT\"\n" +
                ");";
        starRocksAssert.withTable(sql);
        final Table table = starRocksAssert.getCtx().getGlobalStateMgr().getDb(connectContext.getDatabase())
                .getTable("test_create_default_uuid");
        Assert.assertEquals(2, table.getColumns().size());

        String sql2 = "CREATE TABLE `test_create_default_uuid_numeric` (\n" +
                "    k1 int,\n" +
                "    uuid LARGEINT NOT NULL DEFAULT (uuid_numeric())\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 2\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"in_memory\" = \"false\",\n" +
                "    \"storage_format\" = \"DEFAULT\"\n" +
                ");";
        starRocksAssert.withTable(sql2);

        final Table table2 = starRocksAssert.getCtx().getGlobalStateMgr().getDb(connectContext.getDatabase())
                .getTable("test_create_default_uuid_numeric");
        Assert.assertEquals(2, table2.getColumns().size());
    }

    @Test
    public void testCreateTableDefaultUUIDFailed() {
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Varchar type length must be greater than 36 for uuid function",
                () -> createTable("CREATE TABLE test.`test_default_uuid_size_not_enough` (\n" +
                        "    k1 int,\n" +
                        "    uuid VARCHAR(35) NOT NULL DEFAULT (uuid())\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`k1`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 2\n" +
                        "PROPERTIES (\n" +
                        "    \"replication_num\" = \"1\",\n" +
                        "    \"in_memory\" = \"false\",\n" +
                        "    \"storage_format\" = \"DEFAULT\"\n" +
                        ");"));
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Default function uuid() for type INT is not supported",
                () -> createTable("CREATE TABLE test.`test_default_uuid_type_not_match` (\n" +
                        "    k1 int,\n" +
                        "    uuid INT NOT NULL DEFAULT (uuid())\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`k1`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 2\n" +
                        "PROPERTIES (\n" +
                        "    \"replication_num\" = \"1\",\n" +
                        "    \"in_memory\" = \"false\",\n" +
                        "    \"storage_format\" = \"DEFAULT\"\n" +
                        ");"));

        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Default function uuid_numeric() for type VARCHAR(1) is not supported",
                () -> createTable("CREATE TABLE test.`test_default_uuid_type_not_match` (\n" +
                        "    k1 int,\n" +
                        "    uuid VARCHAR NOT NULL DEFAULT (uuid_numeric())\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`k1`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 2\n" +
                        "PROPERTIES (\n" +
                        "    \"replication_num\" = \"1\",\n" +
                        "    \"in_memory\" = \"false\",\n" +
                        "    \"storage_format\" = \"DEFAULT\"\n" +
                        ");"));
    }

    @Test
    public void testCreateBinaryTable() {
        // duplicate table
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.binary_tbl\n" +
                        "(k1 int, j varbinary(10))\n" +
                        "duplicate key(k1)\n" +
                        "partition by range(k1)\n" +
                        "(partition p1 values less than(\"10\"))\n" +
                        "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.binary_tbl1\n" +
                        "(k1 int, j varbinary)\n" +
                        "duplicate key(k1)\n" +
                        "partition by range(k1)\n" +
                        "(partition p1 values less than(\"10\"))\n" +
                        "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.binary_tbl2\n" +
                        "(k1 int, j varbinary(1), j1 varbinary(10), j2 varbinary)\n" +
                        "duplicate key(k1)\n" +
                        "partition by range(k1)\n" +
                        "(partition p1 values less than(\"10\"))\n" +
                        "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));
        // default table
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "create table test.binary_tbl3\n"
                        + "(k1 int, k2 varbinary)\n"
                        + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1');"));

        // unique key table
        ExceptionChecker.expectThrowsNoException(() -> createTable("create table test.binary_tbl4 \n" +
                "(k1 int(40), j varbinary, j1 varbinary(1), j2 varbinary(10))\n" +
                "unique key(k1)\n" +
                "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));

        // primary key table
        ExceptionChecker.expectThrowsNoException(() -> createTable("create table test.binary_tbl5 \n" +
                "(k1 int(40), j varbinary, j1 varbinary, j2 varbinary(10))\n" +
                "primary key(k1)\n" +
                "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));

        // failed
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Invalid data type of key column 'k2': 'VARBINARY'",
                () -> createTable("create table test.binary_tbl0\n"
                        + "(k1 int, k2 varbinary)\n"
                        + "duplicate key(k1, k2)\n"
                        + "distributed by hash(k1) buckets 1\n"
                        + "properties('replication_num' = '1');"));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "VARBINARY(10) column can not be distribution column",
                () -> createTable("create table test.binary_tbl0 \n"
                        + "(k1 int, k2 varbinary(10) )\n"
                        + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');"));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Column[j] type[VARBINARY] cannot be a range partition key",
                () -> createTable("create table test.binary_tbl0 \n" +
                        "(k1 int(40), j varbinary, j1 varbinary(20), j2 varbinary)\n" +
                        "duplicate key(k1)\n" +
                        "partition by range(k1, j)\n" +
                        "(partition p1 values less than(\"10\"))\n" +
                        "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));
    }

    /**
     * Disable varbinary on unique/primary/aggregate key
     */
    @Test
    public void testAlterBinaryTable() {
        // use json as bloomfilter
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.t_binary_bf(\n" +
                        "k1 INT,\n" +
                        "k2 INT,\n" +
                        "k3 VARBINARY\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ")"
        ));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Invalid bloom filter column 'k3': unsupported type VARBINARY",
                () -> alterTableWithNewParser("ALTER TABLE test.t_binary_bf set (\"bloom_filter_columns\"= \"k3\");"));

        // Modify column in unique key
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.t_binary_unique_key (\n" +
                        "k1 INT,\n" +
                        "k2 VARCHAR(20)\n" +
                        ") ENGINE=OLAP\n" +
                        "UNIQUE KEY(k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ")"
        ));
        // Add column in unique key
        ExceptionChecker.expectThrowsNoException(
                () -> alterTableWithNewParser("ALTER TABLE test.t_binary_unique_key ADD COLUMN k3 VARBINARY(12)"));

        // Add column in primary key
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.t_binary_primary_key (\n" +
                        "k1 INT,\n" +
                        "k2 VARCHAR(20)\n" +
                        ") ENGINE=OLAP\n" +
                        "PRIMARY KEY(k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");"
        ));
        ExceptionChecker.expectThrowsNoException(
                () -> alterTableWithNewParser("ALTER TABLE test.t_binary_primary_key ADD COLUMN k3 VARBINARY(21)"));
    }

    @Test
    public void testCreateTableWithBinlogProperties() {
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.binlog_table(\n" +
                        "k1 INT,\n" +
                        "k2 VARCHAR(20)\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n," +
                        "\"binlog_max_size\" = \"100\"\n," +
                        "\"binlog_enable\" = \"true\"\n," +
                        "\"binlog_ttl_second\" = \"100\"\n" +
                        ");"
        ));

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("binlog_table");
        Assert.assertNotNull(table.getCurBinlogConfig());
        Assert.assertTrue(table.isBinlogEnabled());

        long version = table.getBinlogVersion();
        Assert.assertEquals(0, version);
        long binlogMaxSize = table.getCurBinlogConfig().getBinlogMaxSize();
        Assert.assertEquals(100, binlogMaxSize);
        long binlogTtlSecond = table.getCurBinlogConfig().getBinlogTtlSecond();
        Assert.assertEquals(100, binlogTtlSecond);

        ExceptionChecker.expectThrowsNoException(
                () -> alterTableWithNewParser("ALTER TABLE test.binlog_table SET " +
                        "(\"binlog_enable\" = \"false\",\"binlog_max_size\" = \"200\")"));
        Assert.assertFalse(table.isBinlogEnabled());
        Assert.assertEquals(1, table.getBinlogVersion());
        Assert.assertEquals(200, table.getCurBinlogConfig().getBinlogMaxSize());

    }

    @Test
    public void testCreateTableWithoutBinlogProperties() {
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.not_binlog_table(\n" +
                        "k1 INT,\n" +
                        "k2 VARCHAR(20)\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");"
        ));

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("not_binlog_table");

        Assert.assertFalse(table.containsBinlogConfig());
        Assert.assertFalse(table.isBinlogEnabled());

        ExceptionChecker.expectThrowsNoException(
                () -> alterTableWithNewParser("ALTER TABLE test.not_binlog_table SET " +
                        "(\"binlog_enable\" = \"true\",\"binlog_max_size\" = \"200\")"));
        Assert.assertTrue(table.isBinlogEnabled());
        Assert.assertEquals(0, table.getBinlogVersion());
        Assert.assertEquals(200, table.getCurBinlogConfig().getBinlogMaxSize());
    }

    @Test
    public void testTemporaryTable() throws Exception {
        Config.enable_experimental_temporary_table = true;
        createTable(
                "CREATE TABLE test.base_tbl (\n" +
                        "k1 INT,\n" +
                        "k2 VARCHAR(20)\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ")"
        );

        StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(
                "create temporary table test.temp_table select * from test.base_tbl",
                connectContext);
        // String sql = stmt.toSql();
        // Assert.assertEquals("hehe", sql);

        // drop table
        UtFrameUtils.parseStmtWithNewParser("drop temporary table test.base_tbl", connectContext);
    }

    @Test
    public void testCreateTableWithConstraint() {
        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.parent_table1(\n" +
                        "k1 INT,\n" +
                        "k2 VARCHAR(20)\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n," +
                        "\"unique_constraints\" = \"k1,k2\"\n" +
                        ");"
        ));

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        OlapTable table = (OlapTable) db.getTable("parent_table1");

        Assert.assertTrue(table.hasUniqueConstraints());
        List<UniqueConstraint> uniqueConstraint = table.getUniqueConstraints();
        Assert.assertEquals(1, uniqueConstraint.size());
        Assert.assertEquals(2, uniqueConstraint.get(0).getUniqueColumns().size());
        Assert.assertEquals("k1", uniqueConstraint.get(0).getUniqueColumns().get(0));
        Assert.assertEquals("k2", uniqueConstraint.get(0).getUniqueColumns().get(1));

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.parent_table2(\n" +
                        "k1 INT,\n" +
                        "k2 VARCHAR(20)\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n," +
                        "\"unique_constraints\" = \"k1;k2\"\n" +
                        ");"
        ));

        OlapTable table2 = (OlapTable) db.getTable("parent_table2");

        Assert.assertTrue(table2.hasUniqueConstraints());
        List<UniqueConstraint> uniqueConstraint2 = table2.getUniqueConstraints();
        Assert.assertEquals(2, uniqueConstraint2.size());
        Assert.assertEquals(1, uniqueConstraint2.get(0).getUniqueColumns().size());
        Assert.assertEquals("k1", uniqueConstraint2.get(0).getUniqueColumns().get(0));
        Assert.assertEquals(1, uniqueConstraint2.get(1).getUniqueColumns().size());
        Assert.assertEquals("k2", uniqueConstraint2.get(1).getUniqueColumns().get(0));

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.parent_primary_key_table1(\n" +
                        "k1 INT,\n" +
                        "k2 VARCHAR(20)\n" +
                        ") ENGINE=OLAP\n" +
                        "PRIMARY KEY(k1, k2)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1, k2) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");"
        ));

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.parent_unique_key_table1(\n" +
                        "k1 INT,\n" +
                        "k2 VARCHAR(20)\n" +
                        ") ENGINE=OLAP\n" +
                        "UNIQUE KEY(k1, k2)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1, k2) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");"
        ));

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.parent_table3(\n" +
                        "_k1 INT,\n" +
                        "_k2 VARCHAR(20)\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(_k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(_k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n," +
                        "\"unique_constraints\" = \"_k1,_k2\"\n" +
                        ");"
        ));

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.base_table1(\n" +
                        "k1 INT,\n" +
                        "k2 VARCHAR(20),\n" +
                        "k3 INT,\n" +
                        "k4 VARCHAR(20),\n" +
                        "k5 INT,\n" +
                        "k6 VARCHAR(20),\n" +
                        "k7 INT,\n" +
                        "k8 VARCHAR(20),\n" +
                        "k9 INT,\n" +
                        "k10 VARCHAR(20)\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(k1)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"foreign_key_constraints\" = \"(k3,k4) REFERENCES parent_table1(k1, k2);" +
                        " (k5, k6) REFERENCES parent_primary_key_table1(k1, k2 );" +
                        " (k9, k10) references parent_table3(_k1, _k2 );" +
                        " (k7, k8) REFERENCES parent_unique_key_table1(k1, k2 )\"\n" +
                        ");"
        ));

        // column types do not match
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "processing constraint failed when creating table",
                () -> createTable(
                        "CREATE TABLE test.base_table2(\n" +
                                "k1 INT,\n" +
                                "k2 VARCHAR(20),\n" +
                                "k3 INT,\n" +
                                "k4 VARCHAR(20),\n" +
                                "k5 INT,\n" +
                                "k6 VARCHAR(20),\n" +
                                "k7 INT,\n" +
                                "k8 VARCHAR(20),\n" +
                                "k9 INT,\n" +
                                "k10 VARCHAR(20)\n" +
                                ") ENGINE=OLAP\n" +
                                "DUPLICATE KEY(k1)\n" +
                                "COMMENT \"OLAP\"\n" +
                                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"foreign_key_constraints\" = \"(k3,k4) REFERENCES parent_table1(k2, k1)\"\n" +
                                ");"
                ));

        // key size does not match
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "processing constraint failed when creating table",
                () -> createTable(
                        "CREATE TABLE test.base_table2(\n" +
                                "k1 INT,\n" +
                                "k2 VARCHAR(20),\n" +
                                "k3 INT,\n" +
                                "k4 VARCHAR(20),\n" +
                                "k5 INT,\n" +
                                "k6 VARCHAR(20),\n" +
                                "k7 INT,\n" +
                                "k8 VARCHAR(20)\n" +
                                ") ENGINE=OLAP\n" +
                                "DUPLICATE KEY(k1)\n" +
                                "COMMENT \"OLAP\"\n" +
                                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"foreign_key_constraints\" = \"(k3,k4) REFERENCES parent_table2(k1, k2)\"\n" +
                                ");"
                ));

        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "processing constraint failed when creating table",
                () -> createTable(
                        "CREATE TABLE test.base_table2(\n" +
                                "k1 INT,\n" +
                                "k2 VARCHAR(20),\n" +
                                "k3 INT,\n" +
                                "k4 VARCHAR(20),\n" +
                                "k5 INT,\n" +
                                "k6 VARCHAR(20),\n" +
                                "k7 INT,\n" +
                                "k8 VARCHAR(20)\n" +
                                ") ENGINE=OLAP\n" +
                                "DUPLICATE KEY(k1)\n" +
                                "COMMENT \"OLAP\"\n" +
                                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"foreign_key_constraints\" = \"(k3,k4) REFERENCES parent_table2(k1)\"\n" +
                                ");"
                ));
    }

    @Test
    public void testAutomaticPartitionTableLimit() {

        ExceptionChecker.expectThrows(AnalysisException.class, () -> createTable(
                "CREATE TABLE test.site_access_part_partition(\n" +
                        "    event_day DATE NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ") \n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('month', event_day)(\n" +
                        "    START (\"2023-05-01\") END (\"2023-05-03\") EVERY (INTERVAL 1 month)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\n" +
                        "    \"partition_live_number\" = \"3\",\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");"
        ));

        ExceptionChecker.expectThrows(AnalysisException.class, () -> createTable(
                "CREATE TABLE test.site_access_interval_not_1 (\n" +
                        "    event_day DATE NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ") \n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('month', event_day)(\n" +
                        "    START (\"2023-05-01\") END (\"2023-10-01\") EVERY (INTERVAL 2 month)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");"
        ));

        ExceptionChecker.expectThrows(AnalysisException.class, () -> createTable(
                "CREATE TABLE test.site_access_granularity_does_not_match (\n" +
                        "    event_day DATE NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ") \n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('month', event_day)(\n" +
                        "    START (\"2023-05-01\") END (\"2023-10-01\") EVERY (INTERVAL 1 day)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");"
        ));

        ExceptionChecker.expectThrowsNoException(() -> createTable(
                "CREATE TABLE test.site_access_granularity_does_not_match (\n" +
                        "    event_day DATE NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ") \n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('month', event_day)(\n" +
                        "    START (\"2023-05-01\") END (\"2023-10-01\") EVERY (INTERVAL 1 month)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");"
        ));

        ExceptionChecker.expectThrows(AnalysisException.class, () -> createTable(
                "CREATE TABLE site_access_use_time_slice (\n" +
                        "    event_day datetime,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY time_slice(event_day, interval 1 day)(\n" +
                        "\tSTART (\"2023-05-01\") END (\"2023-05-03\") EVERY (INTERVAL 1 day)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\n" +
                        "    \"partition_live_number\" = \"3\",\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");"
        ));

    }

    @Test

    public void testCreatePartitionByExprTable() {
        ExceptionChecker.expectThrowsNoException(
                () -> createTable(
                        "CREATE TABLE test.`bill_detail` (\n" +
                                "  `bill_code` varchar(200) NOT NULL DEFAULT \"\" COMMENT \"\"\n" +
                                ") ENGINE=OLAP \n" +
                                "PRIMARY KEY(`bill_code`)\n" +
                                "PARTITION BY RANGE(cast(substring(bill_code, 3) as bigint))\n" +
                                "(PARTITION p1 VALUES [('0'), ('5000000')),\n" +
                                "PARTITION p2 VALUES [('5000000'), ('10000000')),\n" +
                                "PARTITION p3 VALUES [('10000000'), ('15000000')),\n" +
                                "PARTITION p4 VALUES [('15000000'), ('20000000'))\n" +
                                ")\n" +
                                "DISTRIBUTED BY HASH(`bill_code`) BUCKETS 10 \n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"in_memory\" = \"false\",\n" +
                                "\"storage_format\" = \"DEFAULT\"\n" +
                                ");"
                ));
    }

    @Test
    public void testCannotCreateOlapTable() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Cannot create table without persistent volume in current run mode \"shared_data\"",
                () -> createTable(
                        "CREATE TABLE test.base_table2(\n" +
                                "k1 INT,\n" +
                                "k2 VARCHAR(20),\n" +
                                "k3 INT,\n" +
                                "k4 VARCHAR(20),\n" +
                                "k5 INT,\n" +
                                "k6 VARCHAR(20),\n" +
                                "k7 INT,\n" +
                                "k8 VARCHAR(20)\n" +
                                ") ENGINE=OLAP\n" +
                                "DUPLICATE KEY(k1)\n" +
                                "COMMENT \"OLAP\"\n" +
                                "DISTRIBUTED BY HASH(k1) BUCKETS 3\n" +
                                "PROPERTIES (\n" +
                                "\"storage_volume\" = \"local\"\n" +
                                ");"
                ));
    }

    @Test
    public void testCreateCrossDatabaseColocateTable() throws Exception {
        starRocksAssert.withDatabase("dwd");
        String sql1 = "CREATE TABLE dwd.dwd_site_scan_dtl_test (\n" +
                "ship_id int(11) NOT NULL COMMENT \" \",\n" +
                "sub_ship_id bigint(20) NOT NULL COMMENT \" \"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(ship_id, sub_ship_id) COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(ship_id) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"ship_id_public\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        starRocksAssert.withTable(sql1);

        starRocksAssert.withDatabase("ods");
        String sql2 = "CREATE TABLE ods.reg_bill_info_test (\n" +
                "unit_tm datetime NOT NULL COMMENT \" \",\n" +
                "ship_id int(11) NOT NULL COMMENT \" \",\n" +
                "ins_db_tm datetime NULL COMMENT \" \"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(unit_tm, ship_id)\n" +
                "DISTRIBUTED BY HASH(ship_id) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"ship_id_public\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        starRocksAssert.withTable(sql2);

        List<List<String>> result = GlobalStateMgr.getCurrentState().getColocateTableIndex().getInfos();
        System.out.println(result);
        List<String> groupIds = new ArrayList<>();
        for (List<String> e : result) {
            if (e.get(1).contains("ship_id_public")) {
                groupIds.add(e.get(0));
            }
        }
        Assert.assertEquals(2, groupIds.size());
        System.out.println(groupIds);
        // colocate groups in different db should have same `GroupId.grpId`
        Assert.assertEquals(groupIds.get(0).split("\\.")[1], groupIds.get(1).split("\\.")[1]);
    }
}
