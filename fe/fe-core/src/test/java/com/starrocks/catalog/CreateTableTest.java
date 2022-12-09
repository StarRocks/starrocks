// This file is made available under Elastic License 2.0.
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
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.system.Backend;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateTableTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        Backend be = UtFrameUtils.addMockBackend(10002);
        be.setIsDecommissioned(true);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addMockBackend(10004);
        Config.enable_strict_storage_medium_check = true;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
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

    @Test
    public void testNormal() throws DdlException {

        ExceptionChecker.expectThrowsNoException(
                () -> createTable(
                        "create table test.lp_tbl0\n" + "(k1 bigint, k2 varchar(16))\n" + "duplicate key(k1)\n"
                                + "partition by list(k2)\n" + "(partition p1 values in (\"shanghai\",\"beijing\"))\n"
                                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1');"));

        ExceptionChecker.expectThrowsNoException(
                () -> createTable("create table test.lp_tbl1\n" + "(k1 bigint, k2 varchar(16), dt varchar(10))\n" +
                        "duplicate key(k1)\n"
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

}