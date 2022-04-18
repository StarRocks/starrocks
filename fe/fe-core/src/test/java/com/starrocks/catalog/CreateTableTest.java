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

import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

public class CreateTableTest {
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    private static void alterTable(String sql) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().alterTable(alterTableStmt);
    }

    @Test
    public void testNormal() throws DdlException {
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

        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:test");
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
    public void testAbormal() throws DdlException {
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
                .expectThrowsWithMsg(DdlException.class, "Failed to find enough host in all backends. need: 3",
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
                        "Failed to find enough host with storage medium is SSD in all backends. need: 1",
                        () -> createTable(
                                "create table test.tb7(key1 int, key2 varchar(10)) distributed by hash(key1) \n"
                                        + "buckets 1 properties('replication_num' = '1', 'storage_medium' = 'ssd');"));
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
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "JSON type could only be used in DUPLICATE KEY table",
                () -> createTable("create table test.json_tbl0\n" +
                        "(k1 int(40), j json, j1 json, j2 json)\n" +
                        "unique key(k1)\n" +
                        "distributed by hash(k1) buckets 1\n" + "properties('replication_num' = '1');"));
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "JSON type could only be used in DUPLICATE KEY table",
                () -> createTable("create table test.json_tbl0\n" +
                        "(k1 int(40), j json, j1 json, j2 json)\n" +
                        "primary key(k1)\n" +
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
                () -> alterTable("ALTER TABLE test.t_json_bloomfilter set (\"bloom_filter_columns\"= \"k3\");"));

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
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "JSON must be used in duplicate key",
                () -> alterTable("ALTER TABLE test.t_json_unique_key MODIFY COLUMN k2 JSON"));
        // Add column in unique key
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "JSON must be used in duplicate key",
                () -> alterTable("ALTER TABLE test.t_json_unique_key ADD COLUMN k3 JSON"));

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
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "JSON must be used in duplicate key",
                () -> alterTable("ALTER TABLE test.t_json_primary_key ADD COLUMN k3 JSON"));
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "JSON must be used in duplicate key",
                () -> alterTable("ALTER TABLE test.t_json_primary_key MODIFY COLUMN k3 JSON"));
    }
}
