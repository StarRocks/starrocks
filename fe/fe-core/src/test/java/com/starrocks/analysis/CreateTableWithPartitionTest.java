// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CreateTableWithPartitionTest {
    private static StarRocksAssert starRocksAssert;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("db1").useDatabase("db1");
    }

    @Test
    public void testCreateTablePartitionLessThan() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE testCreateTablePartitionLessThan (\n" +
                "    k1 DATE,\n" +
                "    k2 INT,\n" +
                "    k3 SMALLINT,\n" +
                "    v1 VARCHAR(2048),\n" +
                "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1, k2, k3)\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "    PARTITION p20140101 VALUES LESS THAN (\"2014-01-01\"),\n" +
                "    PARTITION p20140102 VALUES LESS THAN (\"2014-01-02\"),\n" +
                "    PARTITION p20140103 VALUES LESS THAN (\"2014-01-03\")\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p20140101 VALUES LESS THEN ('2014-01-01')"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p20140102 VALUES LESS THEN ('2014-01-02')"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p20140103 VALUES LESS THEN ('2014-01-03')"));
    }

    @Test
    public void testCreateTablePartitionNormal() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE `testCreateTablePartitionNormal` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int(11) NULL COMMENT \"\",\n" +
                "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL DEFAULT \"2014-02-04 15:36:00\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(PARTITION p20140101 VALUES [('0000-01-01'), ('2014-01-01')),\n" +
                "PARTITION p20140102 VALUES [('2014-01-01'), ('2014-01-02')),\n" +
                "PARTITION p20140103 VALUES [('2014-01-02'), ('2014-01-03')))\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20140101 VALUES [('0000-01-01'), ('2014-01-01'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20140102 VALUES [('2014-01-01'), ('2014-01-02'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20140103 VALUES [('2014-01-02'), ('2014-01-03'))"));
    }

    @Test
    public void testCreateTableBatchPartitionDay() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE testCreateTableBatchPartitionDay (\n" +
                "    k1 DATE,\n" +
                "    k2 INT,\n" +
                "    k3 SMALLINT,\n" +
                "    v1 VARCHAR(2048),\n" +
                "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1, k2, k3)\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "    START (\"2014-01-01\") END (\"2014-01-04\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20140101 VALUES [('2014-01-01'), ('2014-01-02'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20140102 VALUES [('2014-01-02'), ('2014-01-03'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20140103 VALUES [('2014-01-03'), ('2014-01-04'))"));
        Assert.assertFalse(
                partitionDesc.toString().contains("PARTITION p20140104 VALUES [('2014-01-04'), ('2014-01-05'))"));

    }

    @Test
    public void testCreateTableBatchPartitionWithDynamicPrefix() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE testCreateTableBatchPartitionDay (\n" +
                "    k1 DATE,\n" +
                "    k2 INT,\n" +
                "    k3 SMALLINT,\n" +
                "    v1 VARCHAR(2048),\n" +
                "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1, k2, k3)\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "    START (\"2014-01-01\") END (\"2014-01-04\") EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"dynamic_partition.prefix\" = \"p_\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p_20140101 VALUES [('2014-01-01'), ('2014-01-02'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p_20140102 VALUES [('2014-01-02'), ('2014-01-03'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p_20140103 VALUES [('2014-01-03'), ('2014-01-04'))"));
        Assert.assertFalse(
                partitionDesc.toString().contains("PARTITION p_20140104 VALUES [('2014-01-04'), ('2014-01-05'))"));

    }

    @Test
    public void testCreateTableBatchPartition5Day() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE testCreateTableBatchPartition5Day (\n" +
                "    k1 DATE,\n" +
                "    k2 INT,\n" +
                "    k3 SMALLINT,\n" +
                "    v1 VARCHAR(2048),\n" +
                "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1, k2, k3)\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "    START (\"2014-01-01\") END (\"2014-01-18\") EVERY (INTERVAL 5 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20140101 VALUES [('2014-01-01'), ('2014-01-06'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20140106 VALUES [('2014-01-06'), ('2014-01-11'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20140111 VALUES [('2014-01-11'), ('2014-01-16'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20140116 VALUES [('2014-01-16'), ('2014-01-21'))"));

    }

    @Test
    public void testCreateTableBatchPartitionWeek() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE testCreateTableBatchPartitionWeek (\n" +
                "    k1 DATE,\n" +
                "    k2 INT,\n" +
                "    k3 SMALLINT,\n" +
                "    v1 VARCHAR(2048),\n" +
                "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1, k2, k3)\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "    START (\"2020-03-25\") END (\"2020-04-10\") EVERY (INTERVAL 1 WEEK)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2020_13 VALUES [('2020-03-25'), ('2020-03-30'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2020_14 VALUES [('2020-03-30'), ('2020-04-06'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2020_15 VALUES [('2020-04-06'), ('2020-04-10'))"));

    }

    @Test
    public void testCreateTableBatchPartitionWeekThroughYear() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE testCreateTableBatchPartitionWeekThroughYear (\n" +
                "    k1 DATE,\n" +
                "    k2 INT,\n" +
                "    k3 SMALLINT,\n" +
                "    v1 VARCHAR(2048),\n" +
                "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1, k2, k3)\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "    START (\"2020-12-25\") END (\"2021-01-15\") EVERY (INTERVAL 1 WEEK)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2020_52 VALUES [('2020-12-25'), ('2020-12-28'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2020_53 VALUES [('2020-12-28'), ('2021-01-04'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2021_02 VALUES [('2021-01-04'), ('2021-01-11'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2021_03 VALUES [('2021-01-11'), ('2021-01-15'))"));

    }

    @Test
    public void testCreateTableBatchPartitionMonth() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE testCreateTableBatchPartitionMonth (\n" +
                "    k1 DATE,\n" +
                "    k2 INT,\n" +
                "    k3 SMALLINT,\n" +
                "    v1 VARCHAR(2048),\n" +
                "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1, k2, k3)\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "    START (\"2020-01-01\") END (\"2020-05-01\") EVERY (INTERVAL 1 MONTH)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p202001 VALUES [('2020-01-01'), ('2020-02-01'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p202002 VALUES [('2020-02-01'), ('2020-03-01'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p202003 VALUES [('2020-03-01'), ('2020-04-01'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p202004 VALUES [('2020-04-01'), ('2020-05-01'))"));
        Assert.assertFalse(partitionDesc.toString().contains("PARTITION p202005 VALUES [('2020-05-01'), ('2020-06-01'))"));

    }

    @Test
    public void testCreateTableBatchPartitionMonthNatural() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE testCreateTableBatchPartitionMonthNatural (\n" +
                "    k1 DATE,\n" +
                "    k2 INT,\n" +
                "    k3 SMALLINT,\n" +
                "    v1 VARCHAR(2048),\n" +
                "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1, k2, k3)\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "    START (\"2020-12-04\") END (\"2021-03-15\") EVERY (INTERVAL 1 MONTH)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p202012 VALUES [('2020-12-04'), ('2021-01-01'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p202101 VALUES [('2021-01-01'), ('2021-02-01'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p202102 VALUES [('2021-02-01'), ('2021-03-01'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p202103 VALUES [('2021-03-01'), ('2021-03-15'))"));
        Assert.assertFalse(partitionDesc.toString().contains("PARTITION p202104 VALUES"));

    }

    @Test
    public void testCreateTableBatchPartitionYear() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE testCreateTableBatchPartitionYear (\n" +
                "    k1 DATE,\n" +
                "    k2 INT,\n" +
                "    k3 SMALLINT,\n" +
                "    v1 VARCHAR(2048),\n" +
                "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k1, k2, k3)\n" +
                "PARTITION BY RANGE (k1) (\n" +
                "    START (\"2019-01-01\") END (\"2021-01-01\") EVERY (INTERVAL 1 YEAR)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2019 VALUES [('2019-01-01'), ('2020-01-01'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2020 VALUES [('2020-01-01'), ('2021-01-01'))"));
        Assert.assertFalse(partitionDesc.toString().contains("PARTITION p2021 VALUES [('2021-01-01'), ('2022-01-01'))"));

    }

    @Test
    public void testCreateTableBatchPartitionNumber() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE testCreateTableBatchPartitionNumber (\n" +
                "    k2 INT,\n" +
                "    k3 SMALLINT,\n" +
                "    v1 VARCHAR(2048),\n" +
                "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                "    START (\"1\") END (\"4\") EVERY (1)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p1 VALUES [('1'), ('2'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2 VALUES [('2'), ('3'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p3 VALUES [('3'), ('4'))"));
        Assert.assertFalse(partitionDesc.toString().contains("PARTITION p4 VALUES [('4'), ('5'))"));

    }

    @Test
    public void testCreateTableBatchPartitionNumberWithSmallInt() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE `table_range2` (\n" +
                "  `k2` smallint(11) NULL COMMENT \"\",\n" +
                "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL DEFAULT \"2014-02-04 15:36:00\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k2`)\n" +
                "(\n" +
                "START (\"1\") END (\"4\")  EVERY (1)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p1 VALUES [('1'), ('2'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2 VALUES [('2'), ('3'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p3 VALUES [('3'), ('4'))"));
        Assert.assertFalse(partitionDesc.toString().contains("PARTITION p4 VALUES [('4'), ('5'))"));

    }

    @Test(expected = AnalysisException.class)
    public void testCreateTableBatchPartitionNumberWithDatekey() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE testCreateTableBatchPartitionNumberWithDatekey (\n" +
                "    k2 INT,\n" +
                "    k3 SMALLINT,\n" +
                "    v1 VARCHAR(2048),\n" +
                "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                "    START (20200429) END (20200503) EVERY (INTERVAL 1 DAY)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
    }

    @Test(expected = AnalysisException.class)
    public void testCreateTableBatchPartitionStringUseNumber() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE testCreateTableBatchPartitionStringUseNumber (\n" +
                "    k2 INT,\n" +
                "    k3 SMALLINT,\n" +
                "    v1 VARCHAR(2048),\n" +
                "    v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                ")\n" +
                "ENGINE=olap\n" +
                "DUPLICATE KEY(k2, k3)\n" +
                "PARTITION BY RANGE (k2) (\n" +
                "    START (\"2020-04-01\") END (\"2020-04-02\") EVERY (1)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
    }

    @Test(expected = AnalysisException.class)
    public void testCreateTableBatchPartitionNotSingleRangeColumn() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE `testCreateTableBatchPartitionNotSingleRangeColumn` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int(11) NULL COMMENT \"\",\n" +
                "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL DEFAULT \"2014-02-04 15:36:00\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`, `k2`)\n" +
                "(START (\"2020-04-29\") END (\"2020-05-03\") EVERY (interval 1 day))\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
    }

    @Test
    public void testCreateTableBatchPartitionMul() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE `testCreateTableBatchPartitionMul` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int(11) NULL COMMENT \"\",\n" +
                "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL DEFAULT \"2014-02-04 15:36:00\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(\n" +
                "    START (\"2013-01-01\") END (\"2016-01-01\") EVERY (interval 1 YEAR),\n" +
                "    START (\"2020-04-29\") END (\"2020-05-03\") EVERY (interval 1 day)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2013 VALUES [('2013-01-01'), ('2014-01-01'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2014 VALUES [('2014-01-01'), ('2015-01-01'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2015 VALUES [('2015-01-01'), ('2016-01-01'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20200429 VALUES [('2020-04-29'), ('2020-04-30'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20200430 VALUES [('2020-04-30'), ('2020-05-01'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20200501 VALUES [('2020-05-01'), ('2020-05-02'))"));
        Assert.assertTrue(
                partitionDesc.toString().contains("PARTITION p20200502 VALUES [('2020-05-02'), ('2020-05-03'))"));
    }

    @Test
    public void testCreateTableBatchPartitionWithDateTimeType() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE `testCreateTableBatchPartitionWithDateTimeType` (\n" +
                "  `k1` datetime NULL COMMENT \"\",\n" +
                "  `k2` int(11) NULL COMMENT \"\",\n" +
                "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL DEFAULT \"2014-02-04 15:36:00\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(\n" +
                "START ('2014-01-01 00:00:00') END ('2014-01-04 00:00:00')  EVERY (interval 1 day)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(partitionDesc.toString()
                .contains("PARTITION p20140101 VALUES [('2014-01-01 00:00:00'), ('2014-01-02 00:00:00'))"));
        Assert.assertTrue(partitionDesc.toString()
                .contains("PARTITION p20140102 VALUES [('2014-01-02 00:00:00'), ('2014-01-03 00:00:00'))"));
        Assert.assertTrue(partitionDesc.toString()
                .contains("PARTITION p20140103 VALUES [('2014-01-03 00:00:00'), ('2014-01-04 00:00:00'))"));
        Assert.assertFalse(partitionDesc.toString()
                .contains("PARTITION p20140104 VALUES [('2014-01-04 00:00:00'), ('2014-01-05 00:00:00'))"));
    }

    @Test
    public void testCreateTableBatchPartitionWithDateKeyType() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE `testCreateTableBatchPartitionWithDateKeyType` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int(11) NULL COMMENT \"\",\n" +
                "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL DEFAULT \"2014-02-04 15:36:00\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(\n" +
                "START ('20140101') END ('20140104')  EVERY (interval 1 day)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p20140101 VALUES [('20140101'), ('20140102'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p20140102 VALUES [('20140102'), ('20140103'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p20140103 VALUES [('20140103'), ('20140104'))"));
        Assert.assertFalse(partitionDesc.toString().contains("PARTITION p20140104 VALUES [('20140104'), ('20140105'))"));
    }

    @Test(expected = AnalysisException.class)
    public void testCreateTableBatchPartitionIntersection() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE `testCreateTableBatchPartitionIntersection` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int(11) NULL COMMENT \"\",\n" +
                "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL DEFAULT \"2014-02-04 15:36:00\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(\n" +
                "START ('2014-01-01') END ('2014-01-06')  EVERY (interval 1 day),\n" +
                "    START ('2014-01-05') END ('2014-01-08')  EVERY (interval 1 day)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
    }

    @Test(expected = AnalysisException.class)
    public void testCreateTableBatchPartitionDateStartLargeThanEnd() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE `testCreateTableBatchPartitionDateStartLargeThanEnd` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int(11) NULL COMMENT \"\",\n" +
                "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL DEFAULT \"2014-02-04 15:36:00\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(\n" +
                "START ('2014-01-06') END ('2014-01-01')  EVERY (interval 1 day)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
    }

    @Test(expected = AnalysisException.class)
    public void testCreateTableBatchPartitionIntStartLargeThanEnd() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE `testCreateTableBatchPartitionIntStartLargeThanEnd` (\n" +
                "  `k2` int(11) NULL COMMENT \"\",\n" +
                "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL DEFAULT \"2014-02-04 15:36:00\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k2`)\n" +
                "(\n" +
                "START (6) END (2)  EVERY (1)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
    }

    @Test(expected = AnalysisException.class)
    public void testCreateTableBatchPartitionZeroDay() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE `testCreateTableBatchPartitionZeroDay` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int(11) NULL COMMENT \"\",\n" +
                "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL DEFAULT \"2014-02-04 15:36:00\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(\n" +
                "START ('20140101') END ('20140104')  EVERY (interval 0 day)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
    }

    @Test(expected = AnalysisException.class)
    public void testCreateTableBatchPartitionNumber0() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createTableSql = "CREATE TABLE `table_range2` (\n" +
                "  `k2` smallint(11) NULL COMMENT \"\",\n" +
                "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                "  `v2` datetime NULL DEFAULT \"2014-02-04 15:36:00\" COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k2`, `k3`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`k2`)\n" +
                "(\n" +
                "START (\"1\") END (\"4\")  EVERY (0)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createTableSql, ctx);
        PartitionDesc partitionDesc = createTableStmt.getPartitionDesc();
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p1 VALUES [('1'), ('2'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p2 VALUES [('2'), ('3'))"));
        Assert.assertTrue(partitionDesc.toString().contains("PARTITION p3 VALUES [('3'), ('4'))"));
        Assert.assertFalse(partitionDesc.toString().contains("PARTITION p4 VALUES [('4'), ('5'))"));

    }
}

