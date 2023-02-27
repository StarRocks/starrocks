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

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.optimizer.statistics.CachedStatisticStorage;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.StatisticStorage;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.statistics.CachedStatisticStorageTest.DEFAULT_CREATE_TABLE_TEMPLATE;

public class CTASAnalyzerTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        // create statistic
        CreateDbStmt dbStmt = new CreateDbStmt(false, StatsConstants.STATISTICS_DB_NAME);
        try {
            GlobalStateMgr.getCurrentState().getMetadata().createDb(dbStmt.getFullDbName());
        } catch (DdlException e) {
            return;
        }
        starRocksAssert.useDatabase(StatsConstants.STATISTICS_DB_NAME);
        starRocksAssert.withTable(DEFAULT_CREATE_TABLE_TEMPLATE);

        starRocksAssert.withDatabase("ctas").useDatabase("ctas")
                .withTable("create table test(c1 varchar(10),c2 varchar(10)) DISTRIBUTED BY HASH(c1) " +
                        "BUCKETS 8 PROPERTIES (\"replication_num\" = \"1\" );")
                .withTable("create table test_notnull(c1 varchar(10) not null, c2 varchar(10) not null) " +
                        "DISTRIBUTED BY HASH(c1) BUCKETS 8 PROPERTIES (\"replication_num\" = \"1\" );")
                .withTable("create table test3(c1 varchar(10),c2 varchar(10)) DISTRIBUTED BY HASH(c1) " +
                        "BUCKETS 8 PROPERTIES (\"replication_num\" = \"1\" );")
                .withTable("CREATE TABLE `lineorder` (\n" +
                        "  `lo_orderdate` date NOT NULL COMMENT \"\",\n" +
                        "  `lo_orderkey` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `lo_linenumber` tinyint(4) NOT NULL COMMENT \"\",\n" +
                        "  `lo_custkey` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `lo_partkey` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `lo_suppkey` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `lo_orderpriority` varchar(16) NULL COMMENT \"\",\n" +
                        "  `lo_shippriority` tinyint(4) NULL COMMENT \"\",\n" +
                        "  `lo_quantity` tinyint(4) NOT NULL COMMENT \"\",\n" +
                        "  `lo_extendedprice` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `lo_ordtotalprice` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `lo_discount` tinyint(4) NOT NULL COMMENT \"\",\n" +
                        "  `lo_revenue` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `lo_supplycost` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `lo_tax` tinyint(4) NOT NULL COMMENT \"\",\n" +
                        "  `lo_commitdate` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `lo_shipmode` varchar(11) NOT NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`lo_orderdate`, `lo_orderkey`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE(`lo_orderdate`)\n" +
                        "(PARTITION p1 VALUES [('1900-01-01'), ('1993-01-01')),\n" +
                        "PARTITION p2 VALUES [('1993-01-01'), ('1994-01-01')),\n" +
                        "PARTITION p3 VALUES [('1994-01-01'), ('1995-01-01')),\n" +
                        "PARTITION p4 VALUES [('1995-01-01'), ('1996-01-01')),\n" +
                        "PARTITION p5 VALUES [('1996-01-01'), ('1997-01-01')),\n" +
                        "PARTITION p6 VALUES [('1997-01-01'), ('1998-01-01')),\n" +
                        "PARTITION p7 VALUES [('1998-01-01'), ('1999-01-01')))\n" +
                        "DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 96 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"colocate_with\" = \"groupc1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\"\n" +
                        ");")
                .withTable("CREATE TABLE `customer` (\n" +
                        "  `c_custkey` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `c_name` varchar(26) NOT NULL COMMENT \"\",\n" +
                        "  `c_address` varchar(41) NOT NULL COMMENT \"\",\n" +
                        "  `c_city` varchar(11) NOT NULL COMMENT \"\",\n" +
                        "  `c_nation` varchar(16) NOT NULL COMMENT \"\",\n" +
                        "  `c_region` varchar(13) NOT NULL COMMENT \"\",\n" +
                        "  `c_phone` varchar(16) NOT NULL COMMENT \"\",\n" +
                        "  `c_mktsegment` varchar(11) NOT NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`c_custkey`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"colocate_with\" = \"groupa2\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\"\n" +
                        ");")
                .withTable("CREATE TABLE `supplier` (\n" +
                        "  `s_suppkey` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `s_name` varchar(26) NOT NULL COMMENT \"\",\n" +
                        "  `s_address` varchar(26) NOT NULL COMMENT \"\",\n" +
                        "  `s_city` varchar(11) NOT NULL COMMENT \"\",\n" +
                        "  `s_nation` varchar(16) NOT NULL COMMENT \"\",\n" +
                        "  `s_region` varchar(13) NOT NULL COMMENT \"\",\n" +
                        "  `s_phone` varchar(16) NOT NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`s_suppkey`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"colocate_with\" = \"groupa4\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\"\n" +
                        ");")
                .withTable("CREATE TABLE `part` (\n" +
                        "  `p_partkey` int(11) NOT NULL COMMENT \"\",\n" +
                        "  `p_name` varchar(23) NOT NULL COMMENT \"\",\n" +
                        "  `p_mfgr` varchar(7) NOT NULL COMMENT \"\",\n" +
                        "  `p_category` varchar(8) NOT NULL COMMENT \"\",\n" +
                        "  `p_brand` varchar(10) NOT NULL COMMENT \"\",\n" +
                        "  `p_color` varchar(12) NOT NULL COMMENT \"\",\n" +
                        "  `p_type` varchar(26) NOT NULL COMMENT \"\",\n" +
                        "  `p_size` tinyint(4) NOT NULL COMMENT \"\",\n" +
                        "  `p_container` varchar(11) NOT NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`p_partkey`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"colocate_with\" = \"groupa5\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\"\n" +
                        ");")
                .withTable("CREATE TABLE `duplicate_table_with_null` (\n" +
                        "    `k1`  date,\n" +
                        "    `k2`  datetime,\n" +
                        "    `k3`  char(20),\n" +
                        "    `k4`  varchar(20),\n" +
                        "    `k5`  boolean,\n" +
                        "    `k6`  tinyint,\n" +
                        "    `k7`  smallint,\n" +
                        "    `k8`  int,\n" +
                        "    `k9`  bigint,\n" +
                        "    `k10` largeint,\n" +
                        "    `k11` float,\n" +
                        "    `k12` double,\n" +
                        "    `k13` decimal(27,9)\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3\n" +
                        "PROPERTIES (\n" +
                        "    \"replication_num\" = \"1\",\n" +
                        "    \"storage_format\" = \"v2\"\n" +
                        ");")
                .withView("CREATE VIEW v1(vc1,vc2) as select k1+1,k2 from duplicate_table_with_null");
    }

    @Test
    public void testSimpleCase() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String ctassql1 = "create table test2 as select * from test;";

        UtFrameUtils.parseStmtWithNewParser(ctassql1, ctx);

        String ctassql2 = "create table test6 as select c1+c2 as cr from test3;";

        UtFrameUtils.parseStmtWithNewParser(ctassql2, ctx);

        String ctassql3 = "create table t1 as select k1,k2,k3,k4,k5,k6,k7,k8 from duplicate_table_with_null;";

        UtFrameUtils.parseStmtWithNewParser(ctassql3, ctx);
    }

    @Test
    public void testSelectColumn() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();

        String sql = "create table t2 as select k1 as a,k2 as b from duplicate_table_with_null t2;";

        StatisticStorage storage = new CachedStatisticStorage();
        Table table = ctx.getGlobalStateMgr().getDb("ctas")
                .getTable("duplicate_table_with_null");
        ColumnStatistic k1cs = new ColumnStatistic(1.5928416E9, 1.5982848E9,
                1.5256461111280627E-4, 4.0, 64.0);
        ColumnStatistic k2cs = new ColumnStatistic(1.5928416E9, 1.598350335E9,
                1.5256461111280627E-4, 8.0, 66109.0);
        storage.addColumnStatistic(table, "k1", k1cs);
        storage.addColumnStatistic(table, "k2", k2cs);

        ctx.getGlobalStateMgr().setStatisticStorage(storage);

        UtFrameUtils.parseStmtWithNewParser(sql, ctx);
    }

    @Test
    public void testCTASWithDatePartition() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String partitionSQL = "create table test2 " +
                "PARTITION BY RANGE(`k1`)(" +
                "START (\"2021-03-01\") END (\"2022-04-01\") EVERY (INTERVAL 1 MONTH)\n" +
                ") AS select k1 from duplicate_table_with_null;";

        UtFrameUtils.parseStmtWithNewParser(partitionSQL, ctx);
    }

    @Test
    public void testTPCH() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String ctastpch = "CREATE TABLE lineorder_flat \n" +
                "    AS SELECT\n" +
                "    l.LO_ORDERKEY AS LO_ORDERKEY_1,\n" +
                "    l.LO_LINENUMBER AS LO_LINENUMBER,\n" +
                "    l.LO_CUSTKEY AS LO_CUSTKEY,\n" +
                "    l.LO_PARTKEY AS LO_PARTKEY,\n" +
                "    l.LO_SUPPKEY AS LO_SUPPKEY,\n" +
                "    l.LO_ORDERDATE AS LO_ORDERDATE,\n" +
                "    l.LO_ORDERPRIORITY AS LO_ORDERPRIORITY,\n" +
                "    l.LO_SHIPPRIORITY AS LO_SHIPPRIORITY,\n" +
                "    l.LO_QUANTITY AS LO_QUANTITY,\n" +
                "    l.LO_EXTENDEDPRICE AS LO_EXTENDEDPRICE,\n" +
                "    l.LO_ORDTOTALPRICE AS LO_ORDTOTALPRICE,\n" +
                "    l.LO_DISCOUNT AS LO_DISCOUNT,\n" +
                "    l.LO_REVENUE AS LO_REVENUE,\n" +
                "    l.LO_SUPPLYCOST AS LO_SUPPLYCOST,\n" +
                "    l.LO_TAX AS LO_TAX,\n" +
                "    l.LO_COMMITDATE AS LO_COMMITDATE,\n" +
                "    l.LO_SHIPMODE AS LO_SHIPMODE,\n" +
                "    c.C_NAME AS C_NAME,\n" +
                "    c.C_ADDRESS AS C_ADDRESS,\n" +
                "    c.C_CITY AS C_CITY,\n" +
                "    c.C_NATION AS C_NATION,\n" +
                "    c.C_REGION AS C_REGION,\n" +
                "    c.C_PHONE AS C_PHONE,\n" +
                "    c.C_MKTSEGMENT AS C_MKTSEGMENT,\n" +
                "    s.S_NAME AS S_NAME,\n" +
                "    s.S_ADDRESS AS S_ADDRESS,\n" +
                "    s.S_CITY AS S_CITY,\n" +
                "    s.S_NATION AS S_NATION,\n" +
                "    s.S_REGION AS S_REGION,\n" +
                "    s.S_PHONE AS S_PHONE,\n" +
                "    p.P_NAME AS P_NAME,\n" +
                "    p.P_MFGR AS P_MFGR,\n" +
                "    p.P_CATEGORY AS P_CATEGORY,\n" +
                "    p.P_BRAND AS P_BRAND,\n" +
                "    p.P_COLOR AS P_COLOR,\n" +
                "    p.P_TYPE AS P_TYPE,\n" +
                "    p.P_SIZE AS P_SIZE,\n" +
                "    p.P_CONTAINER AS P_CONTAINER\n" +
                "FROM lineorder AS l\n" +
                "INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY\n" +
                "INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY\n" +
                "INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY;";
        UtFrameUtils.parseStmtWithNewParser(ctastpch, ctx);
    }

    @Test
    public void testVariousDataTypes() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        starRocksAssert.withTable("" +
                "CREATE TABLE `t2` (\n" +
                "  `c_2_0` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `c_2_1` datetime NOT NULL COMMENT \"\",\n" +
                "  `c_2_2` int(11) NOT NULL COMMENT \"\",\n" +
                "  `c_2_3` varchar(1) NULL COMMENT \"\",\n" +
                "  `c_2_4` decimal64(13, 2) NOT NULL COMMENT \"\",\n" +
                "  `c_2_5` decimal128(28, 18) NULL COMMENT \"\",\n" +
                "  `c_2_6` varchar(1) NOT NULL COMMENT \"\",\n" +
                "  `c_2_7` date NOT NULL COMMENT \"\",\n" +
                "  `c_2_8` boolean NOT NULL COMMENT \"\",\n" +
                "  `c_2_9` smallint(6) NULL COMMENT \"\",\n" +
                "  `c_2_10` char(21) NOT NULL COMMENT \"\",\n" +
                "  `c_2_11` decimal128(30, 9) NULL COMMENT \"\",\n" +
                "  `c_2_12` boolean NOT NULL COMMENT \"\",\n" +
                "  `c_2_13` percentile PERCENTILE_UNION NOT NULL COMMENT \"\",\n" +
                "  `c_2_14` varchar(31) MAX NULL COMMENT \"\",\n" +
                "  `c_2_15` decimal128(25, 6) SUM NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`c_2_0`, `c_2_1`, `c_2_2`, `c_2_3`, `c_2_4`, `c_2_5`, `c_2_6`, `c_2_7`, " +
                "`c_2_8`, `c_2_9`, `c_2_10`, `c_2_11`, `c_2_12`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_2_9`, `c_2_12`, `c_2_0`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        String ctasSql = "CREATE TABLE `decimal_ctas1` as " +
                "SELECT  c_2_0, c_2_1, c_2_2, c_2_3, c_2_4, c_2_5, c_2_6, c_2_7, " +
                "c_2_8, c_2_9, c_2_10, c_2_11, c_2_12, c_2_14, c_2_15 " +
                "FROM     t2 WHERE     (NOT (false)) " +
                "GROUP BY c_2_0, c_2_1, c_2_2, c_2_3, c_2_4, c_2_5, c_2_6, c_2_7, " +
                "c_2_8, c_2_9, c_2_10, c_2_11, c_2_12, c_2_14, c_2_15";
        CreateTableAsSelectStmt createTableStmt =
                (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(ctasSql, ctx);
        createTableStmt.createTable(ctx);

        String ctasSql2 = "CREATE TABLE v2 as select NULL from t2";
        CreateTableAsSelectStmt createTableStmt2 =
                (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(ctasSql2, ctx);

        String ctasSql3 = "CREATE TABLE json_kv as select * from test, lateral json_each(parse_json(c1));";
        CreateTableAsSelectStmt createTableStmt3 =
                (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(ctasSql3, ctx);
    }

    @Test
    public void testPKTable() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "CREATE table test123\n" +
                "PRIMARY KEY(`a`,`b`) \n" +
                "as select 1 as a,2 as b,\"\" as d,\"null\" as c;";
        CreateTableAsSelectStmt createTableStmt =
                (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        KeysDesc keysDesc
                = createTableStmt.getCreateTableStmt().getKeysDesc();
        Assert.assertEquals(KeysType.PRIMARY_KEYS, keysDesc.getKeysType());
    }

    @Test
    public void testCTASView() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "CREATE TABLE tbl as select vc1,vc2 from v1";
        CreateTableAsSelectStmt createTableStmt =
                (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        HashDistributionDesc hashDistributionDesc
                = (HashDistributionDesc) createTableStmt.getCreateTableStmt().getDistributionDesc();
        Assert.assertEquals("vc1", hashDistributionDesc.getDistributionColumnNames().get(0));
    }

    @Test
    public void testCTASReplicaNum() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        Table table = ctx.getGlobalStateMgr().getDb("ctas")
                .getTable("duplicate_table_with_null");
        OlapTable olapTable = (OlapTable) table;
        olapTable.setReplicationNum((short) 3);
        String sql = "CREATE TABLE test_replica as select * from duplicate_table_with_null";
        CreateTableAsSelectStmt createTableStmt =
                (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);

        Map<String, String> properties = createTableStmt.getCreateTableStmt().getProperties();
        Assert.assertTrue(properties.containsKey("replication_num"));
        Assert.assertEquals(properties.get("replication_num"), "3");

        String sql2 = "CREATE TABLE test_replica2 as select 1 as id";
        CreateTableAsSelectStmt createTableStmt2 =
                (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql2, ctx);

        Map<String, String> properties2 = createTableStmt2.getCreateTableStmt().getProperties();
        Assert.assertTrue(properties2.containsKey("replication_num"));
        Assert.assertEquals(properties2.get("replication_num"), "1");
    }

    @Test
    public void testCTASPrimaryKey() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "create table ctas_pk (c1, c2, c3) primary key (c1, c2) DISTRIBUTED BY HASH (c1) BUCKETS 1 \n" +
                "as select t1.c1, t1.c2, t2.c1 as c3 from test_notnull t1 left join test t2 on t1.c2 = t2.c2;";
        CreateTableAsSelectStmt ctasStmt =
                (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        List<ColumnDef> columnDefs = ctasStmt.getCreateTableStmt().getColumnDefs();
        Assert.assertFalse(columnDefs.get(0).isAllowNull());
        Assert.assertFalse(columnDefs.get(1).isAllowNull());
        Assert.assertTrue(columnDefs.get(2).isAllowNull());

        sql = "create table ctas_pk (Cc1, Cc2, c3) primary key (cC1, `cC2`) DISTRIBUTED BY HASH (Cc1) BUCKETS 1 \n" +
                "as select t1.c1, t1.c2, t2.c1 as c3 from test_notnull t1 left join test t2 on t1.c2 = t2.c2;";
        ctasStmt =
                (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        columnDefs = ctasStmt.getCreateTableStmt().getColumnDefs();
        Assert.assertFalse(columnDefs.get(0).isAllowNull());
        Assert.assertFalse(columnDefs.get(1).isAllowNull());
        Assert.assertTrue(columnDefs.get(2).isAllowNull());

        sql = "create table ctas_pk (Cc1, Cc2, c3) DISTRIBUTED BY HASH (Cc1) BUCKETS 1 \n" +
                "as select t1.c1, t1.c2, t2.c1 as c3 from test_notnull t1 left join test t2 on t1.c2 = t2.c2;";
        ctasStmt =
                (CreateTableAsSelectStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        columnDefs = ctasStmt.getCreateTableStmt().getColumnDefs();
        Assert.assertTrue(columnDefs.get(0).isAllowNull());
        Assert.assertTrue(columnDefs.get(1).isAllowNull());
        Assert.assertTrue(columnDefs.get(2).isAllowNull());
    }
}
