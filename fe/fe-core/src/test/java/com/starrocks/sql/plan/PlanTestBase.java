// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.plan;

import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class PlanTestBase extends PlanTestNoneDBBase {
    // use a unique dir so that it won't be conflict with other unit test which
    @BeforeClass
    public static void beforeClass() throws Exception {
        // disable checking tablets
        PlanTestNoneDBBase.beforeClass();
        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t1` (\n" +
                "  `v4` bigint NULL COMMENT \"\",\n" +
                "  `v5` bigint NULL COMMENT \"\",\n" +
                "  `v6` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v4`, `v5`, v6)\n" +
                "DISTRIBUTED BY HASH(`v4`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t2` (\n" +
                "  `v7` bigint NULL COMMENT \"\",\n" +
                "  `v8` bigint NULL COMMENT \"\",\n" +
                "  `v9` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v7`, `v8`, v9)\n" +
                "DISTRIBUTED BY HASH(`v7`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t3` (\n" +
                "  `v10` bigint NULL COMMENT \"\",\n" +
                "  `v11` bigint NULL COMMENT \"\",\n" +
                "  `v12` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v10`, `v11`, v12)\n" +
                "DISTRIBUTED BY HASH(`v10`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t4` (\n" +
                "  `v13` bigint NULL COMMENT \"\",\n" +
                "  `v14` bigint NULL COMMENT \"\",\n" +
                "  `v15` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v13`, `v14`, v15)\n" +
                "DISTRIBUTED BY HASH(`v13`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `t5` (\n" +
                "  `v16` bigint NULL COMMENT \"\",\n" +
                "  `v17` bigint NULL COMMENT \"\",\n" +
                "  `v18` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v16`, `v17`, v18)\n" +
                "DISTRIBUTED BY HASH(`v16`, `v17`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `colocate_t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"colocate_with\" = \"colocate_group_1\"" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `colocate_t1` (\n" +
                "  `v4` bigint NULL COMMENT \"\",\n" +
                "  `v5` bigint NULL COMMENT \"\",\n" +
                "  `v6` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v4`, `v5`, v6)\n" +
                "DISTRIBUTED BY HASH(`v4`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"colocate_with\" = \"colocate_group_1\"" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `colocate_t2` (\n" +
                "  `v7` bigint NULL COMMENT \"\",\n" +
                "  `v8` bigint NULL COMMENT \"\",\n" +
                "  `v9` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v7`, `v8`, v9)\n" +
                "DISTRIBUTED BY HASH(`v7`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"colocate_with\" = \"colocate_group_2\"" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `colocate_t3` (\n" +
                "  `v10` bigint NULL COMMENT \"\",\n" +
                "  `v11` bigint NULL COMMENT \"\",\n" +
                "  `v12` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v10`, `v11`, v12)\n" +
                "DISTRIBUTED BY HASH(`v10`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\",\n" +
                "\"colocate_with\" = \"colocate_group_2\"" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `test_all_type` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\",\n" +
                "  `t1e` float NULL COMMENT \"\",\n" +
                "  `t1f` double NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `id_decimal` decimal(10,2) NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `test_all_type_not_null` (\n" +
                "  `t1a` varchar(20) NOT NULL COMMENT \"\",\n" +
                "  `t1b` smallint(6) NOT NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NOT NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `t1e` float NOT NULL COMMENT \"\",\n" +
                "  `t1f` double NOT NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NOT NULL COMMENT \"\",\n" +
                "  `id_date` date NOT NULL COMMENT \"\", \n" +
                "  `id_decimal` decimal(10,2) NOT NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `test_object` (\n" +
                "  `v1` int(11) NULL,\n" +
                "  `v2` int(11) NULL,\n" +
                "  `v3` int(11) NULL,\n" +
                "  `v4` int(11) NULL,\n" +
                "  `b1` bitmap BITMAP_UNION NULL,\n" +
                "  `b2` bitmap BITMAP_UNION NULL,\n" +
                "  `b3` bitmap BITMAP_UNION NULL,\n" +
                "  `b4` bitmap BITMAP_UNION NULL,\n" +
                "  `h1` hll hll_union NULL,\n" +
                "  `h2` hll hll_union NULL,\n" +
                "  `h3` hll hll_union NULL,\n" +
                "  `h4` hll hll_union NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`v1`, `v2`, `v3`, `v4`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `test_agg` (\n" +
                "  `k1` int(11) NULL,\n" +
                "  `k2` int(11) NULL,\n" +
                "  `k3` int(11) NULL,\n" +
                "  `v1` int(11) MAX NULL,\n" +
                "  `v2` int(11) MIN NULL,\n" +
                "  `v3` int(11) SUM NULL,\n" +
                "  `v4` char(11) REPLACE NULL,\n" +
                "  `v5` int(11) SUM NULL,\n" +
                "  `v6` int(11) SUM NULL,\n" +
                "  `b1` bitmap BITMAP_UNION NULL,\n" +
                "  `h1` hll hll_union NULL," +
                "  `p1` PERCENTILE PERCENTILE_UNION NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`k1`, `k2`, `k3`)\n" +
                "DISTRIBUTED BY HASH(`k2`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tall` (\n" +
                "  `ta` varchar(20) NULL COMMENT \"\",\n" +
                "  `tb` smallint(6) NULL COMMENT \"\",\n" +
                "  `tc` int(11) NULL COMMENT \"\",\n" +
                "  `td` bigint(20) NULL COMMENT \"\",\n" +
                "  `te` float NULL COMMENT \"\",\n" +
                "  `tf` double NULL COMMENT \"\",\n" +
                "  `tg` bigint(20) NULL COMMENT \"\",\n" +
                "  `th` datetime NULL COMMENT \"\",\n" +
                "  `ti` date NULL COMMENT \"\",\n" +
                "  `tt` char(200) NULL " +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ta`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ta`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE region ( R_REGIONKEY  INTEGER NOT NULL,\n" +
                "                            R_NAME       CHAR(25) NOT NULL,\n" +
                "                            R_COMMENT    VARCHAR(152),\n" +
                "                            PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`r_regionkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE supplier ( S_SUPPKEY     INTEGER NOT NULL,\n" +
                "                             S_NAME        CHAR(25) NOT NULL,\n" +
                "                             S_ADDRESS     VARCHAR(40) NOT NULL, \n" +
                "                             S_NATIONKEY   INTEGER NOT NULL,\n" +
                "                             S_PHONE       CHAR(15) NOT NULL,\n" +
                "                             S_ACCTBAL     double NOT NULL,\n" +
                "                             S_COMMENT     VARCHAR(101) NOT NULL,\n" +
                "                             PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`s_suppkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE partsupp ( PS_PARTKEY     INTEGER NOT NULL,\n" +
                "                             PS_SUPPKEY     INTEGER NOT NULL,\n" +
                "                             PS_AVAILQTY    INTEGER NOT NULL,\n" +
                "                             PS_SUPPLYCOST  double  NOT NULL,\n" +
                "                             PS_COMMENT     VARCHAR(199) NOT NULL,\n" +
                "                             PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ps_partkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE orders  ( O_ORDERKEY       INTEGER NOT NULL,\n" +
                "                           O_CUSTKEY        INTEGER NOT NULL,\n" +
                "                           O_ORDERSTATUS    CHAR(1) NOT NULL,\n" +
                "                           O_TOTALPRICE     double NOT NULL,\n" +
                "                           O_ORDERDATE      DATE NOT NULL,\n" +
                "                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  \n" +
                "                           O_CLERK          CHAR(15) NOT NULL, \n" +
                "                           O_SHIPPRIORITY   INTEGER NOT NULL,\n" +
                "                           O_COMMENT        VARCHAR(79) NOT NULL,\n" +
                "                           PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`o_orderkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE customer ( C_CUSTKEY     INTEGER NOT NULL,\n" +
                "                             C_NAME        VARCHAR(25) NOT NULL,\n" +
                "                             C_ADDRESS     VARCHAR(40) NOT NULL,\n" +
                "                             C_NATIONKEY   INTEGER NOT NULL,\n" +
                "                             C_PHONE       CHAR(15) NOT NULL,\n" +
                "                             C_ACCTBAL     double   NOT NULL,\n" +
                "                             C_MKTSEGMENT  CHAR(10) NOT NULL,\n" +
                "                             C_COMMENT     VARCHAR(117) NOT NULL,\n" +
                "                             PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c_custkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `nation` (\n" +
                "  `N_NATIONKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `N_NAME` char(25) NOT NULL COMMENT \"\",\n" +
                "  `N_REGIONKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `N_COMMENT` varchar(152) NULL COMMENT \"\",\n" +
                "  `PAD` char(1) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`N_NATIONKEY`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE part  ( P_PARTKEY     INTEGER NOT NULL,\n" +
                "                          P_NAME        VARCHAR(55) NOT NULL,\n" +
                "                          P_MFGR        CHAR(25) NOT NULL,\n" +
                "                          P_BRAND       CHAR(10) NOT NULL,\n" +
                "                          P_TYPE        VARCHAR(25) NOT NULL,\n" +
                "                          P_SIZE        INTEGER NOT NULL,\n" +
                "                          P_CONTAINER   CHAR(10) NOT NULL,\n" +
                "                          P_RETAILPRICE double NOT NULL,\n" +
                "                          P_COMMENT     VARCHAR(23) NOT NULL,\n" +
                "                          PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`p_partkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE lineitem ( L_ORDERKEY    INTEGER NOT NULL,\n" +
                "                             L_PARTKEY     INTEGER NOT NULL,\n" +
                "                             L_SUPPKEY     INTEGER NOT NULL,\n" +
                "                             L_LINENUMBER  INTEGER NOT NULL,\n" +
                "                             L_QUANTITY    double NOT NULL,\n" +
                "                             L_EXTENDEDPRICE  double NOT NULL,\n" +
                "                             L_DISCOUNT    double NOT NULL,\n" +
                "                             L_TAX         double NOT NULL,\n" +
                "                             L_RETURNFLAG  CHAR(1) NOT NULL,\n" +
                "                             L_LINESTATUS  CHAR(1) NOT NULL,\n" +
                "                             L_SHIPDATE    DATE NOT NULL,\n" +
                "                             L_COMMITDATE  DATE NOT NULL,\n" +
                "                             L_RECEIPTDATE DATE NOT NULL,\n" +
                "                             L_SHIPINSTRUCT CHAR(25) NOT NULL,\n" +
                "                             L_SHIPMODE     CHAR(10) NOT NULL,\n" +
                "                             L_COMMENT      VARCHAR(44) NOT NULL,\n" +
                "                             PAD char(1) NOT NULL)\n" +
                "ENGINE=OLAP\n" +
                "DUPLICATE KEY(`l_orderkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 20\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `lineorder_flat_for_mv` (\n" +
                "  `LO_ORDERDATE` date NOT NULL COMMENT \"\",\n" +
                "  `LO_ORDERKEY` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `LO_LINENUMBER` tinyint(4) NOT NULL COMMENT \"\",\n" +
                "  `LO_CUSTKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_PARTKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_ORDERPRIORITY` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `LO_SHIPPRIORITY` tinyint(4) NOT NULL COMMENT \"\",\n" +
                "  `LO_QUANTITY` tinyint(4) NOT NULL COMMENT \"\",\n" +
                "  `LO_EXTENDEDPRICE` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_ORDTOTALPRICE` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_DISCOUNT` tinyint(4) NOT NULL COMMENT \"\",\n" +
                "  `LO_REVENUE` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_SUPPLYCOST` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_TAX` tinyint(4) NOT NULL COMMENT \"\",\n" +
                "  `LO_COMMITDATE` date NOT NULL COMMENT \"\",\n" +
                "  `LO_SHIPMODE` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_NAME` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_ADDRESS` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_CITY` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_NATION` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_REGION` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_PHONE` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `C_MKTSEGMENT` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `S_NAME` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `S_ADDRESS` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `S_CITY` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `S_NATION` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `S_REGION` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `S_PHONE` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_NAME` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_MFGR` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_CATEGORY` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_BRAND` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_COLOR` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_TYPE` varchar(100) NOT NULL COMMENT \"\",\n" +
                "  `P_SIZE` tinyint(4) NOT NULL COMMENT \"\",\n" +
                "  `P_CONTAINER` varchar(100) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`LO_ORDERDATE`, `LO_ORDERKEY`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`LO_ORDERDATE`)\n" +
                "(PARTITION p1 VALUES [('0000-01-01'), ('1993-01-01')),\n" +
                "PARTITION p2 VALUES [('1993-01-01'), ('1994-01-01')),\n" +
                "PARTITION p3 VALUES [('1994-01-01'), ('1995-01-01')),\n" +
                "PARTITION p4 VALUES [('1995-01-01'), ('1996-01-01')),\n" +
                "PARTITION p5 VALUES [('1996-01-01'), ('1997-01-01')),\n" +
                "PARTITION p6 VALUES [('1997-01-01'), ('1998-01-01')),\n" +
                "PARTITION p7 VALUES [('1998-01-01'), ('1999-01-01')))\n" +
                "DISTRIBUTED BY HASH(`LO_ORDERKEY`) BUCKETS 150 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ")");

        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW lo_count_mv as " +
                "select LO_ORDERDATE,count(LO_LINENUMBER) from lineorder_flat_for_mv group by LO_ORDERDATE;");

        starRocksAssert.withTable("CREATE TABLE `lineitem_partition` (\n" +
                "  `L_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `L_PARTKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `L_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `L_LINENUMBER` int(11) NOT NULL COMMENT \"\",\n" +
                "  `L_QUANTITY` double NOT NULL COMMENT \"\",\n" +
                "  `L_EXTENDEDPRICE` double NOT NULL COMMENT \"\",\n" +
                "  `L_DISCOUNT` double NOT NULL COMMENT \"\",\n" +
                "  `L_TAX` double NOT NULL COMMENT \"\",\n" +
                "  `L_RETURNFLAG` char(1) NOT NULL COMMENT \"\",\n" +
                "  `L_LINESTATUS` char(1) NOT NULL COMMENT \"\",\n" +
                "  `L_SHIPDATE` date NOT NULL COMMENT \"\",\n" +
                "  `L_COMMITDATE` date NOT NULL COMMENT \"\",\n" +
                "  `L_RECEIPTDATE` date NOT NULL COMMENT \"\",\n" +
                "  `L_SHIPINSTRUCT` char(25) NOT NULL COMMENT \"\",\n" +
                "  `L_SHIPMODE` char(10) NOT NULL COMMENT \"\",\n" +
                "  `L_COMMENT` varchar(44) NOT NULL COMMENT \"\",\n" +
                "  `PAD` char(1) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`L_ORDERKEY`, `L_PARTKEY`, `L_SUPPKEY`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`L_SHIPDATE`)\n" +
                "(PARTITION p1992 VALUES [('1992-01-01'), ('1993-01-01')),\n" +
                "PARTITION p1993 VALUES [('1993-01-01'), ('1994-01-01')),\n" +
                "PARTITION p1994 VALUES [('1994-01-01'), ('1995-01-01')),\n" +
                "PARTITION p1995 VALUES [('1995-01-01'), ('1996-01-01')),\n" +
                "PARTITION p1996 VALUES [('1996-01-01'), ('1997-01-01')),\n" +
                "PARTITION p1997 VALUES [('1997-01-01'), ('1998-01-01')),\n" +
                "PARTITION p1998 VALUES [('1998-01-01'), ('1999-01-01')))\n" +
                "DISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `lineitem_partition_colocate` (\n" +
                "  `L_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `L_PARTKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `L_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `L_LINENUMBER` int(11) NOT NULL COMMENT \"\",\n" +
                "  `L_QUANTITY` double NOT NULL COMMENT \"\",\n" +
                "  `L_EXTENDEDPRICE` double NOT NULL COMMENT \"\",\n" +
                "  `L_DISCOUNT` double NOT NULL COMMENT \"\",\n" +
                "  `L_TAX` double NOT NULL COMMENT \"\",\n" +
                "  `L_RETURNFLAG` char(1) NOT NULL COMMENT \"\",\n" +
                "  `L_LINESTATUS` char(1) NOT NULL COMMENT \"\",\n" +
                "  `L_SHIPDATE` date NOT NULL COMMENT \"\",\n" +
                "  `L_COMMITDATE` date NOT NULL COMMENT \"\",\n" +
                "  `L_RECEIPTDATE` date NOT NULL COMMENT \"\",\n" +
                "  `L_SHIPINSTRUCT` char(25) NOT NULL COMMENT \"\",\n" +
                "  `L_SHIPMODE` char(10) NOT NULL COMMENT \"\",\n" +
                "  `L_COMMENT` varchar(44) NOT NULL COMMENT \"\",\n" +
                "  `PAD` char(1) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`L_ORDERKEY`, `L_PARTKEY`, `L_SUPPKEY`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`L_SHIPDATE`)\n" +
                "(PARTITION p1992 VALUES [('1992-01-01'), ('1993-01-01')),\n" +
                "PARTITION p1993 VALUES [('1993-01-01'), ('1994-01-01')),\n" +
                "PARTITION p1994 VALUES [('1994-01-01'), ('1995-01-01')),\n" +
                "PARTITION p1995 VALUES [('1995-01-01'), ('1996-01-01')),\n" +
                "PARTITION p1996 VALUES [('1996-01-01'), ('1997-01-01')),\n" +
                "PARTITION p1997 VALUES [('1997-01-01'), ('1998-01-01')),\n" +
                "PARTITION p1998 VALUES [('1998-01-01'), ('1999-01-01')))\n" +
                "DISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"colocate_with\" = \"colocate_group\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `emp` (\n" +
                "  `id` bigint NULL COMMENT \"\",\n" +
                "  `emp_name` varchar(20) NULL COMMENT \"\",\n" +
                "  `hiredate` date null,\n" +
                "  `salary` double null,\n" +
                "  `dept_id` bigint null\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ")");

        starRocksAssert.withTable("CREATE TABLE `dept` (\n" +
                "  `dept_id` bigint NULL COMMENT \"\",\n" +
                "  `dept_name` varchar(20) NULL COMMENT \"\",\n" +
                "  `state` varchar(20) null\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dept_id`)\n" +
                "DISTRIBUTED BY HASH(`dept_id`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ")");

        starRocksAssert.withTable("CREATE TABLE `bonus` (\n" +
                "  `emp_name` varchar(20) NULL COMMENT \"\",\n" +
                "  `bonus_amt` double null\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`emp_name`)\n" +
                "DISTRIBUTED BY HASH(`emp_name`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ")\n");

        starRocksAssert.withView("create view tview as select * from t0;");

        starRocksAssert.withTable("CREATE TABLE `tarray` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` ARRAY<bigint(20)>  NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `ods_order` (\n" +
                "  `order_dt` date NOT NULL DEFAULT '9999-12-31',\n" +
                "  `order_no` varchar(32) NOT NULL DEFAULT '',\n" +
                "  `org_order_no` varchar(64) NOT NULL DEFAULT '',\n" +
                "  `bank_transaction_id` varchar(32) NOT NULL DEFAULT '',\n" +
                "  `up_trade_no` varchar(32) NOT NULL DEFAULT '',\n" +
                "  `mchnt_no` varchar(15) NOT NULL DEFAULT '',\n" +
                "  `pay_st` tinyint(4) NOT NULL DEFAULT '1'\n" +
                ") ENGINE=mysql\n" +
                "PROPERTIES\n" +
                "    (\n" +
                "    \"host\" = \"127.0.0.1\",\n" +
                "    \"port\" = \"3306\",\n" +
                "    \"user\" = \"mysql_user\",\n" +
                "    \"password\" = \"mysql_password\",\n" +
                "    \"database\" = \"test\",\n" +
                "    \"table\" = \"ods_order\"\n" +
                "    )");

        starRocksAssert.withDatabase("db1");
        starRocksAssert.withTable("create table db1.tbl1(k1 int, k2 varchar(32), v bigint sum) "
                + "AGGREGATE KEY(k1,k2) distributed by hash(k1) buckets 1 properties('replication_num' = '1');");
        starRocksAssert.withTable("create table db1.tbl2(k3 int, k4 varchar(32)) "
                + "DUPLICATE KEY(k3) distributed by hash(k3) buckets 1 properties('replication_num' = '1');");
        starRocksAssert
                .withTable("create table db1.tbl3(c1 int, c2 int, c3 int) DUPLICATE KEY(c1, c2) PARTITION BY RANGE(c1) "
                        + "(PARTITION p1 VALUES [('-2147483648'), ('10')), PARTITION p2 VALUES [('10'), ('20')))"
                        + " DISTRIBUTED BY HASH(`c2`) BUCKETS 2 PROPERTIES('replication_num'='1');");
        starRocksAssert.withTable("create table db1.tbl4(c1 int, c2 int, c3 int) DUPLICATE KEY(c1, c2) "
                + " DISTRIBUTED BY HASH(`c2`) BUCKETS 1 PROPERTIES('replication_num'='1');");
        starRocksAssert
                .withTable("create table db1.tbl5(c1 int, c2 int, c3 int) DUPLICATE KEY(c1, c2) PARTITION BY RANGE(c1) "
                        + "(PARTITION p1 VALUES [('-2147483648'), ('10')), PARTITION p2 VALUES [('10'), ('20')))"
                        +
                        " DISTRIBUTED BY HASH(`c2`) BUCKETS 2 PROPERTIES('replication_num'='1', 'colocate_with'='tmp_t1_1');");
        starRocksAssert.withTable("create table db1.tbl6(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) "
                + "AGGREGATE KEY(k1, k2,k3,k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");

        starRocksAssert.withTable("CREATE TABLE test.bitmap_table (\n" +
                "  `id` int(11) NULL COMMENT \"\",\n" +
                "  `id2` bitmap bitmap_union NULL\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`id`)\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                " \"replication_num\" = \"1\"\n" +
                ");")
                .withTable("CREATE TABLE test.bitmap_table_2 (\n" +
                        "  `id` int(11) NULL COMMENT \"\",\n" +
                        "  `id2` bitmap bitmap_union NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`id`)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                        "PROPERTIES (\n" +
                        " \"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE test.hll_table (\n" +
                        "  `id` int(11) NULL COMMENT \"\",\n" +
                        "  `id2` hll hll_union NULL\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`id`)\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                        "PROPERTIES (\n" +
                        " \"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE test.`baseall` (\n" +
                        "  `k1` tinyint(4) NULL COMMENT \"\",\n" +
                        "  `k2` smallint(6) NULL COMMENT \"\",\n" +
                        "  `k3` int(11) NULL COMMENT \"\",\n" +
                        "  `k4` bigint(20) NULL COMMENT \"\",\n" +
                        "  `k5` decimal(9, 3) NULL COMMENT \"\",\n" +
                        "  `k6` char(5) NULL COMMENT \"\",\n" +
                        "  `k10` date NULL COMMENT \"\",\n" +
                        "  `k11` datetime NULL COMMENT \"\",\n" +
                        "  `k7` varchar(20) NULL COMMENT \"\",\n" +
                        "  `k8` double MAX NULL COMMENT \"\",\n" +
                        "  `k9` float SUM NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE test.`bigtable` (\n" +
                        "  `k1` tinyint(4) NULL COMMENT \"\",\n" +
                        "  `k2` smallint(6) NULL COMMENT \"\",\n" +
                        "  `k3` int(11) NULL COMMENT \"\",\n" +
                        "  `k4` bigint(20) NULL COMMENT \"\",\n" +
                        "  `k5` decimal(9, 3) NULL COMMENT \"\",\n" +
                        "  `k6` char(5) NULL COMMENT \"\",\n" +
                        "  `k10` date NULL COMMENT \"\",\n" +
                        "  `k11` datetime NULL COMMENT \"\",\n" +
                        "  `k7` varchar(20) NULL COMMENT \"\",\n" +
                        "  `k8` double MAX NULL COMMENT \"\",\n" +
                        "  `k9` float SUM NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE test.join1 (\n" +
                        "  `dt` int(11) COMMENT \"\",\n" +
                        "  `id` int(11) COMMENT \"\",\n" +
                        "  `value` varchar(8) COMMENT \"\"\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`dt`, `id`)\n" +
                        "PARTITION BY RANGE(`dt`)\n" +
                        "(PARTITION p1 VALUES LESS THAN (\"10\"))\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "PROPERTIES (\n" +
                        "  \"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE test.join2 (\n" +
                        "  `dt` int(11) COMMENT \"\",\n" +
                        "  `id` int(11) COMMENT \"\",\n" +
                        "  `value` varchar(8) COMMENT \"\"\n" +
                        ") ENGINE=OLAP\n" +
                        "DUPLICATE KEY(`dt`, `id`)\n" +
                        "PARTITION BY RANGE(`dt`)\n" +
                        "(PARTITION p1 VALUES LESS THAN (\"10\"))\n" +
                        "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                        "PROPERTIES (\n" +
                        "  \"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE test.`pushdown_test` (\n" +
                        "  `k1` tinyint(4) NULL COMMENT \"\",\n" +
                        "  `k2` smallint(6) NULL COMMENT \"\",\n" +
                        "  `k3` int(11) NULL COMMENT \"\",\n" +
                        "  `k4` bigint(20) NULL COMMENT \"\",\n" +
                        "  `k5` decimal(9, 3) NULL COMMENT \"\",\n" +
                        "  `k6` char(5) NULL COMMENT \"\",\n" +
                        "  `k10` date NULL COMMENT \"\",\n" +
                        "  `k11` datetime NULL COMMENT \"\",\n" +
                        "  `k7` varchar(20) NULL COMMENT \"\",\n" +
                        "  `k8` double MAX NULL COMMENT \"\",\n" +
                        "  `k9` float SUM NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE(`k1`)\n" +
                        "(PARTITION p1 VALUES [(\"-128\"), (\"-64\")),\n" +
                        "PARTITION p2 VALUES [(\"-64\"), (\"0\")),\n" +
                        "PARTITION p3 VALUES [(\"0\"), (\"64\")))\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\"\n" +
                        ");")
                .withTable("create table test.jointest\n" +
                        "(k1 int, k2 int) distributed by hash(k1) buckets 1\n" +
                        "properties(\"replication_num\" = \"1\");")
                .withTable("CREATE TABLE test.`dynamic_partition` (\n" +
                        "  `k1` date NULL COMMENT \"\",\n" +
                        "  `k2` smallint(6) NULL COMMENT \"\",\n" +
                        "  `k3` int(11) NULL COMMENT \"\",\n" +
                        "  `k4` bigint(20) NULL COMMENT \"\",\n" +
                        "  `k5` decimal(9, 3) NULL COMMENT \"\",\n" +
                        "  `k6` char(5) NULL COMMENT \"\",\n" +
                        "  `k10` date NULL COMMENT \"\",\n" +
                        "  `k11` datetime NULL COMMENT \"\",\n" +
                        "  `k7` varchar(20) NULL COMMENT \"\",\n" +
                        "  `k8` double MAX NULL COMMENT \"\",\n" +
                        "  `k9` float SUM NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP\n" +
                        "AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`, `k6`, `k10`, `k11`, `k7`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE (k1)\n" +
                        "(\n" +
                        "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                        "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                        "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 5\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"dynamic_partition.enable\" = \"true\",\n" +
                        "\"dynamic_partition.start\" = \"-3\",\n" +
                        "\"dynamic_partition.end\" = \"3\",\n" +
                        "\"dynamic_partition.time_unit\" = \"day\",\n" +
                        "\"dynamic_partition.prefix\" = \"p\",\n" +
                        "\"dynamic_partition.buckets\" = \"1\"\n" +
                        ");")
                .withTable("create external table test.mysql_table\n" +
                        "(k1 int, k2 int)\n" +
                        "ENGINE=MYSQL\n" +
                        "PROPERTIES (\n" +
                        "\"host\" = \"127.0.0.1\",\n" +
                        "\"port\" = \"3306\",\n" +
                        "\"user\" = \"root\",\n" +
                        "\"password\" = \"123\",\n" +
                        "\"database\" = \"db1\",\n" +
                        "\"table\" = \"tbl1\"\n" +
                        ");");

        FeConstants.runningUnitTest = true;
        starRocksAssert.withResource("create external resource \"jdbc_test\"\n" +
                "PROPERTIES (\n" +
                "\"type\"=\"jdbc\",\n" +
                "\"user\"=\"test_user\",\n" +
                "\"password\"=\"test_passwd\",\n" +
                "\"driver_url\"=\"test_driver_url\",\n" +
                "\"driver_class\"=\"test.driver.class\",\n" +
<<<<<<< HEAD
                "\"jdbc_uri\"=\"test_uri\"\n" +
=======
                "\"jdbc_uri\"=\"jdbc:mysql://127.0.0.1:3306\"\n" +
>>>>>>> 2.5.18
                ");")
                .withTable("create external table test.jdbc_test\n" +
                        "(a int, b varchar(20), c float)\n" +
                        "ENGINE=jdbc\n" +
                        "PROPERTIES (\n" +
                        "\"resource\"=\"jdbc_test\",\n" +
                        "\"table\"=\"test_table\"\n" +
                        ");");
        FeConstants.runningUnitTest = false;

        starRocksAssert.withTable("CREATE TABLE `t0_not_null` (\n" +
                "  `v1` bigint NOT NULL COMMENT \"\",\n" +
                "  `v2` bigint NOT NULL COMMENT \"\",\n" +
                "  `v3` ARRAY<bigint(20)> NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `test_bool` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\",\n" +
                "  `t1e` float NULL COMMENT \"\",\n" +
                "  `t1f` double NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `id_decimal` decimal(10,2) NULL COMMENT \"\", \n" +
                "  `id_bool` boolean null \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `test_all_type_distributed_by_datetime` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\",\n" +
                "  `t1e` float NULL COMMENT \"\",\n" +
                "  `t1f` double NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `id_decimal` decimal(10,2) NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`id_datetime`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `test_all_type_distributed_by_date` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\",\n" +
                "  `t1e` float NULL COMMENT \"\",\n" +
                "  `t1f` double NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `id_decimal` decimal(10,2) NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`id_date`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `test_all_type_partition_by_datetime` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NOT NULL COMMENT \"\",\n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\",\n" +
                "  `t1e` float NULL COMMENT \"\",\n" +
                "  `t1f` double NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `id_decimal` decimal(10,2) NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`,`id_datetime`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`id_datetime`)\n" +
                "(PARTITION p19910101AM VALUES [('1991-01-01 00:00:00'), ('1991-01-01 12:00:00')),\n" +
                "PARTITION p1990101PM VALUES [('1991-01-01 12:00:00'), ('1992-01-02 00:00:00')),\n" +
                "PARTITION pother VALUES [('1992-01-02 00:00:00'), ('1994-01-01 00:00:00')))" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `test_all_type_partition_by_date` (\n" +
                "  `t1a` varchar(20) NULL COMMENT \"\",\n" +
                "  `id_date` date NULL COMMENT \"\", \n" +
                "  `t1b` smallint(6) NULL COMMENT \"\",\n" +
                "  `t1c` int(11) NULL COMMENT \"\",\n" +
                "  `t1d` bigint(20) NULL COMMENT \"\",\n" +
                "  `t1e` float NULL COMMENT \"\",\n" +
                "  `t1f` double NULL COMMENT \"\",\n" +
                "  `t1g` bigint(20) NULL COMMENT \"\",\n" +
                "  `id_datetime` datetime NULL COMMENT \"\",\n" +
                "  `id_decimal` decimal(10,2) NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`t1a`,`id_date`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`id_date`)\n" +
                "(PARTITION p1992 VALUES [('1991-01-01'), ('1992-01-01')),\n" +
                "PARTITION p1993 VALUES [('1992-01-01'), ('1993-01-01')),\n" +
                "PARTITION p1998 VALUES [('1993-01-01'), ('1994-01-01')))" +
                "DISTRIBUTED BY HASH(`t1a`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `test_partition_prune_optimize_by_date` (\n" +
                "  `session_id` varchar(20) NULL COMMENT \"\",\n" +
                "  `store_id` varchar(20) NULL COMMENT \"店铺id\",\n" +
                "  `dt` date NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`session_id`, `store_id`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p_20211227 VALUES [('2021-12-27'), ('2021-12-28')),\n" +
                "PARTITION p_20211228 VALUES [('2021-12-28'), ('2021-12-29')),\n" +
                "PARTITION p_20211229 VALUES [('2021-12-29'), ('2021-12-30')),\n" +
                "PARTITION p_20211230 VALUES [('2021-12-30'), ('2021-12-31'))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`session_id`) BUCKETS 5 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("create table test.colocate1\n" +
                "(k1 int, k2 int, k3 int) distributed by hash(k1, k2) buckets 1\n" +
                "properties(\"replication_num\" = \"1\"," +
                "\"colocate_with\" = \"group1\");")
                .withTable("create table test.colocate2\n" +
                        "(k1 int, k2 int, k3 int) distributed by hash(k1, k2) buckets 1\n" +
                        "properties(\"replication_num\" = \"1\"," +
                        "\"colocate_with\" = \"group1\");")
                .withTable("create table test.nocolocate3\n" +
                        "(k1 int, k2 int, k3 int) distributed by hash(k1, k2) buckets 10\n" +
                        "properties(\"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `tprimary` (\n" +
                "  `pk` bigint NOT NULL COMMENT \"\",\n" +
                "  `v1` string NOT NULL COMMENT \"\",\n" +
                "  `v2` int NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`pk`)\n" +
                "DISTRIBUTED BY HASH(`pk`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `tjson` (\n" +
                "  `v_int`  bigint NULL COMMENT \"\",\n" +
                "  `v_json` json NULL COMMENT \"\" \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v_int`)\n" +
                "DISTRIBUTED BY HASH(`v_int`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"DEFAULT\"\n" +
                ");");

        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTpchStatisticStorage(1));
        GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().clear();

        connectContext.getSessionVariable().setMaxTransformReorderJoins(8);
        connectContext.getSessionVariable().setEnableReplicationJoin(false);
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(false);
        connectContext.getSessionVariable().setCboPushDownAggregateMode(-1);
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(false);
    }

    @AfterClass
    public static void afterClass() {
        connectContext.getSessionVariable().setEnableLowCardinalityOptimize(true);
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(true);
    }
}
