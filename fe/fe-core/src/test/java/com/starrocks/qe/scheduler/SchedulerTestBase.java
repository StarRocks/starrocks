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

package com.starrocks.qe.scheduler;

import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.MockTpchStatisticStorage;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.atomic.AtomicLong;

public class SchedulerTestBase extends SchedulerTestNoneDBBase {
    private static final String DB_NAME = "test";
    private static final AtomicLong COLOCATE_GROUP_INDEX = new AtomicLong(0L);

    @BeforeClass
    public static void beforeClass() throws Exception {
        SchedulerTestNoneDBBase.beforeClass();

        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);

        final String tpchGroup = "tpch_group_" + COLOCATE_GROUP_INDEX.getAndIncrement();

        // NOTE: Please do not change the order of the following create table statements.
        // Modifying the order will result in changes to the partition ids and tablet ids of a table,
        // which are relied upon by the scheduler plan.
        starRocksAssert.withTable("CREATE TABLE lineitem (\n" +
                "    L_ORDERKEY INTEGER NOT NULL,\n" +
                "    L_PARTKEY INTEGER NOT NULL,\n" +
                "    L_SUPPKEY INTEGER NOT NULL,\n" +
                "    L_LINENUMBER INTEGER NOT NULL,\n" +
                "    L_QUANTITY double NOT NULL,\n" +
                "    L_EXTENDEDPRICE double NOT NULL,\n" +
                "    L_DISCOUNT double NOT NULL,\n" +
                "    L_TAX double NOT NULL,\n" +
                "    L_RETURNFLAG CHAR(1) NOT NULL,\n" +
                "    L_LINESTATUS CHAR(1) NOT NULL,\n" +
                "    L_SHIPDATE DATE NOT NULL,\n" +
                "    L_COMMITDATE DATE NOT NULL,\n" +
                "    L_RECEIPTDATE DATE NOT NULL,\n" +
                "    L_SHIPINSTRUCT CHAR(25) NOT NULL,\n" +
                "    L_SHIPMODE CHAR(10) NOT NULL,\n" +
                "    L_COMMENT VARCHAR(44) NOT NULL,\n" +
                "    PAD char(1) NOT NULL\n" +
                ") ENGINE = OLAP \n" +
                "DUPLICATE KEY(`l_orderkey`) \n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 20 \n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");\n");

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
                "PARTITION BY RANGE(`L_SHIPDATE`)\n" +
                "(PARTITION p1992 VALUES [('1992-01-01'), ('1993-01-01')),\n" +
                "PARTITION p1993 VALUES [('1993-01-01'), ('1994-01-01')),\n" +
                "PARTITION p1994 VALUES [('1994-01-01'), ('1995-01-01')),\n" +
                "PARTITION p1995 VALUES [('1995-01-01'), ('1996-01-01')),\n" +
                "PARTITION p1996 VALUES [('1996-01-01'), ('1997-01-01')),\n" +
                "PARTITION p1997 VALUES [('1997-01-01'), ('1998-01-01')),\n" +
                "PARTITION p1998 VALUES [('1998-01-01'), ('1999-01-01')))\n" +
                "DISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 18\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"" + tpchGroup + "\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE customer (\n" +
                "    c_custkey       INT NOT NULL,\n" +
                "    c_name          VARCHAR(25) NOT NULL,\n" +
                "    c_address       VARCHAR(40) NOT NULL,\n" +
                "    c_nationkey     INT NOT NULL,\n" +
                "    c_phone         VARCHAR(15) NOT NULL,\n" +
                "    c_acctbal       DECIMAL(15, 2) NOT NULL,\n" +
                "    c_mktsegment    VARCHAR(10) NOT NULL,\n" +
                "    c_comment       VARCHAR(117) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c_custkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 24\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `nation` (\n" +
                "    n_nationkey   INT(11) NOT NULL,\n" +
                "    n_name        VARCHAR(25) NOT NULL,\n" +
                "    n_regionkey   INT(11) NOT NULL,\n" +
                "    n_comment     VARCHAR(152) NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`N_NATIONKEY`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE orders (\n" +
                "    o_orderkey       INT NOT NULL,\n" +
                "    o_orderdate      DATE NOT NULL,\n" +
                "    o_custkey        INT NOT NULL,\n" +
                "    o_orderstatus    VARCHAR(1) NOT NULL,\n" +
                "    o_totalprice     DECIMAL(15, 2) NOT NULL,\n" +
                "    o_orderpriority  VARCHAR(15) NOT NULL,\n" +
                "    o_clerk          VARCHAR(15) NOT NULL,\n" +
                "    o_shippriority   INT NOT NULL,\n" +
                "    o_comment        VARCHAR(79) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`o_orderkey`, `o_orderdate`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 18\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"" + tpchGroup + "\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE part (\n" +
                "    p_partkey       INT NOT NULL,\n" +
                "    p_name          VARCHAR(55) NOT NULL,\n" +
                "    p_mfgr          VARCHAR(25) NOT NULL,\n" +
                "    p_brand         VARCHAR(10) NOT NULL,\n" +
                "    p_type          VARCHAR(25) NOT NULL,\n" +
                "    p_size          INT NOT NULL,\n" +
                "    p_container     VARCHAR(10) NOT NULL,\n" +
                "    p_retailprice   DECIMAL(15, 2) NOT NULL,\n" +
                "    p_comment       VARCHAR(23) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`p_partkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 18\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"" + tpchGroup + "\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE partsupp (\n" +
                "    ps_partkey      INT NOT NULL,\n" +
                "    ps_suppkey      INT NOT NULL,\n" +
                "    ps_availqty     INT NOT NULL,\n" +
                "    ps_supplycost   DECIMAL(15, 2) NOT NULL,\n" +
                "    ps_comment      VARCHAR(199) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ps_partkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 18\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"" + tpchGroup + "\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE region (\n" +
                "    r_regionkey     INT NOT NULL,\n" +
                "    r_name          VARCHAR(25) NOT NULL,\n" +
                "    r_comment       VARCHAR(152)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`r_regionkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE supplier (\n" +
                "    s_suppkey       INT NOT NULL,\n" +
                "    s_name          VARCHAR(25) NOT NULL,\n" +
                "    s_address       VARCHAR(40) NOT NULL,\n" +
                "    s_nationkey     INT NOT NULL,\n" +
                "    s_phone         VARCHAR(15) NOT NULL,\n" +
                "    s_acctbal       DECIMAL(15, 2) NOT NULL,\n" +
                "    s_comment       VARCHAR(101) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`s_suppkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");

        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTpchStatisticStorage(connectContext, 100));
        GlobalStateMgr.getCurrentAnalyzeMgr().getBasicStatsMetaMap().clear();

        FeConstants.runningUnitTest = true;
    }

    @AfterClass
    public static void afterClass() {
        FeConstants.runningUnitTest = false;

        SchedulerTestNoneDBBase.afterClass();
    }
}
