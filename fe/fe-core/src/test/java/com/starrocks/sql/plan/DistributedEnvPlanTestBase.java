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


package com.starrocks.sql.plan;

import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class DistributedEnvPlanTestBase extends PlanTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("CREATE TABLE `lineorder_new_l` (\n" +
                "  `LO_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `LO_ORDERDATE` date NOT NULL COMMENT \"\",\n" +
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
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`LO_ORDERKEY`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`LO_ORDERKEY`) BUCKETS 192\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `dates_n` (\n" +
                "  `d_datekey` int(11) NULL COMMENT \"\",\n" +
                "  `d_date` varchar(20) NULL COMMENT \"\",\n" +
                "  `d_dayofweek` varchar(10) NULL COMMENT \"\",\n" +
                "  `d_month` varchar(11) NULL COMMENT \"\",\n" +
                "  `d_year` int(11) NULL COMMENT \"\",\n" +
                "  `d_yearmonthnum` int(11) NULL COMMENT \"\",\n" +
                "  `d_yearmonth` varchar(9) NULL COMMENT \"\",\n" +
                "  `d_daynuminweek` int(11) NULL COMMENT \"\",\n" +
                "  `d_daynuminmonth` int(11) NULL COMMENT \"\",\n" +
                "  `d_daynuminyear` int(11) NULL COMMENT \"\",\n" +
                "  `d_monthnuminyear` int(11) NULL COMMENT \"\",\n" +
                "  `d_weeknuminyear` int(11) NULL COMMENT \"\",\n" +
                "  `d_sellingseason` varchar(14) NULL COMMENT \"\",\n" +
                "  `d_lastdayinweekfl` int(11) NULL COMMENT \"\",\n" +
                "  `d_lastdayinmonthfl` int(11) NULL COMMENT \"\",\n" +
                "  `d_holidayfl` int(11) NULL COMMENT \"\",\n" +
                "  `d_weekdayfl` int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`d_datekey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`d_datekey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");

        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        int scale = 100;
        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTpchStatisticStorage(connectContext, scale));
        OlapTable t0 = (OlapTable) globalStateMgr.getDb("test").getTable("region");
        setTableStatistics(t0, 5);

        OlapTable t5 = (OlapTable) globalStateMgr.getDb("test").getTable("nation");
        setTableStatistics(t5, 25);

        OlapTable t1 = (OlapTable) globalStateMgr.getDb("test").getTable("supplier");
        setTableStatistics(t1, 10000 * scale);

        OlapTable t4 = (OlapTable) globalStateMgr.getDb("test").getTable("customer");
        setTableStatistics(t4, 150000 * scale);

        OlapTable t6 = (OlapTable) globalStateMgr.getDb("test").getTable("part");
        setTableStatistics(t6, 200000 * scale);

        OlapTable t2 = (OlapTable) globalStateMgr.getDb("test").getTable("partsupp");
        setTableStatistics(t2, 800000 * scale);

        OlapTable t3 = (OlapTable) globalStateMgr.getDb("test").getTable("orders");
        setTableStatistics(t3, 1500000 * scale);

        OlapTable t7 = (OlapTable) globalStateMgr.getDb("test").getTable("lineitem");
        setTableStatistics(t7, 6000000 * scale);

        OlapTable t8 = (OlapTable) globalStateMgr.getDb("test").getTable("lineitem_partition");
        setTableStatistics(t8, 6000000 * scale);

        OlapTable testAllType = (OlapTable) globalStateMgr.getDb("test").getTable("test_all_type");
        setTableStatistics(testAllType, 6000000);

        OlapTable lineorderNewL =
                (OlapTable) globalStateMgr.getDb("test").getTable("lineorder_new_l");
        setTableStatistics(lineorderNewL, 1200018434);

        OlapTable datesN = (OlapTable) globalStateMgr.getDb("test").getTable("dates_n");
        setTableStatistics(datesN, 2556);
    }

    @AfterClass
    public static void afterClass() {
        try {
            UtFrameUtils.dropMockBackend(10002);
            UtFrameUtils.dropMockBackend(10003);
        } catch (DdlException e) {
            e.printStackTrace();
        }
    }
}
