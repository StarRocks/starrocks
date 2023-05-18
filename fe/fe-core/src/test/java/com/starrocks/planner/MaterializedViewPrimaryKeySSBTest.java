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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import org.junit.BeforeClass;
import org.junit.Test;

public class MaterializedViewPrimaryKeySSBTest extends MaterializedViewTestBase {
    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.USE_MOCK_DICT_MANAGER = true;
        MaterializedViewTestBase.setUp();

        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        // create SSB tables
        // put lineorder last because it depends on other tables for foreign key constraints
        starRocksAssert.withTable("CREATE TABLE `customer` (\n" +
                "  `c_custkey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `c_name` varchar(26) NOT NULL COMMENT \"\",\n" +
                "  `c_address` varchar(41) NOT NULL COMMENT \"\",\n" +
                "  `c_city` varchar(11) NOT NULL COMMENT \"\",\n" +
                "  `c_nation` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `c_region` varchar(13) NOT NULL COMMENT \"\",\n" +
                "  `c_phone` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `c_mktsegment` varchar(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`c_custkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"groupa2\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `part` (\n" +
                "  `p_partkey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `p_name` varchar(23) NOT NULL COMMENT \"\",\n" +
                "  `p_mfgr` varchar(7) NOT NULL COMMENT \"\",\n" +
                "  `p_category` varchar(8) NOT NULL COMMENT \"\",\n" +
                "  `p_brand` varchar(10) NOT NULL COMMENT \"\",\n" +
                "  `p_color` varchar(12) NOT NULL COMMENT \"\",\n" +
                "  `p_type` varchar(26) NOT NULL COMMENT \"\",\n" +
                "  `p_size` int(11) NOT NULL COMMENT \"\",\n" +
                "  `p_container` varchar(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`p_partkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"groupa5\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");\n");

        starRocksAssert.withTable("\n" +
                "CREATE TABLE `supplier` (\n" +
                "  `s_suppkey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `s_name` varchar(26) NOT NULL COMMENT \"\",\n" +
                "  `s_address` varchar(26) NOT NULL COMMENT \"\",\n" +
                "  `s_city` varchar(11) NOT NULL COMMENT \"\",\n" +
                "  `s_nation` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `s_region` varchar(13) NOT NULL COMMENT \"\",\n" +
                "  `s_phone` varchar(16) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`s_suppkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"groupa4\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");\n");

        starRocksAssert.withTable("CREATE TABLE `dates` (\n" +
                "  `d_datekey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `d_date` varchar(20) NOT NULL COMMENT \"\",\n" +
                "  `d_dayofweek` varchar(10) NOT NULL COMMENT \"\",\n" +
                "  `d_month` varchar(11) NOT NULL COMMENT \"\",\n" +
                "  `d_year` int(11) NOT NULL COMMENT \"\",\n" +
                "  `d_yearmonthnum` int(11) NOT NULL COMMENT \"\",\n" +
                "  `d_yearmonth` varchar(9) NOT NULL COMMENT \"\",\n" +
                "  `d_daynuminweek` int(11) NOT NULL COMMENT \"\",\n" +
                "  `d_daynuminmonth` int(11) NOT NULL COMMENT \"\",\n" +
                "  `d_daynuminyear` int(11) NOT NULL COMMENT \"\",\n" +
                "  `d_monthnuminyear` int(11) NOT NULL COMMENT \"\",\n" +
                "  `d_weeknuminyear` int(11) NOT NULL COMMENT \"\",\n" +
                "  `d_sellingseason` varchar(14) NOT NULL COMMENT \"\",\n" +
                "  `d_lastdayinweekfl` int(11) NOT NULL COMMENT \"\",\n" +
                "  `d_lastdayinmonthfl` int(11) NOT NULL COMMENT \"\",\n" +
                "  `d_holidayfl` int(11) NOT NULL COMMENT \"\",\n" +
                "  `d_weekdayfl` int(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PRIMARY KEY(`d_datekey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`d_datekey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"groupa3\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");

        starRocksAssert.withTable("\n" +
                "CREATE TABLE `lineorder` (\n" +
                "  `lo_orderkey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_linenumber` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_custkey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_partkey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_suppkey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_orderdate` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_orderpriority` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `lo_shippriority` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_quantity` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_extendedprice` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_ordtotalprice` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_discount` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_revenue` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_supplycost` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_tax` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_commitdate` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_shipmode` varchar(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`lo_orderkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`lo_orderdate`)\n" +
                "(PARTITION p1 VALUES [(\"-2147483648\"), (\"19930101\")),\n" +
                "PARTITION p2 VALUES [(\"19930101\"), (\"19940101\")),\n" +
                "PARTITION p3 VALUES [(\"19940101\"), (\"19950101\")),\n" +
                "PARTITION p4 VALUES [(\"19950101\"), (\"19960101\")),\n" +
                "PARTITION p5 VALUES [(\"19960101\"), (\"19970101\")),\n" +
                "PARTITION p6 VALUES [(\"19970101\"), (\"19980101\")),\n" +
                "PARTITION p7 VALUES [(\"19980101\"), (\"19990101\")))\n" +
                "DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"colocate_with\" = \"groupa1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"foreign_key_constraints\" = \"(lo_custkey) REFERENCES customer(c_custkey);(lo_partkey) REFERENCES part(p_partkey);(lo_suppkey) REFERENCES supplier(s_suppkey);(lo_orderdate) REFERENCES dates(d_datekey)\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");\n" +
                "\n");

        // create lineorder_flat_mv
        createMaterializedViews("sql/materialized-view/ssb/", Lists.newArrayList("lineorder_flat_mv"));
    }

    @Test
    public void testQuery1_1() {
        runFileUnitTest("materialized-view/ssb/q1-1");
    }

    @Test
    public void testQuery1_2() {
        runFileUnitTest("materialized-view/ssb/q1-2");
    }

    @Test
    public void testQuery1_3() {
        runFileUnitTest("materialized-view/ssb/q1-3");
    }

    @Test
    public void testQuery2_1() {
        runFileUnitTest("materialized-view/ssb/q2-1");
    }

    @Test
    public void testQuery2_2() {
        runFileUnitTest("materialized-view/ssb/q2-2");
    }

    @Test
    public void testQuery2_3() {
        runFileUnitTest("materialized-view/ssb/q2-3");
    }

    @Test
    public void testQuery3_1() {
        runFileUnitTest("materialized-view/ssb/q3-1");
    }

    @Test
    public void testQuery3_2() {
        runFileUnitTest("materialized-view/ssb/q3-2");
    }

    @Test
    public void testQuery3_3() {
        runFileUnitTest("materialized-view/ssb/q3-3");
    }

    @Test
    public void testQuery3_4() {
        runFileUnitTest("materialized-view/ssb/q3-4");
    }

    @Test
    public void testQuery4_1() {
        runFileUnitTest("materialized-view/ssb/q4-1");
    }

    @Test
    public void testQuery4_2() {
        runFileUnitTest("materialized-view/ssb/q4-2");
    }

    @Test
    public void testQuery4_3() {
        runFileUnitTest("materialized-view/ssb/q4-3");
    }
}
