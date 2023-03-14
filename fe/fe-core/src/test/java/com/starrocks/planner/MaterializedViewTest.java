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

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

public class MaterializedViewTest extends MaterializedViewTestBase {
    private static final List<String> outerJoinTypes = ImmutableList.of("left", "right");

    @BeforeClass
    public static void setUp() throws Exception {
        MaterializedViewTestBase.setUp();

        starRocksAssert.useDatabase(MATERIALIZED_DB_NAME);

        String deptsTable = "" +
                "CREATE TABLE depts(    \n" +
                "   deptno INT NOT NULL,\n" +
                "   name VARCHAR(20)    \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`deptno`)\n" +
                "DISTRIBUTED BY HASH(`deptno`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"unique_constraints\" = \"deptno\"\n," +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        String locationsTable = "" +
                "CREATE TABLE locations(\n" +
                "    locationid INT NOT NULL,\n" +
                "    state CHAR(2), \n" +
                "   name VARCHAR(20)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`locationid`)\n" +
                "DISTRIBUTED BY HASH(`locationid`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        String ependentsTable = "" +
                "CREATE TABLE dependents(\n" +
                "   empid INT NOT NULL,\n" +
                "   name VARCHAR(20)   \n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`empid`)\n" +
                "DISTRIBUTED BY HASH(`empid`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";
        String empsTable = "" +
                "CREATE TABLE emps\n" +
                "(\n" +
                "    empid      INT         NOT NULL,\n" +
                "    deptno     INT         NOT NULL,\n" +
                "    locationid INT         NOT NULL,\n" +
                "    commission INT         NOT NULL,\n" +
                "    name       VARCHAR(20) NOT NULL,\n" +
                "    salary     DECIMAL(18, 2)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`empid`)\n" +
                "DISTRIBUTED BY HASH(`empid`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"foreign_key_constraints\" = \"(deptno) REFERENCES depts(deptno)\"," +
                "    \"replication_num\" = \"1\"\n" +
                ");";

        starRocksAssert
                .withTable(deptsTable)
                .withTable(empsTable)
                .withTable(locationsTable)
                .withTable(ependentsTable);

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `customer` (\n" +
                "    `c_custkey` int(11) NOT NULL COMMENT \"\",\n" +
                "    `c_name` varchar(26) NOT NULL COMMENT \"\",\n" +
                "    `c_address` varchar(41) NOT NULL COMMENT \"\",\n" +
                "    `c_city` varchar(11) NOT NULL COMMENT \"\",\n" +
                "    `c_nation` varchar(16) NOT NULL COMMENT \"\",\n" +
                "    `c_region` varchar(13) NOT NULL COMMENT \"\",\n" +
                "    `c_phone` varchar(16) NOT NULL COMMENT \"\",\n" +
                "    `c_mktsegment` varchar(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c_custkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"groupa2\",\n" +
                "    \"in_memory\" = \"false\",\n" +
                "    \"unique_constraints\" = \"c_custkey\",\n" +
                "    \"storage_format\" = \"DEFAULT\"\n" +
                ")\n");
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `dates` (\n" +
                "    `d_datekey` int(11) NOT NULL COMMENT \"\",\n" +
                "    `d_date` varchar(20) NOT NULL COMMENT \"\",\n" +
                "    `d_dayofweek` varchar(10) NOT NULL COMMENT \"\",\n" +
                "    `d_month` varchar(11) NOT NULL COMMENT \"\",\n" +
                "    `d_year` int(11) NOT NULL COMMENT \"\",\n" +
                "    `d_yearmonthnum` int(11) NOT NULL COMMENT \"\",\n" +
                "    `d_yearmonth` varchar(9) NOT NULL COMMENT \"\",\n" +
                "    `d_daynuminweek` int(11) NOT NULL COMMENT \"\",\n" +
                "    `d_daynuminmonth` int(11) NOT NULL COMMENT \"\",\n" +
                "    `d_daynuminyear` int(11) NOT NULL COMMENT \"\",\n" +
                "    `d_monthnuminyear` int(11) NOT NULL COMMENT \"\",\n" +
                "    `d_weeknuminyear` int(11) NOT NULL COMMENT \"\",\n" +
                "    `d_sellingseason` varchar(14) NOT NULL COMMENT \"\",\n" +
                "    `d_lastdayinweekfl` int(11) NOT NULL COMMENT \"\",\n" +
                "    `d_lastdayinmonthfl` int(11) NOT NULL COMMENT \"\",\n" +
                "    `d_holidayfl` int(11) NOT NULL COMMENT \"\",\n" +
                "    `d_weekdayfl` int(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`d_datekey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`d_datekey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"in_memory\" = \"false\",\n" +
                "    \"colocate_with\" = \"groupa3\",\n" +
                "    \"unique_constraints\" = \"d_datekey\",\n" +
                "    \"storage_format\" = \"DEFAULT\"\n" +
                ")");
        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `part` (\n" +
                "    `p_partkey` int(11) NOT NULL COMMENT \"\",\n" +
                "    `p_name` varchar(23) NOT NULL COMMENT \"\",\n" +
                "    `p_mfgr` varchar(7) NOT NULL COMMENT \"\",\n" +
                "    `p_category` varchar(8) NOT NULL COMMENT \"\",\n" +
                "    `p_brand` varchar(10) NOT NULL COMMENT \"\",\n" +
                "    `p_color` varchar(12) NOT NULL COMMENT \"\",\n" +
                "    `p_type` varchar(26) NOT NULL COMMENT \"\",\n" +
                "    `p_size` int(11) NOT NULL COMMENT \"\",\n" +
                "    `p_container` varchar(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`p_partkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"groupa5\",\n" +
                "    \"in_memory\" = \"false\",\n" +
                "    \"unique_constraints\" = \"p_partkey\",\n" +
                "    \"storage_format\" = \"DEFAULT\"\n" +
                ")");
        starRocksAssert.withTable(" CREATE TABLE IF NOT EXISTS `supplier` (\n" +
                "    `s_suppkey` int(11) NOT NULL COMMENT \"\",\n" +
                "    `s_name` varchar(26) NOT NULL COMMENT \"\",\n" +
                "    `s_address` varchar(26) NOT NULL COMMENT \"\",\n" +
                "    `s_city` varchar(11) NOT NULL COMMENT \"\",\n" +
                "    `s_nation` varchar(16) NOT NULL COMMENT \"\",\n" +
                "    `s_region` varchar(13) NOT NULL COMMENT \"\",\n" +
                "    `s_phone` varchar(16) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`s_suppkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"groupa4\",\n" +
                "    \"in_memory\" = \"false\",\n" +
                "    \"unique_constraints\" = \"s_suppkey\",\n" +
                "    \"storage_format\" = \"DEFAULT\"\n" +
                ")");

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `lineorder` (\n" +
                "    `lo_orderkey` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_linenumber` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_custkey` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_partkey` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_suppkey` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_orderdate` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_orderpriority` varchar(16) NOT NULL COMMENT \"\",\n" +
                "    `lo_shippriority` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_quantity` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_extendedprice` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_ordtotalprice` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_discount` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_revenue` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_supplycost` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_tax` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_commitdate` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_shipmode` varchar(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`lo_orderkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`lo_orderdate`)\n" +
                "(\n" +
                "    PARTITION p1 VALUES [(\"-2147483648\"), (\"19930101\")),\n" +
                "    PARTITION p2 VALUES [(\"19930101\"), (\"19940101\")),\n" +
                "    PARTITION p3 VALUES [(\"19940101\"), (\"19950101\")),\n" +
                "    PARTITION p4 VALUES [(\"19950101\"), (\"19960101\")),\n" +
                "    PARTITION p5 VALUES [(\"19960101\"), (\"19970101\")),\n" +
                "    PARTITION p6 VALUES [(\"19970101\"), (\"19980101\")),\n" +
                "    PARTITION p7 VALUES [(\"19980101\"), (\"19990101\")))\n" +
                "DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"groupa1\",\n" +
                "    \"in_memory\" = \"false\",\n" +
                "    \"foreign_key_constraints\" = \"(lo_custkey) REFERENCES customer(c_custkey);" +
                " (lo_partkey) REFERENCES part(p_partkey);  (lo_suppkey) REFERENCES supplier(s_suppkey);" +
                "  (lo_orderdate) REFERENCES dates(d_datekey)\",\n" +
                "    \"storage_format\" = \"DEFAULT\"\n" +
                ")");

        starRocksAssert.withTable("CREATE TABLE IF NOT EXISTS `lineorder_null` (\n" +
                "    `lo_orderkey` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_linenumber` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_custkey` int(11) COMMENT \"\",\n" +
                "    `lo_partkey` int(11) COMMENT \"\",\n" +
                "    `lo_suppkey` int(11) COMMENT \"\",\n" +
                "    `lo_orderdate` int(11) COMMENT \"\",\n" +
                "    `lo_orderpriority` varchar(16) NOT NULL COMMENT \"\",\n" +
                "    `lo_shippriority` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_quantity` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_extendedprice` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_ordtotalprice` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_discount` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_revenue` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_supplycost` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_tax` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_commitdate` int(11) NOT NULL COMMENT \"\",\n" +
                "    `lo_shipmode` varchar(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`lo_orderkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "PARTITION BY RANGE(`lo_orderdate`)\n" +
                "(\n" +
                "    PARTITION p1 VALUES [(\"-2147483648\"), (\"19930101\")),\n" +
                "    PARTITION p2 VALUES [(\"19930101\"), (\"19940101\")),\n" +
                "    PARTITION p3 VALUES [(\"19940101\"), (\"19950101\")),\n" +
                "    PARTITION p4 VALUES [(\"19950101\"), (\"19960101\")),\n" +
                "    PARTITION p5 VALUES [(\"19960101\"), (\"19970101\")),\n" +
                "    PARTITION p6 VALUES [(\"19970101\"), (\"19980101\")),\n" +
                "    PARTITION p7 VALUES [(\"19980101\"), (\"19990101\")))\n" +
                "DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"groupa1\",\n" +
                "    \"in_memory\" = \"false\",\n" +
                "    \"foreign_key_constraints\" = \"(lo_custkey) REFERENCES customer(c_custkey);" +
                " (lo_partkey) REFERENCES part(p_partkey);  (lo_suppkey) REFERENCES supplier(s_suppkey);" +
                "  (lo_orderdate) REFERENCES dates(d_datekey)\",\n" +
                "    \"storage_format\" = \"DEFAULT\"\n" +
                ")");

        starRocksAssert.withTable(" CREATE TABLE IF NOT EXISTS `t2` (\n" +
                "    `c5` int(11) NOT NULL COMMENT \"\",\n" +
                "    `c6` int(11) NOT NULL COMMENT \"\",\n" +
                "    `c7` int(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c5`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c5`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"groupa4\",\n" +
                "    \"in_memory\" = \"false\",\n" +
                "    \"unique_constraints\" = \"c5\",\n" +
                "    \"storage_format\" = \"DEFAULT\"\n" +
                ")");

        starRocksAssert.withTable(" CREATE TABLE IF NOT EXISTS `t3` (\n" +
                "    `c5` int(11) NOT NULL COMMENT \"\",\n" +
                "    `c6` int(11) NOT NULL COMMENT \"\",\n" +
                "    `c7` int(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c5`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c5`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"groupa4\",\n" +
                "    \"in_memory\" = \"false\",\n" +
                "    \"unique_constraints\" = \"c5\",\n" +
                "    \"storage_format\" = \"DEFAULT\"\n" +
                ")");

        starRocksAssert.withTable(" CREATE TABLE IF NOT EXISTS `t1` (\n" +
                "    `c1` int(11) NOT NULL COMMENT \"\",\n" +
                "    `c2` int(11) NOT NULL COMMENT \"\",\n" +
                "    `c3` int(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c1`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"groupa4\",\n" +
                "    \"in_memory\" = \"false\",\n" +
                "    \"foreign_key_constraints\" = \"(c2) REFERENCES t2(c5);(c3) REFERENCES t2(c5)\",\n" +
                "    \"storage_format\" = \"DEFAULT\"\n" +
                ")");
    }

    @Test
    public void testFilter0() {
        String mv = "select empid + 1 as col1 from emps where deptno = 10";
        testRewriteOK(mv, "select empid + 1 from emps where deptno = 10");
        testRewriteOK(mv, "select max(empid + 1) from emps where deptno = 10");
        testRewriteOK(mv, "select max(empid) from emps where deptno = 10").contains("col1 - 1");

        testRewriteFail(mv, "select max(empid) from emps where deptno = 11");
        testRewriteFail(mv, "select max(empid) from emps");
        testRewriteOK(mv, "select empid from emps where deptno = 10").contains("col1 - 1");
    }

    @Test
    public void testFilterProject0() {
        String mv = "select empid, locationid * 2 as col2 from emps where deptno = 10";
        testRewriteOK(mv, "select empid + 1 from emps where deptno = 10");
        testRewriteOK(mv, "select empid from emps where deptno = 10 and (locationid * 2) < 10");
    }

    @Test
    public void testSwapInnerJoin() {
        String mv = "select count(*) as col1 from emps join locations on emps.locationid = locations.locationid";
        testRewriteOK(mv, "select count(*) from locations join emps on emps.locationid = locations.locationid");
        testRewriteOK(mv, "select count(*) + 1 from  locations join emps on emps.locationid = locations.locationid");
    }

    @Test
    public void testsInnerJoinComplete() {
        String mv = "select deptno as col1, empid as col2, emps.locationid as col3 from emps " +
                "join locations on emps.locationid = locations.locationid";
        testRewriteOK(mv, "select count(*)  from emps " +
                "join locations on emps.locationid = locations.locationid ");
        testRewriteOK(mv, "select count(*)  from emps " +
                "join locations on emps.locationid = locations.locationid " +
                "and emps.locationid > 10");
        testRewriteOK(mv, "select empid as col2, emps.locationid from emps " +
                "join locations on emps.locationid = locations.locationid " +
                "and emps.locationid > 10");
        testRewriteOK(mv, "select empid as col2, locations.locationid from emps " +
                "join locations on emps.locationid = locations.locationid " +
                "and locations.locationid > 10");
    }

    @Test
    public void testMultiInnerJoinQueryDelta() {
        String mv = "select deptno as col1, empid as col2, locations.locationid as col3 " +
                "from emps join locations on emps.locationid = locations.locationid";
        testRewriteOK(mv, "select count(*)  from emps join locations " +
                "join depts on emps.locationid = locations.locationid and depts.deptno = emps.deptno");
    }

    @Test
    public void testMultiInnerJoinViewDelta() {
        String mv = "select emps.deptno as col1, empid as col2, locations.locationid as col3 " +
                "from emps join locations join depts " +
                "on emps.locationid = locations.locationid and depts.deptno = emps.deptno";
        testRewriteOK(mv, "select deptno as col1, empid as col2, locations.locationid as col3 " +
                "from emps join locations on emps.locationid = locations.locationid");
    }

    @Test
    public void testSwapOuterJoin() {
        for (String joinType : outerJoinTypes) {
            String mv = "select count(*) as col1 from " +
                    "emps " + joinType + " join locations on emps.locationid = locations.locationid";
            testRewriteOK(mv, "select count(*)  + 1 from " +
                    "emps " + joinType + " join locations on emps.locationid = locations.locationid");

            // outer join cannot swap join orders
            testRewriteFail(mv, "select count(*) as col1 from " +
                    "locations " + joinType + " join emps on emps.locationid = locations.locationid");
            testRewriteFail(mv, "select count(*) + 1 as col1 from " +
                    "locations " + joinType + " join emps on emps.locationid = locations.locationid");

            // outer join cannot change join type
            if (joinType.equalsIgnoreCase("right")) {
                testRewriteFail(mv, "select count(*)  as col1 from " +
                        "emps left join locations on emps.locationid = locations.locationid");
            } else {
                testRewriteFail(mv, "select count(*) as col1 from " +
                        "emps right join locations on emps.locationid = locations.locationid");
            }
        }
    }

    @Test
    public void testMultiOuterJoinQueryComplete() {
        for (String joinType : outerJoinTypes) {
            String mv = "select deptno as col1, empid as col2, emps.locationid as col3 from emps " +
                    "" + joinType + " join locations on emps.locationid = locations.locationid";
            testRewriteOK(mv, "select count(*) from " +
                    "emps " + joinType + " join locations on emps.locationid = locations.locationid");
            testRewriteOK(mv, "select empid as col2, emps.locationid from " +
                    "emps " + joinType + " join locations on emps.locationid = locations.locationid " +
                    "and emps.locationid > 10");
            testRewriteOK(mv, "select count(*) from " +
                    "emps " + joinType + " join locations on emps.locationid = locations.locationid " +
                    "and emps.locationid > 10");
            testRewriteOK(mv, "select empid as col2, locations.locationid from " +
                    "emps " + joinType + " join locations on emps.locationid = locations.locationid " +
                    "and locations.locationid > 10");
            testRewriteFail(mv, "select empid as col2, locations.locationid from " +
                    "emps inner join locations on emps.locationid = locations.locationid " +
                    "and locations.locationid > 10");
        }
    }

    @Test
    public void testMultiOuterJoinQueryDelta() {
        for (String joinType : outerJoinTypes) {
            String mv = "select deptno as col1, empid as col2, locations.locationid as col3 from emps " +
                    "" + joinType + " join locations on emps.locationid = locations.locationid";
            testRewriteOK(mv, "select count(*)  from emps " +
                    "" + joinType + " join locations on emps.locationid = locations.locationid " +
                    "" + joinType + " join depts on depts.deptno = emps.deptno");
        }
    }

    @Test
    public void testAggregateBasic() {
        String mv = "select deptno as col1, locationid + 1 as b, count(*) as c, sum(empid) as s " +
                "from emps group by locationid + 1, deptno";
        testRewriteOK(mv, "select count(*) as c, deptno from emps group by deptno");
        testRewriteOK(mv, "select count(*) as c, locationid + 1 from emps group by locationid + 1");
    }

    @Test
    public void testAggregate0() {
        testRewriteOK("select deptno, count(*) as c, empid + 2 as d, sum(empid) as s "
                        + "from emps group by empid, deptno",
                "select count(*) + 1 as c, deptno from emps group by deptno");
    }

    @Test
    public void testAggregate1() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select deptno from emps group by deptno");
    }

    @Test
    public void testAggregate2() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by deptno");
    }

    @Test
    public void testAggregate3() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select deptno, empid, sum(empid) as s, count(*) as c\n"
                        + "from emps group by empid, deptno");
    }

    @Test
    public void testAggregate4() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno, sum(empid) as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregate5() {
        testRewriteOK("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregate6() {
        testRewriteOK("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno + 1, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregate7() {

        testRewriteFail("select empid, deptno, count(*) + 1 as c, sum(empid) + 2 as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregate8() {
        testRewriteOK("select empid, deptno + 1, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno + 1, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregate9() {
        testRewriteOK("select sum(salary) as col1, count(salary) + 1 from emps",
                "select sum(salary), count(salary) + 1 from emps");
        testRewriteFail("select empid, deptno," +
                        " sum(salary) as total, count(salary) + 1 as cnt" +
                        " from emps group by empid, deptno ",
                "select sum(salary), count(salary) + 1 from emps");
    }

    @Test
    public void testAggregateWithAggExpr() {
        // support agg expr: empid -> abs(empid)
        testRewriteOK("select empid, deptno," +
                        " sum(salary) as total, count(salary) + 1 as cnt" +
                        " from emps group by empid, deptno ",
                "select abs(empid), sum(salary) from emps group by empid");
        testRewriteOK("select empid, deptno," +
                        " sum(salary) as total, count(salary) + 1 as cnt" +
                        " from emps group by empid, deptno ",
                "select abs(empid), sum(salary) from emps group by empid, deptno");
    }

    @Test
    public void testAggregateWithGroupByKeyExpr() {
        testRewriteOK("select empid, deptno," +
                        " sum(salary) as total, count(salary) + 1 as cnt" +
                        " from emps group by empid, deptno ",
                "select abs(empid), sum(salary) from emps group by abs(empid), deptno")
                .contains("  0:OlapScanNode\n" +
                        "     TABLE: mv0")
                .contains("  2:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: sum(11: total)\n" +
                        "  |  group by: 14: abs, 10: deptno");
    }

    @Test
    @Ignore
    // TODO: Remove const group by keys
    public void testAggregateWithContGroupByKey() {
        testRewriteOK("select empid, floor(cast('1997-01-20 12:34:56' as DATETIME)), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, floor(cast('1997-01-20 12:34:56' as DATETIME))",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME)), sum(empid) as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME))");
    }

    @Test
    public void testAggregateRollup() {
        String mv = "select deptno, count(*) as c, sum(empid) as s from emps group by locationid, deptno";
        testRewriteOK(mv, "select count(*) as c, deptno from emps group by deptno");
        testRewriteOK(mv, "select sum(empid), count(*) as c, deptno from emps group by deptno");
        testRewriteOK(mv, "select count(*) + 1 as c, deptno from emps group by deptno");
    }

    @Test
    public void testsInnerJoinAggregate() {
        String mv = "select count(*) as col1, emps.deptno from " +
                "emps join locations on emps.locationid = locations.locationid + 1 " +
                "group by emps.deptno";
        testRewriteOK(mv, "select count(*) from " +
                "locations join emps on emps.locationid = locations.locationid + 1 " +
                "group by emps.deptno");
        testRewriteFail(mv, "select count(*) , emps.deptno from " +
                "locations join emps on emps.locationid = locations.locationid + 1 " +
                "where emps.deptno > 10 " +
                "group by emps.deptno");
    }

    @Test
    public void testUnionAll0() {
        String mv = "select deptno as col1, count(*) as c, sum(empid) as s from emps group by locationid + 1, deptno";
        testRewriteOK(mv, "select count(*) as c, deptno from emps group by deptno " +
                "union all " +
                "select count(*) as c, deptno from emps where deptno > 10 group by deptno ");
    }

    @Test
    public void testAggregateProject() {
        testRewriteOK("select deptno, count(*) as c, empid + 2, sum(empid) as s "
                        + "from emps group by empid, deptno",
                "select count(*) as c, deptno from emps group by deptno");
        testRewriteOK("select deptno, count(*) as c, empid + 2, sum(empid) as s "
                        + "from emps group by empid, deptno",
                "select count(*) + 1 as c, deptno from emps group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs1() {
        testRewriteOK("select empid, deptno from emps group by empid, deptno",
                "select empid, deptno from emps group by empid, deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs2() {
        testRewriteOK("select empid, deptno from emps group by empid, deptno",
                "select deptno from emps group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs3() {
        testRewriteFail("select deptno from emps group by deptno",
                "select empid, deptno from emps group by empid, deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs4() {
        testRewriteOK("select empid, deptno\n"
                        + "from emps where deptno = 10 group by empid, deptno",
                "select deptno from emps where deptno = 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs5() {
        testRewriteFail("select empid, deptno\n"
                        + "from emps where deptno = 5 group by empid, deptno",
                "select deptno from emps where deptno = 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs6() {
        testRewriteOK("select empid, deptno\n"
                        + "from emps where deptno > 5 group by empid, deptno",
                "select deptno from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs7() {
        testRewriteFail("select empid, deptno\n"
                        + "from emps where deptno > 5 group by empid, deptno",
                "select deptno from emps where deptno < 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs8() {
        testRewriteFail("select empid from emps group by empid, deptno",
                "select deptno from emps group by deptno");
    }

    @Test
    public void testAggregateMaterializationNoAggregateFuncs9() {
        testRewriteFail("select empid, deptno from emps\n"
                        + "where salary > 1000 group by name, empid, deptno",
                "select empid from emps\n"
                        + "where salary > 2000 group by name, empid");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs1() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select deptno from emps group by deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs2() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs3() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select deptno, empid, sum(empid) as s, count(*) as c\n"
                        + "from emps group by empid, deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs4() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno, sum(empid) as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs5() {
        testRewriteOK("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs6() {
        testRewriteFail("select empid, deptno, count(*) + 1 as c, sum(empid) + 2 as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs7() {
        testRewriteOK("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",
                "select deptno + 1, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs8() {
        testRewriteOK("select empid, deptno + 1 as col, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps where deptno >= 10 group by empid, deptno",

                "select deptno + 1, sum(empid) + 1 as s\n"
                        + "from emps where deptno > 10 group by deptno").contains("col - 1");
    }

    @Test
    @Ignore
    // TODO: Remove const group by keys
    public void testAggregateMaterializationAggregateFuncsWithConstGroupByKeys() {
        testRewriteOK("select empid, floor(cast('1997-01-20 12:34:56' as DATETIME)), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, floor(cast('1997-01-20 12:34:56' as DATETIME))",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME)), sum(empid) as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME))");
        testRewriteOK("select empid, floor(cast('1997-01-20 12:34:56' as DATETIME)), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, floor(cast('1997-01-20 12:34:56' as DATETIME))",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME)), sum(empid) + 1 as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME))");
        testRewriteOK("select empid, floor(cast('1997-01-20 12:34:56' as DATETIME)), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, floor(cast('1997-01-20 12:34:56' as DATETIME))",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME) ), sum(empid) as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME) )");
        testRewriteOK("select empid, floor(cast('1997-01-20 12:34:56' as DATETIME)), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, floor(cast('1997-01-20 12:34:56' as DATETIME))",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME)), sum(empid) as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME))");
        testRewriteOK("select empid, cast('1997-01-20 12:34:56' as DATETIME), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, cast('1997-01-20 12:34:56' as DATETIME)",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME)), sum(empid) as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME))");
        testRewriteOK("select empid, floor(cast('1997-01-20 12:34:56' as DATETIME)), "
                        + "count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps\n"
                        + "group by empid, floor(cast('1997-01-20 12:34:56' as DATETIME))",
                "select floor(cast('1997-01-20 12:34:56' as DATETIME)), sum(empid) as s\n"
                        + "from emps group by floor(cast('1997-01-20 12:34:56' as DATETIME))");
        testRewriteOK("select eventid, floor(cast(ts as DATETIME)), "
                        + "count(*) + 1 as c, sum(eventid) as s\n"
                        + "from events group by eventid, floor(cast(ts as DATETIME))",
                "select floor(cast(ts as DATETIME) ), sum(eventid) as s\n"
                        + "from events group by floor(cast(ts as DATETIME) )");
        testRewriteOK("select eventid, cast(ts as DATETIME), count(*) + 1 as c, sum(eventid) as s\n"
                        + "from events group by eventid, cast(ts as DATETIME)",
                "select floor(cast(ts as DATETIME)), sum(eventid) as s\n"
                        + "from events group by floor(cast(ts as DATETIME))");
        testRewriteOK("select eventid, floor(cast(ts as DATETIME)), "
                        + "count(*) + 1 as c, sum(eventid) as s\n"
                        + "from events group by eventid, floor(cast(ts as DATETIME))",
                "select floor(cast(ts as DATETIME)), sum(eventid) as s\n"
                        + "from events group by floor(cast(ts as DATETIME))");
    }

    @Test
    public void testAggregateMaterializationAggregateFuncs18() {
        testRewriteOK("select empid, deptno, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select empid*deptno, sum(empid) as s\n"
                        + "from emps group by empid*deptno");
    }


    @Test
    public void testAggregateMaterializationAggregateFuncs19() {
        testRewriteOK("select empid, deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps group by empid, deptno",
                "select empid + 10, count(*) + 1 as c\n"
                        + "from emps group by empid + 10");
    }


    // TODO: Don't support group by position.
    @Ignore
    public void testAggregateMaterializationAggregateFuncs20() {
        testRewriteOK("select 11 as empno, 22 as sal, count(*) from emps group by 11",
                "select * from\n"
                        + "(select 11 as empno, 22 as sal, count(*)\n"
                        + "from emps group by 11, 22 ) tmp\n"
                        + "where sal = 33");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs1() {
        // If agg push down is open, cannot rewrite.
        testRewriteOK("select empid, depts.deptno from emps\n"
                        + "join depts using (deptno) where depts.deptno > 10\n"
                        + "group by empid, depts.deptno",
                "select empid from emps\n"
                        + "join depts using (deptno) where depts.deptno > 20\n"
                        + "group by empid, depts.deptno");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs2() {
        testRewriteOK("select depts.deptno, empid from depts\n"
                        + "join emps using (deptno) where depts.deptno > 10\n"
                        + "group by empid, depts.deptno",
                "select empid from emps\n"
                        + "join depts using (deptno) where depts.deptno > 20\n"
                        + "group by empid, depts.deptno");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs3() {
        // It does not match, Project on top of query
        testRewriteFail("select empid from emps\n"
                        + "join depts using (deptno) where depts.deptno > 10\n"
                        + "group by empid, depts.deptno",
                "select empid from emps\n"
                        + "join depts using (deptno) where depts.deptno > 20\n"
                        + "group by empid, depts.deptno");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs4() {
        testRewriteOK("select empid, depts.deptno from emps\n"
                        + "join depts using (deptno) where emps.deptno > 10\n"
                        + "group by empid, depts.deptno",
                "select empid from emps\n"
                        + "join depts using (deptno) where depts.deptno > 20\n"
                        + "group by empid, depts.deptno");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs5() {
        testRewriteOK("select depts.deptno, emps.empid from depts\n"
                        + "join emps using (deptno) where emps.empid > 10\n"
                        + "group by depts.deptno, emps.empid",
                "select depts.deptno from depts\n"
                        + "join emps using (deptno) where emps.empid > 15\n"
                        + "group by depts.deptno, emps.empid");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs6() {
        testRewriteOK("select depts.deptno, emps.empid from depts\n"
                        + "join emps using (deptno) where emps.empid > 10\n"
                        + "group by depts.deptno, emps.empid",
                "select depts.deptno from depts\n"
                        + "join emps using (deptno) where emps.empid > 15\n"
                        + "group by depts.deptno");
    }

    @Test
    @Ignore
    // TODO: union all support
    public void testJoinAggregateMaterializationNoAggregateFuncs7() {
        testRewriteOK("select depts.deptno, dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 11\n"
                        + "group by depts.deptno, dependents.empid",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10\n"
                        + "group by dependents.empid");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs8() {
        testRewriteFail("select depts.deptno, dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 20\n"
                        + "group by depts.deptno, dependents.empid",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10 and depts.deptno < 20\n"
                        + "group by dependents.empid");
    }

    @Test
    @Ignore
    // TODO: union all support
    public void testJoinAggregateMaterializationNoAggregateFuncs9() {
        testRewriteOK("select depts.deptno, dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 11 and depts.deptno < 19\n"
                        + "group by depts.deptno, dependents.empid",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10 and depts.deptno < 20\n"
                        + "group by dependents.empid");
    }

    @Test
    public void testJoinAggregateMaterializationNoAggregateFuncs10() {
        testRewriteOK("select depts.name, dependents.name as name2, "
                        + "emps.deptno, depts.deptno as deptno2, "
                        + "dependents.empid\n"
                        + "from depts, dependents, emps\n"
                        + "where depts.deptno > 10\n"
                        + "group by depts.name, dependents.name, "
                        + "emps.deptno, depts.deptno, "
                        + "dependents.empid",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10\n"
                        + "group by dependents.empid");
    }


    @Test
    @Ignore
    // TODO: This test relies on FK-UK relationship
    public void testJoinAggregateMaterializationAggregateFuncs1() {
        testRewriteOK("select empid, depts.deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "group by empid, depts.deptno",
                "select deptno from emps group by deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs2() {
        testRewriteOK("select empid, emps.deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "group by empid, emps.deptno",
                "select depts.deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "group by depts.deptno");
    }

    @Ignore
    @Test
    // TODO: This test relies on FK-UK relationship
    public void testJoinAggregateMaterializationAggregateFuncs3() {
        testRewriteOK("select empid, depts.deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "group by empid, depts.deptno",
                "select deptno, empid, sum(empid) as s, count(*) as c\n"
                        + "from emps group by empid, deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs4() {
        testRewriteOK("select empid, emps.deptno, count(*) as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "where emps.deptno >= 10 group by empid, emps.deptno",
                "select depts.deptno, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "where emps.deptno > 10 group by depts.deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs5() {
        testRewriteOK("select empid, depts.deptno, count(*) + 1 as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "where depts.deptno >= 10 group by empid, depts.deptno",
                "select depts.deptno, sum(empid) + 1 as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "where depts.deptno > 10 group by depts.deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs6() {
        final String m = "select depts.name, sum(salary) as s\n"
                + "from emps\n"
                + "join depts on (emps.deptno = depts.deptno)\n"
                + "group by depts.name";
        final String q = "select dependents.empid, sum(salary) as s\n"
                + "from emps\n"
                + "join depts on (emps.deptno = depts.deptno)\n"
                + "join dependents on (depts.name = dependents.name)\n"
                + "group by dependents.empid";
        testRewriteOK(m, q);
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs7() {
        testRewriteOK("select dependents.empid, emps.deptno, sum(salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno",
                "select dependents.empid, sum(salary) as s\n"
                        + "from emps\n"
                        + "join depts on (emps.deptno = depts.deptno)\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs8() {
        testRewriteOK("select dependents.empid, emps.deptno, sum(salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno",
                "select depts.name, sum(salary) as s\n"
                        + "from emps\n"
                        + "join depts on (emps.deptno = depts.deptno)\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by depts.name");
    }

    @Test
    public void testJoinAggregateMaterializationAggregateFuncs9() {
        testRewriteOK("select dependents.empid, emps.deptno, count(distinct salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno",
                "select emps.deptno, count(distinct salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno");
    }

    @Test
    public void testJoinAggregateMaterializationAggregateFuncs10() {
        testRewriteFail("select dependents.empid, emps.deptno, count(distinct salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno",
                "select emps.deptno, count(distinct salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by emps.deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs11() {
        testRewriteOK("select depts.deptno, dependents.empid, count(emps.salary) as s\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 11 and depts.deptno < 19\n"
                        + "group by depts.deptno, dependents.empid",
                "select dependents.empid, count(emps.salary) + 1\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10 and depts.deptno < 20\n"
                        + "group by dependents.empid");
    }

    @Test
    public void testJoinAggregateMaterializationAggregateFuncs12() {
        testRewriteFail("select depts.deptno, dependents.empid, "
                        + "count(distinct emps.salary) as s\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 11 and depts.deptno < 19\n"
                        + "group by depts.deptno, dependents.empid",
                "select dependents.empid, count(distinct emps.salary) + 1\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10 and depts.deptno < 20\n"
                        + "group by dependents.empid");
    }

    @Test
    public void testJoinAggregateMaterializationAggregateFuncs13() {
        testRewriteFail("select dependents.empid, emps.deptno, count(distinct salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno",
                "select emps.deptno, count(salary) as s\n"
                        + "from emps\n"
                        + "join dependents on (emps.empid = dependents.empid)\n"
                        + "group by dependents.empid, emps.deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs14() {
        testRewriteOK("select empid, emps.name, emps.deptno, depts.name, "
                        + "count(*) as c, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "where (depts.name is not null and emps.name = 'a') or "
                        + "(depts.name is not null and emps.name = 'b')\n"
                        + "group by empid, emps.name, depts.name, emps.deptno",
                "select depts.deptno, sum(empid) as s\n"
                        + "from emps join depts using (deptno)\n"
                        + "where depts.name is not null and emps.name = 'a'\n"
                        + "group by depts.deptno");
    }

    @Test
    @Ignore
    public void testJoinAggregateMaterializationAggregateFuncs15() {
        final String m = ""
                + "SELECT deptno,\n"
                + "  COUNT(*) AS dept_size,\n"
                + "  SUM(salary) AS dept_budget\n"
                + "FROM emps\n"
                + "GROUP BY deptno";
        final String q = ""
                + "SELECT FLOOR(CREATED_AT TO YEAR) AS by_year,\n"
                + "  COUNT(*) AS num_emps\n"
                + "FROM (SELECTdeptno\n"
                + "    FROM emps) AS t\n"
                + "JOIN (SELECT deptno,\n"
                + "        inceptionDate as CREATED_AT\n"
                + "    FROM depts2) using (deptno)\n"
                + "GROUP BY FLOOR(CREATED_AT TO YEAR)";
        testRewriteOK(m, q);
    }

    @Test
    public void testJoinMaterialization1() {
        String q = "select depts.deptno, empid, locationid, t.name \n"
                + "from (select * from emps where empid < 300) t \n"
                + "join depts using (deptno)";
        testRewriteOK("select deptno, empid, locationid, name from emps where empid < 500", q);
    }

    @Test
    public void testJoinMaterialization2() {
        String q = "select *\n"
                + "from emps\n"
                + "join depts using (deptno)";
        String m = "select deptno, empid, locationid, name,\n"
                + "salary, commission from emps";
        testRewriteOK(m, q);
    }

    @Test
    public void testJoinMaterializationStar() {
        String q = "select *\n"
                + "from emps\n"
                + "join depts using (deptno)";
        String m = "select * from emps";
        testRewriteOK(m, q);
    }

    @Test
    public void testJoinMaterialization3() {
        String q = "select empid deptno from emps\n"
                + "join depts using (deptno) where empid = 1";
        String m = "select empid deptno from emps\n"
                + "join depts using (deptno)";
        testRewriteOK(m, q);
    }

    @Test
    public void testJoinMaterialization4() {
        testRewriteOK("select empid deptno from emps\n"
                        + "join depts using (deptno)",
                "select empid deptno from emps\n"
                        + "join depts using (deptno) where empid = 1");
    }

    @Test
    @Ignore
    // TODO: Need add no-loss-cast in lineage factory.
    public void testJoinMaterialization5() {
        testRewriteOK("select cast(empid as BIGINT) as a from emps\n"
                        + "join depts using (deptno)",
                "select empid from emps\n"
                        + "join depts using (deptno) where empid > 1");
    }

    @Test
    @Ignore
    // TODO: Need add no-loss-cast in lineage factory.
    public void testJoinMaterialization6() {
        testRewriteOK("select cast(empid as BIGINT) as a from emps\n"
                        + "join depts using (deptno)",
                "select empid deptno from emps\n"
                        + "join depts using (deptno) where empid = 1");
    }

    @Test
    public void testJoinMaterialization7() {
        testRewriteOK("select depts.name\n"
                        + "from emps\n"
                        + "join depts on (emps.deptno = depts.deptno)",
                "select dependents.empid\n"
                        + "from emps\n"
                        + "join depts on (emps.deptno = depts.deptno)\n"
                        + "join dependents on (depts.name = dependents.name)");
    }

    @Test
    public void testJoinMaterialization8() {
        testRewriteOK("select depts.name\n"
                        + "from emps\n"
                        + "join depts on (emps.deptno = depts.deptno)",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)");
    }

    @Test
    public void testJoinMaterialization9() {
        testRewriteOK("select depts.name\n"
                        + "from emps\n"
                        + "join depts on (emps.deptno = depts.deptno)",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join locations on (locations.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)");
    }


    @Test
    @Ignore
    public void testJoinMaterialization10() {
        testRewriteOK("select depts.deptno, dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 30",
                "select dependents.empid\n"
                        + "from depts\n"
                        + "join dependents on (depts.name = dependents.name)\n"
                        + "join emps on (emps.deptno = depts.deptno)\n"
                        + "where depts.deptno > 10");
    }

    @Test
    public void testJoinMaterialization11() {
        testRewriteFail("select empid from emps\n"
                        + "join depts using (deptno)",
                "select empid from emps\n"
                        + "where deptno in (select deptno from depts)");
    }

    @Test
    @Ignore
    public void testJoinMaterialization12() {
        testRewriteOK("select empid, emps.name as a, emps.deptno, depts.name as b \n"
                        + "from emps join depts using (deptno)\n"
                        + "where (depts.name is not null and emps.name = 'a') or "
                        + "(depts.name is not null and emps.name = 'b') or "
                        + "(depts.name is not null and emps.name = 'c')",
                "select depts.deptno, depts.name\n"
                        + "from emps join depts using (deptno)\n"
                        + "where (depts.name is not null and emps.name = 'a') or "
                        + "(depts.name is not null and emps.name = 'b')");
    }


    @Test
    @Ignore
    // TODO: agg push down below Join
    public void testAggregateOnJoinKeys() {
        testRewriteOK("select deptno, empid, salary "
                        + "from emps\n"
                        + "group by deptno, empid, salary",
                "select empid, depts.deptno "
                        + "from emps\n"
                        + "join depts on depts.deptno = empid group by empid, depts.deptno");
    }


    @Test
    @Ignore
    // TODO: agg need push down below join
    public void testAggregateOnJoinKeys2() {
        testRewriteOK("select deptno, empid, salary, sum(1) as c "
                        + "from emps\n"
                        + "group by deptno, empid, salary",
                "select sum(1) "
                        + "from emps\n"
                        + "join depts on depts.deptno = empid group by empid, depts.deptno");
    }

    @Test
    public void testAggregateMaterializationOnCountDistinctQuery1() {
        // The column empid is already unique, thus DISTINCT is not
        // in the COUNT of the resulting rewriting
        testRewriteOK("select deptno, empid, salary\n"
                        + "from emps\n"
                        + "group by deptno, empid, salary",
                "select deptno, count(distinct empid) as c from (\n"
                        + "select deptno, empid\n"
                        + "from emps\n"
                        + "group by deptno, empid) t\n"
                        + "group by deptno");
    }

    @Test
    public void testAggregateMaterializationOnCountDistinctQuery2() {
        // The column empid is already unique, thus DISTINCT is not
        // in the COUNT of the resulting rewriting
        testRewriteOK("select deptno, salary, empid\n"
                        + "from emps\n"
                        + "group by deptno, salary, empid",
                "select deptno, count(distinct empid) as c from (\n"
                        + "select deptno, empid\n"
                        + "from emps\n"
                        + "group by deptno, empid) t \n"
                        + "group by deptno");
    }

    @Test
    public void testAggregateMaterializationOnCountDistinctQuery3() {
        // The column salary is not unique, thus we end up with
        // a different rewriting
        testRewriteOK("select deptno, empid, salary\n"
                        + "from emps\n"
                        + "group by deptno, empid, salary",
                "select deptno, count(distinct salary) from (\n"
                        + "select deptno, salary\n"
                        + "from emps\n"
                        + "group by deptno, salary) t \n"
                        + "group by deptno");
    }

    @Test
    public void testAggregateMaterializationOnCountDistinctQuery4() {
        // Although there is no DISTINCT in the COUNT, this is
        // equivalent to previous query
        testRewriteOK("select deptno, salary, empid\n"
                        + "from emps\n"
                        + "group by deptno, salary, empid",
                "select deptno, count(salary) from (\n"
                        + "select deptno, salary\n"
                        + "from emps\n"
                        + "group by deptno, salary) t\n"
                        + "group by deptno");
    }

    @Test
    public void testInnerJoinViewDelta() {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000000);
        String mv = "SELECT" +
                " `l`.`LO_ORDERKEY` as col1, `l`.`LO_ORDERDATE`, `l`.`LO_LINENUMBER`, `l`.`LO_CUSTKEY`, `l`.`LO_PARTKEY`," +
                " `l`.`LO_SUPPKEY`, `l`.`LO_ORDERPRIORITY`, `l`.`LO_SHIPPRIORITY`, `l`.`LO_QUANTITY`," +
                " `l`.`LO_EXTENDEDPRICE`, `l`.`LO_ORDTOTALPRICE`, `l`.`LO_DISCOUNT`, `l`.`LO_REVENUE`," +
                " `l`.`LO_SUPPLYCOST`, `l`.`LO_TAX`, `l`.`LO_COMMITDATE`, `l`.`LO_SHIPMODE`," +
                " `c`.`C_NAME`, `c`.`C_ADDRESS`, `c`.`C_CITY`, `c`.`C_NATION`, `c`.`C_REGION`, `c`.`C_PHONE`," +
                " `c`.`C_MKTSEGMENT`, `s`.`S_NAME`, `s`.`S_ADDRESS`, `s`.`S_CITY`, `s`.`S_NATION`, `s`.`S_REGION`," +
                " `s`.`S_PHONE`, `p`.`P_NAME`, `p`.`P_MFGR`, `p`.`P_CATEGORY`, `p`.`P_BRAND`, `p`.`P_COLOR`," +
                " `p`.`P_TYPE`, `p`.`P_SIZE`, `p`.`P_CONTAINER`\n" +
                "FROM `lineorder` AS `l` INNER" +
                " JOIN `customer` AS `c` ON `c`.`C_CUSTKEY` = `l`.`LO_CUSTKEY`" +
                " INNER JOIN `supplier` AS `s` ON `s`.`S_SUPPKEY` = `l`.`LO_SUPPKEY`" +
                " INNER JOIN `part` AS `p` ON `p`.`P_PARTKEY` = `l`.`LO_PARTKEY`;";

        String query = "SELECT `lineorder`.`lo_orderkey`, `lineorder`.`lo_orderdate`, `customer`.`c_custkey` AS `cd`\n" +
                "FROM `lineorder` INNER JOIN `customer` ON `lineorder`.`lo_custkey` = `customer`.`c_custkey`\n" +
                "WHERE `lineorder`.`lo_orderkey` = 100;";

        testRewriteOK(mv, query);
    }

    @Test
    public void testOuterJoinViewDelta() {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000000);
        String mv = "SELECT" +
                " `l`.`LO_ORDERKEY` as col1, `l`.`LO_ORDERDATE`, `l`.`LO_LINENUMBER`, `l`.`LO_CUSTKEY`, `l`.`LO_PARTKEY`," +
                " `l`.`LO_SUPPKEY`, `l`.`LO_ORDERPRIORITY`, `l`.`LO_SHIPPRIORITY`, `l`.`LO_QUANTITY`," +
                " `l`.`LO_EXTENDEDPRICE`, `l`.`LO_ORDTOTALPRICE`, `l`.`LO_DISCOUNT`, `l`.`LO_REVENUE`," +
                " `l`.`LO_SUPPLYCOST`, `l`.`LO_TAX`, `l`.`LO_COMMITDATE`, `l`.`LO_SHIPMODE`," +
                " `c`.`C_NAME`, `c`.`C_ADDRESS`, `c`.`C_CITY`, `c`.`C_NATION`, `c`.`C_REGION`, `c`.`C_PHONE`," +
                " `c`.`C_MKTSEGMENT`, `s`.`S_NAME`, `s`.`S_ADDRESS`, `s`.`S_CITY`, `s`.`S_NATION`, `s`.`S_REGION`," +
                " `s`.`S_PHONE`, `p`.`P_NAME`, `p`.`P_MFGR`, `p`.`P_CATEGORY`, `p`.`P_BRAND`, `p`.`P_COLOR`," +
                " `p`.`P_TYPE`, `p`.`P_SIZE`, `p`.`P_CONTAINER`\n" +
                "FROM `lineorder` AS `l` " +
                " LEFT OUTER JOIN `customer` AS `c` ON `c`.`C_CUSTKEY` = `l`.`LO_CUSTKEY`" +
                " LEFT OUTER JOIN `supplier` AS `s` ON `s`.`S_SUPPKEY` = `l`.`LO_SUPPKEY`" +
                " LEFT OUTER JOIN `part` AS `p` ON `p`.`P_PARTKEY` = `l`.`LO_PARTKEY`;";

        String query = "SELECT `lineorder`.`lo_orderkey`, `lineorder`.`lo_orderdate`, `customer`.`c_custkey` AS `cd`\n" +
                "FROM `lineorder` LEFT OUTER JOIN `customer` ON `lineorder`.`lo_custkey` = `customer`.`c_custkey`\n" +
                "WHERE `lineorder`.`lo_orderkey` = 100;";

        testRewriteOK(mv, query);

        String mv2 = "SELECT" +
                " `l`.`LO_ORDERKEY` as col1, `l`.`LO_ORDERDATE`, `l`.`LO_LINENUMBER`, `l`.`LO_CUSTKEY`, `l`.`LO_PARTKEY`," +
                " `l`.`LO_SUPPKEY`, `l`.`LO_ORDERPRIORITY`, `l`.`LO_SHIPPRIORITY`, `l`.`LO_QUANTITY`," +
                " `l`.`LO_EXTENDEDPRICE`, `l`.`LO_ORDTOTALPRICE`, `l`.`LO_DISCOUNT`, `l`.`LO_REVENUE`," +
                " `l`.`LO_SUPPLYCOST`, `l`.`LO_TAX`, `l`.`LO_COMMITDATE`, `l`.`LO_SHIPMODE`," +
                " `c`.`C_NAME`, `c`.`C_ADDRESS`, `c`.`C_CITY`, `c`.`C_NATION`, `c`.`C_REGION`, `c`.`C_PHONE`," +
                " `c`.`C_MKTSEGMENT`, `s`.`S_NAME`, `s`.`S_ADDRESS`, `s`.`S_CITY`, `s`.`S_NATION`, `s`.`S_REGION`," +
                " `s`.`S_PHONE`, `p`.`P_NAME`, `p`.`P_MFGR`, `p`.`P_CATEGORY`, `p`.`P_BRAND`, `p`.`P_COLOR`," +
                " `p`.`P_TYPE`, `p`.`P_SIZE`, `p`.`P_CONTAINER`\n" +
                "FROM `lineorder_null` AS `l` " +
                " LEFT OUTER JOIN `customer` AS `c` ON `c`.`C_CUSTKEY` = `l`.`LO_CUSTKEY`" +
                " LEFT OUTER JOIN `supplier` AS `s` ON `s`.`S_SUPPKEY` = `l`.`LO_SUPPKEY`" +
                " LEFT OUTER JOIN `part` AS `p` ON `p`.`P_PARTKEY` = `l`.`LO_PARTKEY`;";

        String query2 = "SELECT `lineorder_null`.`lo_orderkey`, `lineorder_null`.`lo_orderdate`, `customer`.`c_custkey` AS `cd`\n" +
                "FROM `lineorder_null` LEFT OUTER JOIN `customer` ON `lineorder_null`.`lo_custkey` = `customer`.`c_custkey`\n" +
                "WHERE `lineorder_null`.`lo_orderkey` = 100;";

        testRewriteOK(mv2, query2);
    }

    @Test
    public void testAggJoinViewDelta() {
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(300000000);
        String mv = "SELECT" +
                " `LO_ORDERKEY` as col1, C_CUSTKEY, S_SUPPKEY, P_PARTKEY," +
                " sum(LO_QUANTITY) as total_quantity, sum(LO_ORDTOTALPRICE) as total_price, count(*) as num" +
                " FROM `lineorder` AS `l` " +
                " LEFT OUTER JOIN `customer` AS `c` ON `c`.`C_CUSTKEY` = `l`.`LO_CUSTKEY`" +
                " LEFT OUTER JOIN `supplier` AS `s` ON `s`.`S_SUPPKEY` = `l`.`LO_SUPPKEY`" +
                " LEFT OUTER JOIN `part` AS `p` ON `p`.`P_PARTKEY` = `l`.`LO_PARTKEY`" +
                " group by col1, C_CUSTKEY, S_SUPPKEY, P_PARTKEY";

        String query = "SELECT `lineorder`.`lo_orderkey`, sum(LO_QUANTITY), sum(LO_ORDTOTALPRICE), count(*)\n" +
                "FROM `lineorder` LEFT OUTER JOIN `customer` ON `lineorder`.`lo_custkey` = `customer`.`c_custkey`\n" +
                " group by lo_orderkey";

        testRewriteOK(mv, query);
    }

    // mv: t1, t2, t2
    // query: t1, t2
    @Test
    public void testViewDeltaJoinUKFK1() {
        String mv = "select c1 as col1, c2, c3, l.c6, r.c7" +
                " from t1 join t2 l on t1.c2 = l.c5" +
                " join t2 r on t1.c3 = r.c5";
        String query = "select c1, c2, c3, c6 from t1 join t2 on t1.c2 = t2.c5";
        testRewriteOK(mv, query);
    }

    // mv: t1, t3, t2, t2
    // query: t1, t3
    @Test
    public void testViewDeltaJoinUKFK2() {
        String mv = "select c1 as col1, c2, c3, l.c6, r.c7" +
                " from t1 join t2 l on t1.c2 = l.c5" +
                " join t2 r on t1.c3 = r.c5" +
                " join t3 on t1.c2 = t3.c5";
        String query = "select c1, c2, c3, t3.c5 from t1 join t3 on t1.c2 = t3.c5";
        testRewriteOK(mv, query);
    }

    @Test
    public void testViewDeltaJoinUKFK3() {
        String mv = "select a.empid, a.deptno from\n"
                + "(select * from emps where empid = 1) a\n"
                + "join depts using (deptno)\n"
                + "join dependents using (empid)";
        String query = "select a.empid from \n"
                + "(select * from emps where empid = 1) a\n"
                + "join dependents using (empid)";
        testRewriteOK(mv, query);
    }

    @Test
    public void testViewDeltaJoinUKFK4() {
        String mv = "select a.empid, a.deptno from\n"
                + "(select * from emps where empid = 1) a\n"
                + "join depts using (deptno)\n"
                + "join dependents using (empid)";
        String query = "select a.name from \n"
                + "(select * from emps where empid = 1) a\n"
                + "join dependents using (empid)";
        testRewriteFail(mv, query);
    }

    @Test
    public void testViewDeltaJoinUKFK5() {
        String mv = "select a.empid, a.deptno from\n"
                + "(select * from emps where empid = 1) a\n"
                + "join depts using (deptno)";
        String query = "select empid from emps where empid = 1";
        testRewriteFail(mv, query);
    }

    @Test
    public void testViewDeltaJoinUKFK6() {
        String mv = "select emps.empid, emps.deptno from emps\n"
                + "join depts using (deptno)\n"
                + "join dependents using (empid)"
                + "where emps.empid = 1";
        String query = "select emps.empid from emps\n"
                + "join dependents using (empid)\n"
                + "where emps.empid = 1";
        testRewriteOK(mv, query);
    }

    @Test
    public void testViewDeltaJoinUKFK7() {
        String mv = "select emps.empid, emps.deptno from emps\n"
                + "join depts a on (emps.deptno=a.deptno)\n"
                + "join depts b on (emps.deptno=b.deptno)\n"
                + "join dependents using (empid)"
                + "where emps.empid = 1";

        String query = "select emps.empid from emps\n"
                + "join dependents using (empid)\n"
                + "where emps.empid = 1";
        testRewriteOK(mv, query);
    }

    // join key is not foreign key
    @Test
    public void testViewDeltaJoinUKFK8() {
        String mv = "select emps.empid, emps.deptno from emps\n"
                + "join depts a on (emps.name=a.name)\n"
                + "join depts b on (emps.name=b.name)\n"
                + "join dependents using (empid)"
                + "where emps.empid = 1";

        String query = "select emps.empid from emps\n"
                + "join dependents using (empid)\n"
                + "where emps.empid = 1";
        testRewriteFail(mv, query);
    }

    // join key is not foreign key
    @Test
    public void testViewDeltaJoinUKFK9() {
        String mv = "select emps.empid, emps.deptno from emps\n"
                + "join depts a on (emps.deptno=a.deptno)\n"
                + "join depts b on (emps.name=b.name)\n"
                + "join dependents using (empid)"
                + "where emps.empid = 1";

        String query = "select emps.empid from emps\n"
                + "join dependents using (empid)\n"
                + "where emps.empid = 1";
        testRewriteFail(mv, query);
    }

    @Test
    public void testViewDeltaJoinUKFK10() {
        String mv = "select emps.empid as empid1, emps.name, emps.deptno, dependents.empid as empid2 from emps\n"
                + "join dependents using (empid)";

        String query = "select emps.empid, dependents.empid, emps.deptno\n"
                + "from emps\n"
                + "join dependents using (empid)"
                + "join depts a on (emps.deptno=a.deptno)\n"
                + "where emps.name = 'Bill'";
        testRewriteOK(mv, query);
    }

    @Test
    public void testViewDeltaJoinUKFK11() {
        String mv = "select emps.empid, emps.deptno, dependents.name from emps\n"
                + "left outer join depts a on (emps.deptno=a.deptno)\n"
                + "inner join depts b on (emps.deptno=b.deptno)\n"
                + "inner join dependents using (empid)"
                + "where emps.empid = 1";

        String query = "select emps.empid, dependents.name from emps\n"
                + "inner join dependents using (empid)\n"
                + "where emps.empid = 1";
        testRewriteOK(mv, query);
    }

    @Test
    public void testViewDeltaJoinUKFK12() {
        String mv = "select emps.empid, emps.deptno, dependents.name from emps\n"
                + "inner join dependents using (empid)"
                + "left outer join depts a on (emps.deptno=a.deptno)\n"
                + "inner join depts b on (emps.deptno=b.deptno)\n"
                + "where emps.empid = 1";

        String query = "select emps.empid, dependents.name from emps\n"
                + "inner join dependents using (empid)\n"
                + "where emps.empid = 1";
        testRewriteOK(mv, query);
    }

    @Test
    public void testViewDeltaJoinUKFK13() {
        String mv = "select emps.empid, emps.deptno, dependents.name from emps\n"
                + "inner join dependents using (empid)"
                + "inner join depts b on (emps.deptno=b.deptno)\n"
                + "left outer join depts a on (emps.deptno=a.deptno)\n"
                + "where emps.empid = 1";

        String query = "select emps.empid, dependents.name from emps\n"
                + "inner join dependents using (empid)\n"
                + "where emps.empid = 1";
        testRewriteOK(mv, query);
    }
}
