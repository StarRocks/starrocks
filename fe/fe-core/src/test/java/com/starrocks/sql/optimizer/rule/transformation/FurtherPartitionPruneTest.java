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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FurtherPartitionPruneTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("CREATE TABLE `ptest` (\n"
                + "  `k1` int(11) NOT NULL COMMENT \"\",\n"
                + "  `d2` date NOT NULL COMMENT \"\",\n"
                + "  `v1` int(11) NULL COMMENT \"\",\n"
                + "  `v2` int(11) NULL COMMENT \"\",\n"
                + "  `v3` int(11) NULL COMMENT \"\",\n"
                + "  `s1` varchar(255) NULL COMMENT \"\"\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`k1`, `d2`)\n"
                + "COMMENT \"OLAP\"\n"
                + "PARTITION BY RANGE(`d2`)\n"
                + "(PARTITION p202001 VALUES [('1000-01-01'), ('2020-01-01')),\n"
                + "PARTITION p202004 VALUES [('2020-01-01'), ('2020-04-01')),\n"
                + "PARTITION p202007 VALUES [('2020-04-01'), ('2020-07-01')),\n"
                + "PARTITION p202012 VALUES [('2020-07-01'), ('2020-12-01')))\n"
                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\",\n"
                + "\"in_memory\" = \"false\"\n"
                + ");");
        starRocksAssert.withTable("CREATE TABLE `tbl_int` (\n" +
                "  `k1` int(11) NULL,\n" +
                "  `b1` boolean NULL ,\n" +
                "  `s1` varchar(5) NULL ,\n" +
                "  `s2` varchar(5) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "PARTITION BY RANGE(`k1`)\n" +
                "(PARTITION p1 VALUES [('0'), ('100')),\n" +
                "PARTITION p2 VALUES [('100'), ('200')),\n" +
                "PARTITION p3 VALUES [('200'), ('300')),\n" +
                "PARTITION p4 VALUES [('300'), ('400')))\n" +
                "DISTRIBUTED BY HASH(`s1`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `two_key` (\n" +
                "  `k1` int(11) NULL,\n" +
                "  `d1` date NULL COMMENT \"\",\n" +
                "  `s1` varchar(5) NULL ,\n" +
                "  `s2` varchar(5) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "PARTITION BY RANGE(`k1`,`d1`)\n" +
                "(PARTITION p1 VALUES [('0', '2020-01-01'), ('100', '2020-03-01')),\n" +
                "PARTITION p2 VALUES [('100', '2020-03-01'), ('200', '2020-06-01')),\n" +
                "PARTITION p3 VALUES [('200', '2020-06-01'), ('300', '2020-09-01')),\n" +
                "PARTITION p4 VALUES [('300', '2020-09-01'), ('400', '2021-01-01')))\n" +
                "DISTRIBUTED BY HASH(`s1`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
        starRocksAssert.withTable("CREATE TABLE `less_than_tbl` " +
                "( `k1` date, `k2` datetime, `k3` char(20), `k4` varchar(20), `k5` boolean) " +
                "ENGINE=OLAP DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) " +
                "PARTITION BY RANGE(`k1`) " +
                "( PARTITION `p202006` VALUES LESS THAN (\"2020-07-01\"), " +
                "PARTITION `p202007` VALUES LESS THAN (\"2020-08-01\"), " +
                "PARTITION `p202008` VALUES LESS THAN (\"2020-09-01\")," +
                "PARTITION `p202009` VALUES LESS THAN (\"2020-10-01\") ) " +
                "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 3 " +
                "PROPERTIES ( \"replication_num\" = \"1\");");

        starRocksAssert.withTable("CREATE TABLE `expr_range` " +
                "( `k1` date, `k2` datetime, `k3` char(20), `k4` varchar(20), `k5` boolean) " +
                "ENGINE=OLAP DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) " +
                "PARTITION BY date_trunc('month', k1) " +
                "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 3 " +
                "PROPERTIES ( \"replication_num\" = \"1\");");
        starRocksAssert.withTable("CREATE TABLE `maxvalue_range` (\n" +
                "  `v1` bigint(20) NULL COMMENT \"\",\n" +
                "  `v2` bigint(20) NULL COMMENT \"\",\n" +
                "  `v3` bigint(20) NULL COMMENT \"\",\n" +
                "  `v4` varchar(20) NULL COMMENT \"\",\n" +
                "  `pay_dt` date NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, `v3`)\n" +
                "PARTITION BY RANGE (v1)\n" +
                "(\n" +
                "    PARTITION p202101 VALUES [('1'), ('100')),\n" +
                "    PARTITION p202102 VALUES [('500'), ('800')),\n" +
                "    PARTITION p202103 VALUES [('1000'), (MAXVALUE))\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");");
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("emptyPartitionSqlList")
    @Order(1)
    void testEmptyPartitionSql(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertTrue(plan, plan.contains("partitions=0/4"));
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("onePartitionSqlList")
    @Order(2)
    void testOnePartitionSql(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertTrue(plan, plan.contains("partitions=1/4"));
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("twoPartitionsSqlList")
    @Order(3)
    void testTwoPartitionsSql(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertTrue(plan, plan.contains("partitions=2/4"));
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("threePartitionsSqlList")
    @Order(3)
    void testThreePartitionsSql(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertTrue(plan, plan.contains("partitions=3/4"));
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("fourPartitionsSqlList")
    @Order(4)
    void testFourPartitionsSql(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertTrue(plan, plan.contains("partitions=4/4"));
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("cannotPrunePredicateSqls")
    @Order(5)
    void testCannotPrunePredicateSqls(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertTrue(plan, plan.contains("PREDICATES: "));
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("canPrunePredicateSqls")
    @Order(6)
    void testCanPrunePredicateSqls(String sql) throws Exception {
        String plan = getFragmentPlan(sql);
        assertFalse(plan, plan.contains("PREDICATES: "));
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("exprPrunePartitionSqls")
    @Order(7)
    void testExprPrunePartition(String sql) throws Exception {
        String plan = getFragmentPlan(sql);

        Pattern pattern = Pattern.compile("partitions=.*");
        Matcher matcher = pattern.matcher(plan);

        while (matcher.find()) {
            String matchedLine = matcher.group();
            System.out.println(matchedLine);
            String[] values = matchedLine.split("=")[1].split("/");
            Assert.assertTrue(matchedLine, Integer.valueOf(values[0]) < Integer.valueOf(values[1]));
        }
    }

    @ParameterizedTest(name = "sql_{index}: {0}.")
    @MethodSource("exprPrunePartitionSqls")
    @Order(8)
    void testDisableExprPrunePartition(String sql) throws Exception {
        connectContext.getSessionVariable().setEnableExprPrunePartition(false);
        String plan = getFragmentPlan(sql);
        Pattern pattern = Pattern.compile("partitions=.*");
        Matcher matcher = pattern.matcher(plan);

        while (matcher.find()) {
            String matchedLine = matcher.group();
            System.out.println(matchedLine);
            String[] values = matchedLine.split("=")[1].split("/");
            Assert.assertTrue(matchedLine, Integer.valueOf(values[0]) == Integer.valueOf(values[1]));
        }
    }

    private static Stream<Arguments> emptyPartitionSqlList() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select * from tbl_int where k1 is null");
        sqlList.add("select * from tbl_int where k1 is null or k1 <=> null");
        sqlList.add("select * from tbl_int where (k1 = 1 or k1 > s1) and k1 is null");
        sqlList.add("select * from ptest where d2 in (null, '2021-01-01')");
        sqlList.add("select * from ptest where (d2 < '1000-01-01') or (d2 in (null, '2021-01-01'))");
        sqlList.add("select * from ptest where d2 not in ('2021-12-01', null)");
        sqlList.add("select * from ptest where (d2 < '1000-01-01' and s1 >= '2020-12-01') or (d2 >= '2020-12-01')");
        sqlList.add(
                "select * from ptest where (d2 not between '1000-01-01' and '2020-12-01' and s1 >= '2020-12-01') " +
                        "or (d2 >= '2020-12-01')");
        sqlList.add("select * from ptest where d2 = '1000-01-01' and d2 not between '1000-01-01' and '2020-12-01'");
        sqlList.add("select * from ptest where s1 like '%1' and d2 in (null, null)");
        sqlList.add("select * from ptest where s1 is null and d2 not in (null, '2022-01-01')");
        sqlList.add("select * from ptest where s1 not like '%s1' and d2 in (null, null)");
        sqlList.add("select * from ptest where d2 not in (null, null) and s1 like '%1'");
        sqlList.add(
                "select * from ptest where (d2 < '1000-01-01' or s1 >= '2020-12-01') and d2 not in ('2020-01-01', null)");
        sqlList.add(
                "select * from ptest where (d2 < '1900-01-01' or d2 in ('2020-01-01') or d2 is null) and d2 > '2020-07-01'");
        sqlList.add("select * from tbl_int where not (k1 in (null, 1) or k1 not in (1,2,3))");
        sqlList.add("select * from tbl_int where not (k1 >= 0 and k1 < 400)");
        sqlList.add("select * from tbl_int where not (k1 >= 0 and k1 < 400 or s1 > s2)");

        sqlList.add(
                "select * from ptest where not not (d2 not between '1000-01-01' and '2020-12-01' and s1 >= '2020-12-01') " +
                        "or (d2 >= '2020-12-01')");

        return sqlList.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> onePartitionSqlList() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select * from tbl_int where k1 = 1 = true");
        sqlList.add("select * from less_than_tbl where k1 is null");
        sqlList.add("select * from less_than_tbl where k1 is null or k1 <=> null");
        sqlList.add("select * from less_than_tbl where k1 = 1 and k1 is null");
        sqlList.add("select * from less_than_tbl where k1 = '2020-06-30' and k1 is null");
        sqlList.add("select * from less_than_tbl where k1 > '2020-06-30' and k1 is null");
        sqlList.add("select * from less_than_tbl where k1 != '2020-06-30' and k1 is null");

        sqlList.add(
                "select * from ptest where (d2 between '1999-01-01' and '1999-12-01' or d2 between '2020-07-01' " +
                        "and '2020-08-01') and d2 < '2020-04-01'");
        sqlList.add(
                "select * from ptest where (d2 between '1999-01-01' and '1999-12-01' or d2 between '2020-07-01' " +
                        "and '2020-08-01') and s1 not like '' and d2 < '2020-04-01' and s1 like 'a'");

        sqlList.add("select * from tbl_int where (k1 in (200,201,202,203,204) or k1 > 400) and k1 <= 300");
        sqlList.add("select * from tbl_int where not not (k1 in (200,201,202,203,204) or k1 > 400) and k1 <= 300");
        sqlList.add(
                "select * from tbl_int where (k1 in (200,201,202,203,204) or k1 > 400 and s1 is null) and k1 <= 300 " +
                        "and s1 in ('a', 'b') and s1 != s2");
        sqlList.add("select * from tbl_int where (k1 in (200,201,202,203,204,200,201,202,203,204,200,201,202,203,204" +
                ",200,201,202,203,204,200,201,202,203,204,200,201,202,203,204,200,201,202,203,204,200,201,202,203,204" +
                ",200,201,202,203,204,200,201,202,203,204,200,201,202,203,204,200,201,202,203,204) or k1 > 400) and k1 <= 300");
        sqlList.add("select * from tbl_int where (k1 = 1 or k1 between 300 and 400) and k1 >=100");
        sqlList.add("select * from tbl_int where not not (k1 = 1 or k1 between 300 and 400) and k1 >=100");

        sqlList.add("select * from tbl_int where ((k1 = 1 or k1 between 300 and 400 and abs(k1) = 1) and k1 >=100)");
        sqlList.add(
                "select * from tbl_int where ((k1 = 1 or k1 between 300 and 400 and abs(k1) = 1) and not not k1 >=100)");
        sqlList.add(
                "select * from tbl_int where ((k1 = 1 or k1 between 300 and 400 and abs(k1) = 1) and k1 >=100) and s1 > s2");
        sqlList.add(
                "select * from tbl_int where not not (((k1 = 1 or k1 between 300 and 400 and abs(k1) = 1) " +
                        "and k1 >=100) and s1 > s2)");

        sqlList.add("select * from tbl_int where (k1 = 1 or k1 not between 50 and 400) and k1 <300");
        sqlList.add("select * from tbl_int where not not (k1 = 1 or k1 not between 50 and 400) and k1 <300");
        sqlList.add(
                "select * from tbl_int where ((k1 = 1 or k1 not between 50 and 400) and k1 <300) or k1 > floor(1000.5)");
        sqlList.add(
                "select * from tbl_int where ((k1 = 1 or k1 not between 50 and 400) and k1 <300) or k1 > floor(1000.5) " +
                        "and s1 > s1 + s2");

        sqlList.add("select * from two_key where k1 = 5");
        sqlList.add("select * from two_key where k1 = 5 and d1 = '2020-09-01'");

        return sqlList.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> twoPartitionsSqlList() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select * from less_than_tbl where k1 = '2020-07-30' or k1 is null");
        sqlList.add("select * from less_than_tbl where k1 < '2020-07-30' or k1 is null");
        sqlList.add("select * from less_than_tbl where k1 < '2020-07-30' or k1 <=> null");

        sqlList.add("select * from tbl_int where k1 between 0 and 99 or k1 between 300 and 399");
        sqlList.add("select * from tbl_int where (k1 between 0 and 99 or k1 between 300 and 399) and s1 > upper(s2)");
        sqlList.add("select * from tbl_int where k1 <= 100 or k1 > 400");
        sqlList.add("select * from tbl_int where k1 < 100 or k1 >= 300");
        sqlList.add("select * from tbl_int where k1 = 0 or k1 >= 300");
        sqlList.add("select * from tbl_int where k1 != 350 and (k1 > 300 or k1 < 100)");
        sqlList.add("select * from tbl_int where k1 = 300 or k1 < 100");
        sqlList.add("select * from tbl_int where (k1 = 300 or k1 < 100) and (s1 = s2 or s1 > 'a' and k1 < s1)");
        sqlList.add("select * from tbl_int where k1 in (1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10" +
                ",1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10,350);");
        sqlList.add("select * from tbl_int where (k1 in (1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10," +
                "1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10,150) or k1 > 400) and k1 <= 300");

        sqlList.add("select * from tbl_int where (k1 >= 0 and k1 < 100) or (k1 >= 300 and k1 < 400)");
        sqlList.add(
                "select * from tbl_int where (k1 >= 0 and k1 < 100) or (k1 >= 300 and k1 < 400) and (k1 not in (1,2,3) " +
                        "or k1 not in (300,301,302))");

        sqlList.add("select * from tbl_int where k1 = 1 or k1 between 200 and 299");
        sqlList.add("select * from tbl_int where k1 in (1,2,3,4,5) or (k1 >= 300 and k1 < 400)");
        sqlList.add(
                "select * from tbl_int where k1 in (1,2,3,4,5) or (k1 >= 300 and k1 < 400) and k1 not in (1,2,3,300,301,302)");
        sqlList.add(
                "select * from tbl_int where k1 in (1,2,3,4,5) or (k1 >= 300 and k1 < 400) and k1 not in (1,2,3,300,301,302) " +
                        "and abs(k1) = 1");
        sqlList.add(
                "select * from tbl_int where k1 in (1,2,3,4,5) or (k1 >= 300 and k1 < 400) and k1 not in (1,2,3,300,301,302) " +
                        "and (k1 = 1 or s1 < s2)");

        sqlList.add("select * from tbl_int where (k1 > 100 and k1 in (1,2,3,4,5,301)) or k1 in (50,99,300)");
        sqlList.add(
                "select * from tbl_int where (k1 > 100 and k1 in (1,2,3,4,5,301,null,null)) or k1 in (50,99,300,null,null)");
        sqlList.add(
                "select * from tbl_int where (k1 > 100 and k1 in (1,2,3,4,5,301,null,null)) or k1 in (50,99,300,null,null)" +
                        " and (k1 in (1,2,3) or k1 != 1)");

        sqlList.add("select * from tbl_int where k1 not in (null,1,2,3,100) or (k1 < 100 or k1 > 300)");
        sqlList.add("select * from tbl_int where s1 like 'a' and (k1 < 100 or k1 > 300)");
        sqlList.add("select * from tbl_int where s1 not like 'a' and (k1 < 100 or k1 > 300)");

        sqlList.add("select * from tbl_int where not (k1 between 100 and 300) and k1 in (1,100,300,305)");

        sqlList.add("select * from tbl_int where not (k1 >= 100 and k1 < 300)");
        sqlList.add("select * from tbl_int where not not not (k1 >= 100 and k1 < 300)");

        sqlList.add("select * from two_key where k1 = 100");
        return sqlList.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> threePartitionsSqlList() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select * from less_than_tbl where k1 < '2020-07-30' or k1 > '2020-09-30' or k1 is null");
        sqlList.add("select * from less_than_tbl where k1 < '2020-07-30' or k1 > '2020-09-30' or k1 <=> null");
        sqlList.add("select s1 from tbl_int where k1 <= 100 or k1 >= 300");
        sqlList.add("select s1 from tbl_int where k1 > 300 or k1 < 200");
        sqlList.add("select s1 from tbl_int where k1 != 0 and (k1 < 100 or (k1 > 150 and k1 <= 200))");
        sqlList.add("select s1 from tbl_int where k1 != 0 and (k1 in (1,5,100,400) or k1 > 300)");
        sqlList.add("select s1 from tbl_int where k1 not in (1,400) and (k1 = 1 or k1 = 200 or k1 > 300)");

        sqlList.add("select s1 from tbl_int where k1 in (1, '1', 2) or (k1 < 100 or k1 > 200)");
        sqlList.add("select s1 from tbl_int where not not (k1 > 200 or k1 < 100)");
        sqlList.add("select * from tbl_int where (k1 in (1, 100) or k1 > 0 and k1 <= 300) and (k1 < 100 or k1 > 200)");
        sqlList.add(
                "select * from tbl_int where not (k1 in (1, 100) or k1 > 0 and k1 <= 300) or (k1 < 100 or k1 > 200)");
        sqlList.add("select * from tbl_int where k1 in (1, 300) or (k1 > 200 and k1 <=350) or k1 = 50");
        sqlList.add(
                "select * from tbl_int where (k1 in (1, 300) or (k1 > 200 and k1 <=350) or k1 = 50) and (k1 > 0 and k1 < 400)");

        sqlList.add(
                "select * from tbl_int where (k1 = 1 and s1 = 2 and s2 = 300) or (k1 > 100 and k1 < 200 and s1 = 1) " +
                        "or (k1 in (300, 350))");
        sqlList.add("select * from tbl_int where (k1 < 400 or k1 > 300) and k1 > 1 and not (k1 >= 200 and k1 < 300)");
        sqlList.add(
                "select * from tbl_int where k1 not in (1,2,3) and s1 not in ('a') and s1 = s2 and (k1 < 200 or k1 > 300)");

        sqlList.add("select * from tbl_int where (k1 in (1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10" +
                ",1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10,1,2,3,4,5,6,7,8,9,10,200) or k1 > 400) and k1 <= 300;");
        sqlList.add("select * from tbl_int where k1 in (1,201,202,203,204,200,201,202,203,204,200,201,202,203,204," +
                "200,201,202,203,204,200,201,202,203,204,200,201,202,203,204,200,201,202,203,204," +
                "200,201,202,203,204,200,201,202,203,204,200,201,202,203,204,200,201,202,203,204,200,201,202,203,350)");

        sqlList.add("select * from two_key where k1 < 100 or (k1 > 200 and k1 < 300)");
        return sqlList.stream().map(e -> Arguments.of(e));
    }

    public static Stream<Arguments> fourPartitionsSqlList() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select * from less_than_tbl where k1 is not null");
        sqlList.add("select * from tbl_int where k1 is not null");
        sqlList.add("select * from tbl_int where k1 < 200 or floor(k1) < 10");
        sqlList.add("select * from tbl_int where k1 < 200 or k1 < k1 + 1");
        sqlList.add("select * from tbl_int where k1 != 5 and k1 != 6");
        sqlList.add("select * from tbl_int where k1 not in (5,15,115,225)");
        sqlList.add("select * from tbl_int where not (k1 > 5 and k1 < 15)");
        sqlList.add("select * from tbl_int where not (k1 between 5 and 15)");
        sqlList.add("select * from tbl_int where not (k1 between 5 and 15 or k1 between 25 and 35)");

        sqlList.add("select * from tbl_int where s1 like 'a' or k1 = 1");
        sqlList.add("select * from tbl_int where (k1 = 1 or k1 > s1) and s1 like 'a'");
        sqlList.add("select * from tbl_int where (k1 = 1 or k1 > s1) or k1 = 2");
        sqlList.add("select * from tbl_int where k1 is null or (k1 != 1)");

        sqlList.add(
                "select * from tbl_int where (k1 < s1 and k1 not in (1,2,3) or k1 =300) and (k1 in (1, 300) " +
                        "or k1 > 0 or k1 <= 350 or k1 = 200)");
        sqlList.add(
                "select * from tbl_int where (s1 like 'a' and k1 > floor(k1) or s2 like 'b') or k1 in (1,100,400) and k1 > 0");
        sqlList.add(
                "select * from tbl_int where not (s1 like 'a' and k1 > floor(k1) or s2 like 'b') " +
                        "or k1 in (1,100,400) and k1 > 0");

        sqlList.add("select * from tbl_int where k1 in (1,'a',200,300)");
        sqlList.add("select * from tbl_int where (k1 in (1,'a') and k1 != s1) or k1 > 150");
        sqlList.add("select * from tbl_int where k1 in (1,100,200,300) or k1 > 300");

        sqlList.add("select * from two_key where k1 < 100 or k1 > 200");
        sqlList.add("select * from tbl_int where b1 or (k1 < 100) or k1 > 200");
        sqlList.add("select * from tbl_int where b1");
        return sqlList.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> cannotPrunePredicateSqls() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select * from tbl_int where (k1 >= 0 and k1 < 200) or s1 = 'a'");
        sqlList.add("select * from tbl_int where (k1 >= 0 and k1 < 200) or k1 = 300");
        sqlList.add("select * from tbl_int where (abs(abs(k1)) >= 0 and abs(abs(k1)) < 200)");
        sqlList.add("select * from tbl_int where k1 >= abs(0) and k1 < abs(200)");
        sqlList.add("select * from tbl_int where k1 in (0, 1, 2, 3)");
        sqlList.add("select * from less_than_tbl where k1 < '2020-08-01'");

        sqlList.add("select * from less_than_tbl where k1 < '2020-08-01' and k1 is not null");
        sqlList.add("select * from less_than_tbl where k1 < '2020-08-01' and k1 is null");
        sqlList.add("select * from ptest where date_trunc('year', d2) = '2020-01-01'");
        sqlList.add("select * from less_than_tbl where date_trunc('year', k1) < '2020-08-01'");
        sqlList.add("select * from less_than_tbl where datediff('2020-08-01', k1) = 1");
        sqlList.add("select * from less_than_tbl where date_format(k1, '%m月%Y年') = '06月-2020年'");
        sqlList.add("select * from less_than_tbl where date_format(k1, '%a-%Y') = 'Wed-2020'");
        sqlList.add("select * from less_than_tbl where date_format(k1, '') is null");
        sqlList.add("select * from less_than_tbl where date_format(k1, '%abcdDdehIjklMprTUvWXxy') = 'Wed-2020'");
        return sqlList.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> canPrunePredicateSqls() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select * from tbl_int where k1 >= 0 and k1 < 200 ");
        sqlList.add("select * from tbl_int where k1 < cast('200' as int)");
        sqlList.add("select * from ptest where d2 < '2020-04-01'");
        sqlList.add("select * from ptest where d2 >= '1000-01-01' and d2 < '2020-04-01'");
        sqlList.add("select * from ptest where cast(d2 as date) >= cast('1000-01-01' as date) " +
                "and cast(d2 as date) < '2020-04-01'");
        sqlList.add("select * from ptest where d2 < cast('20200101' as date)");
        sqlList.add("select * from ptest where d2 < str_to_date('20200401', '%Y%m%d')");
        return sqlList.stream().map(e -> Arguments.of(e));
    }

    private static Stream<Arguments> exprPrunePartitionSqls() {
        List<String> sqlList = Lists.newArrayList();
        sqlList.add("select * from less_than_tbl where date_trunc('year', k1) is null");
        sqlList.add(
                "select * from less_than_tbl where date_trunc('year', k1) is null or k1 in " +
                        "('2020-09-01', '2020-09-02', '2020-09-03')");
        sqlList.add("select * from less_than_tbl where date_trunc('year', k1) > '2050-08-01'");

        sqlList.add("select * from less_than_tbl where date_trunc('month', date_trunc('day', k1)) < '2020-08-01'");
        sqlList.add("select * from less_than_tbl where date_trunc('month', date_trunc('day', k1)) < '2020-08-01' " +
                "or date_trunc('month', date_trunc('day', k1)) > '2020-09-01'");

        sqlList.add(
                "select * from less_than_tbl where date_trunc('month', date_trunc('day', k1)) < '2020-08-01 10:00:00'");
        sqlList.add("select * from maxvalue_range where days_add('2020-01-01' ,v1) > '2050-01-01'");
        sqlList.add("select * from maxvalue_range where hours_add(days_add('2020-01-01' ,v1), 100) > '2050-01-01'");
        sqlList.add("select * from maxvalue_range where hours_add(days_add('2020-01-01' ,v1), 100) > '2050-01-01'");

        sqlList.add("select * from less_than_tbl where k1 != '2020-09-01' and " +
                "(date_trunc('year', k1) > '2030-01-01' or (k1 > '2020-09-01' and k1 <= '2020-10-01'))");

        sqlList.add("select * from less_than_tbl where previous_day(k1, 'Monday') in ('2020-07-01', '2020-08-01', " +
                "'2020-08-06') and k3 = 'a'");
        sqlList.add("select * from less_than_tbl where datediff('2020-08-01', k1) < 0");
        sqlList.add("select * from less_than_tbl where date_format(k1, '%Y年%m月') = '2020年06月'");
        sqlList.add("select * from less_than_tbl where date_format(k1, '%Y年%m月') < '2020年08月'");
        sqlList.add("select * from less_than_tbl where date_format(k1, '%Y-%m-%d %H-%i-%S') < '2020-08-02 12:00:00'");
        return sqlList.stream().map(e -> Arguments.of(e));
    }
}
