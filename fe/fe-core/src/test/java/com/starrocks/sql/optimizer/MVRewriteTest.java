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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/sql/optimizer/MVRewriteTest.java

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

package com.starrocks.sql.optimizer;

import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.optimizer.statistics.EmptyStatisticStorage;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MVRewriteTest {
    private static final String EMPS_TABLE_NAME = "emps";
    private static final String EMPS_MV_NAME = "emps_mv";
    private static final String HR_DB_NAME = "db1";
    private static final String QUERY_USE_EMPS_MV = "rollup: " + EMPS_MV_NAME;
    private static final String QUERY_USE_EMPS = "rollup: " + EMPS_TABLE_NAME;
    private static final String DEPTS_TABLE_NAME = "depts";
    private static final String DEPTS_MV_NAME = "depts_mv";
    private static final String QUERY_USE_DEPTS_MV = "rollup: " + DEPTS_MV_NAME;
    private static final String QUERY_USE_DEPTS = "rollup: " + DEPTS_TABLE_NAME;
    private static final String USER_TAG_TABLE_NAME = "user_tags";
    private static final String USER_TAG_MV_NAME = "user_tags_mv";
    private static final String QUERY_USE_USER_TAG_MV = "rollup: " + USER_TAG_MV_NAME;
    private static final String QUERY_USE_USER_TAG = "rollup: " + USER_TAG_TABLE_NAME;
    private static final String TEST_TABLE_NAME = "test_tb";
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.alter_scheduler_interval_millisecond = 1;
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        GlobalStateMgr.getCurrentState().setStatisticStorage(new EmptyStatisticStorage());
        connectContext = UtFrameUtils.createDefaultCtx();

        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);

        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withEnableMV().withDatabase(HR_DB_NAME).useDatabase(HR_DB_NAME);
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
    }

    @Before
    public void beforeMethod() throws Exception {
        String createTableSQL =
                "create table " + HR_DB_NAME + "." + EMPS_TABLE_NAME + " (time date, empid int, name varchar, "
                        + "deptno int, salary int, commission int) partition by range (time) "
                        + "(partition p1 values less than MAXVALUE) "
                        + "distributed by hash(time) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable(createTableSQL);
        createTableSQL = "create table " + HR_DB_NAME + "." + DEPTS_TABLE_NAME
                + " (time date, deptno int, name varchar, cost int) partition by range (time) "
                + "(partition p1 values less than MAXVALUE) "
                + "distributed by hash(time) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable(createTableSQL);
        createTableSQL = "create table " + HR_DB_NAME + "." + USER_TAG_TABLE_NAME
                + " (time date, user_id int, user_name varchar(20), tag_id int) partition by range (time) "
                + " (partition p1 values less than MAXVALUE) "
                + "distributed by hash(time) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable(createTableSQL);
        createTableSQL = " CREATE TABLE `all_type_table` ( `k1`  date, `k2`  datetime, `k3`  char(20), " +
                "`k4`  varchar(20), `k5`  boolean, `k6`  tinyint, `k7`  smallint, `k8`  int, `k9`  bigint, " +
                "`k10` largeint, `k11` float, `k12` double, `k13` decimal(27,9) ) " +
                "ENGINE=OLAP DUPLICATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`) DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) " +
                "BUCKETS 3 PROPERTIES ( 'replication_num' = '1');";
        starRocksAssert.withTable(createTableSQL);
    }

    @After
    public void afterMethod() throws Exception {
        starRocksAssert.dropTable(EMPS_TABLE_NAME);
        starRocksAssert.dropTable(DEPTS_TABLE_NAME);
        starRocksAssert.dropTable(USER_TAG_TABLE_NAME);
        starRocksAssert.dropTable("all_type_table");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        starRocksAssert.dropTable("ods_order");
    }

    @Test
    public void testConstantPredicate() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select time, sum(salary) from "
                + EMPS_TABLE_NAME + " group by time;";
        String query = "select * from " + EMPS_TABLE_NAME + " where true;";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS);

        query = "select * from ( select *,'v1' as vid from " + EMPS_TABLE_NAME + ") T" + " where vid='v1'";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS);
    }

    @Test
    public void testCountMV1() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select time, sum(salary) from "
                + EMPS_TABLE_NAME + " group by time;";
        String query = "select time, count(1) from " + EMPS_TABLE_NAME + " group by time;";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS);

        query = "select count(1) from " + EMPS_TABLE_NAME + " group by time;";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS);

        query = "select count(1) from " + EMPS_TABLE_NAME;
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS);
    }

    @Test
    public void testCountMV2() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select time, sum(salary) from "
                + EMPS_TABLE_NAME + " group by time;";
        String query = "select time, count(*) from " + EMPS_TABLE_NAME + " group by time;";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS);

        query = "select count(*) from " + EMPS_TABLE_NAME + " group by time;";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS);

        query = "select count(*) from " + EMPS_TABLE_NAME;
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS);
    }

    @Test
    public void testProjectionMV1() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        starRocksAssert.withMaterializedView(createMVSQL);

        String query = "select empid, deptno from " + EMPS_TABLE_NAME + ";";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);

        query = "select empid * 2, deptno from " + EMPS_TABLE_NAME + ";";
        starRocksAssert.query(query)
                .explainContains(QUERY_USE_EMPS_MV, "1:Project\n" +
                        "  |  <slot 4> : 4: deptno\n" +
                        "  |  <slot 7> : CAST(2: empid AS BIGINT) * 2");
    }

    @Test
    public void testProjectionMV2() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from "
                + EMPS_TABLE_NAME + " order by deptno;";
        String query1 = "select empid + 1 from " + EMPS_TABLE_NAME + " where deptno = 10;";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query1).explainContains(QUERY_USE_EMPS_MV);
        String query2 = "select name from " + EMPS_TABLE_NAME + " where deptno -10 = 0;";
        starRocksAssert.query(query2).explainWithout(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testProjectionMV3() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, name from "
                + EMPS_TABLE_NAME + " order by deptno;";
        starRocksAssert.withMaterializedView(createMVSQL);

        String query1 = "select empid +1, name from " + EMPS_TABLE_NAME + " where deptno = 10;";
        starRocksAssert.query(query1).explainContains(QUERY_USE_EMPS_MV);

        String query2 = "select name from " + EMPS_TABLE_NAME + " where deptno - 10 = 0;";
        starRocksAssert.query(query2).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testProjectionMV4() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select name, deptno, salary from "
                + EMPS_TABLE_NAME + ";";
        String query1 = "select name from " + EMPS_TABLE_NAME + " where deptno > 30 and salary > 3000;";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query1).explainContains(QUERY_USE_EMPS_MV);
        String query2 = "select empid from " + EMPS_TABLE_NAME + " where deptno > 30 and empid > 10;";
        starRocksAssert.query(query2).explainWithout(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQueryOnAggMV1() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary), "
                + "max(commission) from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select sum(salary), deptno from " + EMPS_TABLE_NAME + " group by deptno;";
        starRocksAssert.withMaterializedView(createMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);

        query = "select sum(salary), max(commission) from " + EMPS_TABLE_NAME;
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);

        query = "select sum(salary), max(commission) from emps, depts";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQueryOnAggMV2() throws Exception {
        String agg = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " group by deptno";
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as " + agg + ";";
        String query = "select * from (select deptno, sum(salary) as sum_salary from " + EMPS_TABLE_NAME + " group "
                + "by" + " deptno) a where (sum_salary * 2) > 3;";
        starRocksAssert.withMaterializedView(createMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQueryOnAggMV3() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary)"
                + " from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select commission, sum(salary) from " + EMPS_TABLE_NAME + " where commission * (deptno + "
                + "commission) = 100 group by commission;";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQueryOnAggMV4() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary)"
                + " from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where salary>1000 group by deptno;";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQuqeryOnAggMV5() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary)"
                + " from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select * from (select deptno, sum(salary) as sum_salary from " + EMPS_TABLE_NAME
                + " group by deptno) a where sum_salary>10;";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    // There will be a compensating Project + Filter added after matching of the Aggregate.
    @Test
    public void testAggQuqeryOnAggMV6() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary)"
                + " from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select * from (select deptno, sum(salary) as sum_salary from " + EMPS_TABLE_NAME
                + " where deptno>=20 group by deptno) a where sum_salary>10;";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    // Aggregation query at coarser level of aggregation than aggregation materialized view.
    @Test
    public void testAggQuqeryOnAggMV7() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " " + "group by deptno, commission;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where deptno>=20 group by deptno;";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQueryOnAggMV8() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select deptno, sum(salary) + 1 from " + EMPS_TABLE_NAME + " group by deptno;";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testJoinOnLeftProjectToJoin() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME
                + " as select deptno, sum(salary), sum(commission) from " + EMPS_TABLE_NAME + " group by deptno;";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno, max(cost) from "
                + DEPTS_TABLE_NAME + " group by deptno;";
        String query = "select * from (select deptno , sum(salary) from " + EMPS_TABLE_NAME + " group by deptno) A "
                + "join (select deptno, max(cost) from " + DEPTS_TABLE_NAME + " group by deptno ) B on A.deptno = B"
                + ".deptno;";
        starRocksAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVSQL).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testJoinOnRightProjectToJoin() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary), sum" +
                "(commission) from " + EMPS_TABLE_NAME + " group by deptno;";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno, max(cost) from "
                + DEPTS_TABLE_NAME + " group by deptno;";
        String query = "select * from (select deptno , sum(salary), sum(commission) from " + EMPS_TABLE_NAME
                + " group by deptno) A join (select deptno from " + DEPTS_TABLE_NAME + " group by deptno ) B on A"
                + ".deptno = B.deptno;";
        starRocksAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVSQL).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testJoinOnProjectsToJoin() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary), sum" +
                "(commission) from " + EMPS_TABLE_NAME + " group by deptno;";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno, max(cost) from "
                + DEPTS_TABLE_NAME + " group by deptno;";
        String query = "select * from (select deptno , sum(salary) from " + EMPS_TABLE_NAME + " group by deptno) A "
                + "join (select deptno from " + DEPTS_TABLE_NAME + " group by deptno ) B on A.deptno = B.deptno;";
        starRocksAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVSQL).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testJoinOnCalcToJoin0() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from " +
                EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from " +
                DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno from " + EMPS_TABLE_NAME + " where deptno > 10 ) A " +
                "join (select deptno from " + DEPTS_TABLE_NAME + " ) B on A.deptno = B.deptno;";
        starRocksAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVSQL).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testJoinOnCalcToJoin1() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from " +
                EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from " +
                DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno from " + EMPS_TABLE_NAME + " ) A join (select " +
                "deptno from " + DEPTS_TABLE_NAME + " where deptno > 10 ) B on A.deptno = B.deptno;";
        starRocksAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVSQL).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testJoinOnCalcToJoin2() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from " +
                EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from " +
                DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno from " + EMPS_TABLE_NAME + " where empid >10 ) A " +
                "join (select deptno from " + DEPTS_TABLE_NAME + " where deptno > 10 ) B on A.deptno = B.deptno;";
        starRocksAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVSQL).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testJoinOnCalcToJoin3() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from " +
                EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from " +
                DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno + 1 deptno from " + EMPS_TABLE_NAME + " where empid >10 )"
                + " A join (select deptno from " + DEPTS_TABLE_NAME
                + " where deptno > 10 ) B on A.deptno = B.deptno;";
        starRocksAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVSQL).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testJoinOnCalcToJoin4() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from " +
                EMPS_TABLE_NAME + ";";
        String createDeptsMVSQL = "create materialized view " + DEPTS_MV_NAME + " as select deptno from " +
                DEPTS_TABLE_NAME + ";";
        String query = "select * from (select empid, deptno + 1 deptno from " + EMPS_TABLE_NAME
                + " where empid is not null ) A full join (select deptno from " + DEPTS_TABLE_NAME
                + " where deptno is not null ) B on A.deptno = B.deptno;";
        starRocksAssert.withMaterializedView(createDeptsMVSQL).withMaterializedView(createEmpsMVSQL).query(query)
                .explainContains(QUERY_USE_EMPS_MV, QUERY_USE_DEPTS_MV);
    }

    @Test
    public void testOrderByQueryOnProjectView() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from " +
                EMPS_TABLE_NAME + ";";
        String query = "select empid from " + EMPS_TABLE_NAME + " order by deptno";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testOrderByQueryOnOrderByView() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from " +
                EMPS_TABLE_NAME + " order by deptno;";
        String query = "select empid from " + EMPS_TABLE_NAME + " order by deptno";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVAggregateFuncs1() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno from " + EMPS_TABLE_NAME + " group by deptno";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVAggregateFuncs2() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " group by deptno";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVAggregateFuncs3() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, empid, sum(salary) from " + EMPS_TABLE_NAME + " group by deptno, empid";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVAggregateFuncs4() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where deptno > 10 group by deptno";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVAggregateFuncs5() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 group by deptno";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVAggregateFuncs6() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, count(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        starRocksAssert.withMaterializedView(createEmpsMVSQL);

        String query = "select deptno, empid, count(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        starRocksAssert.query(query).explainContains(EMPS_MV_NAME);

        // TODO: support this later.
        query = "select deptno, sum(if(empid=0,0,1)) from " + EMPS_TABLE_NAME + " group by deptno";
        starRocksAssert.query(query).explainWithout(EMPS_MV_NAME);
    }
    
    @Test
    public void testAggregateMVCalcGroupByQuery1() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno+1, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by deptno+1;";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVCalcGroupByQuery2() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno * empid, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 " +
                "group by deptno * empid;";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVCalcGroupByQuery3() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select empid, deptno * empid, sum(salary) + 1 from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by empid, deptno * empid;";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggregateMVCalcAggFunctionQuery() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary + 1) from " + EMPS_TABLE_NAME + " where deptno > 10 "
                + "group by deptno;";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainWithout(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testSubQuery() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid "
                + "from " + EMPS_TABLE_NAME + ";";
        String query = "select empid, deptno, salary from " + EMPS_TABLE_NAME + " e1 where empid = (select max(empid)"
                + " from " + EMPS_TABLE_NAME + " where deptno = e1.deptno);";
        String plan = starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainQuery();
        Assert.assertTrue(plan.contains(QUERY_USE_EMPS_MV));
        Assert.assertTrue(plan.contains(QUERY_USE_EMPS));
    }

    @Test
    public void testDistinctQuery() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, sum(salary) " +
                "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query1 = "select distinct deptno from " + EMPS_TABLE_NAME + ";";
        starRocksAssert.withMaterializedView(createEmpsMVSQL);
        starRocksAssert.query(query1).explainContains(QUERY_USE_EMPS_MV);
        String query2 = "select deptno, sum(distinct salary) from " + EMPS_TABLE_NAME + " group by deptno;";
        starRocksAssert.query(query2).explainWithout(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testSingleMVMultiUsage() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid, salary " +
                "from " + EMPS_TABLE_NAME + " order by deptno;";
        String query = "select * from (select deptno, empid from " + EMPS_TABLE_NAME + " where deptno>100) A join " +
                "(select deptno, empid from " + EMPS_TABLE_NAME + " where deptno >200) B using (deptno);";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV, 2);
    }

    @Test
    public void testMultiMVMultiUsage() throws Exception {
        String createEmpsMVSQL01 = "create materialized view emp_mv_01 as select deptno, empid, salary "
                + "from " + EMPS_TABLE_NAME + " order by deptno;";
        String createEmpsMVSQL02 = "create materialized view emp_mv_02 as select deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select * from (select deptno, empid from " + EMPS_TABLE_NAME + " where deptno>100) A join " +
                "(select deptno, sum(salary) from " + EMPS_TABLE_NAME + " where deptno >200 group by deptno) B "
                + "using (deptno);";
        starRocksAssert.withMaterializedView(createEmpsMVSQL01).withMaterializedView(createEmpsMVSQL02).query(query)
                .explainContains("rollup: emp_mv_01", "rollup: emp_mv_02");
    }

    @Test
    public void testMVOnJoinQuery() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select salary, empid, deptno from " +
                EMPS_TABLE_NAME + " order by salary;";
        String query = "select empid, salary from " + EMPS_TABLE_NAME + " join " + DEPTS_TABLE_NAME
                + " using (deptno) where salary > 300;";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV,
                QUERY_USE_DEPTS);
    }

    @Test
    public void testAggregateMVOnCountDistinctQuery1() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, count(distinct empid) from " + EMPS_TABLE_NAME + " group by deptno;";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(EMPS_TABLE_NAME);
    }

    @Test
    public void testQueryAfterTrimingOfUnusedFields() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from " +
                EMPS_TABLE_NAME + " order by empid, deptno;";
        String query = "select empid, deptno from (select empid, deptno, salary from " + EMPS_TABLE_NAME + ") A;";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testUnionAll() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from " +
                EMPS_TABLE_NAME + " order by empid, deptno;";
        String query = "select empid, deptno from " + EMPS_TABLE_NAME + " where empid >1 union all select empid,"
                + " deptno from " + EMPS_TABLE_NAME + " where empid <0;";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV, 2);
    }

    @Test
    public void testUnionDistinct() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from " +
                EMPS_TABLE_NAME + " order by empid, deptno;";
        String query = "select empid, deptno from " + EMPS_TABLE_NAME + " where empid >1 union select empid," +
                " deptno from " + EMPS_TABLE_NAME + " where empid <0;";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV, 2);
    }

    @Test
    public void testDeduplicateQueryInAgg() throws Exception {
        String aggregateTable = "create table agg_table (k1 int, k2 int, v1 bigint sum) aggregate key (k1, k2) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable(aggregateTable);
        String createRollupSQL = "alter table agg_table add rollup old_key (k1, k2) "
                + "properties ('replication_num' = '1');";
        String query = "select k1, k2 from agg_table;";
        starRocksAssert.withRollup(createRollupSQL).query(query)
                .explainContains("OFF", "old_key");
    }

    @Test
    public void testAggFunctionInHaving() throws Exception {
        String duplicateTable = "CREATE TABLE " + TEST_TABLE_NAME + " ( k1 int(11) NOT NULL ,  k2  int(11) NOT NULL ,"
                + "v1  varchar(4096) NOT NULL, v2  float NOT NULL , v3  decimal(20, 7) NOT NULL ) ENGINE=OLAP "
                + "DUPLICATE KEY( k1 ,  k2 ) DISTRIBUTED BY HASH( k1 ,  k2 ) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1'); ";
        starRocksAssert.withTable(duplicateTable);
        String createK1K2MV = "create materialized view k1_k2 as select k1,k2 from " + TEST_TABLE_NAME + " group by "
                + "k1,k2;";
        String query = "select k1 from " + TEST_TABLE_NAME + " group by k1 having max(v1) > 10;";
        starRocksAssert.withMaterializedView(createK1K2MV).query(query).explainWithout("k1_k2");
        starRocksAssert.dropTable(TEST_TABLE_NAME);
    }

    @Test
    public void testAggFunctionInOrder() throws Exception {
        String duplicateTable = "CREATE TABLE " + TEST_TABLE_NAME + " ( k1 int(11) NOT NULL ,  k2  int(11) NOT NULL ,"
                + "v1  varchar(4096) NOT NULL, v2  float NOT NULL , v3  decimal(20, 7) NOT NULL ) ENGINE=OLAP "
                + "DUPLICATE KEY( k1 ,  k2 ) DISTRIBUTED BY HASH( k1 ,  k2 ) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1'); ";
        starRocksAssert.withTable(duplicateTable);
        String createK1K2MV = "create materialized view k1_k2 as select k1,k2 from " + TEST_TABLE_NAME + " group by "
                + "k1,k2;";
        String query = "select k1 from " + TEST_TABLE_NAME + " group by k1 order by max(v1);";
        starRocksAssert.withMaterializedView(createK1K2MV).query(query).explainWithout("k1_k2");
        starRocksAssert.dropTable(TEST_TABLE_NAME);
    }

    @Test
    public void testWindowsFunctionInQuery() throws Exception {
        String duplicateTable = "CREATE TABLE " + TEST_TABLE_NAME + " ( k1 int(11) NOT NULL ,  k2  int(11) NOT NULL ,"
                + "v1  varchar(4096) NOT NULL, v2  float NOT NULL , v3  decimal(20, 7) NOT NULL ) ENGINE=OLAP "
                + "DUPLICATE KEY( k1 ,  k2 ) DISTRIBUTED BY HASH( k1 ,  k2 ) BUCKETS 3 "
                + "PROPERTIES ('replication_num' = '1'); ";
        starRocksAssert.withTable(duplicateTable);
        String createK1K2MV = "create materialized view k1_k2 as select k1,k2 from " + TEST_TABLE_NAME + " group by "
                + "k1,k2;";
        String query = "select k1 , sum(k2) over (partition by v1 ) from " + TEST_TABLE_NAME + ";";
        starRocksAssert.withMaterializedView(createK1K2MV).query(query).explainWithout("k1_k2");
        starRocksAssert.dropTable(TEST_TABLE_NAME);
    }

    @Test
    public void testUniqueTableInQuery() throws Exception {
        String uniqueTable = "CREATE TABLE " + TEST_TABLE_NAME + " (k1 int, v1 int) UNIQUE KEY (k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ('replication_num' = '1');";
        starRocksAssert.withTable(uniqueTable);
        String createK1K2MV = "create materialized view only_k1 as select k1 from " + TEST_TABLE_NAME + " group by "
                + "k1;";
        String query = "select * from " + TEST_TABLE_NAME + ";";
        starRocksAssert.withMaterializedView(createK1K2MV).query(query).explainContains(TEST_TABLE_NAME);
        starRocksAssert.dropTable(TEST_TABLE_NAME);
    }

    @Test
    public void testBitmapUnionInSubquery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_id from " + USER_TAG_TABLE_NAME + " where user_id in (select user_id from " +
                USER_TAG_TABLE_NAME + " group by user_id having bitmap_union_count(to_bitmap(tag_id)) >1 ) ;";
        starRocksAssert.query(query).explainContains(USER_TAG_MV_NAME, USER_TAG_TABLE_NAME);
    }

    @Test
    public void testIncorrectMVRewriteInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, "
                + "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String createEMPMVSQL = "create materialized view " + EMPS_MV_NAME + " as select name, deptno from " +
                EMPS_TABLE_NAME + ";";
        starRocksAssert.withMaterializedView(createEMPMVSQL);
        String query = "select user_name, bitmap_union_count(to_bitmap(tag_id)) a from " + USER_TAG_TABLE_NAME + ", "
                + "(select name, deptno from " + EMPS_TABLE_NAME + ") a" + " where user_name=a.name group by "
                + "user_name having a>1 order by a;";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
        starRocksAssert.query(query).explainWithout(QUERY_USE_USER_TAG_MV);
    }

    @Test
    public void testIncorrectMVRewriteInSubquery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_id, bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " where " +
                "user_name in (select user_name from " + USER_TAG_TABLE_NAME + " group by user_name having " +
                "bitmap_union_count(to_bitmap(tag_id)) >1 )" + " group by user_id;";
        starRocksAssert.query(query).explainContains(QUERY_USE_USER_TAG);
    }

    @Test
    public void testAggTableCountDistinctInBitmapType() throws Exception {
        String aggTable = "CREATE TABLE " + TEST_TABLE_NAME + " (k1 int, v1 bitmap bitmap_union) Aggregate KEY (k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ('replication_num' = '1');";
        starRocksAssert.withTable(aggTable);
        String query = "select k1, count(distinct v1) from " + TEST_TABLE_NAME + " group by k1;";
        starRocksAssert.query(query).explainContains(TEST_TABLE_NAME, "bitmap_union_count");
        starRocksAssert.dropTable(TEST_TABLE_NAME);
    }

    @Test
    public void testAggTableCountDistinctInHllType() throws Exception {
        String aggTable = "CREATE TABLE " + TEST_TABLE_NAME + " (k1 int, v1 hll " + FunctionSet.HLL_UNION +
                ") Aggregate KEY (k1) " +
                "DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES ('replication_num' = '1');";
        starRocksAssert.withTable(aggTable);
        String query = "select k1, count(distinct v1) from " + TEST_TABLE_NAME + " group by k1;";
        starRocksAssert.query(query).explainContains(TEST_TABLE_NAME, "hll_union_agg");
        starRocksAssert.dropTable(TEST_TABLE_NAME);
    }

    @Test
    public void testCountDistinctToBitmap() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select count(distinct tag_id) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainContains(USER_TAG_MV_NAME, "bitmap_union_count");
    }

    @Test
    public void testIncorrectRewriteCountDistinct() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_name, count(distinct tag_id) from " + USER_TAG_TABLE_NAME + " group by user_name;";
        starRocksAssert.query(query).explainContains(USER_TAG_TABLE_NAME, FunctionSet.COUNT);
    }

    @Test
    public void testNDVToHll() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from " +
                USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select ndv(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainContains(USER_TAG_MV_NAME, "hll_union_agg");
    }

    @Test
    public void testApproxCountDistinctToHll() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME +
                " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select approx_count_distinct(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainContains(USER_TAG_MV_NAME, "hll_union_agg");
    }

    @Test
    public void testAggInHaving() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno from "
                + EMPS_TABLE_NAME + " group by empid, deptno;";
        starRocksAssert.withMaterializedView(createMVSQL);
        String query = "select empid from " + EMPS_TABLE_NAME + " group by empid having max(salary) > 1;";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testCountFieldInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "count(tag_id) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select count(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainContains(QUERY_USE_USER_TAG_MV);
        query = "select user_name, count(tag_id) from " + USER_TAG_TABLE_NAME + " group by user_name;";
        starRocksAssert.query(query).explainWithout(USER_TAG_MV_NAME);
        query = "select sum(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainWithout(QUERY_USE_USER_TAG_MV);
    }

    @Test
    public void testInvalidColumnInCreateMVStmt() throws Exception {
        String createMVSQL = "create materialized view " + USER_TAG_MV_NAME + " as select invalid_column, user_id from "
                + USER_TAG_TABLE_NAME + ";";
        try {
            starRocksAssert.withMaterializedView(createMVSQL);
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void testCreateMVBaseBitmapAggTable() throws Exception {
        String createTableSQL = "create table " + HR_DB_NAME + ".agg_table "
                + "(empid int, name varchar, salary bitmap " + FunctionSet.BITMAP_UNION + ") "
                + "aggregate key (empid, name) "
                + "partition by range (empid) "
                + "(partition p1 values less than MAXVALUE) "
                + "distributed by hash(empid) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable(createTableSQL);
        String createMVSQL = "create materialized view mv as select empid, " + FunctionSet.BITMAP_UNION
                + "(salary) from agg_table "
                + "group by empid;";
        starRocksAssert.withMaterializedView(createMVSQL);
        String query = "select count(distinct salary) from agg_table;";
        starRocksAssert.query(query).explainContains("mv");
        starRocksAssert.dropTable("agg_table");
    }

    @Test
    public void testBitmapUnionToBitmap() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME
                + " as select user_id, bitmap_union(to_bitmap(tag_id)) from " +
                USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_id, count(distinct tag_id) a from " + USER_TAG_TABLE_NAME
                + " group by user_id;";
        starRocksAssert.query(query).explainContains(QUERY_USE_USER_TAG_MV, FunctionSet.BITMAP_UNION_COUNT);
    }

    @Test
    public void testMultiDistinctCount() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME
                + " as select user_name, bitmap_union(to_bitmap(tag_id)), " +
                "bitmap_union(to_bitmap(user_id)) from " +
                USER_TAG_TABLE_NAME + " group by user_name;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_name, count(distinct tag_id), count(distinct user_id) from "
                + USER_TAG_TABLE_NAME + " group by user_name;";
        starRocksAssert.query(query).explainContains(QUERY_USE_USER_TAG_MV, FunctionSet.BITMAP_UNION_COUNT);
    }

    @Test
    public void testBitmapUnionInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME
                + " as select user_id, bitmap_union(to_bitmap(tag_id)) from " +
                USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_id, bitmap_union_count(to_bitmap(tag_id)) a from " + USER_TAG_TABLE_NAME
                + " group by user_id having a>1 order by a;";
        starRocksAssert.query(query).explainContains(QUERY_USE_USER_TAG_MV, FunctionSet.BITMAP_UNION_COUNT);
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testHLLUnionFamilyRewrite() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME +
                " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select `" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) from " +
                USER_TAG_TABLE_NAME + ";";
        System.out.println(starRocksAssert.query(query).explainQuery());
        starRocksAssert.query(query).explainContains(USER_TAG_MV_NAME);
        query = "select hll_union_agg(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainContains(USER_TAG_MV_NAME, FunctionSet.HLL_UNION_AGG);
        query = "select hll_raw_agg(" + FunctionSet.HLL_HASH + "(tag_id)) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainContains(USER_TAG_MV_NAME, FunctionSet.HLL_RAW_AGG);
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testTwoTupleInQuery() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "bitmap_union(to_bitmap(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select * from (select user_id, bitmap_union_count(to_bitmap(tag_id)) x from " +
                USER_TAG_TABLE_NAME + " group by user_id) a, (select user_name, bitmap_union_count(to_bitmap(tag_id))"
                + "" + " y from " + USER_TAG_TABLE_NAME + " group by user_name) b where a.x=b.y;";
        starRocksAssert.query(query).explainContains(QUERY_USE_USER_TAG, QUERY_USE_USER_TAG_MV);
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testPercentile() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "percentile_union(percentile_hash(tag_id)) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_id, percentile_approx(tag_id, 1) from user_tags group by user_id";
        starRocksAssert.query(query).explainContains(QUERY_USE_USER_TAG_MV, "percentile_union(4: tag_id)",
                FunctionSet.PERCENTILE_APPROX_RAW);
        String query2 = "select user_id, round(percentile_approx(tag_id, 1),0) from user_tags group by user_id";
        starRocksAssert.query(query2).explainContains(QUERY_USE_USER_TAG_MV, "round");
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testPercentile2() throws Exception {
        String createUserTagMVSql = "create materialized view " + EMPS_MV_NAME + " as select empid, " +
                "percentile_union(percentile_hash(salary)), percentile_union(percentile_hash(commission)) from " +
                EMPS_TABLE_NAME + " group by empid;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query =
                "select empid, percentile_approx(salary, 1), percentile_approx(commission, 1) from emps group by empid";
        System.out.println(starRocksAssert.query(query).explainQuery());
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV, "  2:AGGREGATE (update serialize)\n",
                "output: percentile_union");
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testPercentileDouble() throws Exception {
        String createTableSql = "CREATE TABLE `duplicate_table_with_null` (\n" +
                "  `k1` date NULL COMMENT \"\",\n" +
                "  `k11` float NULL COMMENT \"\",\n" +
                "  `k12` double NULL \n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\" \n" +
                ");";
        starRocksAssert.withTable(createTableSql);
        String createMVSql = "CREATE MATERIALIZED VIEW percentile_mv\n" +
                "                             AS SELECT k1,\n" +
                "                             percentile_union(percentile_hash(k12)) " +
                " from duplicate_table_with_null group by k1";
        starRocksAssert.withMaterializedView(createMVSql);
        String query = "select round(percentile_approx(k12, 0.99),0) " +
                "from duplicate_table_with_null group by k1";
        starRocksAssert.query(query).explainContains("percentile_mv",
                FunctionSet.PERCENTILE_APPROX_RAW);
    }

    // Aggregation query with groupSets at coarser level of aggregation than
    // aggregation materialized view.
    @Test
    public void testGroupingSetQueryOnAggMV() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) " +
                "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select sum(salary), empid, deptno from " + EMPS_TABLE_NAME + " group by rollup(empid,deptno);";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    // Query with cube and arithmetic expr
    // TODO(kks): enable this later if we support cube
    @Test
    public void testAggQueryOnAggMV9() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, commission, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by deptno, commission;";
        String query = "select deptno, commission, sum(salary) + 1 from " + EMPS_TABLE_NAME
                + " group by cube(deptno,commission);";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQueryOnAggMV10() throws Exception {
        String createMVSQL =
                "create materialized view " + EMPS_MV_NAME + " as select deptno, bitmap_union(to_bitmap(salary)) "
                        + "from " + EMPS_TABLE_NAME + " group by deptno;";
        String query = "select deptno, count(distinct salary) from " + EMPS_TABLE_NAME + " group by deptno UNION ALL " +
                "select deptno, count(distinct salary) from " + EMPS_TABLE_NAME + " group by deptno";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testUnionQueryOnProjectionMV() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from " +
                EMPS_TABLE_NAME + " order by deptno;";
        String union = "select empid from " + EMPS_TABLE_NAME + " where deptno > 300" + " union all select empid from"
                + " " + EMPS_TABLE_NAME + " where deptno < 200";
        starRocksAssert.withMaterializedView(createMVSQL).query(union).explainContains(QUERY_USE_EMPS_MV);
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testUnionQueryOnProjectionMV2() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from " +
                EMPS_TABLE_NAME + " group by deptno,empid;";
        String union = "select a.empid from (select empid from " + EMPS_TABLE_NAME + " where deptno > 300" +
                " union all select empid from"
                + " " + EMPS_TABLE_NAME + " where deptno < 200) a group by a.empid";
        starRocksAssert.withMaterializedView(createMVSQL).query(union).explainContains(QUERY_USE_EMPS_MV);
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testUnionQueryOnAggMV1() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from " +
                EMPS_TABLE_NAME + " group by deptno,empid;";
        String union = "select a.cnt from (select count(1) as cnt from " + EMPS_TABLE_NAME + " where deptno > 300" +
                " union all select count(1) as cnt from"
                + " " + EMPS_TABLE_NAME + " where deptno < 200) a ";
        starRocksAssert.withMaterializedView(createMVSQL).query(union).explainContains(QUERY_USE_EMPS);
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testUnionQueryOnAggMV2() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from " +
                EMPS_TABLE_NAME + " group by deptno,empid;";
        String union = "select a.empid, a.deptno from (select empid, deptno from " + EMPS_TABLE_NAME +
                " where deptno > 300 group by empid, deptno" +
                " union all select empid, deptno from"
                + " " + EMPS_TABLE_NAME + " where deptno < 200 group by empid, deptno) a ";
        starRocksAssert.withMaterializedView(createMVSQL).query(union).explainContains(QUERY_USE_EMPS_MV);
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testUnionQueryOnAggMV3() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from " +
                EMPS_TABLE_NAME + " group by deptno,empid;";
        String union = "select a.empid, count(1) from (select empid, deptno from " + EMPS_TABLE_NAME +
                " where deptno > 300 group by empid, deptno" +
                " union all select empid, deptno from"
                + " " + EMPS_TABLE_NAME + " where deptno < 200 group by empid, deptno) a group by a.empid";
        starRocksAssert.withMaterializedView(createMVSQL).query(union).explainContains(QUERY_USE_EMPS_MV);
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testUnionQueryOnAggMV4() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as select deptno, empid from " +
                EMPS_TABLE_NAME + " group by deptno,empid;";
        String union = "select a.empid, sum(cnt) from (select empid, deptno as cnt from " + EMPS_TABLE_NAME +
                " where deptno > 300 group by empid, deptno" +
                " union all select empid, count(1) as cnt from"
                + " " + EMPS_TABLE_NAME + " where deptno < 200 group by empid) a group by a.empid";
        String plan = starRocksAssert.withMaterializedView(createMVSQL).query(union).explainQuery();
        Assert.assertTrue(plan.contains("1:OlapScanNode\n" +
                "     TABLE: emps\n" +
                "     PREAGGREGATION: OFF. Reason: Predicates include the value column\n" +
                "     PREDICATES: 4: deptno > 300\n" +
                "     partitions=1/1\n" +
                "     rollup: emps_mv"));
        Assert.assertTrue(plan.contains("7:OlapScanNode\n" +
                "     TABLE: emps\n" +
                "     PREAGGREGATION: ON\n" +
                "     PREDICATES: 11: deptno < 200\n" +
                "     partitions=1/1"));
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testNoGroupByQueryWithGroupByAggTable() throws Exception {
        String createTableSQL = "CREATE TABLE `ocs_node_monitor_v1` (\n" +
                "  `node_ip` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `ts` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `dest_uri` varchar(6144) NOT NULL COMMENT \"\",\n" +
                "  `ac` bigint(20) REPLACE_IF_NOT_NULL NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "AGGREGATE KEY(`node_ip`, `ts`,`dest_uri`)\n" +
                "DISTRIBUTED BY HASH(`node_ip`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(createTableSQL);
        String createMVSQL = "CREATE MATERIALIZED VIEW test AS SELECT ts, node_ip, dest_uri " +
                "from ocs_node_monitor_v1 group by ts, node_ip, dest_uri;";
        starRocksAssert.withMaterializedView(createMVSQL);

        String query = "select ts from ocs_node_monitor_v1 where ts = 1625328001 and node_ip = '192.168.146.185' " +
                "limit 100;";
        starRocksAssert.query(query).explainContains("rollup: test");
    }

    @Test
    public void testCreateMVEmptyPartition() throws Exception {
        String createTableSQL = "create table empty_partition_table"
                + " (  `k1` date NULL COMMENT \"\",\n" +
                "  `k2` int(11) NULL COMMENT \"\",\n" +
                "  `k3` smallint(6) NULL COMMENT \"\",\n" +
                "  `v1` varchar(2048) NULL COMMENT \"\") partition by range (k1) "
                + " () "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        starRocksAssert.withTable(createTableSQL);

        String createMVSQL = "create materialized view empty_partition_table_view as select k1, k2, count(k3) " +
                "from empty_partition_table group by k1,k2";
        starRocksAssert.withMaterializedView(createMVSQL);
        starRocksAssert.dropTable("empty_partition_table");
    }

    @Test
    public void testCaseWhenSelectMV() throws Exception {
        // NOTE(yan): add a field not used in query, so optimized plan will select mv
        // otherwise I doubt that will use fact table.
        String createTableSQL = "CREATE TABLE kkk (\n" +
                "  `is_finish` char(1) NULL ,\n" +
                "  `is_subscribe` char(100) NULL ,\n" +
                "  `dt` date NULL ,\n" +
                "  `user_id_td` bitmap BITMAP_UNION NULL \n" +
                ") ENGINE=OLAP\n" +
                "DISTRIBUTED BY HASH(`is_finish`, `is_subscribe`, `dt`) BUCKETS 16\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(createTableSQL);
        String createMVSQL = "CREATE materialized view kkk_mv as\n" +
                "select dt, is_finish, bitmap_union(user_id_td)\n" +
                "FROM kkk AS T1 group by dt, is_finish;";
        starRocksAssert.withMaterializedView(createMVSQL);
        String query = "SELECT T1.dt AS `c0`,\n" +
                "       0 AS `c1`,\n" +
                "       0 AS `c2`,\n" +
                "       bitmap_count ( BITMAP_UNION (T1.user_id_td)) AS `c3`,\n" +
                "       bitmap_count ( BITMAP_UNION ( CASE WHEN (T1.is_finish = '1') " +
                "THEN T1.user_id_td ELSE NULL END)) AS `c4`\n" +
                "FROM kkk AS T1\n" +
                "GROUP BY T1.dt";
        starRocksAssert.query(query).explainContains("rollup: kkk_mv");
        starRocksAssert.dropTable("kkk");
    }

    @Test
    public void testQueryOnStar() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select time, deptno, empid, name, " +
                "salary from " + EMPS_TABLE_NAME + " order by time, deptno, empid;";
        String query = "select * from " + EMPS_TABLE_NAME + " where deptno = 1";
        // mv lacks of `commission` field, so can not use mv.
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS);
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testQueryOnStarAndJoin() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select time, deptno, empid, name, " +
                "salary from " + EMPS_TABLE_NAME + " order by time, deptno, empid;";
        String query = "select * from " + EMPS_TABLE_NAME + " join depts on " + EMPS_TABLE_NAME + ".deptno = " +
                DEPTS_TABLE_NAME + ".deptno";
        // mv lacks of `commission` field, so can not use mv.
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS);
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testJoinProjectRewrite() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME +
                " as select time, empid, bitmap_union(to_bitmap(deptno)),hll_union(hll_hash(salary)) from " +
                EMPS_TABLE_NAME + " group by time, empid";
        starRocksAssert.withMaterializedView(createEmpsMVSQL);
        String query = "select count(distinct emps.deptno) from emps, depts";
        starRocksAssert.query(query).explainContains("emps_mv", "bitmap_union_count(7: mv_bitmap_union_deptno)");

        query = "select count(distinct emps.deptno) from emps, depts where emps.deptno = depts.deptno";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS);

        query = "select count(distinct emps.deptno) from emps, depts where emps.time = depts.time";
        starRocksAssert.query(query).explainContains(EMPS_MV_NAME);

        query = "select count(distinct emps.deptno) from emps left outer join depts on emps.time = depts.time";
        starRocksAssert.query(query).explainContains("emps_mv");

        query = "select emps.time, count(distinct emps.deptno) from emps, depts where emps.time = depts.time " +
                "group by emps.time";
        starRocksAssert.query(query).explainContains("emps_mv");

        query = "select count(distinct emps.deptno * 2) from emps left outer join depts on emps.time = depts.time";
        starRocksAssert.query(query).explainWithout("emps_mv");

        query = "select unnest, count(distinct deptno) from " +
                "(select deptno, unnest from emps,unnest(split(name, \",\"))) t" +
                " group by unnest";
        starRocksAssert.query(query).explainContains("emps_mv", "bitmap_union_count(7: mv_bitmap_union_deptno)");

        query = "select approx_count_distinct(salary) from emps left outer join depts on emps.time = depts.time";
        starRocksAssert.query(query).explainContains("emps_mv");
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testMultipleAggregate() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select deptno, sum(salary) as ssalary from " + EMPS_TABLE_NAME + " group by deptno";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS_MV);
        query = "select count(distinct deptno), MAX(ssalary) from (" + query + ") as zxcv123 group by deptno";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testCaseWhenAggregate() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        starRocksAssert.withMaterializedView(createEmpsMVSQL);
        String query =
                "select deptno, sum(case salary when 1 then 2 when 2 then 3 end) as ssalary from " + EMPS_TABLE_NAME +
                        " group by deptno";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);

        query = "select deptno, sum(case deptno when 1 then 2 when 2 then 3 end) as ssalary from " + EMPS_TABLE_NAME +
                " group by deptno";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);

        query = "select deptno, sum(case deptno when 1 then salary when 2 then 3 end) as ssalary from " +
                EMPS_TABLE_NAME + " group by deptno";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);

        query = "select deptno, sum(case deptno when 1 then salary when 2 then truncate(3.14,1) end) as ssalary from " +
                EMPS_TABLE_NAME + " group by deptno";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);

        query = "select deptno, sum(case deptno when 1 then salary when 2 then salary end) as ssalary from " +
                EMPS_TABLE_NAME + " group by deptno";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);

        query = "select deptno, sum(case empid when 1 then salary when 2 then salary end) as ssalary from " +
                EMPS_TABLE_NAME + " group by deptno";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);

        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testCaseWhenAggWithPartialOrderBy() throws Exception {
        String query = "select k6, k7 from all_type_table where k6 = 1 group by k6, k7";

        String createMVSQL = "CREATE MATERIALIZED VIEW partial_order_by_mv AS " +
                "SELECT k6, k7 FROM all_type_table GROUP BY k6, k7 ORDER BY k6";
        CreateMaterializedViewStmt createMaterializedViewStmt =
                (CreateMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(createMVSQL, starRocksAssert.getCtx());
        createMaterializedViewStmt.getMVColumnItemList().forEach(k -> Assert.assertTrue(k.isKey()));

        starRocksAssert.withMaterializedView(createMVSQL).query(query).explainContains("rollup: partial_order_by_mv");
        starRocksAssert.dropMaterializedView("partial_order_by_mv");

        String createMVSQL2 = "CREATE MATERIALIZED VIEW order_by_mv AS " +
                "SELECT k6, k7 FROM all_type_table GROUP BY k6, k7 ORDER BY k6, k7";
        starRocksAssert.withMaterializedView(createMVSQL2).query(query).explainContains("rollup: order_by_mv");

        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, "all_type_table");
    }

    @Test
    public void testCast() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query =
                "select deptno, sum(cast(salary as smallint)) as ssalary from " + EMPS_TABLE_NAME + " group by deptno";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS);

        query = "select deptno, sum(cast(salary as bigint)) as ssalary from " + EMPS_TABLE_NAME + " group by deptno";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);

        query = "select deptno, sum(cast(salary as decimal(9, 3))) as ssalary from " + EMPS_TABLE_NAME +
                " group by deptno";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS);

        starRocksAssert.assertMVWithoutComplexExpression(HR_DB_NAME, EMPS_TABLE_NAME);
    }

    @Test
    public void testCast2() throws Exception {
        String mvSQL = "CREATE MATERIALIZED VIEW percentile_mv\n" +
                "                             AS SELECT k1, \n" +
                "                             percentile_union(percentile_hash(k7)),\n" +
                "                             percentile_union(percentile_hash(k8)),\n" +
                "                             percentile_union(percentile_hash(k9)),\n" +
                "                             percentile_union(percentile_hash(k10)),\n" +
                "                             percentile_union(percentile_hash(k11)),\n" +
                "                             percentile_union(percentile_hash(k12)),     \n" +
                "                             percentile_union(percentile_hash(k13)) from all_type_table group by k1 \n";
        String query = "select round(percentile_approx(k8, 0.99),0),round(percentile_approx(k9, 0.99),0)," +
                "round(percentile_approx(k10, 0.99),0),round(percentile_approx(k11, 0.99),0)," +
                "round(percentile_approx(k12, 0.99),0),round(percentile_approx(k13, 0.99),0) " +
                "from all_type_table";
        starRocksAssert.withMaterializedView(mvSQL).query(query).explainContains("rollup: percentile_mv");
    }

    @Test
    public void testWithMysql() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno, sum(salary) "
                + "from " + EMPS_TABLE_NAME + " group by empid, deptno;";
        String query = "select * from ods_order where bank_transaction_id " +
                "not in (select sum(cast(salary as smallint)) as ssalary from " +
                        EMPS_TABLE_NAME + " group by deptno)";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS);
    }

    @Test
    public void testPredicateIsCallOperator() throws Exception {
        String createEmpsMVSQL = "create materialized view " + EMPS_MV_NAME + " as select empid, deptno "
                + "from " + EMPS_TABLE_NAME + ";";
        String query = "select count(*) from " + EMPS_TABLE_NAME + " where bitmap_contains(to_bitmap(1),2)";
        starRocksAssert.withMaterializedView(createEmpsMVSQL).query(query).explainContains(QUERY_USE_EMPS);
    }

    @Test
    public void testProjectionWithComplexExpressMV1() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME +
                " as select deptno * 2 as col1, empid + 1 as col2 from " + EMPS_TABLE_NAME + ";";
        starRocksAssert.withMaterializedView(createMVSQL);

        String query = "select empid, deptno from " + EMPS_TABLE_NAME + ";";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);

        query = "select empid * 2, deptno from " + EMPS_TABLE_NAME + ";";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);

        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        query = "select empid + 1, deptno * 2 from " + EMPS_TABLE_NAME + ";";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);

        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(true);
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testProjectionWithComplexExpressMV2() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME +
                " as select deptno * 2 as col1, empid + 1 as col2 from " + EMPS_TABLE_NAME
                + " order by deptno;";
        starRocksAssert.withMaterializedView(createMVSQL);

        // TODO: new mv rewrite framework doesn't support order by in mv for rewrite.
        String query = "select deptno * 2 as col1, empid + 1 as col2 from " + EMPS_TABLE_NAME
                + " order by deptno;";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);

        query = "select empid + 1, deptno * 2 from " + EMPS_TABLE_NAME + ";";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testProjectionWithComplexExpressMV3() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as " +
                "select name, deptno * 2 as col1, salary % 100 as col2 " +
                "from " + EMPS_TABLE_NAME + ";";
        starRocksAssert.withMaterializedView(createMVSQL);

        String query = "select name from " + EMPS_TABLE_NAME + " where (deptno*2) > 30 and (salary % 100) > 30;";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);

        query = "select empid from " + EMPS_TABLE_NAME + " where (deptno*2) > 30 and (salary % 100) > 30;";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQueryWithComplexExpressionMV1() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as " +
                "select deptno, sum(salary * 2) as sum1, max(commission) " +
                "from " + EMPS_TABLE_NAME + " group by deptno;";
        starRocksAssert.withMaterializedView(createMVSQL);

        String query = "select sum(salary), deptno from " + EMPS_TABLE_NAME + " group by deptno;";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);

        query = "select sum(salary), max(commission) from " + EMPS_TABLE_NAME;
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);

        query = "select sum(salary * 2), max(commission) from emps group by deptno";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);

        query = "select sum(salary * 2), max(commission) from emps";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQueryWithComplexExpressionMV2() throws Exception {
        String createMVSQL = "create materialized view " + EMPS_MV_NAME + " as " +
                "select deptno, sum(cast((salary * 2) as bigint)) as sum1, max(commission) " +
                "from " + EMPS_TABLE_NAME + " group by deptno;";
        starRocksAssert.withMaterializedView(createMVSQL);

        String query = "select sum(salary), deptno from " + EMPS_TABLE_NAME + " group by deptno;";
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);

        query = "select sum(salary), max(commission) from " + EMPS_TABLE_NAME;
        starRocksAssert.query(query).explainWithout(QUERY_USE_EMPS_MV);

        query = "select sum(salary * 2), max(commission) from emps group by deptno";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);

        query = "select sum(salary * 2), max(commission) from emps";
        starRocksAssert.query(query).explainContains(QUERY_USE_EMPS_MV);
    }

    @Test
    public void testAggQueryWithComplexExpressionMV3() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "bitmap_union(to_bitmap(tag_id % 10)) as uv1 from " + USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_id, count(distinct tag_id % 10) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.query(query).explainContains(USER_TAG_MV_NAME, FunctionSet.COUNT);

        query = "select user_id, count(distinct tag_id) from " + USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.query(query).explainWithout(USER_TAG_MV_NAME);
    }

    @Test
    public void testAggQueryWithComplexExpressionMV4() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id % 100)) as agg1 from " +
                USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select ndv(tag_id % 100) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainContains(USER_TAG_MV_NAME, "hll_union");

        query = "select ndv(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainWithout(USER_TAG_MV_NAME);
    }

    @Test
    public void testAggQueryWithComplexExpressionMV5() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id * 10)) as agg1 " +
                "from " + USER_TAG_TABLE_NAME +
                " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);

        String query = "select approx_count_distinct(tag_id * 10) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainContains(USER_TAG_MV_NAME, "hll_union");

        query = "select approx_count_distinct(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainWithout(USER_TAG_MV_NAME);
    }

    @Test
    public void testAggQueryWithComplexExpressionMV7() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "percentile_union(percentile_hash(tag_id % 100)) as agg1 from " + USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select user_id, percentile_approx(tag_id % 100, 1) from user_tags group by user_id";
        starRocksAssert.query(query).explainContains(QUERY_USE_USER_TAG_MV,
                "percentile_union(9: mv_agg1)", FunctionSet.PERCENTILE_APPROX_RAW);

        query = "select user_id, round(percentile_approx(tag_id % 100, 1),0) from user_tags group by user_id";
        starRocksAssert.query(query).explainContains(QUERY_USE_USER_TAG_MV);

        query = "select user_id, percentile_approx(tag_id, 1) from user_tags group by user_id";
        starRocksAssert.query(query).explainWithout(QUERY_USE_USER_TAG_MV);
    }

    @Test
    public void testAggQueryWithComplexExpressionMV8() throws Exception {
        String t1 = "CREATE TABLE `test3` (\n" +
                "  `k1` tinyint(4) NULL DEFAULT \"0\",\n" +
                "  `k2` varchar(64) NULL DEFAULT \"\",\n" +
                "  `k3` bigint NULL DEFAULT \"0\",\n" +
                "  `k4` varchar(64) NULL DEFAULT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n";
        starRocksAssert.withTable(t1);
        String mv1 = "CREATE MATERIALIZED VIEW test_mv3\n" +
                "        as\n" +
                "        select k1, k1 * 2 as k11, length(k2) as k2, sum(k3), hll_union(hll_hash(k4)) as k5 " +
                "from test3 group by k1, k2;";
        starRocksAssert.withMaterializedView(mv1);

        String query = "select k1 * 2, length(k2), sum(k3), hll_union(hll_hash(k4)) as k5 from test3 group by k1, k2;";
        starRocksAssert.query(query).explainContains("test_mv3");
    }

    @Test
    public void testAggQueryWithComplexExpressionMV9() throws Exception {
        String createUserTagMVSql = "create materialized view " + USER_TAG_MV_NAME + " as select user_id, " +
                "`" + FunctionSet.HLL_UNION + "`(" + FunctionSet.HLL_HASH + "(tag_id)) as agg1 from " +
                USER_TAG_TABLE_NAME + " group by user_id;";
        starRocksAssert.withMaterializedView(createUserTagMVSql);
        String query = "select ndv(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainContains(USER_TAG_MV_NAME, "hll_union");

        query = "select ndv(tag_id) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainContains(USER_TAG_MV_NAME);

        query = "select ndv(tag_id % 10) from " + USER_TAG_TABLE_NAME + ";";
        starRocksAssert.query(query).explainWithout(USER_TAG_MV_NAME);
    }

    @Test
    public void testCaseWhenComplexExpressionMV1() throws Exception {
        String t1 = "CREATE TABLE case_when_t1 (\n" +
                "    k1 INT,\n" +
                "    k2 char(20))\n" +
                "DUPLICATE KEY(k1)\n" +
                "DISTRIBUTED BY HASH(k1)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ")\n";
        starRocksAssert.withTable(t1);
        String mv1 = "create materialized view case_when_mv1 AS SELECT k1, " +
                "(CASE k2 WHEN 'beijing' THEN 'bigcity' ELSE 'smallcity' END) as city FROM case_when_t1;\n";
        starRocksAssert.withMaterializedView(mv1);

        String query = "SELECT k1, (CASE k2 WHEN 'beijing' THEN 'bigcity' ELSE 'smallcity' END) as city FROM case_when_t1;";
        starRocksAssert.query(query).explainContains("case_when_mv1");
    }
}
