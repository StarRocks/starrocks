// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/CreateMaterializedViewStmtTest.java

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

package com.starrocks.analysis;

import com.starrocks.catalog.RefreshType;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.RefreshSchemeDesc;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CreateMaterializedViewTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {

        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_experimental_mv = true;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl1\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2020-02-01'),\n" +
                        "    PARTITION p2 values less than('2020-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl2\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k2)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('10'),\n" +
                        "    PARTITION p2 values less than('20')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE test.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withTable("CREATE TABLE `t1` (\n" +
                        "  `c_1_0` decimal128(30, 4) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_1` boolean NOT NULL COMMENT \"\",\n" +
                        "  `c_1_2` date NULL COMMENT \"\",\n" +
                        "  `c_1_3` date NOT NULL COMMENT \"\",\n" +
                        "  `c_1_4` double NULL COMMENT \"\",\n" +
                        "  `c_1_5` double NULL COMMENT \"\",\n" +
                        "  `c_1_6` datetime NULL COMMENT \"\",\n" +
                        "  `c_1_7` ARRAY<int(11)> NULL COMMENT \"\",\n" +
                        "  `c_1_8` smallint(6) NULL COMMENT \"\",\n" +
                        "  `c_1_9` bigint(20) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_10` varchar(31) NOT NULL COMMENT \"\",\n" +
                        "  `c_1_11` decimal128(22, 18) NULL COMMENT \"\",\n" +
                        "  `c_1_12` boolean NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`c_1_0`, `c_1_1`, `c_1_2`, `c_1_3`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "PARTITION BY RANGE(`c_1_3`)\n" +
                        "(PARTITION p20000101 VALUES [('2000-01-01'), ('2010-12-31')),\n" +
                        "PARTITION p20101231 VALUES [('2010-12-31'), ('2021-12-30')),\n" +
                        "PARTITION p20211230 VALUES [('2021-12-30'), ('2032-12-29')))\n" +
                        "DISTRIBUTED BY HASH(`c_1_3`, `c_1_2`, `c_1_0`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\"\n" +
                        ");")
                .withTable("CREATE EXTERNAL TABLE mysql_external_table\n" +
                        "(\n" +
                        "    k1 DATE,\n" +
                        "    k2 INT,\n" +
                        "    k3 SMALLINT,\n" +
                        "    k4 VARCHAR(2048),\n" +
                        "    k5 DATETIME\n" +
                        ")\n" +
                        "ENGINE=mysql\n" +
                        "PROPERTIES\n" +
                        "(\n" +
                        "    \"host\" = \"127.0.0.1\",\n" +
                        "    \"port\" = \"3306\",\n" +
                        "    \"user\" = \"mysql_user\",\n" +
                        "    \"password\" = \"mysql_passwd\",\n" +
                        "    \"database\" = \"mysql_db_test\",\n" +
                        "    \"table\" = \"mysql_table_test\"\n" +
                        ");")
                .withDatabase("test2").useDatabase("test2")
                .withTable("CREATE TABLE test2.tbl3\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p1 values less than('2021-02-01'),\n" +
                        "    PARTITION p2 values less than('2021-03-01')\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .useDatabase("test");
    }

    @Test
    public void testDisabled(){
        Config.enable_materialized_view = false;
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select date_format(tbl1.k1,'ym') ss, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "The experimental mv is disabled");
        } finally {
            Config.enable_materialized_view = true;
        }
    }

    @Test
    public void testExists(){
        String sql = "create materialized view tbl1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select date_format(tbl1.k1,'ym') ss, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Table 'tbl1' already exists");
        }
    }

    @Test
    public void testIfNotExists(){
        String sql = "create materialized view if not exists tbl1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select date_format(tbl1.k1,'ym') ss, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
            currentState.createMaterializedView((CreateMaterializedViewStatement) statementBase);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }


    @Test
    public void testFullDescPartitionByFunction() {
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select date_format(tbl1.k1,'ym') ss, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFullDescPartitionNoDataBase() {
        starRocksAssert.withoutUseDatabase();
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select date_format(tbl1.k1,'ym') ss, k2 from test.tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "No database selected");
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testFullDescPartitionHasDataBase() {
        starRocksAssert.withoutUseDatabase();
        String sql = "create materialized view test.mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select date_format(tbl1.k1,'ym') ss, k2 from test.tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            starRocksAssert.useDatabase("test");
        }
    }

    @Test
    public void testMultiPartitionByExpression() {
        String sql = "create materialized view mv1 " +
                "partition by (ss, k2) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select date_format(tbl1.k1,'ym') ss, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Partition expressions currently only support one");
        }
    }

    @Test
    public void testPartitionKeyNoBaseTablePartitionKey() {
        String sql = "create materialized view mv1 " +
                "partition by (s1) " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select k1 s1, k2 s2 from tbl2 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Materialized view partition key in partition exp must be base table partition key");
        }
    }

    @Test
    public void testBaseTableNoPartitionKey() {
        String sql = "create materialized view mv1 " +
                "partition by (s1) " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select k1 s1, k2 s2 from tbl3 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Materialized view partition key in partition exp must be base table partition key");
        }
    }

    @Test
    public void testFullDescPartitionByColumn() {
        String sql = "create materialized view mv1 " +
                "partition by (s1) " +
                "distributed by hash(s2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select k1 s1, k2 s2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFullDescPartitionByColumnNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by (k1) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select k1, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFullDescPartitionByColumnMixAlias1() {
        String sql = "create materialized view mv1 " +
                "partition by (k1) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select tbl1.k1, tbl1.k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFullDescPartitionByColumnMixAlias2() {
        String sql = "create materialized view mv1 " +
                "partition by (k1) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select k1, tbl1.k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNoPartitionExp() {
        String sql = "create materialized view mv1 " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select k1, tbl1.k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testSelectHasStar() {
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(ss) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select k1 ss, *  from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Select * is not supported in materialized view");
        }
    }

    @Test
    public void testPartitionByFunctionNotInSelect() {
        String sql = "create materialized view mv1 " +
                "partition by (s8) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select sqrt(tbl1.k1) s1, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view partition exp column can't find in query statement");
        }
    }

    @Test
    public void testPartitionByAllowedFunctionNoNeedParams() {
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select date_format(tbl1.k1) ss, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "No matching function with signature: date_format(date).");
        }
    }

    @Test
    public void testPartitionByNoAllowedFunction() {
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select sqrt(tbl1.k1) ss, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view partition function sqrt is not support");
        }
    }

    @Test
    public void testPartitionByNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by (date_format(k1,'%Y%m')) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select k1, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Partition exp date_format(k1, '%Y%m') must be alias of select item");
        }
    }

    @Test
    public void testDistributeByIsNull1() {
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select tbl1.k1 ss from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view should contain distribution desc");
        }
    }

    @Test
    public void testDistributeByIsNull2() {
        connectContext.getSessionVariable().setAllowDefaultPartition(true);
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select tbl1.k1 ss from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            connectContext.getSessionVariable().setAllowDefaultPartition(false);
        }
    }

    @Test
    public void testRefreshAsyncOnlyEvery() {
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async EVERY(INTERVAL 2 MINUTE)" +
                "as select tbl1.k1 ss, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) statementBase;
            RefreshSchemeDesc refreshSchemeDesc = createMaterializedViewStatement.getRefreshSchemeDesc();
            AsyncRefreshSchemeDesc asyncRefreshSchemeDesc = (AsyncRefreshSchemeDesc) refreshSchemeDesc;
            assertEquals(refreshSchemeDesc.getType(), RefreshType.ASYNC);
            assertNotNull(asyncRefreshSchemeDesc.getStartTime());
            assertEquals(((IntLiteral) asyncRefreshSchemeDesc.getIntervalLiteral().getValue()).getValue(), 2);
            assertEquals(asyncRefreshSchemeDesc.getIntervalLiteral().getUnitIdentifier().getDescription(), "MINUTE");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testRefreshSync() {
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh sync " +
                "as select tbl1.k1 ss, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Unsupported refresh type: sync");
        }
    }

    @Test
    public void testRefreshManual() {
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh manual " +
                "as select tbl1.k1 ss, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Unsupported refresh type: manual");
        }
    }

    @Test
    public void testRefreshNoEvery(){
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') " +
                "as select date_format(tbl1.k1,'ym') ss, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Refresh every must be specified");
        }
    }

    @Test
    public void testRefreshStartBeforeCurr(){
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2016-12-31') " +
                "as select date_format(tbl1.k1,'ym') ss, k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Refresh start must be after current time");
        }
    }

    @Test
    public void testQueryStatementNoSelectRelation() {
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select date_format(t1.k1,'%Y%m') ss, t1.k2 from tbl1 t1 union select * from tbl2 t2 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view query statement only support select");
        }
    }

    @Test
    public void testTableNotInOneDatabase() {
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select date_format(t1.k1,'%Y%m') ss, t1.k2 from test2.tbl3 t1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view do not support table which is in other database");
        }
    }

    @Test
    public void testTableNoOlapTable() {
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select date_format(tbl1.k1,'%Y%m') ss, tbl1.k2 from mysql_external_table tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(), "Materialized view only support olap tables");
        }

    }

    @Test
    public void testSelectItemNoAlias() {
        String sql = "create materialized view mv1 " +
                "partition by (ss) " +
                "distributed by hash(k2) " +
                "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                "as select date_format(tbl1.k1,'%Y%m'), k2 from tbl1 " +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "Materialized view query statement select item date_format(`tbl1`.`k1`, '%Y%m') must has alias except base select item");
        }
    }

}

