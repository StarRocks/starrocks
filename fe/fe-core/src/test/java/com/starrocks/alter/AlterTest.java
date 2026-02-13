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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/AlterTest.java

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

package com.starrocks.alter;

import com.google.common.collect.Lists;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.GlobalConstraintManager;
import com.starrocks.catalog.constraint.TableWithFKConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.Util;
import com.starrocks.persist.ListPartitionPersistInfo;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.persist.OperationType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterDatabaseQuotaStmt;
import com.starrocks.sql.ast.AlterDatabaseRenameStatement;
import com.starrocks.sql.ast.AlterMaterializedViewStmt;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CancelRefreshMaterializedViewStmt;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateUserStmt;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropMaterializedViewStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.MultiItemListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.RefreshMaterializedViewStatement;
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.ast.TruncatePartitionClause;
import com.starrocks.sql.ast.TruncateTableStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.DefaultWarehouse;
import com.starrocks.warehouse.Warehouse;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class AlterTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
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
                                "    v1 int sum\n" +
                                ")\n" +
                                "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                                "PROPERTIES('replication_num' = '1');")

                    .withTable("CREATE TABLE test.tbl3\n" +
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

                    .withTable("CREATE TABLE test.tbl4\n" +
                                "(\n" +
                                "    k1 date,\n" +
                                "    k2 int,\n" +
                                "    v1 int sum\n" +
                                ")\n" +
                                "PARTITION BY RANGE(k1)\n" +
                                "(\n" +
                                "    PARTITION p1 values less than('2020-02-01'),\n" +
                                "    PARTITION p2 values less than('2020-03-01'),\n" +
                                "    PARTITION p3 values less than('2020-04-01'),\n" +
                                "    PARTITION p4 values less than('2020-05-01')\n" +
                                ")\n" +
                                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                                "PROPERTIES" +
                                "(" +
                                "    'replication_num' = '1',\n" +
                                "    'in_memory' = 'false',\n" +
                                "    'storage_medium' = 'SSD',\n" +
                                "    'storage_cooldown_time' = '9999-12-31 00:00:00'\n" +
                                ");")
                    .withTable("CREATE TABLE t_recharge_detail(\n" +
                                "    id bigint  ,\n" +
                                "    user_id  bigint  ,\n" +
                                "    recharge_money decimal(32,2) , \n" +
                                "    province varchar(20) not null,\n" +
                                "    dt varchar(20) not null\n" +
                                ") ENGINE=OLAP\n" +
                                "DUPLICATE KEY(id)\n" +
                                "PARTITION BY LIST (dt,province) (\n" +
                                "   PARTITION p1 VALUES IN ((\"2022-04-01\", \"beijing\")),\n" +
                                "   PARTITION p2 VALUES IN ((\"2022-04-01\", \"shanghai\"))\n" +
                                ")\n" +
                                "DISTRIBUTED BY HASH(`id`) BUCKETS 10 \n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"in_memory\" = \"false\"\n" +
                                ");")
                    .withTable("CREATE TABLE test.site_access_date_trunc (\n" +
                                "    event_day DATETIME NOT NULL,\n" +
                                "    site_id INT DEFAULT '10',\n" +
                                "    city_code VARCHAR(100),\n" +
                                "    user_name VARCHAR(32) DEFAULT '',\n" +
                                "    pv BIGINT DEFAULT '0'\n" +
                                ")\n" +
                                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                                "PARTITION BY date_trunc('day', event_day)\n" +
                                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                                "PROPERTIES(\n" +
                                "    \"replication_num\" = \"1\"\n" +
                                ");")
                    .withTable("CREATE TABLE site_access_time_slice (\n" +
                                "    event_day datetime,\n" +
                                "    site_id INT DEFAULT '10',\n" +
                                "    city_code VARCHAR(100),\n" +
                                "    user_name VARCHAR(32) DEFAULT '',\n" +
                                "    pv BIGINT DEFAULT '0'\n" +
                                ")\n" +
                                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                                "PARTITION BY time_slice(event_day, interval 1 day)\n" +
                                "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                                "PROPERTIES(\n" +
                                "    \"partition_live_number\" = \"3\",\n" +
                                "    \"replication_num\" = \"1\"\n" +
                                ");");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table test_partition_exception";
        try {
            DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        } catch (Exception ex) {

        }
    }

    private static void checkTableStateToNormal(OlapTable tb) throws InterruptedException {
        // waiting table state to normal
        int retryTimes = 5;
        while (tb.getState() != OlapTable.OlapTableState.NORMAL && retryTimes > 0) {
            Thread.sleep(5000);
            retryTimes--;
        }
        Assertions.assertEquals(OlapTable.OlapTableState.NORMAL, tb.getState());
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
    }

    private static void createMaterializedView(String sql) throws Exception {
        CreateMaterializedViewStatement createMaterializedViewStatement =
                    (CreateMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createMaterializedView(createMaterializedViewStatement);
    }

    private static void dropMaterializedView(String sql) throws Exception {
        DropMaterializedViewStmt dropMaterializedViewStmt =
                    (DropMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropMaterializedView(dropMaterializedViewStmt);
    }

    private static void alterMaterializedView(String sql, boolean expectedException) throws Exception {
        AlterMaterializedViewStmt alterMaterializedViewStmt =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().alterMaterializedView(alterMaterializedViewStmt);
            if (expectedException) {
                Assertions.fail();
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (!expectedException) {
                Assertions.fail();
            }
        }
    }

    private static void refreshMaterializedView(String sql) throws Exception {
        RefreshMaterializedViewStatement refreshMaterializedViewStatement =
                    (RefreshMaterializedViewStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .refreshMaterializedView(refreshMaterializedViewStatement.getMvName().getDb(),
                                    refreshMaterializedViewStatement.getMvName().getTbl(), false, null,
                                    Constants.TaskRunPriority.LOWEST.value(), false, true);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }

    private static void cancelRefreshMaterializedView(String sql, boolean expectedException) throws Exception {
        CancelRefreshMaterializedViewStmt cancelRefresh =
                    (CancelRefreshMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().cancelRefreshMaterializedView(cancelRefresh);
            if (expectedException) {
                Assertions.fail();
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (!expectedException) {
                Assertions.fail();
            }
        }
    }

    public static void alterTableWithNewParser(String sql, boolean expectedException) throws Exception {
        try {
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            DDLStmtExecutor.execute(alterTableStmt, connectContext);
            if (expectedException) {
                Assertions.fail();
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (!expectedException) {
                Assertions.fail();
            }
        }
    }

    private static void alterTableWithNewParserAndExceptionMsg(String sql, String msg) throws Exception {
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(connectContext, alterTableStmt);
        } catch (Exception e) {
            Assertions.assertEquals(msg, e.getMessage());
        }
    }

    @Test
    public void testRenameMaterializedView() throws Exception {
        starRocksAssert.useDatabase("test")
                    .withTable("CREATE TABLE test.testTable1\n" +
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
                                "PROPERTIES('replication_num' = '1');");
        String sql = "create materialized view mv1 " +
                    "partition by k1 " +
                    "distributed by hash(k2) " +
                    "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select k1, k2 from test.testTable1;";
        createMaterializedView(sql);
        String alterStmt = "alter materialized view test.mv1 rename mv2";
        alterMaterializedView(alterStmt, false);
        MaterializedView materializedView = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().
                    getDb("test").getTable("mv2");
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        Task task = taskManager.getTask(TaskBuilder.getMvTaskName(materializedView.getId()));
        Assertions.assertEquals("insert overwrite `mv2` SELECT `test`.`testTable1`.`k1`, `test`.`testTable1`.`k2`\n" +
                    "FROM `test`.`testTable1`", task.getDefinition());
        ConnectContext.get().setCurrentUserIdentity(UserIdentity.ROOT);
        ConnectContext.get().setCurrentRoleIds(UserIdentity.ROOT);
        dropMaterializedView("drop materialized view test.mv2");
    }

    @Test
    public void testCouldNotFindMaterializedView() throws Exception {
        starRocksAssert.useDatabase("test")
                    .withTable("CREATE TABLE test.testTable1\n" +
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
                    .withTable("CREATE TABLE test.testTable2\n" +
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
                                "PROPERTIES('replication_num' = '1');");
        String sql = "create materialized view mv1 " +
                    "partition by k1 " +
                    "distributed by hash(k2) " +
                    "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select k1, k2 from test.testTable1;";
        createMaterializedView(sql);
        starRocksAssert.getCtx().setCurrentRoleIds(GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdsByUser(
                    starRocksAssert.getCtx().getCurrentUserIdentity()));
        dropMaterializedView("drop materialized view test.mv1");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getTable("testTable1");
        // this for mock olapTable.getIndexNameById(mvIdx.getId()) == Null
        table.deleteIndexInfo("testTable1");
        try {
            dropMaterializedView("drop materialized view test.mv1");
            Assertions.fail();
        } catch (MetaNotFoundException ex) {
            // pass
        }
    }

    @Test
    public void testRenameTable() throws Exception {
        starRocksAssert.useDatabase("test")
                    .withTable("CREATE TABLE test.testRenameTable1\n" +
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
                                "PROPERTIES('replication_num' = '1');");
        String alterStmt = "alter table test.testRenameTable1 rename testRenameTable2";
        alterTableWithNewParser(alterStmt, false);
    }

    @Test
    public void testChangeMaterializedViewRefreshScheme() throws Exception {
        starRocksAssert.useDatabase("test")
                    .withTable("CREATE TABLE test.testTable2\n" +
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
                                "PROPERTIES('replication_num' = '1');");
        String sql = "create materialized view mv1 " +
                    "partition by k1 " +
                    "distributed by hash(k2) " +
                    "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select k1, k2 from test.testTable2;";
        createMaterializedView(sql);
        String alterStmt = "alter materialized view mv1 refresh async EVERY(INTERVAL 1 minute)";
        alterMaterializedView(alterStmt, false);
        alterStmt = "alter materialized view mv1 refresh manual";
        alterMaterializedView(alterStmt, false);
        ConnectContext.get().setCurrentUserIdentity(UserIdentity.ROOT);
        ConnectContext.get().setCurrentRoleIds(UserIdentity.ROOT);
        dropMaterializedView("drop materialized view test.mv1");
    }

    @Test
    public void testRefreshMaterializedView() throws Exception {
        starRocksAssert.useDatabase("test")
                    .withTable("CREATE TABLE test.testTable3\n" +
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
                                "PROPERTIES('replication_num' = '1');");
        String sql = "create materialized view mv1 " +
                    "partition by k1 " +
                    "distributed by hash(k2) " +
                    "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select k1, k2 from test.testTable3;";
        createMaterializedView(sql);
        String alterStmt = "refresh materialized view test.mv1";
        refreshMaterializedView(alterStmt);
        starRocksAssert.getCtx().setCurrentRoleIds(GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdsByUser(
                    starRocksAssert.getCtx().getCurrentUserIdentity()));
        dropMaterializedView("drop materialized view test.mv1");
    }

    @Test
    public void testCancelRefreshMaterializedView() throws Exception {
        starRocksAssert.useDatabase("test")
                    .withTable("CREATE TABLE test.testTable4\n" +
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
                                "PROPERTIES('replication_num' = '1');");
        String sql = "create materialized view mv1 " +
                    "partition by k1 " +
                    "distributed by hash(k2) " +
                    "refresh async START('2122-12-31') EVERY(INTERVAL 1 HOUR) " +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ") " +
                    "as select k1, k2 from test.testTable4;";
        starRocksAssert.getCtx().setCurrentRoleIds(GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdsByUser(
                    starRocksAssert.getCtx().getCurrentUserIdentity()));
        createMaterializedView(sql);
        String alterStmt = "refresh materialized view test.mv1";
        refreshMaterializedView(alterStmt);
        cancelRefreshMaterializedView("cancel refresh materialized view test.mv1", false);
        dropMaterializedView("drop materialized view test.mv1");
    }

    @Test
    public void testConflictAlterOperations() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl1");

        String stmt =
                    "alter table test.tbl1 add partition p3 values less than('2020-04-01'), " +
                                "add partition p4 values less than('2020-05-01')";
        alterTableWithNewParser(stmt, true);

        stmt = "alter table test.tbl1 add partition p3 values less than('2020-04-01'), drop partition p4";
        alterTableWithNewParser(stmt, true);

        stmt = "alter table test.tbl1 drop partition p3, drop partition p4";
        alterTableWithNewParser(stmt, true);

        stmt = "alter table test.tbl1 drop partition p3, add column k3 int";
        // alterTable(stmt, true);

        // no conflict
        stmt = "alter table test.tbl1 add column k3 int, add column k4 int";
        alterTableWithNewParser(stmt, false);
        waitSchemaChangeJobDone(false, tbl);

        stmt = "alter table test.tbl1 add rollup r1 (k1)";
        alterTableWithNewParser(stmt, false);
        waitSchemaChangeJobDone(true, tbl);

        stmt = "alter table test.tbl1 add rollup r2 (k1), r3 (k1)";
        alterTableWithNewParser(stmt, false);
        waitSchemaChangeJobDone(true, tbl);

        // enable dynamic partition
        // not adding the `start` property so that it won't drop the origin partition p1, p2 and p3
        stmt = "alter table test.tbl1 set (\n" +
                    "'dynamic_partition.enable' = 'true',\n" +
                    "'dynamic_partition.time_unit' = 'DAY',\n" +
                    "'dynamic_partition.end' = '3',\n" +
                    "'dynamic_partition.prefix' = 'p',\n" +
                    "'dynamic_partition.buckets' = '3'\n" +
                    " );";
        alterTableWithNewParser(stmt, false);

        Assertions.assertTrue(tbl.getTableProperty().getDynamicPartitionProperty().isEnabled());
        Assertions.assertEquals(4, tbl.getIndexIdToSchema().size());

        // add partition when dynamic partition is enable
        stmt = "alter table test.tbl1 add partition p3 values less than('2020-04-01') " +
                    "distributed by hash(k2) buckets 4 PROPERTIES ('replication_num' = '1')";
        alterTableWithNewParser(stmt, true);

        // add temp partition when dynamic partition is enable
        stmt = "alter table test.tbl1 add temporary partition tp3 values less than('2020-04-01') " +
                    "distributed by hash(k2) buckets 4 PROPERTIES ('replication_num' = '1')";
        alterTableWithNewParser(stmt, false);
        Assertions.assertEquals(1, tbl.getTempPartitions().size());

        // disable the dynamic partition
        stmt = "alter table test.tbl1 set ('dynamic_partition.enable' = 'false')";
        alterTableWithNewParser(stmt, false);
        Assertions.assertFalse(tbl.getTableProperty().getDynamicPartitionProperty().isEnabled());

        // add partition when dynamic partition is disable
        stmt = "alter table test.tbl1 add partition p3 values less than('2020-04-01') " +
                    "distributed by hash(k2) buckets 4";
        alterTableWithNewParser(stmt, false);

        // set table's default replication num
        Assertions.assertEquals(Short.valueOf("1"), tbl.getDefaultReplicationNum());
        stmt = "alter table test.tbl1 set ('default.replication_num' = '3');";
        alterTableWithNewParser(stmt, false);
        Assertions.assertEquals(Short.valueOf("3"), tbl.getDefaultReplicationNum());

        // set table_query_timeout
        stmt = "alter table test.tbl1 set ('table_query_timeout' = '120');";
        alterTableWithNewParser(stmt, false);
        Assertions.assertEquals(120, tbl.getTableQueryTimeout());

        // set range table's real replication num
        Partition p1 = tbl.getPartition("p1");
        Assertions.assertEquals(Short.valueOf("1"), Short.valueOf(tbl.getPartitionInfo().getReplicationNum(p1.getId())));
        stmt = "alter table test.tbl1 set ('replication_num' = '3');";
        alterTableWithNewParser(stmt, true);
        Assertions.assertEquals(Short.valueOf("1"), Short.valueOf(tbl.getPartitionInfo().getReplicationNum(p1.getId())));

        // set un-partitioned table's real replication num
        OlapTable tbl2 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl2");
        Partition partition = tbl2.getPartition(tbl2.getName());
        Assertions.assertEquals(Short.valueOf("1"),
                    Short.valueOf(tbl2.getPartitionInfo().getReplicationNum(partition.getId())));
        // partition replication num and table default replication num are updated at the same time in unpartitioned table
        stmt = "alter table test.tbl2 set ('replication_num' = '3');";
        alterTableWithNewParser(stmt, false);
        Assertions.assertEquals(Short.valueOf("3"),
                    Short.valueOf(tbl2.getPartitionInfo().getReplicationNum(partition.getId())));
        Assertions.assertEquals(Short.valueOf("3"), tbl2.getDefaultReplicationNum());
        stmt = "alter table test.tbl2 set ('default.replication_num' = '2');";
        alterTableWithNewParser(stmt, false);
        Assertions.assertEquals(Short.valueOf("2"),
                    Short.valueOf(tbl2.getPartitionInfo().getReplicationNum(partition.getId())));
        Assertions.assertEquals(Short.valueOf("2"), tbl2.getDefaultReplicationNum());
        stmt = "alter table test.tbl2 modify partition tbl2 set ('replication_num' = '1');";
        alterTableWithNewParser(stmt, false);
        Assertions.assertEquals(Short.valueOf("1"),
                    Short.valueOf(tbl2.getPartitionInfo().getReplicationNum(partition.getId())));
        Assertions.assertEquals(Short.valueOf("1"), tbl2.getDefaultReplicationNum());

        Thread.sleep(5000); // sleep to wait dynamic partition scheduler run
        // add partition without set replication num
        stmt = "alter table test.tbl1 add partition p4 values less than('2020-04-10')";
        alterTableWithNewParser(stmt, true);

        // add partition when dynamic partition is disable
        stmt = "alter table test.tbl1 add partition p4 values less than('2020-04-10') ('replication_num' = '1')";
        alterTableWithNewParser(stmt, false);

        stmt = "alter table test.tbl1 " +
                    "add TEMPORARY partition p5 values [('2020-04-10'), ('2020-05-10')) ('replication_num' = '1') " +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 3 PROPERTIES('replication_num' = '1');";
        alterTableWithNewParser(stmt, false);
        //rename table
        stmt = "alter table test.tbl1 rename newTableName";
        alterTableWithNewParser(stmt, false);
    }

    // test batch update range partitions' properties
    @Test
    public void testBatchUpdatePartitionProperties() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl4 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl4");
        Partition p1 = tbl4.getPartition("p1");
        Partition p2 = tbl4.getPartition("p2");
        Partition p3 = tbl4.getPartition("p3");
        Partition p4 = tbl4.getPartition("p4");

        // batch update replication_num property
        String stmt = "alter table test.tbl4 modify partition (p1, p2, p4) set ('replication_num' = '3')";
        List<Partition> partitionList = Lists.newArrayList(p1, p2, p4);
        for (Partition partition : partitionList) {
            Assertions.assertEquals(Short.valueOf("1"),
                        Short.valueOf(tbl4.getPartitionInfo().getReplicationNum(partition.getId())));
        }
        alterTableWithNewParser(stmt, false);
        for (Partition partition : partitionList) {
            Assertions.assertEquals(Short.valueOf("3"),
                        Short.valueOf(tbl4.getPartitionInfo().getReplicationNum(partition.getId())));
        }
        Assertions.assertEquals(Short.valueOf("1"), Short.valueOf(tbl4.getPartitionInfo().getReplicationNum(p3.getId())));

        // batch update in_memory property
        stmt = "alter table test.tbl4 modify partition (p1, p2, p3) set ('in_memory' = 'true')";
        partitionList = Lists.newArrayList(p1, p2, p3);
        for (Partition partition : partitionList) {
            Assertions.assertEquals(false, tbl4.getPartitionInfo().getIsInMemory(partition.getId()));
        }
        alterTableWithNewParser(stmt, false);
        for (Partition partition : partitionList) {
            Assertions.assertEquals(true, tbl4.getPartitionInfo().getIsInMemory(partition.getId()));
        }
        Assertions.assertEquals(false, tbl4.getPartitionInfo().getIsInMemory(p4.getId()));

        // batch update storage_medium and storage_cool_down properties
        stmt = "alter table test.tbl4 modify partition (p2, p3, p4) set ('storage_medium' = 'HDD')";
        DateLiteral dateLiteral = new DateLiteral("9999-12-31 00:00:00", Type.DATETIME);
        long coolDownTimeMs = dateLiteral.unixTimestamp(TimeUtils.getTimeZone());
        DataProperty oldDataProperty = new DataProperty(TStorageMedium.SSD, coolDownTimeMs);
        partitionList = Lists.newArrayList(p2, p3, p4);
        for (Partition partition : partitionList) {
            Assertions.assertEquals(oldDataProperty, tbl4.getPartitionInfo().getDataProperty(partition.getId()));
        }
        alterTableWithNewParser(stmt, false);
        DataProperty newDataProperty = new DataProperty(TStorageMedium.HDD, DataProperty.MAX_COOLDOWN_TIME_MS);
        for (Partition partition : partitionList) {
            Assertions.assertEquals(newDataProperty, tbl4.getPartitionInfo().getDataProperty(partition.getId()));
        }
        Assertions.assertEquals(oldDataProperty, tbl4.getPartitionInfo().getDataProperty(p1.getId()));

        // batch update range partitions' properties with *
        stmt = "alter table test.tbl4 modify partition (*) set ('replication_num' = '1')";
        partitionList = Lists.newArrayList(p1, p2, p3, p4);
        alterTableWithNewParser(stmt, false);
        for (Partition partition : partitionList) {
            Assertions.assertEquals(Short.valueOf("1"),
                        Short.valueOf(tbl4.getPartitionInfo().getReplicationNum(partition.getId())));
        }
    }

    //Move test to Regression Testing
    /*
    @Test
    public void testDynamicPartitionDropAndAdd() throws Exception {
        // test day range
        String stmt = "alter table test.tbl3 set (\n" +
                "'dynamic_partition.enable' = 'true',\n" +
                "'dynamic_partition.time_unit' = 'DAY',\n" +
                "'dynamic_partition.start' = '-3',\n" +
                "'dynamic_partition.end' = '3',\n" +
                "'dynamic_partition.prefix' = 'p',\n" +
                "'dynamic_partition.buckets' = '3'\n" +
                " );";
        alterTable(stmt, false);
        Thread.sleep(5000); // sleep to wait dynamic partition scheduler run

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl3");
        Assert.assertEquals(4, tbl.getPartitionNames().size());
        Assert.assertNull(tbl.getPartition("p1"));
        Assert.assertNull(tbl.getPartition("p2"));
    }
    */
    private void waitSchemaChangeJobDone(boolean rollupJob, OlapTable tb) throws InterruptedException {
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        if (rollupJob) {
            alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        }
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                            "alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println(alterJobV2.getType() + " alter job " + alterJobV2.getJobId() + " is done. state: " +
                        alterJobV2.getJobState());
            Assertions.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
        checkTableStateToNormal(tb);
    }

    @Test
    public void testSetDynamicPropertiesInNormalTable() throws Exception {
        String tableName = "no_dynamic_table";
        String createOlapTblStmt = "CREATE TABLE test.`" + tableName + "` (\n" +
                    "  `k1` date NULL COMMENT \"\",\n" +
                    "  `k2` int NULL COMMENT \"\",\n" +
                    "  `k3` smallint NULL COMMENT \"\",\n" +
                    "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                    "  `v2` datetime NULL COMMENT \"\"\n" +
                    ") ENGINE=OLAP\n" +
                    "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                    "COMMENT \"OLAP\"\n" +
                    "PARTITION BY RANGE (k1)\n" +
                    "(\n" +
                    "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                    "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                    "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ");";
        createTable(createOlapTblStmt);
        String alterStmt = "alter table test." + tableName + " set (\"dynamic_partition.enable\" = \"true\");";
        alterTableWithNewParserAndExceptionMsg(alterStmt, "Table test.no_dynamic_table is not a dynamic partition table.");
        // test set dynamic properties in a no dynamic partition table
        String stmt = "alter table test." + tableName + " set (\n" +
                    "'dynamic_partition.enable' = 'true',\n" +
                    "'dynamic_partition.time_unit' = 'DAY',\n" +
                    "'dynamic_partition.start' = '-3',\n" +
                    "'dynamic_partition.end' = '3',\n" +
                    "'dynamic_partition.prefix' = 'p',\n" +
                    "'dynamic_partition.buckets' = '3'\n" +
                    " );";
        alterTableWithNewParser(stmt, false);
    }

    @Test
    public void testSetDynamicPropertiesInDynamicPartitionTable() throws Exception {
        String tableName = "dynamic_table";
        String createOlapTblStmt = "CREATE TABLE test.`" + tableName + "` (\n" +
                    "  `k1` date NULL COMMENT \"\",\n" +
                    "  `k2` int NULL COMMENT \"\",\n" +
                    "  `k3` smallint NULL COMMENT \"\",\n" +
                    "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                    "  `v2` datetime NULL COMMENT \"\"\n" +
                    ") ENGINE=OLAP\n" +
                    "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                    "COMMENT \"OLAP\"\n" +
                    "PARTITION BY RANGE (k1)\n" +
                    "(\n" +
                    "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                    "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                    "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\",\n" +
                    "\"dynamic_partition.enable\" = \"true\",\n" +
                    "\"dynamic_partition.start\" = \"-3\",\n" +
                    "\"dynamic_partition.end\" = \"3\",\n" +
                    "\"dynamic_partition.time_unit\" = \"day\",\n" +
                    "\"dynamic_partition.prefix\" = \"p\",\n" +
                    "\"dynamic_partition.buckets\" = \"1\"\n" +
                    ");";

        createTable(createOlapTblStmt);
        String alterStmt1 = "alter table test." + tableName + " set (\"dynamic_partition.enable\" = \"false\");";
        alterTableWithNewParser(alterStmt1, false);
        String alterStmt2 = "alter table test." + tableName + " set (\"dynamic_partition.time_unit\" = \"week\");";
        alterTableWithNewParser(alterStmt2, false);
        String alterStmt3 = "alter table test." + tableName + " set (\"dynamic_partition.start\" = \"-10\");";
        alterTableWithNewParser(alterStmt3, false);
        String alterStmt4 = "alter table test." + tableName + " set (\"dynamic_partition.end\" = \"10\");";
        alterTableWithNewParser(alterStmt4, false);
        String alterStmt5 = "alter table test." + tableName + " set (\"dynamic_partition.prefix\" = \"pp\");";
        alterTableWithNewParser(alterStmt5, false);
        String alterStmt6 = "alter table test." + tableName + " set (\"dynamic_partition.buckets\" = \"5\");";
        alterTableWithNewParser(alterStmt6, false);
    }

    @Test
    public void testDynamicPartitionTableMetaFailed() throws Exception {
        String tableName = "dynamic_table_test";
        String createOlapTblStmt = "CREATE TABLE test.`" + tableName + "` (\n" +
                    "  `k1` date NULL COMMENT \"\",\n" +
                    "  `k2` int NULL COMMENT \"\",\n" +
                    "  `k3` smallint NULL COMMENT \"\",\n" +
                    "  `v1` varchar(2048) NULL COMMENT \"\",\n" +
                    "  `v2` datetime NULL COMMENT \"\"\n" +
                    ") ENGINE=OLAP\n" +
                    "DUPLICATE KEY(`k1`, `k2`, `k3`)\n" +
                    "COMMENT \"OLAP\"\n" +
                    "PARTITION BY RANGE (k1)\n" +
                    "(\n" +
                    "PARTITION p1 VALUES LESS THAN (\"2014-01-01\"),\n" +
                    "PARTITION p2 VALUES LESS THAN (\"2014-06-01\"),\n" +
                    "PARTITION p3 VALUES LESS THAN (\"2014-12-01\")\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(`k1`) BUCKETS 32\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\",\n" +
                    "\"dynamic_partition.enable\" = \"true\",\n" +
                    "\"dynamic_partition.start\" = \"-3\",\n" +
                    "\"dynamic_partition.end\" = \"3\",\n" +
                    "\"dynamic_partition.time_unit\" = \"day\",\n" +
                    "\"dynamic_partition.prefix\" = \"p\",\n" +
                    "\"dynamic_partition.buckets\" = \"1\"\n" +
                    ");";
        createTable(createOlapTblStmt);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getTable(tableName);
        olapTable.getTableProperty().getProperties().remove("dynamic_partition.end");
        olapTable.getTableProperty().gsonPostProcess();
    }

    @Test
    public void testSwapTable() throws Exception {
        String stmt1 = "CREATE TABLE test.replace1\n" +
                    "(\n" +
                    "    k1 int, k2 int, k3 int sum\n" +
                    ")\n" +
                    "AGGREGATE KEY(k1, k2)\n" +
                    "DISTRIBUTED BY HASH(k1) BUCKETS 10\n" +
                    "rollup (\n" +
                    "r1(k1),\n" +
                    "r2(k2, k3)\n" +
                    ")\n" +
                    "PROPERTIES(\"replication_num\" = \"1\");";

        String stmt2 = "CREATE TABLE test.r1\n" +
                    "(\n" +
                    "    k1 int, k2 int\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k1) BUCKETS 11\n" +
                    "PROPERTIES(\"replication_num\" = \"1\");";

        String stmt3 = "CREATE TABLE test.replace2\n" +
                    "(\n" +
                    "    k1 int, k2 int\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k1) BUCKETS 11\n" +
                    "PROPERTIES(\"replication_num\" = \"1\");";

        String stmt4 = "CREATE TABLE test.replace3\n" +
                    "(\n" +
                    "    k1 int, k2 int, k3 int sum\n" +
                    ")\n" +
                    "PARTITION BY RANGE(k1)\n" +
                    "(\n" +
                    "\tPARTITION p1 values less than(\"100\"),\n" +
                    "\tPARTITION p2 values less than(\"200\")\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                    "rollup (\n" +
                    "r3(k1),\n" +
                    "r4(k2, k3)\n" +
                    ")\n" +
                    "PROPERTIES(\"replication_num\" = \"1\");";

        createTable(stmt1);
        createTable(stmt2);
        createTable(stmt3);
        createTable(stmt4);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        // name conflict
        String replaceStmt = "ALTER TABLE test.replace1 SWAP WITH r1";
        alterTableWithNewParser(replaceStmt, true);

        // replace1 with replace2
        replaceStmt = "ALTER TABLE test.replace1 SWAP WITH replace2";
        OlapTable replace1 =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "replace1");
        OlapTable replace2 =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "replace2");
        Assertions.assertEquals(3,
                    replace1.getPartition("replace1").getDefaultPhysicalPartition()
                            .getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        Assertions.assertEquals(1,
                    replace2.getPartition("replace2").getDefaultPhysicalPartition()
                            .getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());

        alterTableWithNewParser(replaceStmt, false);

        replace1 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "replace1");
        replace2 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "replace2");
        Assertions.assertEquals(1,
                    replace1.getPartition("replace1").getDefaultPhysicalPartition()
                            .getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        Assertions.assertEquals(3,
                    replace2.getPartition("replace2").getDefaultPhysicalPartition()
                            .getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        Assertions.assertEquals("replace1", replace1.getIndexNameById(replace1.getBaseIndexId()));
        Assertions.assertEquals("replace2", replace2.getIndexNameById(replace2.getBaseIndexId()));
    }

    @Test
    public void testSwapTableWithUniqueConstraints() throws Exception {
        String s1 = "CREATE TABLE test.s1 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'unique_constraints'='test.s1.k1');";
        String s2 = "CREATE TABLE test.s2 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'unique_constraints'='test.s2.k1');";

        createTable(s1);
        createTable(s2);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String replaceStmt = "ALTER TABLE s1 SWAP WITH s2";
        alterTableWithNewParser(replaceStmt, false);

        OlapTable tbl1 = (OlapTable) db.getTable("s1");
        List<UniqueConstraint> uk1 = tbl1.getUniqueConstraints();
        Assertions.assertEquals(1, uk1.size());
        UniqueConstraint uk10 = uk1.get(0);
        Assertions.assertEquals("s1", uk10.getTableName());

        OlapTable tbl2 = (OlapTable) db.getTable("s2");
        List<UniqueConstraint> uk2 = tbl2.getUniqueConstraints();
        Assertions.assertEquals(1, uk2.size());
        UniqueConstraint uk20 = uk2.get(0);
        Assertions.assertEquals("s2", uk20.getTableName());
        starRocksAssert.dropTable("s1");
        starRocksAssert.dropTable("s2");
    }

    @Test
    public void testSwapTableWithForeignConstraints1() throws Exception {
        String s1 = "CREATE TABLE test.s1 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'unique_constraints'='test.s1.k1');";
        String s2 = "CREATE TABLE test.s2 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'foreign_key_constraints'='s2(k1) REFERENCES s1(k1)');";
        String s3 = "CREATE TABLE test.s3 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'foreign_key_constraints'='s3(k1) REFERENCES s1(k1)');";
        createTable(s1);
        createTable(s2);
        createTable(s3);

        String mvSql = "create materialized view test_mv12\n" +
                "REFRESH DEFERRED MANUAL\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "     'foreign_key_constraints'='s2(k1) REFERENCES s1(k1)',\n" +
                "     'unique_constraints'='s1.k1'\n" +
                ") \n" +
                "as select s1.k1 as s11, s1.k2 as s12, s1.k3 as s13, s2.k1 s21, s2.k2 s22, s2.k3 s23 from s1 join s2 " +
                "on s1.k1 = s2.k1;";
        starRocksAssert.withMaterializedView(mvSql);

        MaterializedView mv = starRocksAssert.getMv("test", "test_mv12");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        // swap child tables
        String replaceStmt = "ALTER TABLE s2 SWAP WITH s3";
        alterTableWithNewParser(replaceStmt, false);

        OlapTable tbl1 = (OlapTable) db.getTable("s1");
        List<UniqueConstraint> uk1 = tbl1.getUniqueConstraints();
        Assertions.assertEquals(1, uk1.size());
        UniqueConstraint uk10 = uk1.get(0);
        Assertions.assertEquals("s1", uk10.getTableName());

        OlapTable tbl2 = (OlapTable) db.getTable("s2");
        List<ForeignKeyConstraint> fk2 = tbl2.getForeignKeyConstraints();
        Assertions.assertEquals(1, fk2.size());
        ForeignKeyConstraint fk20 = fk2.get(0);
        BaseTableInfo baseTableInfo20 = fk20.getChildTableInfo();
        Assertions.assertTrue(baseTableInfo20 == null);
        BaseTableInfo parentTableInfo = fk20.getParentTableInfo();
        Assertions.assertTrue(parentTableInfo != null);
        Assertions.assertEquals("s1", parentTableInfo.getTableName());
        Assertions.assertEquals(tbl1.getId(), parentTableInfo.getTableId());

        OlapTable tbl3 = (OlapTable) db.getTable("s3");
        List<ForeignKeyConstraint> fk3 = tbl3.getForeignKeyConstraints();
        Assertions.assertEquals(1, fk3.size());
        ForeignKeyConstraint fk30 = fk3.get(0);
        BaseTableInfo baseTableInfo30 = fk30.getChildTableInfo();
        Assertions.assertTrue(baseTableInfo30 == null);
        parentTableInfo = fk30.getParentTableInfo();
        Assertions.assertTrue(parentTableInfo != null);
        Assertions.assertEquals("s1", parentTableInfo.getTableName());
        Assertions.assertEquals(tbl1.getId(), parentTableInfo.getTableId());

        starRocksAssert.alterMvProperties("ALTER materialized view test_mv12 active;");

        Assertions.assertTrue(mv.isActive());
        List<ForeignKeyConstraint> mvFKs = mv.getForeignKeyConstraints();
        List<UniqueConstraint> mvUKs = mv.getUniqueConstraints();
        Assertions.assertTrue(CollectionUtils.isEmpty(mvFKs));
        Assertions.assertTrue(CollectionUtils.isEmpty(mvUKs));

        // test global constraint manager
        GlobalConstraintManager cm = GlobalStateMgr.getCurrentState().getGlobalConstraintManager();
        Assertions.assertTrue(cm != null);

        Set<TableWithFKConstraint> tableWithFKConstraintSet = cm.getRefConstraints(tbl1);
        Assertions.assertTrue(tableWithFKConstraintSet != null);
        Assertions.assertTrue(tableWithFKConstraintSet.size() == 3);
        Assertions.assertTrue(tableWithFKConstraintSet.contains(TableWithFKConstraint.of(tbl2, fk20)));
        Assertions.assertTrue(tableWithFKConstraintSet.contains(TableWithFKConstraint.of(tbl3, fk30)));

        starRocksAssert.dropTable("s1");
        starRocksAssert.dropTable("s2");
        starRocksAssert.dropTable("s3");
    }

    @Test
    public void testSwapTableWithForeignConstraints2() throws Exception {
        String s1 = "CREATE TABLE test.s1 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'unique_constraints'='test.s1.k1');";
        String s2 = "CREATE TABLE test.s2 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'unique_constraints'='test.s2.k1');";
        String s3 = "CREATE TABLE test.s3 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'foreign_key_constraints'='s3(k1) REFERENCES s1(k1)');";
        createTable(s1);
        createTable(s2);
        createTable(s3);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        // swap parent tables
        String replaceStmt = "ALTER TABLE s2 SWAP WITH s1";
        alterTableWithNewParser(replaceStmt, false);

        OlapTable tbl1 = (OlapTable) db.getTable("s1");
        List<UniqueConstraint> uk1 = tbl1.getUniqueConstraints();
        Assertions.assertEquals(1, uk1.size());
        UniqueConstraint uk10 = uk1.get(0);
        Assertions.assertEquals("s1", uk10.getTableName());

        OlapTable tbl2 = (OlapTable) db.getTable("s2");
        List<UniqueConstraint> uk2 = tbl2.getUniqueConstraints();
        Assertions.assertEquals(1, uk2.size());
        UniqueConstraint uk20 = uk2.get(0);
        Assertions.assertEquals("s2", uk20.getTableName());

        OlapTable tbl3 = (OlapTable) db.getTable("s3");
        List<ForeignKeyConstraint> fk3 = tbl3.getForeignKeyConstraints();
        Assertions.assertEquals(1, fk3.size());
        ForeignKeyConstraint fk30 = fk3.get(0);
        BaseTableInfo baseTableInfo30 = fk30.getChildTableInfo();
        Assertions.assertTrue(baseTableInfo30 == null);
        BaseTableInfo parentTableInfo = fk30.getParentTableInfo();
        Assertions.assertTrue(parentTableInfo != null);
        Assertions.assertEquals("s1", parentTableInfo.getTableName());
        Assertions.assertEquals(tbl1.getId(), parentTableInfo.getTableId());

        // test global constraint manager
        GlobalConstraintManager cm = GlobalStateMgr.getCurrentState().getGlobalConstraintManager();
        Assertions.assertTrue(cm != null);

        Set<TableWithFKConstraint> tableWithFKConstraintSet = cm.getRefConstraints(tbl1);
        Assertions.assertTrue(tableWithFKConstraintSet != null);
        Assertions.assertTrue(tableWithFKConstraintSet.size() == 1);
        TableWithFKConstraint expect = tableWithFKConstraintSet.iterator().next();
        Assertions.assertTrue(expect.getChildTable().equals(tbl3));
        Assertions.assertTrue(expect.getRefConstraint().equals(fk30));

        starRocksAssert.dropTable("s1");
        starRocksAssert.dropTable("s2");
        starRocksAssert.dropTable("s3");
    }

    @Test
    public void testSwapTableWithForeignConstraints3() throws Exception {
        String s1 = "CREATE TABLE test.s1 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'unique_constraints'='test.s1.k1');";
        String s2 = "CREATE TABLE test.s2 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\");";
        String s3 = "CREATE TABLE test.s3 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'foreign_key_constraints'='s3(k1) REFERENCES s1(k1)');";
        createTable(s1);
        createTable(s2);
        createTable(s3);
        // swap parent tables
        String replaceStmt = "ALTER TABLE s2 SWAP WITH s1";
        alterTableWithNewParser(replaceStmt, true);

        starRocksAssert.dropTable("s1");
        starRocksAssert.dropTable("s2");
        starRocksAssert.dropTable("s3");
    }

    @Test
    public void testSwapTableWithForeignConstraints4() throws Exception {
        String s1 = "CREATE TABLE test.s1 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'unique_constraints'='test.s1.k1');";
        String s2 = "CREATE TABLE test.s2 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\", 'foreign_key_constraints'='s2(k1) REFERENCES s1(k1)');";
        String s3 = "CREATE TABLE test.s3 \n" +
                "(\n" +
                "    k1 int, k2 int, k3 int\n" +
                ")\n" +
                "DUPLICATE KEY(k1, k2)\n" +
                "DISTRIBUTED BY RANDOM \n" +
                "PROPERTIES(\"replication_num\" = \"1\");";
        createTable(s1);
        createTable(s2);
        createTable(s3);
        // swap child tables
        String replaceStmt = "ALTER TABLE s2 SWAP WITH s3";
        alterTableWithNewParser(replaceStmt, true);

        starRocksAssert.dropTable("s1");
        starRocksAssert.dropTable("s2");
        starRocksAssert.dropTable("s3");
    }

    @Test
    public void testCatalogAddPartitionsDay() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    "    START (\"20140101\") END (\"20140104\") EVERY (INTERVAL 1 DAY)\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                    "    PARTITIONS START (\"2017-01-03\") END (\"2017-01-07\") EVERY (interval 1 day)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition");

        Assertions.assertNotNull(table.getPartition("p20170103"));
        Assertions.assertNotNull(table.getPartition("p20170104"));
        Assertions.assertNotNull(table.getPartition("p20170105"));
        Assertions.assertNotNull(table.getPartition("p20170106"));
        Assertions.assertNull(table.getPartition("p20170107"));

        dropSQL = "drop table test_partition";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

    }

    @Test
    public void testAddPhysicalPartition() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "DISTRIBUTED BY RANDOM BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition");
        Optional<Partition> partition = table.getPartitions().stream().findFirst();
        Assertions.assertTrue(partition.isPresent());
        Assertions.assertEquals(table.getPhysicalPartitions().size(), 1);

        GlobalStateMgr.getCurrentState().getLocalMetastore().addSubPartitions(db, table, partition.get(), 1,
                    WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals(partition.get().getSubPartitions().size(), 2);
        Assertions.assertEquals(table.getPhysicalPartitions().size(), 2);

        GlobalStateMgr.getCurrentState().getLocalMetastore().addSubPartitions(db, table, partition.get(), 2,
                    WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals(partition.get().getSubPartitions().size(), 4);
        Assertions.assertEquals(table.getPhysicalPartitions().size(), 4);

        for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
            Assertions.assertEquals(physicalPartition.getVisibleVersion(), 1);
            Assertions.assertEquals(physicalPartition.getParentId(), partition.get().getId());
            Assertions.assertNotNull(physicalPartition.getBaseIndex());
            Assertions.assertFalse(physicalPartition.isImmutable());
            Assertions.assertEquals(physicalPartition.getShardGroupId(), PhysicalPartition.INVALID_SHARD_GROUP_ID);
            Assertions.assertTrue(physicalPartition.hasStorageData());
            Assertions.assertFalse(physicalPartition.isFirstLoad());
        }

        dropSQL = "drop table test_partition";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testAddRangePhysicalPartition() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    "    START (\"20140101\") END (\"20140104\") EVERY (INTERVAL 1 DAY)\n" +
                    ")\n" +
                    "DISTRIBUTED BY RANDOM BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition");
        Assertions.assertEquals(table.getPhysicalPartitions().size(), 3);

        Partition partition = table.getPartition("p20140101");
        Assertions.assertNotNull(partition);

        GlobalStateMgr.getCurrentState().getLocalMetastore().addSubPartitions(db, table, partition, 1,
                    WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals(table.getPhysicalPartitions().size(), 4);
        Assertions.assertEquals(partition.getSubPartitions().size(), 2);

        partition = table.getPartition("p20140103");
        Assertions.assertNotNull(partition);

        GlobalStateMgr.getCurrentState().getLocalMetastore().addSubPartitions(db, table, partition, 2,
                    WarehouseManager.DEFAULT_RESOURCE);
        Assertions.assertEquals(table.getPhysicalPartitions().size(), 6);
        Assertions.assertEquals(partition.getSubPartitions().size(), 3);

        dropSQL = "drop table test_partition";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testAddPhysicalPartitionForHash() {
        assertThrows(DdlException.class, () -> {
            ConnectContext ctx = starRocksAssert.getCtx();
            String dropSQL = "drop table if exists test_partition";
            DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
            String createSQL = "CREATE TABLE test.test_partition (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

            CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
            StarRocksAssert.utCreateTableWithRetry(createTableStmt);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition");
            Optional<Partition> partition = table.getPartitions().stream().findFirst();
            Assertions.assertTrue(partition.isPresent());
            Assertions.assertEquals(table.getPhysicalPartitions().size(), 1);

            GlobalStateMgr.getCurrentState().getLocalMetastore().addSubPartitions(db, table, partition.get(), 1,
                    WarehouseManager.DEFAULT_RESOURCE);
        });
    }

    @Test
    public void testAddBackend() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();

        String addBackendSql = "ALTER SYSTEM ADD BACKEND \"192.168.1.1:8080\",\"192.168.1.2:8080\"";
        AlterSystemStmt addBackendStmt = (AlterSystemStmt) UtFrameUtils.parseStmtWithNewParser(addBackendSql, ctx);

        String dropBackendSql = "ALTER SYSTEM DROP BACKEND \"192.168.1.1:8080\",\"192.168.1.2:8080\"";
        AlterSystemStmt dropBackendStmt = (AlterSystemStmt) UtFrameUtils.parseStmtWithNewParser(dropBackendSql, ctx);

        String addObserverSql = "ALTER SYSTEM ADD OBSERVER \"192.168.1.1:8080\"";
        AlterSystemStmt addObserverStmt = (AlterSystemStmt) UtFrameUtils.parseStmtWithNewParser(addObserverSql, ctx);

        String dropObserverSql = "ALTER SYSTEM DROP OBSERVER \"192.168.1.1:8080\"";
        AlterSystemStmt dropObserverStmt = (AlterSystemStmt) UtFrameUtils.parseStmtWithNewParser(dropObserverSql, ctx);

        String addFollowerSql = "ALTER SYSTEM ADD FOLLOWER \"192.168.1.1:8080\"";
        AlterSystemStmt addFollowerStmt = (AlterSystemStmt) UtFrameUtils.parseStmtWithNewParser(addFollowerSql, ctx);

        String dropFollowerSql = "ALTER SYSTEM DROP FOLLOWER \"192.168.1.1:8080\"";
        AlterSystemStmt dropFollowerStmt = (AlterSystemStmt) UtFrameUtils.parseStmtWithNewParser(dropFollowerSql, ctx);
    }

    @Test
    public void testCatalogAddPartitions5Day() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                    "    PARTITIONS START (\"2017-01-03\") END (\"2017-01-15\") EVERY (interval 5 day)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition");

        Assertions.assertNotNull(table.getPartition("p20170103"));
        Assertions.assertNotNull(table.getPartition("p20170108"));
        Assertions.assertNotNull(table.getPartition("p20170113"));

        String dropSQL = "drop table test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsDayConflictException() {
        assertThrows(AnalysisException.class, () -> {
            ConnectContext ctx = starRocksAssert.getCtx();
            String createSQL = "CREATE TABLE test.test_partition_exception (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    "    START (\"20140101\") END (\"20140104\") EVERY (INTERVAL 1 DAY)\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

            CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
            StarRocksAssert.utCreateTableWithRetry(createTableStmt);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

            String alterSQL = "ALTER TABLE test_partition_exception ADD\n" +
                    "    PARTITIONS START (\"2014-01-01\") END (\"2014-01-04\") EVERY (interval 1 day)";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
            AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition_exception", addPartitionClause);
        });
    }

    @Test
    public void testCatalogAddPartitionsWeekWithoutCheck() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        Config.enable_create_partial_partition_in_batch = true;
        String createSQL = "CREATE TABLE test.test_partition_week (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String alterSQL = "ALTER TABLE test_partition_week ADD\n" +
                    "    PARTITIONS START (\"2017-03-25\") END (\"2017-04-10\") EVERY (interval 1 week)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition_week", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition_week");

        Assertions.assertNotNull(table.getPartition("p2017_12"));
        Assertions.assertNotNull(table.getPartition("p2017_13"));
        Assertions.assertNotNull(table.getPartition("p2017_14"));

        String dropSQL = "drop table test_partition_week";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        Config.enable_create_partial_partition_in_batch = false;
    }

    @Test
    public void testCatalogAddPartitionsWeekWithCheck() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition_week (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String alterSQL = "ALTER TABLE test_partition_week ADD\n" +
                    "    PARTITIONS START (\"2017-03-20\") END (\"2017-04-10\") EVERY (interval 1 week)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition_week", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition_week");

        Assertions.assertNotNull(table.getPartition("p2017_12"));
        Assertions.assertNotNull(table.getPartition("p2017_13"));
        Assertions.assertNotNull(table.getPartition("p2017_14"));

        String dropSQL = "drop table test_partition_week";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsMonth() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                    "    PARTITIONS START (\"2017-01-01\") END (\"2017-04-01\") EVERY (interval 1 month)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition");

        Assertions.assertNotNull(table.getPartition("p201701"));
        Assertions.assertNotNull(table.getPartition("p201702"));
        Assertions.assertNotNull(table.getPartition("p201703"));
        Assertions.assertNull(table.getPartition("p201704"));

        dropSQL = "drop table test_partition";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsYear() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                    "    PARTITIONS START (\"2017-01-01\") END (\"2020-01-01\") EVERY (interval 1 YEAR)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition");

        Assertions.assertNotNull(table.getPartition("p2017"));
        Assertions.assertNotNull(table.getPartition("p2018"));
        Assertions.assertNotNull(table.getPartition("p2019"));
        Assertions.assertNull(table.getPartition("p2020"));

        dropSQL = "drop table test_partition";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsNumber() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                    "      k2 INT,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                    "    PARTITIONS START (\"1\") END (\"4\") EVERY (1)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition");

        Assertions.assertNotNull(table.getPartition("p1"));
        Assertions.assertNotNull(table.getPartition("p2"));
        Assertions.assertNotNull(table.getPartition("p3"));
        Assertions.assertNull(table.getPartition("p4"));

        dropSQL = "drop table test_partition";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsAtomicRange() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        String createSQL = "CREATE TABLE test_partition (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    "    START (\"20140101\") END (\"20150101\") EVERY (INTERVAL 1 YEAR)\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        try {
            String alterSQL = "ALTER TABLE test_partition ADD\n" +
                        "          PARTITIONS START (\"2014-01-01\") END (\"2014-01-06\") EVERY (interval 1 day);";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
            AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition", addPartitionClause);
            Assertions.fail();
        } catch (AnalysisException ex) {

        }

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition");

        Assertions.assertNull(table.getPartition("p20140101"));
        Assertions.assertNull(table.getPartition("p20140102"));
        Assertions.assertNull(table.getPartition("p20140103"));

        dropSQL = "drop table test_partition";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

    }

    public void testCatalogAddPartitionsZeroDay() throws Exception {

        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test_partition_0day (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    "    START (\"20140104\") END (\"20150104\") EVERY (INTERVAL 1 YEAR)\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        try {
            String alterSQL = "ALTER TABLE test_partition_0day ADD\n" +
                        "          PARTITIONS START (\"2014-01-01\") END (\"2014-01-06\") EVERY (interval 0 day);";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
            AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition_0day", addPartitionClause);
            Assertions.fail();
        } catch (AnalysisException ex) {

        }

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition_0day");

        Assertions.assertNull(table.getPartition("p20140101"));
        Assertions.assertNull(table.getPartition("p20140102"));
        Assertions.assertNull(table.getPartition("p20140103"));

        String dropSQL = "drop table test_partition_0day";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);

    }

    @Test
    public void testCatalogAddPartitionsWithoutPartitions() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        String createSQL = "CREATE TABLE test_partition (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    "    START (\"20140101\") END (\"20150101\") EVERY (INTERVAL 1 YEAR)\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String alterSQL = "ALTER TABLE test_partition ADD\n" +
                    "         START (\"2015-01-01\") END (\"2015-01-06\") EVERY (interval 1 day);";
        try {
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
            AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition", addPartitionClause);
            Assertions.fail();
        } catch (AnalysisException ex) {

        }

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition");

        Assertions.assertNull(table.getPartition("p20140101"));
        Assertions.assertNull(table.getPartition("p20140102"));
        Assertions.assertNull(table.getPartition("p20140103"));

        dropSQL = "drop table test_partition";
        dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsIfNotExist() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test_partition_exists (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    "    START (\"20140101\") END (\"20150101\") EVERY (INTERVAL 1 YEAR)\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String alterSQL =
                    "ALTER TABLE test_partition_exists ADD PARTITION IF NOT EXISTS p20210701 VALUES LESS THAN ('2021-07-01')";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition_exists", addPartitionClause);

        String alterSQL2 =
                    "ALTER TABLE test_partition_exists ADD PARTITION IF NOT EXISTS p20210701 VALUES LESS THAN ('2021-07-02')";
        AlterTableStmt alterTableStmt2 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL2, ctx);
        AddPartitionClause addPartitionClause2 = (AddPartitionClause) alterTableStmt2.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition_exists", addPartitionClause2);

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition_exists");

        Assertions.assertEquals(2, table.getPartitions().size());

        String dropSQL = "drop table test_partition_exists";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsSameNameShouldNotThrowError() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test_partition_exists2 (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    "    START (\"20140101\") END (\"20150101\") EVERY (INTERVAL 1 YEAR)\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String alterSQL =
                    "ALTER TABLE test_partition_exists2 ADD PARTITION IF NOT EXISTS p20210701 VALUES LESS THAN ('2021-07-01')";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition_exists2", addPartitionClause);

        String alterSQL2 =
                    "ALTER TABLE test_partition_exists2 ADD PARTITION IF NOT EXISTS p20210701 VALUES LESS THAN ('2021-07-01')";
        AlterTableStmt alterTableStmt2 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL2, ctx);
        AddPartitionClause addPartitionClause2 = (AddPartitionClause) alterTableStmt2.getAlterClauseList().get(0);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition_exists2", addPartitionClause2);

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition_exists2");

        Assertions.assertEquals(2, table.getPartitions().size());

        String dropSQL = "drop table test_partition_exists2";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testCatalogAddPartitionsShouldThrowError() {
        assertThrows(AnalysisException.class, () -> {
            ConnectContext ctx = starRocksAssert.getCtx();
            String createSQL = "CREATE TABLE test_partition_exists3 (\n" +
                    "      k2 DATE,\n" +
                    "      k3 SMALLINT,\n" +
                    "      v1 VARCHAR(2048),\n" +
                    "      v2 DATETIME DEFAULT \"2014-02-04 15:36:00\"\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(k2, k3)\n" +
                    "PARTITION BY RANGE (k2) (\n" +
                    "    START (\"20140101\") END (\"20150101\") EVERY (INTERVAL 1 YEAR)\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(k2) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

            CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
            StarRocksAssert.utCreateTableWithRetry(createTableStmt);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

            String alterSQL = "ALTER TABLE test_partition_exists3 ADD PARTITION p20210701 VALUES LESS THAN ('2021-07-01')";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL, ctx);
            AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition_exists3", addPartitionClause);

            String alterSQL2 = "ALTER TABLE test_partition_exists3 ADD PARTITION p20210701 VALUES LESS THAN ('2021-07-01')";
            AlterTableStmt alterTableStmt2 = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterSQL2, ctx);
            AddPartitionClause addPartitionClause2 = (AddPartitionClause) alterTableStmt2.getAlterClauseList().get(0);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition_exists3", addPartitionClause2);

            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition_exists3");

            Assertions.assertEquals(2, ((OlapTable) table).getPartitions().size());

            String dropSQL = "drop table test_partition_exists3";
            DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        });
    }

    @Test
    public void testRenameDb() throws Exception {
        String createUserSql = "CREATE USER 'testuser' IDENTIFIED BY ''";
        CreateUserStmt createUserStmt =
                    (CreateUserStmt) UtFrameUtils.parseStmtWithNewParser(createUserSql, starRocksAssert.getCtx());
        AuthenticationMgr authenticationManager =
                    starRocksAssert.getCtx().getGlobalStateMgr().getAuthenticationMgr();
        authenticationManager.createUser(createUserStmt);

        String sql = "grant ALTER on database test to testuser";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx()),
                    starRocksAssert.getCtx());

        UserIdentity testUser = new UserIdentity("testuser", "%");
        testUser.analyze();

        starRocksAssert.getCtx().setQualifiedUser("testuser");
        starRocksAssert.getCtx().setCurrentUserIdentity(testUser);
        starRocksAssert.getCtx().setCurrentRoleIds(
                    GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdsByUser(testUser));
        starRocksAssert.getCtx().setRemoteIP("%");

        starRocksAssert.withDatabase("test_to_rename");
        String renameDb = "alter database test_to_rename rename test_to_rename_2";
        AlterDatabaseRenameStatement renameDbStmt =
                    (AlterDatabaseRenameStatement) UtFrameUtils.parseStmtWithNewParser(renameDb, starRocksAssert.getCtx());
        DDLStmtExecutor.execute(renameDbStmt, starRocksAssert.getCtx());
    }

    @Test
    public void testAddMultiItemListPartition() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                    "      id BIGINT,\n" +
                    "      age SMALLINT,\n" +
                    "      dt VARCHAR(10) not null,\n" +
                    "      province VARCHAR(64) not null\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(id)\n" +
                    "PARTITION BY LIST (dt,province) (\n" +
                    "     PARTITION p1 VALUES IN ((\"2022-04-01\", \"beijing\"),(\"2022-04-01\", \"chongqing\")),\n" +
                    "     PARTITION p2 VALUES IN ((\"2022-04-01\", \"shanghai\")) \n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        List<String> values = Lists.newArrayList("2022-04-01", "shandong");
        List<List<String>> multiValues = Lists.newArrayList();
        multiValues.add(values);
        PartitionDesc partitionDesc = new MultiItemListPartitionDesc(false, "p3", multiValues, new HashMap<>());
        AddPartitionClause addPartitionClause = new AddPartitionClause(partitionDesc, null, new HashMap<>(), false);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test")
                    .getTable("test_partition");

        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        analyzer.analyze(Util.getOrCreateInnerContext(), addPartitionClause);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition", addPartitionClause);

        ListPartitionInfo partitionInfo = (ListPartitionInfo) table.getPartitionInfo();
        Map<Long, List<List<String>>> idToValues = partitionInfo.getIdToMultiValues();

        long id3 = table.getPartition("p3").getId();
        List<List<String>> list3 = idToValues.get(id3);
        Assertions.assertEquals("2022-04-01", list3.get(0).get(0));
        Assertions.assertEquals("shandong", list3.get(0).get(1));

        String dropSQL = "drop table test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testModifyPartitionBucket() {
        assertThrows(AlterJobException.class, () -> {
            ConnectContext ctx = starRocksAssert.getCtx();
            String createSQL = "CREATE TABLE modify_bucket (\n" +
                    "  chuangyi varchar(65533) NULL COMMENT \"创意\",\n" +
                    "  guanggao varchar(65533) NULL COMMENT \"广告\"\n" +
                    ") ENGINE=OLAP\n" +
                    "DUPLICATE KEY(chuangyi, guanggao)\n" +
                    "COMMENT \"OLAP\"\n" +
                    "DISTRIBUTED BY HASH(chuangyi, guanggao) BUCKETS 3\n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\"\n" +
                    ");";
            CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
            StarRocksAssert.utCreateTableWithRetry(createTableStmt);
            String stmt = "alter table modify_bucket set (\"dynamic_partition.buckets\" = \"10\");\n";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
            GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt);
        });
    }

    @Test
    public void testAddSingleItemListPartition() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                    "      id BIGINT,\n" +
                    "      age SMALLINT,\n" +
                    "      dt VARCHAR(10),\n" +
                    "      province VARCHAR(64) not null\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(id)\n" +
                    "PARTITION BY LIST (province) (\n" +
                    "     PARTITION p1 VALUES IN (\"beijing\",\"chongqing\") ,\n" +
                    "     PARTITION p2 VALUES IN (\"guangdong\") \n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        List<String> values = Lists.newArrayList("shanxi", "shanghai");
        PartitionDesc partitionDesc = new SingleItemListPartitionDesc(false, "p3", values, new HashMap<>());
        AddPartitionClause addPartitionClause = new AddPartitionClause(partitionDesc, null, new HashMap<>(), false);
        OlapTable table =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_partition");
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
        analyzer.analyze(Util.getOrCreateInnerContext(), addPartitionClause);
        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition", addPartitionClause);
        ListPartitionInfo partitionInfo = (ListPartitionInfo) table.getPartitionInfo();
        Map<Long, List<String>> idToValues = partitionInfo.getIdToValues();

        long id3 = table.getPartition("p3").getId();
        List<String> list3 = idToValues.get(id3);
        Assertions.assertEquals("shanxi", list3.get(0));
        Assertions.assertEquals("shanghai", list3.get(1));

        String dropSQL = "drop table test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testSingleItemPartitionPersistInfo() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                    "      id BIGINT,\n" +
                    "      age SMALLINT,\n" +
                    "      dt VARCHAR(10),\n" +
                    "      province VARCHAR(64) not null\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(id)\n" +
                    "PARTITION BY LIST (province) (\n" +
                    "     PARTITION p1 VALUES IN (\"beijing\",\"chongqing\") \n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_partition");
        ListPartitionInfo partitionInfo = (ListPartitionInfo) table.getPartitionInfo();

        long dbId = db.getId();
        long tableId = table.getId();
        Partition partition = table.getPartition("p1");
        long partitionId = partition.getId();
        List<String> values = partitionInfo.getIdToValues().get(partitionId);
        DataProperty dataProperty = partitionInfo.getDataProperty(partitionId);
        short replicationNum = partitionInfo.getReplicationNum(partitionId);
        boolean isInMemory = partitionInfo.getIsInMemory(partitionId);
        boolean isTempPartition = false;
        ListPartitionPersistInfo partitionPersistInfoOut = new ListPartitionPersistInfo(dbId, tableId, partition,
                    dataProperty, replicationNum, isInMemory, isTempPartition, values, new ArrayList<>(),
                    partitionInfo.getDataCacheInfo(partitionId));

        // replay log
        partitionInfo.setValues(partitionId, null);
        GlobalStateMgr.getCurrentState().getLocalMetastore().replayAddPartition(partitionPersistInfoOut);
        Assertions.assertNotNull(partitionInfo.getIdToValues().get(partitionId));

        String dropSQL = "drop table test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testMultiItemPartitionPersistInfo() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String createSQL = "CREATE TABLE test.test_partition (\n" +
                    "      id BIGINT,\n" +
                    "      age SMALLINT,\n" +
                    "      dt VARCHAR(10) not null,\n" +
                    "      province VARCHAR(64) not null\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(id)\n" +
                    "PARTITION BY LIST (dt , province) (\n" +
                    "     PARTITION p1 VALUES IN ((\"2022-04-01\", \"beijing\"),(\"2022-04-01\", \"chongqing\"))\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table =
                    (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_partition");
        ListPartitionInfo partitionInfo = (ListPartitionInfo) table.getPartitionInfo();

        long dbId = db.getId();
        long tableId = table.getId();
        Partition partition = table.getPartition("p1");
        long partitionId = partition.getId();
        List<List<String>> multiValues = partitionInfo.getIdToMultiValues().get(partitionId);
        DataProperty dataProperty = partitionInfo.getDataProperty(partitionId);
        short replicationNum = partitionInfo.getReplicationNum(partitionId);
        boolean isInMemory = partitionInfo.getIsInMemory(partitionId);
        boolean isTempPartition = false;
        ListPartitionPersistInfo partitionPersistInfoIn = new ListPartitionPersistInfo(dbId, tableId, partition,
                    dataProperty, replicationNum, isInMemory, isTempPartition, new ArrayList<>(), multiValues,
                    partitionInfo.getDataCacheInfo(partitionId));

        List<List<String>> assertMultiValues = partitionPersistInfoIn.asListPartitionPersistInfo().getMultiValues();
        Assertions.assertEquals(multiValues.size(), assertMultiValues.size());
        for (int i = 0; i < multiValues.size(); i++) {
            List<String> valueItem = multiValues.get(i);
            List<String> assertValueItem = assertMultiValues.get(i);
            for (int j = 0; j < valueItem.size(); j++) {
                Assertions.assertEquals(valueItem.get(i), assertValueItem.get(i));
            }
        }

        // replay log
        partitionInfo.setMultiValues(partitionId, null);
        GlobalStateMgr.getCurrentState().getLocalMetastore().replayAddPartition(partitionPersistInfoIn);
        Assertions.assertNotNull(partitionInfo.getIdToMultiValues().get(partitionId));

        String dropSQL = "drop table test_partition";
        DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
    }

    @Test
    public void testAddSingleListPartitionSamePartitionNameShouldThrowError() {
        assertThrows(SemanticException.class, () -> {
            ConnectContext ctx = starRocksAssert.getCtx();
            String createSQL = "CREATE TABLE test.test_partition_1 (\n" +
                    "      id BIGINT,\n" +
                    "      age SMALLINT,\n" +
                    "      dt VARCHAR(10),\n" +
                    "      province VARCHAR(64) not null\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(id)\n" +
                    "PARTITION BY LIST (province) (\n" +
                    "     PARTITION p1 VALUES IN (\"beijing\",\"chongqing\") ,\n" +
                    "     PARTITION p2 VALUES IN (\"guangdong\") \n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

            CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
            StarRocksAssert.utCreateTableWithRetry(createTableStmt);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

            List<String> values = Lists.newArrayList("shanxi", "heilongjiang");
            PartitionDesc partitionDesc = new SingleItemListPartitionDesc(false, "p1", values, new HashMap<>());
            AddPartitionClause addPartitionClause = new AddPartitionClause(partitionDesc, null, new HashMap<>(), false);

            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_partition_1");
            AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
            analyzer.analyze(Util.getOrCreateInnerContext(), addPartitionClause);

            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition_1", addPartitionClause);
        });
    }

    @Test
    public void testAddMultiListPartitionSamePartitionNameShouldThrowError() {
        assertThrows(SemanticException.class, () -> {
            ConnectContext ctx = starRocksAssert.getCtx();
            String createSQL = "CREATE TABLE test.test_partition_2 (\n" +
                    "      id BIGINT,\n" +
                    "      age SMALLINT,\n" +
                    "      dt VARCHAR(10) not null,\n" +
                    "      province VARCHAR(64) not null\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(id)\n" +
                    "PARTITION BY LIST (dt,province) (\n" +
                    "     PARTITION p1 VALUES IN ((\"2022-04-01\", \"beijing\"),(\"2022-04-01\", \"chongqing\")),\n" +
                    "     PARTITION p2 VALUES IN ((\"2022-04-01\", \"shanghai\")) \n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

            CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
            StarRocksAssert.utCreateTableWithRetry(createTableStmt);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

            List<String> values1 = Lists.newArrayList("2022-04-01", "beijing");
            List<String> values2 = Lists.newArrayList("2022-04-01", "chongqing");
            List<List<String>> multiValues = Lists.newArrayList();
            multiValues.add(values1);
            multiValues.add(values2);
            PartitionDesc partitionDesc = new MultiItemListPartitionDesc(false, "p1", multiValues, new HashMap<>());
            AddPartitionClause addPartitionClause = new AddPartitionClause(partitionDesc, null, new HashMap<>(), false);

            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_partition_2");
            AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
            analyzer.analyze(Util.getOrCreateInnerContext(), addPartitionClause);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "test_partition_2", addPartitionClause);
        });
    }

    @Test
    public void testAddSingleListPartitionSamePartitionValueShouldThrowError() {
        assertThrows(SemanticException.class, () -> {
            ConnectContext ctx = starRocksAssert.getCtx();
            String createSQL = "CREATE TABLE test.test_partition_3 (\n" +
                    "      id BIGINT,\n" +
                    "      age SMALLINT,\n" +
                    "      dt VARCHAR(10),\n" +
                    "      province VARCHAR(64) not null\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(id)\n" +
                    "PARTITION BY LIST (province) (\n" +
                    "     PARTITION p1 VALUES IN (\"beijing\",\"chongqing\") ,\n" +
                    "     PARTITION p2 VALUES IN (\"guangdong\") \n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

            CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
            StarRocksAssert.utCreateTableWithRetry(createTableStmt);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

            List<String> values = Lists.newArrayList("beijing", "chongqing");
            PartitionDesc partitionDesc = new SingleItemListPartitionDesc(false, "p3", values, new HashMap<>());
            AddPartitionClause addPartitionClause = new AddPartitionClause(partitionDesc, null, new HashMap<>(), false);
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_partition_3");
            AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
            analyzer.analyze(Util.getOrCreateInnerContext(), addPartitionClause);
        });
    }

    @Test
    public void testAddMultiItemListPartitionSamePartitionValueShouldThrowError() {
        assertThrows(SemanticException.class, () -> {
            ConnectContext ctx = starRocksAssert.getCtx();
            String createSQL = "CREATE TABLE test.test_partition_4 (\n" +
                    "      id BIGINT,\n" +
                    "      age SMALLINT,\n" +
                    "      dt VARCHAR(10) not null,\n" +
                    "      province VARCHAR(64) not null\n" +
                    ")\n" +
                    "ENGINE=olap\n" +
                    "DUPLICATE KEY(id)\n" +
                    "PARTITION BY LIST (dt, province) (\n" +
                    "     PARTITION p1 VALUES IN ((\"2022-04-01\", \"beijing\"),(\"2022-04-01\", \"chongqing\")),\n" +
                    "     PARTITION p2 VALUES IN ((\"2022-04-01\", \"shanghai\")) \n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(id) BUCKETS 10\n" +
                    "PROPERTIES (\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ")";

            CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
            StarRocksAssert.utCreateTableWithRetry(createTableStmt);
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

            List<String> values = Lists.newArrayList("2022-04-01", "shanghai");
            List<List<String>> multiValues = Lists.newArrayList();
            multiValues.add(values);
            PartitionDesc partitionDesc = new MultiItemListPartitionDesc(false, "p3", multiValues, new HashMap<>());
            AddPartitionClause addPartitionClause = new AddPartitionClause(partitionDesc, null, new HashMap<>(), false);

            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_partition_4");
            AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(table);
            analyzer.analyze(Util.getOrCreateInnerContext(), addPartitionClause);
        });
    }

    @Test
    public void testCatalogAddColumn() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                    .withTable("CREATE TABLE test.tbl1\n" +
                                "(\n" +
                                "    k1 date,\n" +
                                "    v1 int \n" +
                                ")\n" +
                                "DUPLICATE KEY(`k1`)" +
                                "DISTRIBUTED BY HASH (k1) BUCKETS 3\n" +
                                "PROPERTIES('replication_num' = '1');");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl1");

        String stmt = "alter table test.tbl1 add column k2 int";
        alterTableWithNewParser(stmt, false);
        waitSchemaChangeJobDone(false, tbl);

        stmt = "alter table test.tbl1 add column k3 int default '0' after k2";
        alterTableWithNewParser(stmt, false);
        waitSchemaChangeJobDone(false, tbl);

        stmt = "alter table test.tbl1 add column k4 int first";
        alterTableWithNewParserAndExceptionMsg(stmt, "Invalid column order. value should be after key. index[tbl1]");

        stmt = "alter table test.tbl1 add column k5 int after k3 in `testRollup`";
        alterTableWithNewParserAndExceptionMsg(stmt, "Index[testRollup] does not exist in table[tbl1]");

        Assertions.assertEquals(tbl.getColumns().size(), 4);
    }

    @Test
    public void testCatalogAddColumns() throws Exception {
        String stmt = "alter table test.tbl1 add column (`col1` int(11) not null default \"0\" comment \"\", "
                    + "`col2` int(11) not null default \"0\" comment \"\") in `testTable`;";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        AddColumnsClause clause = (AddColumnsClause) alterTableStmt.getAlterClauseList().get(0);
        Assertions.assertEquals(2, clause.getColumns().size());
        Assertions.assertEquals(0, clause.getProperties().size());
        Assertions.assertEquals("testTable", clause.getRollupName());

        stmt = "alter table test.tbl1 add column (`col1` int(11) not null default \"0\" comment \"\", "
                    + "`col2` int(11) not null default \"0\" comment \"\");";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        clause = (AddColumnsClause) alterTableStmt.getAlterClauseList().get(0);
        Assertions.assertEquals(null, clause.getRollupName());
    }

    @Test
    public void testCreateTemporaryPartitionInBatch() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        starRocksAssert.withDatabase("test2");
        String createSQL = "CREATE TABLE test2.site_access(\n" +
                    "    event_day datetime,\n" +
                    "    site_id INT DEFAULT '10',\n" +
                    "    city_code VARCHAR(100),\n" +
                    "    user_name VARCHAR(32) DEFAULT '',\n" +
                    "    pv BIGINT DEFAULT '0'\n" +
                    ")\n" +
                    "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                    "PARTITION BY date_trunc('day', event_day)(\n" +
                    " START (\"2023-03-27\") END (\"2023-03-30\") EVERY (INTERVAL 1 day)\n" +
                    ")\n" +
                    "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                    "PROPERTIES(\n" +
                    "    \"replication_num\" = \"1\"\n" +
                    ");";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test2");

        String sql = "alter table test2.site_access add TEMPORARY partitions " +
                    "START (\"2023-03-27\") END (\"2023-03-30\") EVERY (INTERVAL 1 day);";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);

        GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .addPartitions(Util.getOrCreateInnerContext(), db, "site_access", addPartitionClause);

        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test2")
                    .getTable("site_access");
        OlapTable olapTable = (OlapTable) table;
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        Assertions.assertEquals(3, rangePartitionInfo.getIdToRange(true).size());

    }

    @Test
    public void testCreateTemporaryPartitionInBatchHourly() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        starRocksAssert.withDatabase("test2");
        String createSQL = "CREATE TABLE test2.site_access_hourly(\n" +
                "    event_hour datetime,\n" +
                "    site_id INT DEFAULT '10',\n" +
                "    city_code VARCHAR(100),\n" +
                "    user_name VARCHAR(32) DEFAULT '',\n" +
                "    pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_hour, site_id, city_code, user_name)\n" +
                "PARTITION BY date_trunc('hour', event_hour)(\n" +
                " START (\"2023-03-27\") END (\"2023-03-30\") EVERY (INTERVAL 1 hour)\n" +
                ")\n" +
                "DISTRIBUTED BY HASH(event_hour, site_id) BUCKETS 32\n" +
                "PROPERTIES(\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");";

        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(createSQL, ctx);
        StarRocksAssert.utCreateTableWithRetry(createTableStmt);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test2");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test2")
                .getTable("site_access_hourly");
        OlapTable olapTable = (OlapTable) table;

        // We use reflection to create a new ScalarType instance for DATETIME.
        // This intentionally bypasses the normal static singleton returned by Type.DATETIME.
        // The goal is to simulate a scenario where type identity (==) fails—demonstrating
        // that reference equality is unsafe for type checks, and .isDatetime() must be used for correctness.
        Constructor<ScalarType> ctor = ScalarType.class.getDeclaredConstructor(PrimitiveType.class);
        ctor.setAccessible(true);
        Type evilDatetime = ctor.newInstance(PrimitiveType.DATETIME);
        Column eventHourCol = table.getColumn("event_hour");
        eventHourCol.setType(evilDatetime);

        String sql = "alter table test2.site_access_hourly add TEMPORARY partitions " +
                "START (\"2023-03-27 16:00:00\") END (\"2023-03-27 17:00:00\") EVERY (INTERVAL 1 hour);";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        AddPartitionClause addPartitionClause = (AddPartitionClause) alterTableStmt.getAlterClauseList().get(0);

        GlobalStateMgr.getCurrentState().getLocalMetastore()
                .addPartitions(Util.getOrCreateInnerContext(), db, "site_access_hourly", addPartitionClause);

        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
        Assertions.assertEquals(1, rangePartitionInfo.getIdToRange(true).size());
    }

    @Test
    public void testCatalogDropColumn() throws Exception {
        starRocksAssert.withDatabase("test").useDatabase("test")
                    .withTable("CREATE TABLE test.tbl1\n" +
                                "(\n" +
                                "    k1 date,\n" +
                                "    k2 int,\n" +
                                "    v1 int sum\n" +
                                ")\n" +
                                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                                "PROPERTIES('replication_num' = '1');");
        String stmt = "alter table test.tbl1 drop column k2 from `testRollup`";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        DropColumnClause clause = (DropColumnClause) alterTableStmt.getAlterClauseList().get(0);
        Assertions.assertEquals(0, clause.getProperties().size());
        Assertions.assertEquals("testRollup", clause.getRollupName());

        stmt = "alter table test.tbl1 drop column col1, drop column col2";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        Assertions.assertEquals("test", alterTableStmt.getTbl().getDb());
        Assertions.assertEquals(2, alterTableStmt.getAlterClauseList().size());
    }

    @Test
    public void testCatalogModifyColumn() throws Exception {
        String stmt = "alter table test.tbl1 modify column k2 bigint first from `testRollup`";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        ModifyColumnClause clause = (ModifyColumnClause) alterTableStmt.getAlterClauseList().get(0);
        Assertions.assertEquals(0, clause.getProperties().size());
        Assertions.assertEquals("testRollup", clause.getRollupName());

        stmt = "alter table test.tbl1 modify column k3 bigint comment 'add comment' after k2";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        clause = (ModifyColumnClause) alterTableStmt.getAlterClauseList().get(0);
        Assertions.assertEquals("k2", clause.getColPos().getLastCol());
        Assertions.assertEquals(null, clause.getRollupName());
    }

    @Test
    public void testCatalogRenameColumn() throws Exception {
        String stmt = "alter table test.tbl1 rename column k3 TO k3_new";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        ColumnRenameClause clause = (ColumnRenameClause) alterTableStmt.getAlterClauseList().get(0);
        Assertions.assertEquals(clause.getNewColName(), "k3_new");
    }

    @Test
    public void testCatalogRenameColumnReserved() throws Exception {
        String stmt = "alter table test.tbl1 rename column __op TO __op";
        Assertions.assertThrows(StarRocksException.class, () ->
                UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx()));
    }

    @Test
    public void testCatalogReorderColumns() throws Exception {
        List<String> cols = Lists.newArrayList("k1", "k2");
        String stmt = "alter table test.tbl1 order by (k1, k2)";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        ReorderColumnsClause clause = (ReorderColumnsClause) alterTableStmt.getAlterClauseList().get(0);
        Assertions.assertEquals(cols, clause.getColumnsByPos());
        Assertions.assertEquals(0, clause.getProperties().size());
        Assertions.assertNull(clause.getRollupName());

        stmt = "alter table test.tbl1 order by (k1, k2) from `testRollup`";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
        clause = (ReorderColumnsClause) alterTableStmt.getAlterClauseList().get(0);
        Assertions.assertEquals(clause.getRollupName(), "testRollup");
        Assertions.assertEquals("[k1, k2]", clause.getColumnsByPos().toString());
    }

    @Test
    public void testAlterDatabaseQuota() throws Exception {
        new MockUp<GlobalStateMgr>() {
            @Mock
            public void alterDatabaseQuota(AlterDatabaseQuotaStmt stmt) throws DdlException {
            }
        };
        String sql = "alter database test set data quota 1KB;";
        AlterDatabaseQuotaStmt stmt = (AlterDatabaseQuotaStmt) UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
        DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx());
    }

    @Test
    public void testFindTruncatePartitionEntrance() {
        assertThrows(DdlException.class, () -> {

            Database db = new Database();
            OlapTable table = new OlapTable(Table.TableType.OLAP);
            table.setState(OlapTable.OlapTableState.NORMAL);
            new MockUp<GlobalStateMgr>() {
                @Mock
                public Database getDb(String name) {
                    return db;
                }

                @Mock
                public void truncateTable(TruncateTableStmt truncateTableStmt) throws DdlException {
                    throw new DdlException("test DdlException");
                }
            };
            new MockUp<Database>() {
                @Mock
                public Table getTable(String tableName) {
                    return table;
                }
            };
            List<AlterClause> cList = new ArrayList<>();
            PartitionNames partitionNames = new PartitionNames(true, Arrays.asList("p1"));
            TruncatePartitionClause clause = new TruncatePartitionClause(partitionNames);
            cList.add(clause);
            AlterJobMgr alter = new AlterJobMgr(
                    new SchemaChangeHandler(),
                    new MaterializedViewHandler(),
                    new SystemHandler());
            TableName tableName = new TableName("test_db", "test_table");
            AlterTableStmt stmt = new AlterTableStmt(tableName, cList);
            DDLStmtExecutor.execute(stmt, starRocksAssert.getCtx());
        });
    }

    @Test
    public void testDropListPartition() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "ALTER TABLE t_recharge_detail DROP PARTITION p2 force;";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        DDLStmtExecutor.execute(alterTableStmt, ctx);
    }

    @Test
    public void testAutoPartitionTableUnsupported() {
        assertThrows(AnalysisException.class, () -> {
            ConnectContext ctx = starRocksAssert.getCtx();
            String sql =
                    "ALTER TABLE site_access_date_trunc ADD PARTITION p20210101 VALUES [(\"2021-01-01\"), (\"2021-01-02\"));";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt);
        });
    }

    @Test
    public void testAutoPartitionTableUnsupported2() {
        assertThrows(AnalysisException.class, () -> {
            ConnectContext ctx = starRocksAssert.getCtx();
            String sql = "ALTER TABLE site_access_time_slice\n" +
                    "ADD PARTITIONS START (\"2022-05-01\") END (\"2022-05-03\") EVERY (INTERVAL 1 day)";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt);
        });
    }

    @Test
    public void testAutoPartitionTableUnsupported3() {
        assertThrows(AnalysisException.class, () -> {
            ConnectContext ctx = starRocksAssert.getCtx();
            String sql = "ALTER TABLE site_access_date_trunc\n" +
                    "ADD PARTITIONS START (\"2022-05-01\") END (\"2022-05-03\") EVERY (INTERVAL 2 day)";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt);
        });
    }

    @Test
    public void testAlterMvWithResourceGroup() throws Exception {
        starRocksAssert.executeResourceGroupDdlSql("create resource group if not exists mv_rg" +
                    "   with (" +
                    "   'cpu_core_limit' = '10'," +
                    "   'mem_limit' = '20%'," +
                    "   'concurrency_limit' = '11'," +
                    "   'type' = 'mv'" +
                    "    );");
        starRocksAssert.useDatabase("test")
                    .withMaterializedView("CREATE MATERIALIZED VIEW `mv2` (a comment \"a1\", b comment \"b2\", c)\n" +
                                "COMMENT \"MATERIALIZED_VIEW\"\n" +
                                "DISTRIBUTED BY HASH(a) BUCKETS 12\n" +
                                "REFRESH ASYNC\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"replicated_storage\" = \"true\",\n" +
                                "\"resource_group\" = \"mv_rg\",\n" +
                                "\"storage_medium\" = \"HDD\"\n" +
                                ")\n" +
                                "AS SELECT k1, k2, v1 from test.tbl1");
        MaterializedView mv =
                    (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test").getTable("mv2");
        Assertions.assertEquals("mv_rg", mv.getTableProperty().getResourceGroup());
        ConnectContext ctx = starRocksAssert.getCtx();
        String sql = "ALTER MATERIALIZED VIEW mv2\n" +
                    "set (\"resource_group\" =\"\" )";
        AlterMaterializedViewStmt alterTableStmt = (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterMaterializedView(alterTableStmt);
        Assertions.assertEquals("", mv.getTableProperty().getResourceGroup());
        sql = "ALTER MATERIALIZED VIEW mv2\n" +
                    "set (\"resource_group\" =\"not_exist_rg\" )";
        AlterMaterializedViewStmt alterTableStmt2 =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Assertions.assertThrows(SemanticException.class,
                () -> GlobalStateMgr.getCurrentState().getLocalMetastore().alterMaterializedView(alterTableStmt2),
                "resource_group not_exist_rg does not exist.");
        sql = "ALTER MATERIALIZED VIEW mv2\n" +
                    "set (\"resource_group\" =\"mv_rg\" )";
        AlterMaterializedViewStmt alterTableStmt3 =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterMaterializedView(alterTableStmt3);
        Assertions.assertEquals("mv_rg", mv.getTableProperty().getResourceGroup());

        new MockUp<WarehouseManager>() {
            @Mock
            public Warehouse getWarehouse(String warehouseName) {
                return new DefaultWarehouse(1L, "w1");
            }
        };
        sql = "ALTER MATERIALIZED VIEW mv2 set (\"warehouse\" = \"w1\")";
        AlterMaterializedViewStmt alterTableStmt4 =
                    (AlterMaterializedViewStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        GlobalStateMgr.getCurrentState().getLocalMetastore().alterMaterializedView(alterTableStmt4);
        Assertions.assertEquals(1L, mv.getWarehouseId());
    }

    @Test
    public void testAlterListPartitionUseBatchBuildPartition() {
        assertThrows(ErrorReportException.class, () -> {
            starRocksAssert.useDatabase("test").withTable("CREATE TABLE t2 (\n" +
                    "    dt datetime  not null,\n" +
                    "    user_id  bigint  not null,\n" +
                    "    recharge_money decimal(32,2) not null, \n" +
                    "    province varchar(20) not null,\n" +
                    "    id varchar(20) not null\n" +
                    ") ENGINE=OLAP\n" +
                    "DUPLICATE KEY(dt)\n" +
                    "PARTITION BY (dt)\n" +
                    "DISTRIBUTED BY HASH(`dt`) BUCKETS 10 \n" +
                    "PROPERTIES (\n" +
                    "\"replication_num\" = \"1\",\n" +
                    "\"in_memory\" = \"false\"\n" +
                    ");");
            ConnectContext ctx = starRocksAssert.getCtx();
            String sql = "ALTER TABLE t2 ADD PARTITIONS START (\"2021-01-04\") END (\"2021-01-06\") EVERY (INTERVAL 1 DAY);";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().alterTable(ctx, alterTableStmt);
        });
    }

    @Test
    public void testAlterForeignKey() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        {
            // inner table
            starRocksAssert.useDatabase("test").withMaterializedView("create materialized view if not exists `fk_mv_1` " +
                        "refresh manual " +
                        "as " +
                        "select t1.event_day, t1.site_id, t2.user_name " +
                        "from site_access_date_trunc t1 join site_access_time_slice t2 " +
                        "on t1.site_id = t2.site_id");
            connectContext.executeSql("alter materialized view fk_mv_1 set " +
                        "( 'unique_constraints'='site_access_date_trunc.site_id'); ");
            connectContext.executeSql("alter materialized view fk_mv_1 set " +
                        "( 'foreign_key_constraints'='site_access_time_slice(site_id)" +
                        " REFERENCES site_access_date_trunc(site_id)'); ");
            while (true) {
                ModifyTablePropertyOperationLog modifyMvLog =
                            (ModifyTablePropertyOperationLog) UtFrameUtils.PseudoJournalReplayer.
                                        replayNextJournal(OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES);
                Assertions.assertNotNull(modifyMvLog);
                if (modifyMvLog.getProperties().containsKey("foreign_key_constraints")) {
                    Assertions.assertEquals("default_catalog.10001.10145(site_id) " +
                                            "REFERENCES default_catalog.10001.10129(site_id)",
                                modifyMvLog.getProperties().get("foreign_key_constraints"));
                    break;
                }
            }
        }

        {
            // external table
            starRocksAssert.withMaterializedView("create materialized view if not exists `fk_mv_2` " +
                        "refresh manual " +
                        "as " +
                        "select t1.l_orderkey, t1.l_partkey, t2.o_totalprice " +
                        "from hive0.tpch.lineitem t1 join hive0.tpch.orders t2 " +
                        "on t1.l_orderkey = t2.o_orderkey");
            connectContext.executeSql("alter materialized view fk_mv_2 set " +
                        "( 'unique_constraints'='hive0.tpch.orders.o_orderkey'); ");
            connectContext.executeSql("alter materialized view fk_mv_2 set " +
                        "( 'foreign_key_constraints'='hive0.tpch.lineitem(l_orderkey) " +
                        "REFERENCES hive0.tpch.orders(o_orderkey)'); ");
            while (true) {
                ModifyTablePropertyOperationLog modifyMvLog =
                            (ModifyTablePropertyOperationLog) UtFrameUtils.PseudoJournalReplayer.
                                        replayNextJournal(OperationType.OP_ALTER_MATERIALIZED_VIEW_PROPERTIES);
                Assertions.assertNotNull(modifyMvLog);
                if (modifyMvLog.getProperties().containsKey("foreign_key_constraints")) {
                    Assertions.assertEquals("hive0.tpch.lineitem:0(l_orderkey) REFERENCES hive0.tpch.orders:0(o_orderkey)",
                                modifyMvLog.getProperties().get("foreign_key_constraints"));
                    break;
                }
            }
        }
    }
}
