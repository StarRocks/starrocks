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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/ShowMaterializedViewTest.java

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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.system.information.MaterializedViewsSystemTable;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRun;
import com.starrocks.scheduler.TaskRunBuilder;
import com.starrocks.scheduler.TaskRunManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.ShowMaterializedViewsStmt;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.platform.commons.util.Preconditions;

import java.util.HashMap;
import java.util.List;

public class ShowMaterializedViewTest {

    private static ConnectContext ctx;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.tbl6\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "PARTITION BY RANGE(k1)\n" +
                        "(\n" +
                        "    PARTITION p0 values [('2021-12-01'),('2022-01-01')),\n" +
                        "    PARTITION p1 values [('2022-01-01'),('2022-02-01')),\n" +
                        "    PARTITION p2 values [('2022-02-01'),('2022-03-01')),\n" +
                        "    PARTITION p3 values [('2022-03-01'),('2022-04-01')),\n" +
                        "    PARTITION p4 values [('2022-04-01'),('2022-05-01'))\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');")
                .withMaterializedView("create materialized view test.mv_refresh_priority\n" +
                        "partition by date_trunc('month',k1) \n" +
                        "distributed by hash(k2) buckets 10\n" +
                        "refresh deferred manual\n" +
                        "properties('replication_num' = '1', 'partition_refresh_number'='1')\n" +
                        "as select k1, k2 from tbl6;");

    }

    @Test
    public void testNormal() throws Exception {
        ctx.setDatabase("testDb");

        ShowMaterializedViewsStmt stmt = new ShowMaterializedViewsStmt("");

        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("testDb", stmt.getDb());
        checkShowMaterializedViewsStmt(stmt);

        stmt = new ShowMaterializedViewsStmt("abc", (String) null);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("abc", stmt.getDb());
        checkShowMaterializedViewsStmt(stmt);

        stmt = new ShowMaterializedViewsStmt("abc", "bcd");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.assertEquals("bcd", stmt.getPattern());
        Assert.assertEquals("abc", stmt.getDb());
        checkShowMaterializedViewsStmt(stmt);

        stmt = (ShowMaterializedViewsStmt) UtFrameUtils.parseStmtWithNewParser(
                "SHOW MATERIALIZED VIEWS FROM abc where name = 'mv1';", ctx);
        Preconditions.notNull(stmt.toSelectStmt().getOrigStmt(), "stmt's original stmt should not be null");

        Assert.assertEquals("abc", stmt.getDb());
        Assert.assertEquals(
                "SELECT information_schema.materialized_views.MATERIALIZED_VIEW_ID AS id, " +
                        "information_schema.materialized_views.TABLE_SCHEMA AS database_name, " +
                        "information_schema.materialized_views.TABLE_NAME AS name, " +
                        "information_schema.materialized_views.refresh_type AS refresh_type, " +
                        "information_schema.materialized_views.is_active AS is_active, " +
                        "information_schema.materialized_views.partition_type AS partition_type, " +
                        "information_schema.materialized_views.task_id AS task_id, " +
                        "information_schema.materialized_views.task_name AS task_name, " +
                        "information_schema.materialized_views.last_refresh_start_time AS last_refresh_start_time, " +
                        "information_schema.materialized_views.last_refresh_finished_time AS last_refresh_finished_time, " +
                        "information_schema.materialized_views.last_refresh_duration AS last_refresh_duration, " +
                        "information_schema.materialized_views.last_refresh_state AS last_refresh_state, " +
                        "information_schema.materialized_views.last_refresh_force_refresh AS last_refresh_force_refresh, " +
                        "information_schema.materialized_views.last_refresh_start_partition AS last_refresh_start_partition," +
                        " information_schema.materialized_views.last_refresh_end_partition AS last_refresh_end_partition, " +
                        "information_schema.materialized_views.last_refresh_base_refresh_partitions " +
                        "AS last_refresh_base_refresh_partitions," +
                        " information_schema.materialized_views.last_refresh_mv_refresh_partitions " +
                        "AS last_refresh_mv_refresh_partitions, " +
                        "information_schema.materialized_views.last_refresh_error_code AS last_refresh_error_code, " +
                        "information_schema.materialized_views.last_refresh_error_message AS last_refresh_error_message, " +
                        "information_schema.materialized_views.TABLE_ROWS AS rows, " +
                        "information_schema.materialized_views.MATERIALIZED_VIEW_DEFINITION AS text " +
                        "FROM information_schema.materialized_views " +
                        "WHERE (information_schema.materialized_views.TABLE_SCHEMA = 'abc') AND (information_schema.materialized_views.TABLE_NAME = 'mv1')",
                AstToStringBuilder.toString(stmt.toSelectStmt()));
        checkShowMaterializedViewsStmt(stmt);
    }

    private void checkShowMaterializedViewsStmt(ShowMaterializedViewsStmt stmt) {
        Table schemaMVTable = MaterializedViewsSystemTable.create();
        Assert.assertEquals(schemaMVTable.getBaseSchema().size(), stmt.getMetaData().getColumnCount());

        List<Column> schemaCols = schemaMVTable.getFullSchema();
        for (int i = 0; i < schemaCols.size(); i++) {
            if (schemaCols.get(i).getName().equalsIgnoreCase("MATERIALIZED_VIEW_ID")) {
                Assert.assertEquals("id", stmt.getMetaData().getColumn(i).getName());
            } else if (schemaCols.get(i).getName().equalsIgnoreCase("TABLE_SCHEMA")) {
                Assert.assertEquals("database_name", stmt.getMetaData().getColumn(i).getName());
            } else if (schemaCols.get(i).getName().equalsIgnoreCase("TABLE_NAME")) {
                Assert.assertEquals("name", stmt.getMetaData().getColumn(i).getName());
            } else if (schemaCols.get(i).getName().equalsIgnoreCase("MATERIALIZED_VIEW_DEFINITION")) {
                Assert.assertEquals("text", stmt.getMetaData().getColumn(i).getName());
            } else if (schemaCols.get(i).getName().equalsIgnoreCase("TABLE_ROWS")) {
                Assert.assertEquals("rows", stmt.getMetaData().getColumn(i).getName());
            } else {
                Assert.assertEquals(schemaCols.get(i).getName().toLowerCase(), stmt.getMetaData().getColumn(i).getName());
            }
        }
    }

    @Test(expected = SemanticException.class)
    public void testNoDb() throws Exception {
        ctx = UtFrameUtils.createDefaultCtx();
        ShowMaterializedViewsStmt stmt = new ShowMaterializedViewsStmt("");
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        Assert.fail("No exception throws");
    }

    private void setPartitionVersion(Partition partition, long version) {
        partition.setVisibleVersion(version, System.currentTimeMillis());
        MaterializedIndex baseIndex = partition.getBaseIndex();
        List<Tablet> tablets = baseIndex.getTablets();
        for (Tablet tablet : tablets) {
            List<Replica> replicas = ((LocalTablet) tablet).getImmutableReplicas();
            for (Replica replica : replicas) {
                replica.updateVersionInfo(version, -1, version);
            }
        }
    }

    @Test
    public void testTaskRun() throws Exception {
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) throws Exception {
                if (stmt instanceof InsertStmt) {
                    InsertStmt insertStmt = (InsertStmt) stmt;
                    TableName tableName = insertStmt.getTableName();
                    Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
                    OlapTable tbl = ((OlapTable) testDb.getTable(tableName.getTbl()));
                    for (Partition partition : tbl.getPartitions()) {
                        if (insertStmt.getTargetPartitionIds().contains(partition.getId())) {
                            setPartitionVersion(partition, partition.getVisibleVersion() + 1);
                        }
                    }
                }
            }
        };
        String mvName = "mv_refresh_priority";
        Database testDb = GlobalStateMgr.getCurrentState().getDb("test");
        MaterializedView materializedView = ((MaterializedView) testDb.getTable(mvName));
        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
        TaskRunManager trm = tm.getTaskRunManager();

        String insertSql = "insert into tbl6 partition(p1) values('2022-01-02',2,10);";
        new StmtExecutor(ctx, insertSql).execute();
        insertSql = "insert into tbl6 partition(p2) values('2022-02-02',2,10);";
        new StmtExecutor(ctx, insertSql).execute();

        // refresh materialized view
        HashMap<String, String> taskRunProperties = new HashMap<>();
        taskRunProperties.put(TaskRun.FORCE, Boolean.toString(true));
        Task task = TaskBuilder.buildMvTask(materializedView, testDb.getFullName());
        TaskRun taskRun = TaskRunBuilder.newBuilder(task).build();
        taskRun.initStatus(UUIDUtil.genUUID().toString(), System.currentTimeMillis());
        taskRun.executeTaskRun();

        // without db name
        Assert.assertFalse(tm.showTaskRunStatus(null).isEmpty());
        Assert.assertFalse(tm.showTasks(null).isEmpty());
        Assert.assertFalse(tm.showMVLastRefreshTaskRunStatus(null).isEmpty());

        // specific db
        String dbName = "test";
        Assert.assertFalse(tm.showTaskRunStatus(dbName).isEmpty());
        Assert.assertFalse(tm.showTasks(dbName).isEmpty());
        Assert.assertFalse(tm.showMVLastRefreshTaskRunStatus(dbName).isEmpty());

        long taskId = tm.getTask(TaskBuilder.getMvTaskName(materializedView.getId())).getId();
        Assert.assertNotNull(tm.getTaskRunManager().getRunnableTaskRun(taskId));
        while (MapUtils.isNotEmpty(trm.getRunningTaskRunMap())) {
            Thread.sleep(100);
        }
    }
}
