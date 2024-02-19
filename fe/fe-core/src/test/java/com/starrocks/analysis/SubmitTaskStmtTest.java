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

package com.starrocks.analysis;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class SubmitTaskStmtTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

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
                        "PROPERTIES('replication_num' = '1');");
    }

    @Test
    public void BasicSubmitStmtTest() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        ctx.setExecutionId(UUIDUtil.toTUniqueId(UUIDUtil.genUUID()));
        String submitSQL = "submit task as create table temp as select count(*) as cnt from tbl1";
        SubmitTaskStmt submitTaskStmt = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL, ctx);

        Assert.assertEquals(submitTaskStmt.getDbName(), "test");
        Assert.assertNull(submitTaskStmt.getTaskName());
        Assert.assertEquals(submitTaskStmt.getProperties().size(), 0);

        String submitSQL2 = "submit /*+ SET_VAR(query_timeout = 1) */ task as " +
                "create table temp as select count(*) as cnt from tbl1";

        SubmitTaskStmt submitTaskStmt2 = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL2, ctx);
        Assert.assertEquals(submitTaskStmt2.getDbName(), "test");
        Assert.assertNull(submitTaskStmt2.getTaskName());
        Assert.assertEquals(submitTaskStmt2.getProperties().size(), 1);
        Map<String, String> properties = submitTaskStmt2.getProperties();
        for (String key : properties.keySet()) {
            Assert.assertEquals(key, "query_timeout");
            Assert.assertEquals(properties.get(key), "1");
        }

        String submitSQL3 = "submit task task_name as create table temp as select count(*) as cnt from tbl1";
        SubmitTaskStmt submitTaskStmt3 = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL3, ctx);

        Assert.assertEquals(submitTaskStmt3.getDbName(), "test");
        Assert.assertEquals(submitTaskStmt3.getTaskName(), "task_name");
        Assert.assertEquals(submitTaskStmt3.getProperties().size(), 0);

        String submitSQL4 = "submit task test.task_name as create table temp as select count(*) as cnt from tbl1";
        SubmitTaskStmt submitTaskStmt4 = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(submitSQL4, ctx);

        Assert.assertEquals(submitTaskStmt4.getDbName(), "test");
        Assert.assertEquals(submitTaskStmt4.getTaskName(), "task_name");
        Assert.assertEquals(submitTaskStmt4.getProperties().size(), 0);
    }

    @Test
    public void SubmitStmtShouldShow() throws Exception {
        AnalyzeTestUtil.init();
        ConnectContext ctx = starRocksAssert.getCtx();
        String submitSQL = "SUBMIT TASK test1 AS CREATE TABLE t1 AS SELECT SLEEP(5);";
        StatementBase submitStmt = AnalyzeTestUtil.analyzeSuccess(submitSQL);
        Assert.assertTrue(submitStmt instanceof SubmitTaskStmt);
        SubmitTaskStmt statement = (SubmitTaskStmt) submitStmt;
        ShowResultSet showResult = DDLStmtExecutor.execute(statement, ctx);
        Assert.assertNotNull(showResult);
    }

    @Test
    public void testSubmitInsert() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.test_insert\n" +
                        "(\n" +
                        "    k1 date,\n" +
                        "    k2 int,\n" +
                        "    v1 int sum\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                        "PROPERTIES('replication_num' = '1');");

        String sql1 = "submit task task1 as insert into test.test_insert select * from test.test_insert";
        UtFrameUtils.parseStmtWithNewParser(sql1, ctx);

        String sql2 = "submit task task1 as insert overwrite test.test_insert select * from test.test_insert";
        UtFrameUtils.parseStmtWithNewParser(sql2, ctx);

        SubmitTaskStmt submitStmt = (SubmitTaskStmt) UtFrameUtils.parseStmtWithNewParser(sql1, ctx);
        Assert.assertNotNull(submitStmt.getDbName());
        Assert.assertNotNull(submitStmt.getSqlText());
    }

    @Test
    public void testDropTaskForce() throws Exception {
        String name = "mv_force";
        starRocksAssert.withMaterializedView("create materialized view " + name +
                " distributed by hash(k1) " +
                " refresh async every(interval 1 minute)" +
                "as select * from tbl1");
        MaterializedView mv = starRocksAssert.getMv("test", name);
        String taskName = TaskBuilder.getMvTaskName(mv.getId());

        // regular drop
        Exception e = assertThrows(RuntimeException.class,
                () -> starRocksAssert.ddl(String.format("drop task `%s`", taskName)));
        assertEquals("Can not drop task generated by materialized view. " +
                "You can use DROP MATERIALIZED VIEW to drop task, when the materialized view is deleted, " +
                "the related task will be deleted automatically.", e.getMessage());

        // force drop
        starRocksAssert.ddl(String.format("drop task `%s` force", taskName));
        TaskManager tm = GlobalStateMgr.getCurrentState().getTaskManager();
        Assert.assertNull(tm.getTask(taskName));
        starRocksAssert.dropMaterializedView(name);
    }
}
