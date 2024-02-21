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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/AlterJobV2Test.java

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

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowExecutor;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

import static com.starrocks.sql.optimizer.MVTestUtils.waitForSchemaChangeAlterJobFinish;

public class AlterJobV2Test {
    private static ConnectContext connectContext;

    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.setUpForPersistTest();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);

        // set default config for async mvs
        UtFrameUtils.setDefaultConfigForAsyncMVTest(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE test.schema_change_test(k1 int, k2 int, k3 int) " +
                        "distributed by hash(k1) buckets 3 properties('replication_num' = '1');")
                .withTable("CREATE TABLE test.segmentv2(k1 int, k2 int, v1 int sum) " +
                        "distributed by hash(k1) buckets 3 properties('replication_num' = '1');")
                .withTable("CREATE TABLE test.properties_change_test(k1 int, v1 int) " +
                        "primary key(k1) distributed by hash(k1) properties('replication_num' = '1');")
                .withTable("CREATE TABLE modify_column_test(k1 int, k2 int, k3 int) ENGINE = OLAP " +
                        "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) properties('replication_num' = '1');");
    }

    @AfterClass
    public static void teardown() throws Exception {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static void checkTableStateToNormal(OlapTable tb) throws InterruptedException {
        // waiting table state to normal
        int retryTimes = 5;
        while (tb.getState() != OlapTable.OlapTableState.NORMAL && retryTimes > 0) {
            Thread.sleep(5000);
            retryTimes--;
        }
        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, tb.getState());
    }

    @Test
    public void testSchemaChange() throws Exception {
        // 1. process a schema change job
        String alterStmtStr = "alter table test.schema_change_test add column k4 int default '1'";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);
        // 2. check alter job
        waitForSchemaChangeAlterJobFinish();

        // 3. check show alter table column
        String showAlterStmtStr = "show alter table column from test;";
        ShowAlterStmt showAlterStmt =
                (ShowAlterStmt) UtFrameUtils.parseStmtWithNewParser(showAlterStmtStr, connectContext);
        ShowExecutor showExecutor = new ShowExecutor(connectContext, showAlterStmt);
        ShowResultSet showResultSet = showExecutor.execute();
        System.out.println(showResultSet.getMetaData());
        System.out.println(showResultSet.getResultRows());
    }

    @Test
    public void testRollup() throws Exception {
        // 1. process a rollup job
        String alterStmtStr = "alter table test.schema_change_test add rollup test_rollup(k1, k2);";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);
        // 2. check alter job
        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getRollupHandler().getAlterJobsV2();
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                System.out.println(
                        "alter job " + alterJobV2.getJobId() + " is running. state: " + alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            System.out.println("alter job " + alterJobV2.getJobId() + " is done. state: " + alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());
        }
        // 3. check show alter table column
        String showAlterStmtStr = "show alter table rollup from test;";
        ShowAlterStmt showAlterStmt =
                (ShowAlterStmt) UtFrameUtils.parseStmtWithNewParser(showAlterStmtStr, connectContext);
        ShowExecutor showExecutor = new ShowExecutor(connectContext, showAlterStmt);
        ShowResultSet showResultSet = showExecutor.execute();
        System.out.println(showResultSet.getMetaData());
        System.out.println(showResultSet.getResultRows());
    }

    @Test
    public void testModifyTableProperties() throws Exception {
        // 1. process a modify table properties job(enable_persistent_index)
        String alterStmtStr = "alter table test.properties_change_test set ('enable_persistent_index' = 'true');";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);
        // 2. check alter job
        waitForSchemaChangeAlterJobFinish();

        // 3. check enable persistent index
        String showCreateTableStr = "show create table test.properties_change_test;";
        ShowCreateTableStmt showCreateTableStmt =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showCreateTableStr, connectContext);
        ShowExecutor showExecutor = new ShowExecutor(connectContext, showCreateTableStmt);
        ShowResultSet showResultSet = showExecutor.execute();
        System.out.println(showResultSet.getMetaData());
        System.out.println(showResultSet.getResultRows());

        // 4. process a modify table properties job(in_memory)
        String alterStmtStr2 = "alter table test.properties_change_test set ('in_memory' = 'true');";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr2, connectContext);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);

        // 4. check alter job
        waitForSchemaChangeAlterJobFinish();

        // 5. check enable persistent index
        showCreateTableStmt =
                (ShowCreateTableStmt) UtFrameUtils.parseStmtWithNewParser(showCreateTableStr, connectContext);
        showResultSet = showExecutor.execute();
        System.out.println(showResultSet.getMetaData());
        System.out.println(showResultSet.getResultRows());
    }

    @Test
    public void testModifyRelatedColumnWithMv() {
        try {
            String sql = "CREATE MATERIALIZED VIEW test.mv2 DISTRIBUTED BY HASH(k1) " +
                    " BUCKETS 10 REFRESH ASYNC properties('replication_num' = '1') AS SELECT k1, k2 FROM modify_column_test";
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .createMaterializedView((CreateMaterializedViewStatement) statementBase);

            // modify column which define in mv
            String alterStmtStr = "alter table test.modify_column_test modify column k2 varchar(10)";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);

            waitForSchemaChangeAlterJobFinish();
            MaterializedView mv2 = (MaterializedView) GlobalStateMgr.getCurrentState().getDb("test").getTable("mv2");
            Assert.assertFalse(mv2.isActive());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // Test modify mv with star select by modifying column name.
    @Test
    public void testModifyWithSelectStarMV1() throws Exception {
        try {
            starRocksAssert.withTable("CREATE TABLE modify_column_test3(k1 int, k2 int, k3 int) ENGINE = OLAP " +
                    "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) properties('replication_num' = '1');");
            String sql = "CREATE MATERIALIZED VIEW test.mv3 DISTRIBUTED BY HASH(k1) " +
                    " BUCKETS 10 REFRESH ASYNC properties('replication_num' = '1') " +
                    "AS SELECT * FROM modify_column_test3";
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .createMaterializedView((CreateMaterializedViewStatement) statementBase);

            String alterStmtStr = "alter table test.modify_column_test3 modify column k2 varchar(20)";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);

            waitForSchemaChangeAlterJobFinish();
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState()
                    .getDb("test").getTable("mv3");
            Assert.assertTrue(!mv.isActive());
        } finally {
            starRocksAssert.dropTable("modify_column_test3");
        }
    }

    @Test
    // Test modify mv with star select by adding column name.
    public void testModifyWithSelectStarMV2() throws Exception {
        try {
            starRocksAssert.withTable("CREATE TABLE testModifyWithSelectStarMV2(k1 int, k2 int, k3 int) ENGINE = OLAP " +
                    "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) properties('replication_num' = '1');");
            String sql = "CREATE MATERIALIZED VIEW test.mv6 DISTRIBUTED BY HASH(k1) " +
                    " BUCKETS 10 REFRESH ASYNC properties('replication_num' = '1') " +
                    "AS SELECT * FROM testModifyWithSelectStarMV2";
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .createMaterializedView((CreateMaterializedViewStatement) statementBase);

            String alterStmtStr = "alter table test.testModifyWithSelectStarMV2 add column k4 bigint";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);

            waitForSchemaChangeAlterJobFinish();
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState()
                    .getDb("test").getTable("mv6");
            Assert.assertTrue(mv.isActive());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            starRocksAssert.dropTable("testModifyWithSelectStarMV2");
        }
    }

    @Test
    // Test modify mv with star select by dropping column name.
    public void testModifyWithSelectStarMV3() throws Exception {
        try {
            starRocksAssert.withTable("CREATE TABLE modify_column_test5(k1 int, k2 int, k3 int) ENGINE = OLAP " +
                    "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) properties('replication_num' = '1');");
            String sql = "CREATE MATERIALIZED VIEW test.mv5 DISTRIBUTED BY HASH(k1) " +
                    " BUCKETS 10 REFRESH ASYNC properties('replication_num' = '1') " +
                    "AS SELECT * FROM modify_column_test5";
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .createMaterializedView((CreateMaterializedViewStatement) statementBase);

            String alterStmtStr = "alter table test.modify_column_test5 drop column k2";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);

            waitForSchemaChangeAlterJobFinish();
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState()
                    .getDb("test").getTable("mv5");
            Assert.assertTrue(!mv.isActive());
        } catch (Exception e) {
            Assert.fail();
        } finally {
            starRocksAssert.dropTable("modify_column_test5");
        }
    }

    @Test
    public void testModifyWithExpr() throws Exception {
        try {
            starRocksAssert.withTable("CREATE TABLE modify_column_test4(k1 int, k2 int, k3 int) ENGINE = OLAP " +
                    "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) properties('replication_num' = '1');");
            String sql = "CREATE MATERIALIZED VIEW test.mv4 DISTRIBUTED BY HASH(k1) " +
                    " BUCKETS 10 REFRESH ASYNC properties('replication_num' = '1') AS SELECT k1, k2 + 1 FROM modify_column_test4";
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .createMaterializedView((CreateMaterializedViewStatement) statementBase);

            {
                // modify column which not define in mv
                String alterStmtStr = "alter table test.modify_column_test4 modify column k3 varchar(10)";
                AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr,
                        connectContext);
                GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);

                waitForSchemaChangeAlterJobFinish();
                MaterializedView mv = (MaterializedView) GlobalStateMgr
                        .getCurrentState().getDb("test").getTable("mv4");
                Assert.assertTrue(mv.isActive());
            }

            {
                // modify column which define in mv
                String alterStmtStr = "alter table test.modify_column_test4 modify column k2 varchar(30) ";
                AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr,
                        connectContext);
                GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);

                waitForSchemaChangeAlterJobFinish();
                MaterializedView mv = (MaterializedView) GlobalStateMgr
                        .getCurrentState().getDb("test").getTable("mv4");
                Assert.assertFalse(mv.isActive());
                System.out.println(mv.getInactiveReason());
                Assert.assertTrue(mv.getInactiveReason().contains("base table schema changed for columns: k2"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            starRocksAssert.dropTable("modify_column_test4");
        }
    }

    @Test
    public void testModifyUnRelatedColumnWithMv() {
        try {
            String sql = "CREATE MATERIALIZED VIEW test.mv1 DISTRIBUTED BY HASH(k1) " +
                    " BUCKETS 10 REFRESH ASYNC properties('replication_num' = '1') AS SELECT k1, k2 FROM modify_column_test";
            StatementBase statementBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .createMaterializedView((CreateMaterializedViewStatement) statementBase);

            // modify column which not define in mv
            String alterStmtStr = "alter table test.modify_column_test modify column k3 varchar(10)";
            AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
            GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);

            waitForSchemaChangeAlterJobFinish();
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getDb("test").getTable("mv1");
            Assert.assertTrue(mv.isActive());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void test() throws Exception {
        starRocksAssert.withTable("CREATE TABLE test.schema_change_test_load(k1 int, k2 int, k3 int) " +
                "distributed by hash(k1) buckets 3 properties('replication_num' = '1');");

        String alterStmtStr = "alter table test.schema_change_test_load add column k4 int default '1'";
        AlterTableStmt alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(alterStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getAlterJobMgr().processAlterTable(alterTableStmt);
        waitForSchemaChangeAlterJobFinish();

        Map<Long, AlterJobV2> alterJobs = GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2();
        AlterJobV2 alterJobV2Old = new ArrayList<>(alterJobs.values()).get(0);

        UtFrameUtils.PseudoImage alterImage = new UtFrameUtils.PseudoImage();
        GlobalStateMgr.getCurrentState().getAlterJobMgr().save(alterImage.getDataOutputStream());

        SRMetaBlockReader reader = new SRMetaBlockReader(alterImage.getDataInputStream());
        GlobalStateMgr.getCurrentState().getAlterJobMgr().load(reader);
        reader.close();

        AlterJobV2 alterJobV2New =
                GlobalStateMgr.getCurrentState().getSchemaChangeHandler().getAlterJobsV2().get(alterJobV2Old.jobId);

        Assert.assertEquals(alterJobV2Old.jobId, alterJobV2New.jobId);
    }
}
