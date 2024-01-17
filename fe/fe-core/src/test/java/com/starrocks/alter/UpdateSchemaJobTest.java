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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/SchemaChangeJobV2Test.java

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

import com.starrocks.alter.AlterJobV2.JobState;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;

public class UpdateSchemaJobTest extends DDLTestBase {
    private static String fileName = "./UpdateSchemaJobTest";
    private AlterTableStmt alterTableStmt;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private static final Logger LOG = LogManager.getLogger(SchemaChangeJobV2Test.class);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        String stmt = "alter table testTable1 add column add_v int default '1' after v3";
        alterTableStmt = (AlterTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, starRocksAssert.getCtx());
    }

    @After
    public void clear() {
        GlobalStateMgr.getCurrentState().getUpdateSchemaHandler().clearJobs();
    }

    @Test
    public void testAddUpdateSchema() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        
        UpdateSchemaHandler updateSchemaHandler = GlobalStateMgr.getCurrentState().getUpdateSchemaHandler();
        Map<Long, AlterJobV2> updateSchemaJobs = updateSchemaHandler.getUpdateSchemaJobs();
        Assert.assertEquals(1, updateSchemaJobs.size());
        Assert.assertEquals(OlapTableState.NORMAL, olapTable.getState());
    }

    @Test
    public void testUpdateSchemaFinish() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        
        UpdateSchemaHandler updateSchemaHandler = GlobalStateMgr.getCurrentState().getUpdateSchemaHandler();
        Map<Long, AlterJobV2> updateSchemaJobs = updateSchemaHandler.getUpdateSchemaJobs();
        Assert.assertEquals(1, updateSchemaJobs.size());
        UpdateSchemaJob updateSchemaJob = (UpdateSchemaJob) updateSchemaJobs.values().stream().findAny().get();

        // runPendingJob
        updateSchemaJob.runPendingJob();
        Assert.assertEquals(JobState.RUNNING, updateSchemaJob.getJobState());

        // runRunningJob
        int retryCount = 0;
        int maxRetry = 5;
        try {
            updateSchemaJob.runRunningJob();
            while (retryCount < maxRetry) {
                ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
                if (updateSchemaJob.getJobState() == JobState.CANCELLED) {
                    break;
                }
                retryCount++;
                LOG.info("testUpdateSchemaJob is waiting for JobState retryCount:" + retryCount);
            }
            updateSchemaJob.cancel("");
        } catch (AlterCancelException e) {
            updateSchemaJob.cancel(e.getMessage());
        }

        // finish alter tasks
        Assert.assertEquals(JobState.FINISHED, updateSchemaJob.getJobState());
    }

    @Test
    public void testUpdateSchemaJobWhileTableNotStable() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        
        UpdateSchemaHandler updateSchemaHandler = GlobalStateMgr.getCurrentState().getUpdateSchemaHandler();
        Map<Long, AlterJobV2> updateSchemaJobs = updateSchemaHandler.getUpdateSchemaJobs();
        Assert.assertEquals(1, updateSchemaJobs.size());
        UpdateSchemaJob updateSchemaJob = (UpdateSchemaJob) updateSchemaJobs.values().stream().findAny().get();
    
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);
        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        LocalTablet baseTablet = (LocalTablet) baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getImmutableReplicas();
        Replica replica1 = replicas.get(0);

        // runPendingJob
        replica1.setState(Replica.ReplicaState.DECOMMISSION);
        updateSchemaJob.runPendingJob();
        Assert.assertEquals(JobState.PENDING, updateSchemaJob.getJobState());

        // table is stable and runPendingJob again
        replica1.setState(Replica.ReplicaState.NORMAL);
        updateSchemaJob.runPendingJob();
        Assert.assertEquals(JobState.RUNNING, updateSchemaJob.getJobState());

        // runRunningJob
        int retryCount = 0;
        int maxRetry = 5;
        try {
            updateSchemaJob.runRunningJob();
            while (retryCount < maxRetry) {
                ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
                if (updateSchemaJob.getJobState() == JobState.CANCELLED) {
                    break;
                }
                retryCount++;
                LOG.info("testUpdateSchemaJob is waiting for JobState retryCount:" + retryCount);
            }
            updateSchemaJob.cancel("");
        } catch (AlterCancelException e) {
            updateSchemaJob.cancel(e.getMessage());
        }

        // finish alter tasks
        Assert.assertEquals(JobState.FINISHED, updateSchemaJob.getJobState());
    }

    @Test
    public void testUpdateSchemaReplay() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        
        UpdateSchemaHandler updateSchemaHandler = GlobalStateMgr.getCurrentState().getUpdateSchemaHandler();
        Map<Long, AlterJobV2> updateSchemaJobs = updateSchemaHandler.getUpdateSchemaJobs();
        Assert.assertEquals(1, updateSchemaJobs.size());
        UpdateSchemaJob updateSchemaJob = (UpdateSchemaJob) updateSchemaJobs.values().stream().findAny().get();

        UpdateSchemaJob replayUpdateSchemaJob = new UpdateSchemaJob(
                updateSchemaJob.getJobId(), updateSchemaJob.getDbId(), updateSchemaJob.getTableId(),
                updateSchemaJob.getTableName(), updateSchemaJob.getTimeoutMs());
        
        
        replayUpdateSchemaJob.replay(updateSchemaJob);
        Assert.assertEquals(JobState.PENDING, replayUpdateSchemaJob.getJobState());

        // runPendingJob
        updateSchemaJob.runPendingJob();
        Assert.assertEquals(JobState.RUNNING, updateSchemaJob.getJobState());

        // runRunningJob
        AgentBatchTask updateSchemaBatchTask = updateSchemaJob.getUpdateSchemaBatchTask();
        for (AgentTask task : updateSchemaBatchTask.getAllTasks()) {
            task.setFinished(true);
        }
        updateSchemaJob.runRunningJob();

        // finish alter tasks
        Assert.assertEquals(JobState.FINISHED, updateSchemaJob.getJobState());
    }

    @Test 
    public void testUpdateSchemaDbNull() throws Exception {
        SchemaChangeHandler schemaChangeHandler = GlobalStateMgr.getCurrentState().getSchemaChangeHandler();
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        schemaChangeHandler.process(alterTableStmt.getOps(), db, olapTable);
        
        UpdateSchemaHandler updateSchemaHandler = GlobalStateMgr.getCurrentState().getUpdateSchemaHandler();
        Map<Long, AlterJobV2> updateSchemaJobs = updateSchemaHandler.getUpdateSchemaJobs();
        Assert.assertEquals(1, updateSchemaJobs.size());
        UpdateSchemaJob updateSchemaJob = (UpdateSchemaJob) updateSchemaJobs.values().stream().findAny().get();
    
        // db not exist
        updateSchemaJob.setDbId(db.getId() + 1000);
        updateSchemaJob.runPendingJob();
        Assert.assertEquals(JobState.FINISHED, updateSchemaJob.getJobState());
    
        // table not exist
        updateSchemaJob.setDbId(db.getId());
        updateSchemaJob.setJobState(JobState.PENDING);
        updateSchemaJob.setTableId(olapTable.getId() + 1000);
        updateSchemaJob.runPendingJob();
        Assert.assertEquals(JobState.FINISHED, updateSchemaJob.getJobState());

        // db not exist
        updateSchemaJob.setDbId(db.getId() + 1000);
        updateSchemaJob.setJobState(JobState.RUNNING);
        updateSchemaJob.runRunningJob();
        Assert.assertEquals(JobState.FINISHED, updateSchemaJob.getJobState());

        //table not exit
        updateSchemaJob.setDbId(db.getId());
        updateSchemaJob.setJobState(JobState.RUNNING);
        updateSchemaJob.runRunningJob();
        Assert.assertEquals(JobState.FINISHED, updateSchemaJob.getJobState());
    }
}