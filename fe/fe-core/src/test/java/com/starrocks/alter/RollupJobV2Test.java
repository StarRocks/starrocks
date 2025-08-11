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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/alter/RollupJobV2Test.java

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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.ThreadUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AlterTableClauseAnalyzer;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.task.AgentTaskQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RollupJobV2Test extends DDLTestBase {
    private static AddRollupClause clause;
    private static AddRollupClause clause2;

    private static final Logger LOG = LogManager.getLogger(SchemaChangeJobV2Test.class);

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();
        clause = new AddRollupClause(GlobalStateMgrTestUtil.testRollupIndex2, Lists.newArrayList("v1"), null,
                    GlobalStateMgrTestUtil.testTable1, null);
        AlterTableClauseAnalyzer analyzer = new AlterTableClauseAnalyzer(null);
        analyzer.analyze(null, clause);

        clause2 = new AddRollupClause(GlobalStateMgrTestUtil.testRollupIndex3, Lists.newArrayList("v1", "v2"), null,
                    GlobalStateMgrTestUtil.testTable1, null);
        analyzer.analyze(null, clause2);

        AgentTaskQueue.clearAllTasks();
    }

    @AfterEach
    public void tearDown() {
        GlobalStateMgr.getCurrentState().getRollupHandler().clearJobs();
    }

    @Test
    public void testRunRollupJobConcurrentLimit() throws StarRocksException {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        alterClauses.add(clause2);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();

        materializedViewHandler.runAfterCatalogReady();

        assertEquals(Config.max_running_rollup_job_num_per_table,
                    materializedViewHandler.getTableRunningJobMap().get(olapTable.getId()).size());
        assertEquals(2, alterJobsV2.size());
        assertEquals(OlapTableState.ROLLUP, olapTable.getState());
    }

    @Test
    public void testAddSchemaChange() throws StarRocksException {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());
        assertEquals(OlapTableState.ROLLUP, olapTable.getState());
    }

    // start a schema change, then finished
    @Test
    public void testSchemaChange1() throws Exception {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();
        materializedViewHandler.clearJobs(); // Disable the execution of job in background thread

        // runPendingJob
        rollupJob.runPendingJob();
        assertEquals(AlterJobV2.JobState.WAITING_TXN, rollupJob.getJobState());
        assertEquals(2, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).size());
        assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        rollupJob.runWaitingTxnJob();
        assertEquals(AlterJobV2.JobState.RUNNING, rollupJob.getJobState());

        int retryCount = 0;
        while (!rollupJob.getJobState().isFinalState()) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            rollupJob.runRunningJob();
            if (rollupJob.getJobState() == AlterJobV2.JobState.FINISHED) {
                break;
            }
            retryCount++;
            LOG.info("rollupJob is waiting for JobState retryCount:" + retryCount);
        }

        // finish alter tasks
        assertEquals(AlterJobV2.JobState.FINISHED, rollupJob.getJobState());
    }

    @Test
    public void testSchemaChangeWhileTabletNotStable() throws Exception {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();

        MaterializedIndex baseIndex = testPartition.getDefaultPhysicalPartition().getBaseIndex();
        assertEquals(MaterializedIndex.IndexState.NORMAL, baseIndex.getState());
        assertEquals(Partition.PartitionState.NORMAL, testPartition.getState());
        assertEquals(OlapTableState.ROLLUP, olapTable.getState());

        LocalTablet baseTablet = (LocalTablet) baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getImmutableReplicas();
        Replica replica1 = replicas.get(0);

        assertEquals(1, replica1.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(1, replica1.getLastSuccessVersion());

        // runPendingJob
        replica1.setState(Replica.ReplicaState.DECOMMISSION);
        rollupJob.runPendingJob();
        assertEquals(AlterJobV2.JobState.PENDING, rollupJob.getJobState());

        // table is stable, runPendingJob again
        replica1.setState(Replica.ReplicaState.NORMAL);
        rollupJob.runPendingJob();
        assertEquals(AlterJobV2.JobState.WAITING_TXN, rollupJob.getJobState());
        assertEquals(2, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).size());
        assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        assertEquals(1, testPartition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        rollupJob.runWaitingTxnJob();
        assertEquals(AlterJobV2.JobState.RUNNING, rollupJob.getJobState());

        int retryCount = 0;
        int maxRetry = 5;
        while (retryCount < maxRetry) {
            ThreadUtil.sleepAtLeastIgnoreInterrupts(2000L);
            rollupJob.runRunningJob();
            if (rollupJob.getJobState() == AlterJobV2.JobState.FINISHED) {
                break;
            }
            retryCount++;
            LOG.info("rollupJob is waiting for JobState retryCount:" + retryCount);
        }
    }

    @Test
    public void testReplayPendingRollupJob() throws Exception {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());

        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();
        rollupJob.replay(rollupJob);
    }

    @Test
    public void testCancelPendingJobWithFlag() throws Exception {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), GlobalStateMgrTestUtil.testTable1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();
        materializedViewHandler.clearJobs(); // Disable the execution of job in background thread

        rollupJob.setIsCancelling(true);
        rollupJob.runPendingJob();
        rollupJob.setIsCancelling(false);

        rollupJob.setWaitingCreatingReplica(true);
        rollupJob.cancel("");
        rollupJob.setWaitingCreatingReplica(false);
    }
}
