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
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Type;
import com.starrocks.common.conf.Config;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.DDLTestBase;
import com.starrocks.sql.ast.AddRollupClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.optimizer.rule.mv.MVUtils;
import com.starrocks.task.AgentTaskQueue;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RollupJobV2Test extends DDLTestBase {
    private static AddRollupClause clause;
    private static AddRollupClause clause2;

    private static final Logger LOG = LogManager.getLogger(SchemaChangeJobV2Test.class);

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clause = new AddRollupClause(GlobalStateMgrTestUtil.testRollupIndex2, Lists.newArrayList("v1"), null,
                GlobalStateMgrTestUtil.testTable1, null);
        clause.analyze(analyzer);

        clause2 = new AddRollupClause(GlobalStateMgrTestUtil.testRollupIndex3, Lists.newArrayList("v1", "v2"), null,
                GlobalStateMgrTestUtil.testTable1, null);
        clause2.analyze(analyzer);

        AgentTaskQueue.clearAllTasks();
    }

    @After
    public void tearDown() {
        GlobalStateMgr.getCurrentState().getRollupHandler().clearJobs();
    }

    @Test
    public void testRunRollupJobConcurrentLimit() throws UserException {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        alterClauses.add(clause2);
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();

        materializedViewHandler.runAfterCatalogReady();

        assertEquals(Config.max_running_rollup_job_num_per_table,
                materializedViewHandler.getTableRunningJobMap().get(olapTable.getId()).size());
        assertEquals(2, alterJobsV2.size());
        assertEquals(OlapTableState.ROLLUP, olapTable.getState());
    }

    @Test
    public void testAddSchemaChange() throws UserException {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
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
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();

        // runPendingJob
        rollupJob.runPendingJob();
        assertEquals(AlterJobV2.JobState.WAITING_TXN, rollupJob.getJobState());
        assertEquals(2, testPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).size());
        assertEquals(1, testPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        assertEquals(1, testPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());

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

        // finish alter tasks
        assertEquals(AlterJobV2.JobState.FINISHED, rollupJob.getJobState());
    }

    @Test
    public void testSchemaChangeWhileTabletNotStable() throws Exception {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);

        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testTable1);

        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();

        MaterializedIndex baseIndex = testPartition.getBaseIndex();
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
        assertEquals(2, testPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).size());
        assertEquals(1, testPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE).size());
        assertEquals(1, testPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());

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
    public void testSerializeOfRollupJob() throws IOException,
            AnalysisException {
        Config.enable_materialized_view = true;
        // prepare file
        String fileName = "./RollupJobV2Test";
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        short keysCount = 1;
        List<Column> columns = Lists.newArrayList();
        String mvColumnName = MVUtils.MATERIALIZED_VIEW_NAME_PREFIX + "bitmap_union_" + "c1";
        Column column = new Column(mvColumnName, Type.BITMAP, false, AggregateType.BITMAP_UNION, false,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("1")), "");
        columns.add(column);
        RollupJobV2 rollupJobV2 = new RollupJobV2(1, 1, 1, "test", 1, 1,
                1, "test", "rollup", 0, columns, null, 1, 1,
                KeysType.AGG_KEYS, keysCount,
                new OriginStatement("create materialized view rollup as select bitmap_union(to_bitmap(c1)) from test",
                        0), "", false);

        // write rollup job
        rollupJobV2.write(out);
        out.flush();
        out.close();

        // read objects from file

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        RollupJobV2 result = (RollupJobV2) AlterJobV2.read(in);
        List<Column> resultColumns = Deencapsulation.getField(result, "rollupSchema");
        assertEquals(1, resultColumns.size());
        Column resultColumn1 = resultColumns.get(0);
        assertEquals(mvColumnName,
                resultColumn1.getName());
        Assert.assertTrue(resultColumn1.getDefineExpr() instanceof FunctionCallExpr);
        FunctionCallExpr resultFunctionCall = (FunctionCallExpr) resultColumn1.getDefineExpr();
        assertEquals("to_bitmap", resultFunctionCall.getFnName().getFunction());
    }

    @Test
    public void testReplayPendingRollupJob() throws Exception {
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = GlobalStateMgr.getCurrentState().getDb(GlobalStateMgrTestUtil.testDb1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTable1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        assertEquals(1, alterJobsV2.size());

        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();
        rollupJob.replay(rollupJob);
    }
}
