// This file is made available under Elastic License 2.0.
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
import com.starrocks.alter.AlterJobV2.JobState;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.analysis.AddRollupClause;
import com.starrocks.analysis.AlterClause;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.FakeGlobalStateMgr;
import com.starrocks.catalog.GlobalStateMgrTestUtil;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.OlapTable.OlapTableState;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.meta.MetaContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.thrift.TStorageFormat;
import com.starrocks.thrift.TTaskType;
import com.starrocks.transaction.FakeTransactionIDGenerator;
import com.starrocks.transaction.GlobalTransactionMgr;
import mockit.Mock;
import mockit.MockUp;
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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RollupJobV2Test {
    private static String fileName = "./RollupJobV2Test";

    private static FakeTransactionIDGenerator fakeTransactionIDGenerator;
    private static GlobalTransactionMgr masterTransMgr;
    private static GlobalTransactionMgr slaveTransMgr;
    private static GlobalStateMgr masterGlobalStateMgr;
    private static GlobalStateMgr slaveGlobalStateMgr;

    private static String transactionSource = "localfe";
    private static Analyzer analyzer;
    private static AddRollupClause clause;
    private static AddRollupClause clause2;

    private FakeGlobalStateMgr fakeGlobalStateMgr;
    private FakeEditLog fakeEditLog;

    @Before
    public void setUp() throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, AnalysisException {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        fakeEditLog = new FakeEditLog();
        fakeTransactionIDGenerator = new FakeTransactionIDGenerator();
        masterGlobalStateMgr = GlobalStateMgrTestUtil.createTestState();
        slaveGlobalStateMgr = GlobalStateMgrTestUtil.createTestState();
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_61);
        metaContext.setThreadLocalInfo();
        masterTransMgr = masterGlobalStateMgr.getGlobalTransactionMgr();
        masterTransMgr.setEditLog(masterGlobalStateMgr.getEditLog());

        slaveTransMgr = slaveGlobalStateMgr.getGlobalTransactionMgr();
        slaveTransMgr.setEditLog(slaveGlobalStateMgr.getEditLog());
        analyzer = AccessTestUtil.fetchAdminAnalyzer(false);
        clause = new AddRollupClause(GlobalStateMgrTestUtil.testRollupIndex2, Lists.newArrayList("k1", "v"), null,
                GlobalStateMgrTestUtil.testIndex1, null);
        clause.analyze(analyzer);

        clause2 = new AddRollupClause(GlobalStateMgrTestUtil.testRollupIndex3, Lists.newArrayList("k1", "v"), null,
                GlobalStateMgrTestUtil.testIndex1, null);
        clause2.analyze(analyzer);

        FeConstants.runningUnitTest = true;
        AgentTaskQueue.clearAllTasks();

        new MockUp<GlobalStateMgr>() {
            @Mock
            public GlobalStateMgr getCurrentState() {
                return masterGlobalStateMgr;
            }
        };
    }

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testRunRollupJobConcurrentLimit() throws UserException {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        fakeEditLog = new FakeEditLog();
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        alterClauses.add(clause2);
        Database db = masterGlobalStateMgr.getDb(GlobalStateMgrTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTableId1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();

        materializedViewHandler.runAfterCatalogReady();

        Assert.assertEquals(Config.max_running_rollup_job_num_per_table,
                materializedViewHandler.getTableRunningJobMap().get(GlobalStateMgrTestUtil.testTableId1).size());
        Assert.assertEquals(2, alterJobsV2.size());
        Assert.assertEquals(OlapTableState.ROLLUP, olapTable.getState());
    }

    @Test
    public void testAddSchemaChange() throws UserException {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        fakeEditLog = new FakeEditLog();
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterGlobalStateMgr.getDb(GlobalStateMgrTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTableId1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        Assert.assertEquals(OlapTableState.ROLLUP, olapTable.getState());
    }

    // start a schema change, then finished
    @Test
    public void testSchemaChange1() throws Exception {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        fakeEditLog = new FakeEditLog();
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterGlobalStateMgr.getDb(GlobalStateMgrTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testPartitionId1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();

        // runPendingJob
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.WAITING_TXN, rollupJob.getJobState());
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // runWaitingTxnJob, task not finished
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // finish all tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }
        MaterializedIndex shadowIndex = testPartition.getMaterializedIndices(IndexExtState.SHADOW).get(0);
        for (Tablet shadowTablet : shadowIndex.getTablets()) {
            for (Replica shadowReplica : ((LocalTablet) shadowTablet).getReplicas()) {
                shadowReplica.updateRowCount(testPartition.getVisibleVersion(),
                        shadowReplica.getDataSize(),
                        shadowReplica.getRowCount());
            }
        }

        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.FINISHED, rollupJob.getJobState());
    }

    @Test
    public void testSchemaChangeWhileTabletNotStable() throws Exception {
        fakeGlobalStateMgr = new FakeGlobalStateMgr();
        fakeEditLog = new FakeEditLog();
        FakeGlobalStateMgr.setGlobalStateMgr(masterGlobalStateMgr);
        MaterializedViewHandler materializedViewHandler = GlobalStateMgr.getCurrentState().getRollupHandler();

        // add a rollup job
        ArrayList<AlterClause> alterClauses = new ArrayList<>();
        alterClauses.add(clause);
        Database db = masterGlobalStateMgr.getDb(GlobalStateMgrTestUtil.testDbId1);
        OlapTable olapTable = (OlapTable) db.getTable(GlobalStateMgrTestUtil.testTableId1);
        Partition testPartition = olapTable.getPartition(GlobalStateMgrTestUtil.testPartitionId1);
        materializedViewHandler.process(alterClauses, db, olapTable);
        Map<Long, AlterJobV2> alterJobsV2 = materializedViewHandler.getAlterJobsV2();
        Assert.assertEquals(1, alterJobsV2.size());
        RollupJobV2 rollupJob = (RollupJobV2) alterJobsV2.values().stream().findAny().get();

        MaterializedIndex baseIndex = testPartition.getBaseIndex();
        assertEquals(MaterializedIndex.IndexState.NORMAL, baseIndex.getState());
        assertEquals(Partition.PartitionState.NORMAL, testPartition.getState());
        assertEquals(OlapTableState.ROLLUP, olapTable.getState());

        LocalTablet baseTablet = (LocalTablet) baseIndex.getTablets().get(0);
        List<Replica> replicas = baseTablet.getReplicas();
        Replica replica1 = replicas.get(0);
        Replica replica2 = replicas.get(1);
        Replica replica3 = replicas.get(2);

        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replica1.getVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replica2.getVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replica3.getVersion());
        assertEquals(-1, replica1.getLastFailedVersion());
        assertEquals(-1, replica2.getLastFailedVersion());
        assertEquals(-1, replica3.getLastFailedVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replica1.getLastSuccessVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replica2.getLastSuccessVersion());
        assertEquals(GlobalStateMgrTestUtil.testStartVersion, replica3.getLastSuccessVersion());

        // runPendingJob
        replica1.setState(Replica.ReplicaState.DECOMMISSION);
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.PENDING, rollupJob.getJobState());

        // table is stable, runPendingJob again
        replica1.setState(Replica.ReplicaState.NORMAL);
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.WAITING_TXN, rollupJob.getJobState());
        Assert.assertEquals(2, testPartition.getMaterializedIndices(IndexExtState.ALL).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.VISIBLE).size());
        Assert.assertEquals(1, testPartition.getMaterializedIndices(IndexExtState.SHADOW).size());

        // runWaitingTxnJob
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // runWaitingTxnJob, task not finished
        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.RUNNING, rollupJob.getJobState());

        // finish all tasks
        List<AgentTask> tasks = AgentTaskQueue.getTask(TTaskType.ALTER);
        Assert.assertEquals(3, tasks.size());
        for (AgentTask agentTask : tasks) {
            agentTask.setFinished(true);
        }
        MaterializedIndex shadowIndex = testPartition.getMaterializedIndices(IndexExtState.SHADOW).get(0);
        for (Tablet shadowTablet : shadowIndex.getTablets()) {
            for (Replica shadowReplica : ((LocalTablet) shadowTablet).getReplicas()) {
                shadowReplica.updateRowCount(testPartition.getVisibleVersion(),
                        shadowReplica.getDataSize(),
                        shadowReplica.getRowCount());
            }
        }

        materializedViewHandler.runAfterCatalogReady();
        Assert.assertEquals(JobState.FINISHED, rollupJob.getJobState());
    }

    @Test
    public void testSerializeOfRollupJob() throws IOException,
            AnalysisException {
        Config.enable_materialized_view = true;
        // prepare file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        short keysCount = 1;
        List<Column> columns = Lists.newArrayList();
        String mvColumnName = CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX + "bitmap_union_" + "c1";
        Column column = new Column(mvColumnName, Type.BITMAP, false, AggregateType.BITMAP_UNION, false,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("1")), "");
        columns.add(column);
        RollupJobV2 rollupJobV2 = new RollupJobV2(1, 1, 1, "test", 1, 1, 1, "test", "rollup", columns, 1, 1,
                KeysType.AGG_KEYS, keysCount,
                new OriginStatement("create materialized view rollup as select bitmap_union(to_bitmap(c1)) from test",
                        0));
        rollupJobV2.setStorageFormat(TStorageFormat.V2);

        // write rollup job
        rollupJobV2.write(out);
        out.flush();
        out.close();

        // read objects from file
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_86);
        metaContext.setThreadLocalInfo();

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        RollupJobV2 result = (RollupJobV2) AlterJobV2.read(in);
        Assert.assertEquals(TStorageFormat.V2, Deencapsulation.getField(result, "storageFormat"));
        List<Column> resultColumns = Deencapsulation.getField(result, "rollupSchema");
        Assert.assertEquals(1, resultColumns.size());
        Column resultColumn1 = resultColumns.get(0);
        Assert.assertEquals(mvColumnName,
                resultColumn1.getName());
        Assert.assertTrue(resultColumn1.getDefineExpr() instanceof FunctionCallExpr);
        FunctionCallExpr resultFunctionCall = (FunctionCallExpr) resultColumn1.getDefineExpr();
        Assert.assertEquals("to_bitmap", resultFunctionCall.getFnName().getFunction());

    }
}
