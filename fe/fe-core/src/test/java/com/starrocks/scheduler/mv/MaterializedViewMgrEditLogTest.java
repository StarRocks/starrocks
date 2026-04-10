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

package com.starrocks.scheduler.mv;

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.OperationType;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TWriteQuorumType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

public class MaterializedViewMgrEditLogTest {
    private static final String DB_NAME = "test_mv_mgr_editlog";
    private static final String MV_NAME = "test_mv";
    private static final long DB_ID = 80001L;
    private static final long MV_ID = 80002L;
    private static final long PARTITION_ID = 80004L;
    private static final long PHYSICAL_PARTITION_ID = 80005L;
    private static final long INDEX_ID = 80006L;
    private static final long TABLET_ID = 80007L;

    @BeforeEach
    public void setUp() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        // Create database directly (no mincluster)
        LocalMetastore metastore = GlobalStateMgr.getCurrentState().getLocalMetastore();
        Database db = new Database(DB_ID, DB_NAME);
        metastore.unprotectCreateDb(db);
        
        // Clear jobMap to ensure clean state for each test
        MaterializedViewMgr mvMgr = GlobalStateMgr.getCurrentState().getMaterializedViewMgr();
        mvMgr.clearJobs();
    }

    @AfterEach
    public void tearDown() {
        UtFrameUtils.tearDownForPersisTest();
    }

    private static MaterializedView createMaterializedView(long mvId, String mvName) {
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("v1", IntegerType.BIGINT);
        col1.setIsKey(true);
        columns.add(col1);
        columns.add(new Column("v2", IntegerType.BIGINT));

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setDataProperty(PARTITION_ID, com.starrocks.catalog.DataProperty.DEFAULT_DATA_PROPERTY);
        partitionInfo.setReplicationNum(PARTITION_ID, (short) 1);

        DistributionInfo distributionInfo = new HashDistributionInfo(3, List.of(col1));

        MaterializedIndex baseIndex = new MaterializedIndex(INDEX_ID, MaterializedIndex.IndexState.NORMAL);
        LocalTablet tablet = new LocalTablet(TABLET_ID);
        TabletMeta tabletMeta = new TabletMeta(DB_ID, mvId, PARTITION_ID, INDEX_ID, TStorageMedium.HDD);
        baseIndex.addTablet(tablet, tabletMeta);

        Partition partition = new Partition(PARTITION_ID, PHYSICAL_PARTITION_ID, "p1", baseIndex, distributionInfo);

        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.INCREMENTAL);
        
        MaterializedView mv = new MaterializedView(mvId, DB_ID, mvName, columns, KeysType.DUP_KEYS, 
                partitionInfo, distributionInfo, refreshScheme);
        mv.setIndexMeta(INDEX_ID, mvName, columns, 0, 0, (short) 1, TStorageType.COLUMN, KeysType.DUP_KEYS);
        mv.setBaseIndexMetaId(INDEX_ID);
        mv.addPartition(partition);
        mv.setTableProperty(new com.starrocks.catalog.TableProperty(new HashMap<>()));
        
        // Set base table info
        List<BaseTableInfo> baseTableInfos = new ArrayList<>();
        BaseTableInfo baseTableInfo = new BaseTableInfo(DB_ID, DB_NAME, "base_table", 80003L);
        baseTableInfos.add(baseTableInfo);
        mv.setBaseTableInfos(baseTableInfos);
        
        // Set maintenance plan (required for MVMaintenanceJob)
        ExecPlan execPlan = new ExecPlan();
        mv.setMaintenancePlan(execPlan);
        
        return mv;
    }

    private CreateMaterializedViewStatement createCreateMaterializedViewStatement(MaterializedView mv) throws Exception {
        // Create a mock CreateMaterializedViewStatement with maintenance plan
        CreateMaterializedViewStatement stmt = new CreateMaterializedViewStatement(
                null, false, null, null, null, null, null, null, null, null, null, 0, 0, null, null);
        
        // Create ExecPlan with PlanFragment and OlapTableSink
        ExecPlan execPlan = new ExecPlan();
        PlanFragment fragment = new PlanFragment(new com.starrocks.planner.PlanFragmentId(1), null, DataPartition.UNPARTITIONED);
        // Create OlapTableSink with required parameters
        OlapTableSink tableSink = new OlapTableSink(mv, null, mv.getAllPartitionIds(),
                TWriteQuorumType.MAJORITY, false, false, false);
        fragment.setSink(tableSink);
        execPlan.getFragments().add(fragment);
        
        // Set physicalPlan to avoid NullPointerException in IMTCreator.createIMT
        // Create a simple OptExpression to satisfy IMTCreator.createIMT requirements
        OptExpression physicalPlan = OptExpression.create(null);
        Field physicalPlanField = ExecPlan.class.getDeclaredField("physicalPlan");
        physicalPlanField.setAccessible(true);
        physicalPlanField.set(execPlan, physicalPlan);
        
        // Set maintenance plan with ColumnRefFactory
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        stmt.setMaintenancePlan(execPlan, columnRefFactory);
        return stmt;
    }

    @Test
    public void testPrepareMaintenanceWorkJobAlreadyExists() throws Exception {
        // Mock IMTCreator.createIMT to avoid exceptions during testing
        new MockUp<IMTCreator>() {
            @Mock
            public static void createIMT(CreateMaterializedViewStatement stmt, MaterializedView view) {
                // Do nothing - skip IMT creation for testing
            }
        };

        // 1. Create materialized view
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = createMaterializedView(MV_ID, MV_NAME);
        db.registerTableUnlocked(mv);

        // 2. Create CreateMaterializedViewStatement with maintenance plan
        CreateMaterializedViewStatement stmt = createCreateMaterializedViewStatement(mv);

        // 3. Create job first time
        MaterializedViewMgr mvMgr = GlobalStateMgr.getCurrentState().getMaterializedViewMgr();
        MvId mvId = new MvId(DB_ID, MV_ID);
        mvMgr.prepareMaintenanceWork(stmt, mv);

        // 4. Verify job was created
        Assertions.assertNotNull(mvMgr.getJob(mvId), "Job should be created after first call");

        // 5. Try to create job again - should fail silently (exception is caught)
        // The method catches DdlException and removes the job from jobMap
        mvMgr.prepareMaintenanceWork(stmt, mv);
    }

    @Test
    public void testPrepareMaintenanceWorkNonIncremental() throws Exception {
        // 1. Create materialized view with non-incremental refresh
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = createMaterializedView(MV_ID, MV_NAME);
        // Change refresh type to non-incremental
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);
        mv.setRefreshScheme(refreshScheme);
        db.registerTableUnlocked(mv);

        // 2. Create CreateMaterializedViewStatement with maintenance plan
        CreateMaterializedViewStatement stmt = createCreateMaterializedViewStatement(mv);

        // 3. Call prepareMaintenanceWork - should return early for non-incremental MV
        MaterializedViewMgr mvMgr = GlobalStateMgr.getCurrentState().getMaterializedViewMgr();
        mvMgr.prepareMaintenanceWork(stmt, mv);

        // 4. Verify no job was created
        MvId mvId = new MvId(DB_ID, MV_ID);
        Assertions.assertNull(mvMgr.getJob(mvId), "Job should not be created for non-incremental MV");
    }

    @Test
    public void testPrepareMaintenanceWorkReplay() throws Exception {
        // Mock IMTCreator.createIMT to avoid exceptions during testing
        new MockUp<IMTCreator>() {
            @Mock
            public static void createIMT(CreateMaterializedViewStatement stmt, MaterializedView view) {
                // Do nothing - skip IMT creation for testing
            }
        };

        // 1. Create materialized view
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = createMaterializedView(MV_ID, MV_NAME);
        db.registerTableUnlocked(mv);

        // 2. Create CreateMaterializedViewStatement with maintenance plan
        CreateMaterializedViewStatement stmt = createCreateMaterializedViewStatement(mv);

        // 3. Call prepareMaintenanceWork on master
        MaterializedViewMgr masterMvMgr = GlobalStateMgr.getCurrentState().getMaterializedViewMgr();
        MvId mvId = new MvId(DB_ID, MV_ID);
        masterMvMgr.prepareMaintenanceWork(stmt, mv);

        // 4. Verify master state - job was created
        MVMaintenanceJob masterJob = masterMvMgr.getJob(mvId);
        Assertions.assertNotNull(masterJob, "Job should be created on master");
        Assertions.assertEquals(MV_ID, masterJob.getJobId());
        Assertions.assertEquals(DB_ID, masterJob.getDbId());
        Assertions.assertEquals(MV_ID, masterJob.getViewId());
        Assertions.assertEquals(MVMaintenanceJob.JobState.INIT, masterJob.getState());

        // 5. Test follower replay
        MVMaintenanceJob replayJob = (MVMaintenanceJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MV_JOB_STATE);

        // Verify replay job
        Assertions.assertNotNull(replayJob);
        Assertions.assertEquals(MV_ID, replayJob.getJobId());
        Assertions.assertEquals(DB_ID, replayJob.getDbId());
        Assertions.assertEquals(MV_ID, replayJob.getViewId());
        Assertions.assertEquals(MVMaintenanceJob.JobState.INIT, replayJob.getState());

        // 6. Create follower metastore and the same id objects, then replay
        LocalMetastore followerMetastore = new LocalMetastore(GlobalStateMgr.getCurrentState(), null, null);
        Database followerDb = new Database(DB_ID, DB_NAME);
        followerMetastore.unprotectCreateDb(followerDb);

        // Create materialized view with same ID
        MaterializedView followerMv = createMaterializedView(MV_ID, MV_NAME);
        followerDb.registerTableUnlocked(followerMv);

        // Replay the operation
        MaterializedViewMgr followerMvMgr = new MaterializedViewMgr();
        followerMvMgr.replay(replayJob);

        // 7. Verify follower state
        MVMaintenanceJob followerJob = followerMvMgr.getJob(mvId);
        Assertions.assertNotNull(followerJob, "Job should be created on follower after replay");
        Assertions.assertEquals(MV_ID, followerJob.getJobId());
        Assertions.assertEquals(DB_ID, followerJob.getDbId());
        Assertions.assertEquals(MV_ID, followerJob.getViewId());
        Assertions.assertEquals(MVMaintenanceJob.JobState.INIT, followerJob.getState());

        // Verify job properties match original
        Assertions.assertEquals(masterJob.getJobId(), followerJob.getJobId());
        Assertions.assertEquals(masterJob.getDbId(), followerJob.getDbId());
        Assertions.assertEquals(masterJob.getViewId(), followerJob.getViewId());
        Assertions.assertEquals(masterJob.getState(), followerJob.getState());
    }

    @Test
    public void testPrepareMaintenanceWorkEditLogException() throws Exception {
        // Mock IMTCreator.createIMT to avoid exceptions during testing
        new MockUp<IMTCreator>() {
            @Mock
            public static void createIMT(CreateMaterializedViewStatement stmt, MaterializedView view) {
                // Do nothing - skip IMT creation for testing
            }
        };

        // 1. Create materialized view
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = createMaterializedView(MV_ID, MV_NAME);
        db.registerTableUnlocked(mv);

        // 2. Create CreateMaterializedViewStatement with maintenance plan
        CreateMaterializedViewStatement stmt = createCreateMaterializedViewStatement(mv);

        // 3. Mock EditLog.logMVJobState to throw exception
        EditLog currentEditLog = GlobalStateMgr.getCurrentState().getEditLog();
        EditLog spyEditLog = spy(currentEditLog);
        doThrow(new RuntimeException("EditLog write failed"))
            .when(spyEditLog).logMVJobState(any(MVMaintenanceJob.class), any());

        // Temporarily set spy EditLog
        GlobalStateMgr.getCurrentState().setEditLog(spyEditLog);

        // 4. Verify initial state - no job in MaterializedViewMgr
        MaterializedViewMgr mvMgr = GlobalStateMgr.getCurrentState().getMaterializedViewMgr();
        MvId mvId = new MvId(DB_ID, MV_ID);
        Assertions.assertNull(mvMgr.getJob(mvId), "Job should not exist initially");

        // 5. Execute prepareMaintenanceWork - exception is caught internally, so it won't throw
        // But the job should not be added to jobMap if EditLog fails
        mvMgr.prepareMaintenanceWork(stmt, mv);

        Assertions.assertNull(mvMgr.getJob(mvId));
    }
}
