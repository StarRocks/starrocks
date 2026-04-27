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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
        
        return mv;
    }

    private static MVMaintenanceJob createLegacyJob(MaterializedView mv) {
        return new MVMaintenanceJob(mv);
    }

    private static void logLegacyJob(MaterializedViewMgr mvMgr, MVMaintenanceJob job) {
        EditLog editLog = GlobalStateMgr.getCurrentState().getEditLog();
        editLog.logJsonObject(OperationType.OP_MV_JOB_STATE, job, wal -> {
            try {
                mvMgr.replay((MVMaintenanceJob) wal);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testReplayLegacyJobFromEditLog() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = createMaterializedView(MV_ID, MV_NAME);
        db.registerTableUnlocked(mv);

        MaterializedViewMgr mvMgr = GlobalStateMgr.getCurrentState().getMaterializedViewMgr();
        MvId mvId = new MvId(DB_ID, MV_ID);
        MVMaintenanceJob job = createLegacyJob(mv);
        logLegacyJob(mvMgr, job);

        MVMaintenanceJob masterJob = mvMgr.getJob(mvId);
        Assertions.assertNotNull(masterJob, "Job should be created on master");
        Assertions.assertEquals(MV_ID, masterJob.getJobId());
        Assertions.assertEquals(DB_ID, masterJob.getDbId());
        Assertions.assertEquals(MV_ID, masterJob.getViewId());
        Assertions.assertEquals(MVMaintenanceJob.JobState.INIT, masterJob.getState());

        MVMaintenanceJob replayJob = (MVMaintenanceJob) UtFrameUtils
                .PseudoJournalReplayer.replayNextJournal(OperationType.OP_MV_JOB_STATE);

        Assertions.assertNotNull(replayJob);
        Assertions.assertEquals(MV_ID, replayJob.getJobId());
        Assertions.assertEquals(DB_ID, replayJob.getDbId());
        Assertions.assertEquals(MV_ID, replayJob.getViewId());
        Assertions.assertEquals(MVMaintenanceJob.JobState.INIT, replayJob.getState());

        MaterializedViewMgr followerMvMgr = new MaterializedViewMgr();
        followerMvMgr.replay(replayJob);

        MVMaintenanceJob followerJob = followerMvMgr.getJob(mvId);
        Assertions.assertNotNull(followerJob, "Job should be created on follower after replay");
        Assertions.assertEquals(MV_ID, followerJob.getJobId());
        Assertions.assertEquals(DB_ID, followerJob.getDbId());
        Assertions.assertEquals(MV_ID, followerJob.getViewId());
        Assertions.assertEquals(MVMaintenanceJob.JobState.INIT, followerJob.getState());

        Assertions.assertEquals(masterJob.getJobId(), followerJob.getJobId());
        Assertions.assertEquals(masterJob.getDbId(), followerJob.getDbId());
        Assertions.assertEquals(masterJob.getViewId(), followerJob.getViewId());
        Assertions.assertEquals(masterJob.getState(), followerJob.getState());
    }

    @Test
    public void testLoadLegacyJobFromImage() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = createMaterializedView(MV_ID, MV_NAME);
        db.registerTableUnlocked(mv);

        MaterializedViewMgr mvMgr = GlobalStateMgr.getCurrentState().getMaterializedViewMgr();
        MvId mvId = new MvId(DB_ID, MV_ID);
        MVMaintenanceJob masterJob = createLegacyJob(mv);
        mvMgr.replay(masterJob);

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        mvMgr.save(image.getImageWriter());

        MaterializedViewMgr restoredMvMgr = new MaterializedViewMgr();
        restoredMvMgr.load(image.getMetaBlockReader());

        MVMaintenanceJob restoredJob = restoredMvMgr.getJob(mvId);
        Assertions.assertNotNull(restoredJob, "Job should be restored from image");
        Assertions.assertEquals(masterJob.getJobId(), restoredJob.getJobId());
        Assertions.assertEquals(masterJob.getDbId(), restoredJob.getDbId());
        Assertions.assertEquals(masterJob.getViewId(), restoredJob.getViewId());
        Assertions.assertEquals(masterJob.getState(), restoredJob.getState());
    }

    @Test
    public void testReplayLegacyEpochFromEditLog() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = createMaterializedView(MV_ID, MV_NAME);
        db.registerTableUnlocked(mv);

        MaterializedViewMgr mvMgr = GlobalStateMgr.getCurrentState().getMaterializedViewMgr();
        MvId mvId = new MvId(DB_ID, MV_ID);
        mvMgr.replay(createLegacyJob(mv));

        MVEpoch epoch = new MVEpoch(mvId);
        epoch.setState(MVEpoch.EpochState.COMMITTED);
        GlobalStateMgr.getCurrentState().getEditLog().logJsonObject(OperationType.OP_MV_EPOCH_UPDATE, epoch);

        MVEpoch replayEpoch = (MVEpoch) UtFrameUtils.PseudoJournalReplayer
                .replayNextJournal(OperationType.OP_MV_EPOCH_UPDATE);
        Assertions.assertNotNull(replayEpoch);
        Assertions.assertEquals(MVEpoch.EpochState.COMMITTED, replayEpoch.getState());

        MaterializedViewMgr followerMvMgr = new MaterializedViewMgr();
        followerMvMgr.replay(createLegacyJob(mv));
        followerMvMgr.replayEpoch(replayEpoch);

        Assertions.assertEquals(MVEpoch.EpochState.COMMITTED, followerMvMgr.getJob(mvId).getEpoch().getState());
    }

    @Test
    public void testLoadLegacyJobWithMissingTargetMvSkipsJobAndEpochReplay() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
        MaterializedView mv = createMaterializedView(MV_ID, MV_NAME);
        db.registerTableUnlocked(mv);

        MaterializedViewMgr mvMgr = GlobalStateMgr.getCurrentState().getMaterializedViewMgr();
        MvId mvId = new MvId(DB_ID, MV_ID);
        mvMgr.replay(createLegacyJob(mv));

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        mvMgr.save(image.getImageWriter());

        db.dropTable(mv.getName());

        MaterializedViewMgr restoredMvMgr = new MaterializedViewMgr();
        restoredMvMgr.load(image.getMetaBlockReader());

        Assertions.assertNull(restoredMvMgr.getJob(mvId), "Missing target MV should skip legacy job restore");

        MVEpoch epoch = new MVEpoch(mvId);
        epoch.setState(MVEpoch.EpochState.COMMITTED);
        restoredMvMgr.replayEpoch(epoch);

        Assertions.assertNull(restoredMvMgr.getJob(mvId), "Epoch replay should remain a no-op for skipped legacy jobs");
    }
}
