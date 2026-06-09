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

package com.starrocks.lake.snapshot;

import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.AlterTest;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.alter.SchemaChangeJobV2;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.leader.CheckpointController;
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class ClusterSnapshotTest {
    @Mocked
    private EditLog editLog;

    private StarOSAgent starOSAgent = new StarOSAgent();

    private String storageVolumeName = StorageVolumeMgr.BUILTIN_STORAGE_VOLUME;
    private ClusterSnapshotMgr clusterSnapshotMgr = new ClusterSnapshotMgr();
    private boolean initSv = false;

    private AtomicLong nextId = new AtomicLong(0);

    @BeforeAll
    public static void beforeClass() throws Exception {
        AlterTest.beforeClass();
        AnalyzeTestUtil.init();
    }

    @BeforeEach
    public void setUp() {
        try {
            initStorageVolume();
        } catch (Exception ignore) {
        }

        new Expectations() {
            {
                editLog.logClusterSnapshotLog((ClusterSnapshotLog) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logClusterSnapshotLog(ClusterSnapshotLog log) {
                    }
                };
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }

            @Mock
            public ClusterSnapshotMgr getClusterSnapshotMgr() {
                return clusterSnapshotMgr;
            }

            @Mock
            public long getNextId() {
                long id = nextId.incrementAndGet();
                return id;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            public String getRawServiceId() {
                return "qwertty";
            }
        };

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        new MockUp<HdfsFsManager>() {
            @Mock
            public void copyFromLocal(String srcPath, String destPath, Map<String, String> properties) {
                return;
            } // IOException

            @Mock
            public void deletePath(String path, Map<String, String> loadProperties) {
                return;
            } // IOException
        };

        setAutomatedSnapshotOff(false);
    }

    private void setAutomatedSnapshotOn(boolean testReplay) {
        if (!testReplay) {
            GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().setAutomatedSnapshotOn(
                    new AdminSetAutomatedSnapshotOnStmt(storageVolumeName));
        } else {
            GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().setAutomatedSnapshotOn(storageVolumeName);
        }
    }

    private void setAutomatedSnapshotOff(boolean testReplay) {
        if (!testReplay) {
            GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().setAutomatedSnapshotOff(
                    new AdminSetAutomatedSnapshotOffStmt());
        } else {
            GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().setAutomatedSnapshotOff();
        }
    }

    private void initStorageVolume() throws AlreadyExistsException, DdlException, MetaNotFoundException {
        if (!initSv) {
            List<String> locations = Arrays.asList("s3://abc");
            Map<String, String> storageParams = new HashMap<>();
            storageParams.put(AWS_S3_REGION, "region");
            storageParams.put(AWS_S3_ENDPOINT, "endpoint");
            storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
            String svKey = GlobalStateMgr.getCurrentState().getStorageVolumeMgr()
                    .createStorageVolume(storageVolumeName, "S3", locations, storageParams, Optional.empty(), "");
            Assertions.assertEquals(true, GlobalStateMgr.getCurrentState().getStorageVolumeMgr().exists(storageVolumeName));
            Assertions.assertEquals(storageVolumeName,
                    GlobalStateMgr.getCurrentState().getStorageVolumeMgr().getStorageVolumeName(svKey));
            initSv = true;
        }
    }

    @Test
    public void testOperationOfAutomatedSnapshot() throws DdlException {
        // 1. test analyer and execution
        String turnOnSql = "ADMIN SET AUTOMATED CLUSTER SNAPSHOT ON";
        // no sv
        analyzeFail(turnOnSql + " STORAGE VOLUME testSv");

        analyzeSuccess(turnOnSql);
        setAutomatedSnapshotOn(false);
        // duplicate creation
        analyzeFail(turnOnSql);

        setAutomatedSnapshotOff(false);

        String turnOFFSql = "ADMIN SET AUTOMATED CLUSTER SNAPSHOT OFF";
        analyzeFail(turnOFFSql);
        setAutomatedSnapshotOn(false);
        analyzeSuccess(turnOFFSql);
        setAutomatedSnapshotOff(false);

        // 2. test getInfo and network utils
        setAutomatedSnapshotOn(false);
        ClusterSnapshotJob job = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().createAutomatedSnapshotJob();
        job.setState(ClusterSnapshotJobState.FINISHED);
        ClusterSnapshot snapshot = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAutomatedSnapshot();
        Assertions.assertTrue(job.getInfo() != null);
        Assertions.assertTrue(snapshot.getInfo() != null);
        Assertions.assertTrue(
                GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllSnapshotsInfo().getItemsSize() == 1);
        Assertions.assertTrue(
                GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllSnapshotJobsInfo().getItemsSize() == 1);

        ExceptionChecker.expectThrowsNoException(
                () -> ClusterSnapshotUtils.uploadAutomatedSnapshotToRemote(job.getSnapshotName()));
        ExceptionChecker.expectThrowsNoException(
                () -> ClusterSnapshotUtils.clearAutomatedSnapshotFromRemote(job.getSnapshotName()));
        setAutomatedSnapshotOff(false);
    }

    @Test
    public void testReplayClusterSnapshotLog() {
        // create atuomated snapshot request log
        ClusterSnapshotLog logCreate = new ClusterSnapshotLog();
        logCreate.setAutomatedSnapshotOn(storageVolumeName);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logCreate);
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isAutomatedSnapshotOn());

        // drop automated snapshot request log
        ClusterSnapshotLog logDrop = new ClusterSnapshotLog();
        logDrop.setAutomatedSnapshotOff();
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logDrop);
        Assertions.assertTrue(!GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isAutomatedSnapshotOn());

        // create snapshot job log
        ClusterSnapshotLog logSnapshotJob = new ClusterSnapshotLog();
        ClusterSnapshotJob job = clusterSnapshotMgr.createAutomatedSnapshotJob();
        job.setState(ClusterSnapshotJobState.INITIALIZING);
        logSnapshotJob.setSnapshotJob(job);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logSnapshotJob);
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllSnapshotJobsInfo()
                .getItems().get(0).state == "INITIALIZING");
        job.setState(ClusterSnapshotJobState.SNAPSHOTING);
        logSnapshotJob.setSnapshotJob(job);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logSnapshotJob);
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllSnapshotJobsInfo()
                .getItems().get(0).state == "SNAPSHOTING");
        job.setState(ClusterSnapshotJobState.UPLOADING);
        logSnapshotJob.setSnapshotJob(job);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logSnapshotJob);
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllSnapshotJobsInfo()
                .getItems().get(0).state == "UPLOADING");
        job.setState(ClusterSnapshotJobState.FINISHED);
        logSnapshotJob.setSnapshotJob(job);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logSnapshotJob);
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllSnapshotJobsInfo()
                .getItems().get(0).state == "FINISHED");
        job.setState(ClusterSnapshotJobState.ERROR);
        logSnapshotJob.setSnapshotJob(job);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logSnapshotJob);
        Assertions.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllSnapshotJobsInfo()
                .getItems().get(0).state == "ERROR");
    }

    public void testCheckpointScheduler() {
        new MockUp<CheckpointController>() {
            @Mock
            public long getImageJournalId() {
                return 1L;
            }

            @Mock
            public Pair<Boolean, String> runCheckpointControllerWithIds(long imageJournalId, long maxJournalId) {
                return Pair.create(true, "");
            }
        };

        new MockUp<BDBJEJournal>() {
            @Mock
            public long getMaxJournalId() {
                return 10;
            }
        };

        setAutomatedSnapshotOn(false);
        Config.automated_cluster_snapshot_interval_seconds = 1;
        CheckpointController feController = new CheckpointController("fe", new BDBJEJournal(null, ""), "");
        CheckpointController starMgrController = new CheckpointController("starMgr", new BDBJEJournal(null, ""), "");
        ClusterSnapshotCheckpointScheduler scheduler = new ClusterSnapshotCheckpointScheduler(feController,
                starMgrController);
        scheduler.start();

        while (GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAutomatedSnapshot() == null) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }

        scheduler.setStop();

        while (scheduler.isRunning()) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
            }
        }
        setAutomatedSnapshotOff(false);
    }

    @Test
    public void testSnapshotHealthAccessors() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
        ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
        Assertions.assertEquals(0, mgr.getConsecutiveFailureCount());
        Assertions.assertEquals(0L, mgr.getLastSuccessTimeMs());

        mgr.setAutomatedSnapshotOn(storageVolumeName);
        ClusterSnapshotJob ok = mgr.createAutomatedSnapshotJob();
        ok.setState(ClusterSnapshotJobState.FINISHED);
        ClusterSnapshotJob e1 = mgr.createAutomatedSnapshotJob();
        e1.setState(ClusterSnapshotJobState.ERROR);
        ClusterSnapshotJob e2 = mgr.createAutomatedSnapshotJob();
        e2.setState(ClusterSnapshotJobState.ERROR);
        // A newer in-progress job must not be counted as a failure nor stop the count.
        mgr.createAutomatedSnapshotJob(); // stays INITIALIZING (in-progress)

        Assertions.assertEquals(2, mgr.getConsecutiveFailureCount());
        Assertions.assertTrue(mgr.getLastSuccessTimeMs() > 0);
        mgr.setAutomatedSnapshotOff();
    }

    @Test
    public void testDeletionControl() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        {
            final ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
            // snapshot OFF -> no constraint
            Assertions.assertEquals(Long.MAX_VALUE, mgr.getSafeDeletionTimeMs());

            mgr.setAutomatedSnapshotOn(storageVolumeName);
            // ON, zero jobs -> nothing to protect -> ~now (recycle bin free)
            long before = System.currentTimeMillis();
            long safe0 = mgr.getSafeDeletionTimeMs();
            Assertions.assertTrue(safe0 >= before && safe0 <= System.currentTimeMillis() + 5000,
                    "expected ~now, got " + safe0);

            // exactly one FINISHED -> protect it
            ClusterSnapshotJob job1 = mgr.createAutomatedSnapshotJob();
            job1.setState(ClusterSnapshotJobState.FINISHED);
            Assertions.assertEquals(job1.getCreatedTimeMs(), mgr.getSafeDeletionTimeMs());

            // two FINISHED -> protect the 2nd-newest (unchanged behavior)
            ClusterSnapshotJob job2 = mgr.createAutomatedSnapshotJob();
            job2.setState(ClusterSnapshotJobState.FINISHED);
            Assertions.assertEquals(job1.getCreatedTimeMs(), mgr.getSafeDeletionTimeMs());

            mgr.setAutomatedSnapshotOff();
        }
        {
            // all-ERROR history (production scenario) -> ~now (recycle bin free, not frozen)
            final ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
            mgr.setAutomatedSnapshotOn(storageVolumeName);
            ClusterSnapshotJob jobE = mgr.createAutomatedSnapshotJob();
            jobE.setState(ClusterSnapshotJobState.ERROR);
            long before2 = System.currentTimeMillis();
            long safeE = mgr.getSafeDeletionTimeMs();
            Assertions.assertTrue(safeE >= before2 && safeE <= System.currentTimeMillis() + 5000,
                    "expected ~now, got " + safeE);
            mgr.setAutomatedSnapshotOff();
        }
        {
            // zero finished + an in-progress job -> protect the in-progress job's createdTime
            final ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
            mgr.setAutomatedSnapshotOn(storageVolumeName);
            ClusterSnapshotJob init = mgr.createAutomatedSnapshotJob(); // stays INITIALIZING (unfinished)
            Assertions.assertEquals(init.getCreatedTimeMs(), mgr.getSafeDeletionTimeMs());
            mgr.setAutomatedSnapshotOff();
        }
        {
            // one FINISHED + a newer in-progress job -> min(finished, in-progress) = the FINISHED createdTime
            final ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
            mgr.setAutomatedSnapshotOn(storageVolumeName);
            ClusterSnapshotJob good = mgr.createAutomatedSnapshotJob();
            good.setState(ClusterSnapshotJobState.FINISHED);
            ClusterSnapshotJob newer = mgr.createAutomatedSnapshotJob(); // INITIALIZING, created after good
            Assertions.assertEquals(good.getCreatedTimeMs(), mgr.getSafeDeletionTimeMs());
            mgr.setAutomatedSnapshotOff();
        }
        {
            // one FINISHED followed by a newer ERROR -> still protect the last good snapshot
            final ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
            mgr.setAutomatedSnapshotOn(storageVolumeName);
            ClusterSnapshotJob good = mgr.createAutomatedSnapshotJob();
            good.setState(ClusterSnapshotJobState.FINISHED);
            ClusterSnapshotJob failAfter = mgr.createAutomatedSnapshotJob();
            failAfter.setState(ClusterSnapshotJobState.ERROR);
            Assertions.assertEquals(good.getCreatedTimeMs(), mgr.getSafeDeletionTimeMs());
            mgr.setAutomatedSnapshotOff();
        }
        {
            // newer FINISHED with an older EXPIRED below it -> 2nd-completed boundary = the EXPIRED createdTime
            final ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
            mgr.setAutomatedSnapshotOn(storageVolumeName);
            ClusterSnapshotJob older = mgr.createAutomatedSnapshotJob();
            older.setState(ClusterSnapshotJobState.FINISHED);
            older.setState(ClusterSnapshotJobState.EXPIRED);
            ClusterSnapshotJob newer = mgr.createAutomatedSnapshotJob();
            newer.setState(ClusterSnapshotJobState.FINISHED);
            Assertions.assertEquals(older.getCreatedTimeMs(), mgr.getSafeDeletionTimeMs());
            mgr.setAutomatedSnapshotOff();
        }
        {
            // production lifecycle: an old snapshot fully retired (FINISHED -> EXPIRED -> DELETED), a newer
            // FINISHED, then a trailing ERROR. The DELETED job still anchors the 2nd-completed boundary
            // (historical behavior); the trailing ERROR protects nothing.
            final ClusterSnapshotMgr mgr = new ClusterSnapshotMgr();
            mgr.setAutomatedSnapshotOn(storageVolumeName);
            ClusterSnapshotJob retired = mgr.createAutomatedSnapshotJob();
            retired.setState(ClusterSnapshotJobState.FINISHED);
            retired.setState(ClusterSnapshotJobState.EXPIRED);
            retired.setState(ClusterSnapshotJobState.DELETED);
            ClusterSnapshotJob newer = mgr.createAutomatedSnapshotJob();
            newer.setState(ClusterSnapshotJobState.FINISHED);
            ClusterSnapshotJob failAfter = mgr.createAutomatedSnapshotJob();
            failAfter.setState(ClusterSnapshotJobState.ERROR);
            Assertions.assertEquals(retired.getCreatedTimeMs(), mgr.getSafeDeletionTimeMs());
            mgr.setAutomatedSnapshotOff();
        }

        AlterJobV2 alterjob1 = new SchemaChangeJobV2(1, 2, 10, "table1", 100000);
        AlterJobV2 alterjob2 = new SchemaChangeJobV2(2, 2, 11, "table2", 100000);
        alterjob1.setJobState(AlterJobV2.JobState.FINISHED);
        alterjob1.setFinishedTimeMs(1000); // ancient alter (table 10): older than any snapshot boundary
        alterjob2.setJobState(AlterJobV2.JobState.FINISHED);
        alterjob2.setFinishedTimeMs(Long.MAX_VALUE); // "future" alter (table 11): newer than any boundary
        MaterializedViewHandler rollupHandler = new MaterializedViewHandler();
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        schemaChangeHandler.addAlterJobV2(alterjob1);
        schemaChangeHandler.addAlterJobV2(alterjob2);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public SchemaChangeHandler getSchemaChangeHandler() {
                return schemaChangeHandler;
            }

            @Mock
            public MaterializedViewHandler getRollupHandler() {
                return rollupHandler;
            }
        };

        {
            final ClusterSnapshotMgr localClusterSnapshotMgr = new ClusterSnapshotMgr();
            // snapshot OFF -> always safe to delete tablets
            Assertions.assertTrue(localClusterSnapshotMgr.isTableSafeToDeleteTablet(10));
            Assertions.assertTrue(localClusterSnapshotMgr.isTableSafeToDeleteTablet(11));

            localClusterSnapshotMgr.setAutomatedSnapshotOn(storageVolumeName);
            // ON but no successful snapshot -> recycle bin is NOT frozen: an alter older than the
            // safe-deletion boundary (table 10, finished at epoch 1000) is safe to delete; an alter
            // newer than the boundary (table 11, finished in the future) is still protected.
            Assertions.assertTrue(localClusterSnapshotMgr.isTableSafeToDeleteTablet(10));
            Assertions.assertTrue(!localClusterSnapshotMgr.isTableSafeToDeleteTablet(11));

            ClusterSnapshotJob j1 = localClusterSnapshotMgr.createAutomatedSnapshotJob();
            j1.setState(ClusterSnapshotJobState.FINISHED);
            Assertions.assertTrue(localClusterSnapshotMgr.isTableSafeToDeleteTablet(10));
            Assertions.assertTrue(!localClusterSnapshotMgr.isTableSafeToDeleteTablet(11));
    
            ClusterSnapshotJob j2 = localClusterSnapshotMgr.createAutomatedSnapshotJob();
            j2.setState(ClusterSnapshotJobState.FINISHED);
            Assertions.assertTrue(localClusterSnapshotMgr.isTableSafeToDeleteTablet(10));
            Assertions.assertTrue(!localClusterSnapshotMgr.isTableSafeToDeleteTablet(11));
            localClusterSnapshotMgr.setAutomatedSnapshotOff();
        }
    }

    @Test
    public void testResetStateAfterRestore() {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };

        ClusterSnapshotMgr localClusterSnapshotMgr = new ClusterSnapshotMgr();
        localClusterSnapshotMgr.setAutomatedSnapshotOn(storageVolumeName);

        ClusterSnapshotJob job2 = localClusterSnapshotMgr.createAutomatedSnapshotJob();
        RestoredSnapshotInfo restoredSnapshotInfo = new RestoredSnapshotInfo(job2.getSnapshotName(), 666L, 6666L);
        localClusterSnapshotMgr.setLastJobFinishedAfterRestored(restoredSnapshotInfo);

        Assertions.assertTrue(job2.getFeJournalId() == 666L);
        Assertions.assertTrue(job2.getStarMgrJournalId() == 6666L);
        Assertions.assertTrue(job2.isFinished());
        localClusterSnapshotMgr.setAutomatedSnapshotOff();
    }
}
