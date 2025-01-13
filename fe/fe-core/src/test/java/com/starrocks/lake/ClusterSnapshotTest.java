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

<<<<<<< HEAD
//import com.google.common.collect.Lists;
=======
>>>>>>> e33211689 ([Feature] Support Cluster Snapshot Backup: checkpoint and image backup (part3) (#54695))
import com.starrocks.alter.AlterTest;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.MetaNotFoundException;
<<<<<<< HEAD
//import com.starrocks.common.Pair;
//import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.lake.StarOSAgent;
//import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
=======
import com.starrocks.common.Pair;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.snapshot.ClusterSnapshotJob.ClusterSnapshotJobState;
import com.starrocks.leader.CheckpointController;
>>>>>>> e33211689 ([Feature] Support Cluster Snapshot Backup: checkpoint and image backup (part3) (#54695))
import com.starrocks.persist.ClusterSnapshotLog;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
<<<<<<< HEAD
//import com.starrocks.server.RunMode;
=======
import com.starrocks.server.RunMode;
>>>>>>> e33211689 ([Feature] Support Cluster Snapshot Backup: checkpoint and image backup (part3) (#54695))
import com.starrocks.server.StorageVolumeMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOffStmt;
import com.starrocks.sql.ast.AdminSetAutomatedSnapshotOnStmt;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    @BeforeClass
    public static void beforeClass() throws Exception {
        AlterTest.beforeClass();
        AnalyzeTestUtil.init();
    }

    @Before
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
                return 0L;
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
            } //IOException

            @Mock
            public void deletePath(String path, Map<String, String> loadProperties) {
                return;
            } //IOException
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
            Assert.assertEquals(true, GlobalStateMgr.getCurrentState().getStorageVolumeMgr().exists(storageVolumeName));
            Assert.assertEquals(storageVolumeName,
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
        job.addAutomatedClusterSnapshot();
        ClusterSnapshot snapshot = GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAutomatedSnapshot();
        Assert.assertTrue(job.getInfo() != null);
        Assert.assertTrue(snapshot.getInfo() != null);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllInfo().getItemsSize() == 1);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllJobsInfo().getItemsSize() == 1);

        ExceptionChecker.expectThrowsNoException(() ->
                         ClusterSnapshotUtils.uploadAutomatedSnapshotToRemote(job.getSnapshotName()));
        ExceptionChecker.expectThrowsNoException(() ->
                         ClusterSnapshotUtils.clearAutomatedSnapshotFromRemote(job.getSnapshotName()));
        setAutomatedSnapshotOff(false);
    }

    @Test
    public void testReplayClusterSnapshotLog() {
        // create atuomated snapshot request log
        ClusterSnapshotLog logCreate = new ClusterSnapshotLog();
        logCreate.setCreateSnapshotNamePrefix(ClusterSnapshotMgr.AUTOMATED_NAME_PREFIX, storageVolumeName);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logCreate);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isAutomatedSnapshotOn());

        // drop automated snapshot request log
        ClusterSnapshotLog logDrop = new ClusterSnapshotLog();
        logDrop.setDropSnapshot(ClusterSnapshotMgr.AUTOMATED_NAME_PREFIX);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logDrop);
        Assert.assertTrue(!GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().isAutomatedSnapshotOn());

        // create snapshot job log
        ClusterSnapshotLog logSnapshotJob = new ClusterSnapshotLog();
        ClusterSnapshotJob job = clusterSnapshotMgr.createAutomatedSnapshotJob();
        job.setState(ClusterSnapshotJobState.INITIALIZING);
        logSnapshotJob.setSnapshotJob(job);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logSnapshotJob);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllJobsInfo()
                                                                                  .getItems().get(0).state == "INITIALIZING");
        job.setState(ClusterSnapshotJobState.SNAPSHOTING);
        logSnapshotJob.setSnapshotJob(job);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logSnapshotJob);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllJobsInfo()
                                                                                  .getItems().get(0).state == "SNAPSHOTING");
        job.setState(ClusterSnapshotJobState.UPLOADING);
        logSnapshotJob.setSnapshotJob(job);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logSnapshotJob);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllJobsInfo()
                                                                                  .getItems().get(0).state == "UPLOADING");
        job.setState(ClusterSnapshotJobState.FINISHED);
        logSnapshotJob.setSnapshotJob(job);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logSnapshotJob);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllJobsInfo()
                                                                                  .getItems().get(0).state == "FINISHED");
        job.setState(ClusterSnapshotJobState.ERROR);
        logSnapshotJob.setSnapshotJob(job);
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logSnapshotJob);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAllJobsInfo()
                                                                                  .getItems().get(0).state == "ERROR");

        Assert.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAutomatedSnapshot() == null);
        ClusterSnapshotLog logSnapshot = new ClusterSnapshotLog();
        logSnapshot.setCreateSnapshot(new ClusterSnapshot(0, "my_name", "my_sv", 12345, -1, 0, 0));
        GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().replayLog(logSnapshot);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getClusterSnapshotMgr().getAutomatedSnapshot() != null);
    }

    @Test
    public void testCheckpointScheduler() {
        new MockUp<CheckpointController>() {
            @Mock
            public Pair<Long, Long> getCheckpointJournalIds() {
                return Pair.create(1L, 2L);
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
        ClusterSnapshotCheckpointScheduler scheduler = new ClusterSnapshotCheckpointScheduler(feController, starMgrController);
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
}