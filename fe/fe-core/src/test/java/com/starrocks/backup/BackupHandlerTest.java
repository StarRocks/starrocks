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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/backup/BackupHandlerTest.java

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

package com.starrocks.backup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.catalog.BrokerMgr;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.BackupRestoreAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.BackupStmt;
import com.starrocks.sql.ast.CancelBackupStmt;
import com.starrocks.sql.ast.CreateRepositoryStmt;
import com.starrocks.sql.ast.DropRepositoryStmt;
import com.starrocks.sql.ast.RestoreStmt;
import com.starrocks.task.DirMoveTask;
import com.starrocks.task.DownloadTask;
import com.starrocks.task.SnapshotTask;
import com.starrocks.task.UploadTask;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TSnapshotRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class BackupHandlerTest {

    private BackupHandler handler;

    private Database db;

    private long idGen = 0;

    private File rootDir;

    private String tmpPath = "./tmp" + System.currentTimeMillis();

    private TabletInvertedIndex invertedIndex = new TabletInvertedIndex();

    public void setUpMocker(GlobalStateMgr globalStateMgr, BrokerMgr brokerMgr, EditLog editLog) {
        Config.tmp_dir = tmpPath;
        rootDir = new File(Config.tmp_dir);
        rootDir.mkdirs();

        MetricRepo.init();

        new Expectations() {
            {
                globalStateMgr.getBrokerMgr();
                minTimes = 0;
                result = brokerMgr;

                globalStateMgr.getNextId();
                minTimes = 0;
                result = idGen++;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                GlobalStateMgr.getCurrentInvertedIndex();
                minTimes = 0;
                result = invertedIndex;
            }
        };

        try {
            db = CatalogMocker.mockDb();
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }

        new Expectations() {
            {
                globalStateMgr.getDb(anyString);
                minTimes = 0;
                result = db;
            }
        };
    }

    @After
    public void done() {
        if (rootDir != null) {
            try {
                Files.walk(Paths.get(Config.tmp_dir),
                                FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testInit(@Mocked GlobalStateMgr globalStateMgr, @Mocked BrokerMgr brokerMgr, @Mocked EditLog editLog) {
        setUpMocker(globalStateMgr, brokerMgr, editLog);
        handler = new BackupHandler(globalStateMgr);
        handler.runAfterCatalogReady();

        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        Assert.assertTrue(backupDir.exists());
    }

    @Test
    public void testCreateAndDropRepository(
            @Mocked GlobalStateMgr globalStateMgr, @Mocked BrokerMgr brokerMgr, @Mocked EditLog editLog) throws Exception {
        setUpMocker(globalStateMgr, brokerMgr, editLog);
        new Expectations() {
            {
                editLog.logCreateRepository((Repository) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logCreateRepository(Repository repo) {

                    }
                };

                editLog.logDropRepository(anyString);
                minTimes = 0;
                result = new Delegate() {
                    public void logDropRepository(String repoName) {

                    }
                };
            }
        };

        new MockUp<Repository>() {
            @Mock
            public Status initRepository() {
                return Status.OK;
            }

            @Mock
            public Status listSnapshots(List<String> snapshotNames) {
                snapshotNames.add("ss2");
                return Status.OK;
            }

            @Mock
            public Status getSnapshotInfoFile(String label, String backupTimestamp, List<BackupJobInfo> infos) {
                OlapTable tbl = (OlapTable) db.getTable(CatalogMocker.TEST_TBL_NAME);
                List<Table> tbls = Lists.newArrayList();
                tbls.add(tbl);
                Map<Long, SnapshotInfo> snapshotInfos = Maps.newHashMap();
                for (Partition part : tbl.getPartitions()) {
                    for (MaterializedIndex idx : part.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        for (Tablet tablet : idx.getTablets()) {
                            List<String> files = Lists.newArrayList();
                            SnapshotInfo sinfo = new SnapshotInfo(db.getId(), tbl.getId(), part.getId(), idx.getId(),
                                    tablet.getId(), -1, 0, "./path", files);
                            snapshotInfos.put(tablet.getId(), sinfo);
                        }
                    }
                }

                BackupJobInfo info = BackupJobInfo.fromCatalog(System.currentTimeMillis(),
                        "ss2", CatalogMocker.TEST_DB_NAME,
                        CatalogMocker.TEST_DB_ID, tbls, snapshotInfos);
                infos.add(info);
                return Status.OK;
            }
        };

        new Expectations() {
            {
                brokerMgr.containsBroker(anyString);
                minTimes = 0;
                result = true;
            }
        };

        // add repo
        handler = new BackupHandler(globalStateMgr);
        CreateRepositoryStmt stmt = new CreateRepositoryStmt(false, "repo", "broker", "bos://location",
                Maps.newHashMap());
        try {
            handler.createRepository(stmt);
        } catch (DdlException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // process backup
        List<TableRef> tblRefs = Lists.newArrayList();
        tblRefs.add(new TableRef(new TableName(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME), null));
        BackupStmt backupStmt = new BackupStmt(new LabelName(CatalogMocker.TEST_DB_NAME, "label1"), "repo", tblRefs,
                null);
        try {
            handler.process(backupStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // handleFinishedSnapshotTask
        BackupJob backupJob = (BackupJob) handler.getJob(CatalogMocker.TEST_DB_ID);
        SnapshotTask snapshotTask = new SnapshotTask(null, 0, 0, backupJob.getJobId(), CatalogMocker.TEST_DB_ID,
                0, 0, 0, 0, 0, 0, 1, false);
        TFinishTaskRequest request = new TFinishTaskRequest();
        List<String> snapshotFiles = Lists.newArrayList();
        request.setSnapshot_files(snapshotFiles);
        request.setSnapshot_path("./snapshot/path");
        request.setTask_status(new TStatus(TStatusCode.OK));
        handler.handleFinishedSnapshotTask(snapshotTask, request);

        // handleFinishedSnapshotUploadTask
        Map<String, String> srcToDestPath = Maps.newHashMap();
        UploadTask uploadTask = new UploadTask(null, 0, 0, backupJob.getJobId(), CatalogMocker.TEST_DB_ID,
                srcToDestPath, null, null);
        request = new TFinishTaskRequest();
        Map<Long, List<String>> tabletFiles = Maps.newHashMap();
        request.setTablet_files(tabletFiles);
        request.setTask_status(new TStatus(TStatusCode.OK));
        handler.handleFinishedSnapshotUploadTask(uploadTask, request);

        // test file persist
        File tmpFile = new File("./tmp" + System.currentTimeMillis());
        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpFile));
            handler.write(out);
            out.flush();
            out.close();
            DataInputStream in = new DataInputStream(new FileInputStream(tmpFile));
            BackupHandler.read(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            tmpFile.delete();
        }

        // cancel backup
        try {
            handler.cancel(new CancelBackupStmt(CatalogMocker.TEST_DB_NAME, false));
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // process primary key table backup
        List<TableRef> tblRefs1 = Lists.newArrayList();
        tblRefs1.add(new TableRef(new TableName(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL3_NAME), null));
        BackupStmt backupStmt1 =
                new BackupStmt(new LabelName(CatalogMocker.TEST_DB_NAME, "label2"), "repo", tblRefs1,
                        null);
        try {
            handler.process(backupStmt1);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // handleFinishedSnapshotTask
        BackupJob backupJob1 = (BackupJob) handler.getJob(CatalogMocker.TEST_DB_ID);
        SnapshotTask snapshotTask1 = new SnapshotTask(null, 0, 0, backupJob1.getJobId(), CatalogMocker.TEST_DB_ID,
                0, 0, 0, 0, 0, 0, 1, false);
        TFinishTaskRequest request1 = new TFinishTaskRequest();
        List<String> snapshotFiles1 = Lists.newArrayList();
        request1.setSnapshot_files(snapshotFiles1);
        request1.setSnapshot_path("./snapshot/path1");
        request1.setTask_status(new TStatus(TStatusCode.OK));
        handler.handleFinishedSnapshotTask(snapshotTask1, request1);

        // handleFinishedSnapshotUploadTask
        Map<String, String> srcToDestPath1 = Maps.newHashMap();
        UploadTask uploadTask1 = new UploadTask(null, 0, 0, backupJob1.getJobId(), CatalogMocker.TEST_DB_ID,
                srcToDestPath1, null, null);
        request1 = new TFinishTaskRequest();
        Map<Long, List<String>> tabletFiles1 = Maps.newHashMap();
        request1.setTablet_files(tabletFiles1);
        request1.setTask_status(new TStatus(TStatusCode.OK));
        handler.handleFinishedSnapshotUploadTask(uploadTask1, request1);

        // test file persist
        File tmpFile1 = new File("./tmp1" + System.currentTimeMillis());
        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpFile1));
            handler.write(out);
            out.flush();
            out.close();
            DataInputStream in = new DataInputStream(new FileInputStream(tmpFile1));
            BackupHandler.read(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            tmpFile1.delete();
        }

        // cancel backup
        try {
            handler.cancel(new CancelBackupStmt(CatalogMocker.TEST_DB_NAME, false));
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // process restore
        List<TableRef> tblRefs2 = Lists.newArrayList();
        tblRefs2.add(new TableRef(new TableName(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME), null));
        Map<String, String> properties = Maps.newHashMap();
        properties.put("backup_timestamp", "2018-08-08-08-08-08");
        RestoreStmt restoreStmt = new RestoreStmt(new LabelName(CatalogMocker.TEST_DB_NAME, "ss2"), "repo", tblRefs2,
                properties);
        try {
            BackupRestoreAnalyzer.analyze(restoreStmt, new ConnectContext());
        } catch (SemanticException e2) {
            e2.printStackTrace();
            Assert.fail();
        }

        try {
            handler.process(restoreStmt);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // handleFinishedSnapshotTask
        RestoreJob restoreJob = (RestoreJob) handler.getJob(CatalogMocker.TEST_DB_ID);
        snapshotTask = new SnapshotTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID,
                0, 0, 0, 0, 0, 0, 1, true);
        request = new TFinishTaskRequest();
        request.setSnapshot_path("./snapshot/path");
        request.setTask_status(new TStatus(TStatusCode.OK));
        handler.handleFinishedSnapshotTask(snapshotTask, request);

        // handleDownloadSnapshotTask
        DownloadTask downloadTask = new DownloadTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID,
                srcToDestPath, null, null);
        request = new TFinishTaskRequest();
        List<Long> downloadedTabletIds = Lists.newArrayList();
        request.setDownloaded_tablet_ids(downloadedTabletIds);
        request.setTask_status(new TStatus(TStatusCode.OK));
        handler.handleDownloadSnapshotTask(downloadTask, request);

        // handleDirMoveTask
        DirMoveTask dirMoveTask = new DirMoveTask(null, 0, 0, restoreJob.getJobId(), CatalogMocker.TEST_DB_ID, 0, 0, 0,
                0, "", 0, true);
        request = new TFinishTaskRequest();
        request.setTask_status(new TStatus(TStatusCode.OK));
        handler.handleDirMoveTask(dirMoveTask, request);

        // test file persist
        tmpFile = new File("./tmp" + System.currentTimeMillis());
        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpFile));
            handler.write(out);
            out.flush();
            out.close();
            DataInputStream in = new DataInputStream(new FileInputStream(tmpFile));
            BackupHandler.read(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            tmpFile.delete();
        }

        new MockUp<ConnectContext>() {
            @Mock
            GlobalStateMgr getGlobalStateMgr() {
                return globalStateMgr;
            }
        };
        new MockUp<GlobalStateMgr>() {
            @Mock
            BackupHandler getBackupHandler() {
                return handler;
            }
        };
        // cancel restore
        handler.cancel(new CancelBackupStmt(CatalogMocker.TEST_DB_NAME, true));

        // process primary key table restore
        List<TableRef> tblRefs3 = Lists.newArrayList();
        tblRefs3.add(new TableRef(new TableName(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME), null));
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put("backup_timestamp", "2018-08-08-08-08-08");
        RestoreStmt restoreStmt1 = new RestoreStmt(new LabelName(CatalogMocker.TEST_DB_NAME, "label2"), "repo", tblRefs3,
                properties1);
        try {
            BackupRestoreAnalyzer.analyze(restoreStmt1, new ConnectContext());
        } catch (SemanticException e2) {
            e2.printStackTrace();
            Assert.fail();
        }

        try {
            handler.process(restoreStmt1);
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        // handleFinishedSnapshotTask
        RestoreJob restoreJob1 = (RestoreJob) handler.getJob(CatalogMocker.TEST_DB_ID);
        snapshotTask1 = new SnapshotTask(null, 0, 0, restoreJob1.getJobId(), CatalogMocker.TEST_DB_ID,
                0, 0, 0, 0, 0, 0, 1, true);
        request1 = new TFinishTaskRequest();
        request1.setSnapshot_path("./snapshot/path1");
        request1.setTask_status(new TStatus(TStatusCode.OK));
        handler.handleFinishedSnapshotTask(snapshotTask1, request1);

        // handleDownloadSnapshotTask
        DownloadTask downloadTask1 = new DownloadTask(null, 0, 0, restoreJob1.getJobId(), CatalogMocker.TEST_DB_ID,
                srcToDestPath1, null, null);
        request1 = new TFinishTaskRequest();
        List<Long> downloadedTabletIds1 = Lists.newArrayList();
        request1.setDownloaded_tablet_ids(downloadedTabletIds1);
        request1.setTask_status(new TStatus(TStatusCode.OK));
        handler.handleDownloadSnapshotTask(downloadTask1, request1);

        // handleDirMoveTask
        DirMoveTask dirMoveTask1 = new DirMoveTask(null, 0, 0, restoreJob1.getJobId(), CatalogMocker.TEST_DB_ID, 0, 0, 0,
                0, "", 0, true);
        request1 = new TFinishTaskRequest();
        request1.setTask_status(new TStatus(TStatusCode.OK));
        handler.handleDirMoveTask(dirMoveTask1, request1);

        // test file persist
        tmpFile1 = new File("./tmp1" + System.currentTimeMillis());
        try {
            DataOutputStream out = new DataOutputStream(new FileOutputStream(tmpFile1));
            handler.write(out);
            out.flush();
            out.close();
            DataInputStream in = new DataInputStream(new FileInputStream(tmpFile1));
            BackupHandler.read(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            tmpFile1.delete();
        }

        // cancel restore
        try {
            handler.cancel(new CancelBackupStmt(CatalogMocker.TEST_DB_NAME, true));
        } catch (DdlException e1) {
            e1.printStackTrace();
            Assert.fail();
        }

        TSnapshotRequest requestSnapshot = snapshotTask1.toThrift();

        // drop repo
        DDLStmtExecutor.execute(new DropRepositoryStmt("repo"), new ConnectContext());
    }

    @Test
    public void testExpired() throws Exception {
        UtFrameUtils.setUpForPersistTest();

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        handler = new BackupHandler(globalStateMgr);
        Assert.assertEquals(0, handler.dbIdToBackupOrRestoreJob.size());
        long now = System.currentTimeMillis();

        // 1. create 3 jobs
        // running jobs, won't expire
        BackupJob runningJob = new BackupJob("running_job", 1, "test_db", new ArrayList<>(), 10000, globalStateMgr, 1);
        handler.dbIdToBackupOrRestoreJob.put(runningJob.getDbId(), runningJob);
        // just finished job, won't expire
        BackupJob goodJob = new BackupJob("good_job", 2, "test_db", new ArrayList<>(), 10000, globalStateMgr, 1);
        goodJob.finishedTime = now;
        goodJob.state = BackupJob.BackupJobState.FINISHED;
        handler.dbIdToBackupOrRestoreJob.put(goodJob.getDbId(), goodJob);
        // expired job
        BackupJob badJob = new BackupJob("bad_job", 3, "test_db", new ArrayList<>(), 10000, globalStateMgr, 1);
        badJob.finishedTime = now - (Config.history_job_keep_max_second + 10) * 1000;
        badJob.state = BackupJob.BackupJobState.FINISHED;
        handler.dbIdToBackupOrRestoreJob.put(badJob.getDbId(), badJob);
        Assert.assertEquals(3, handler.dbIdToBackupOrRestoreJob.size());

        // 2. save image & reload
        UtFrameUtils.PseudoImage pseudoImage = new UtFrameUtils.PseudoImage();
        handler.write(pseudoImage.getDataOutputStream());
        BackupHandler reloadHandler = BackupHandler.read(pseudoImage.getDataInputStream());
        // discard expired job
        Assert.assertEquals(2, reloadHandler.dbIdToBackupOrRestoreJob.size());
        Assert.assertNotNull(reloadHandler.getJob(1));
        Assert.assertNotNull(reloadHandler.getJob(2));
        Assert.assertNull(reloadHandler.getJob(3));

        // 3. clean expire
        handler.removeOldJobs();
        Assert.assertEquals(2, handler.dbIdToBackupOrRestoreJob.size());
        Assert.assertNotNull(handler.getJob(1));
        Assert.assertNotNull(handler.getJob(2));
        Assert.assertNull(handler.getJob(3));

        UtFrameUtils.tearDownForPersisTest();
    }

    @Test
    public void testSaveLoadJsonFormatImage() throws Exception {
        UtFrameUtils.setUpForPersistTest();
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        handler = new BackupHandler(globalStateMgr);
        BackupJob runningJob = new BackupJob("running_job", 1, "test_db", new ArrayList<>(), 10000, globalStateMgr, 1);
        handler.dbIdToBackupOrRestoreJob.put(runningJob.getDbId(), runningJob);


        UtFrameUtils.PseudoImage pseudoImage = new UtFrameUtils.PseudoImage();
        handler.saveBackupHandlerV2(pseudoImage.getDataOutputStream());
        BackupHandler followerHandler = new BackupHandler(globalStateMgr);
        SRMetaBlockReader reader = new SRMetaBlockReader(pseudoImage.getDataInputStream());
        followerHandler.loadBackupHandlerV2(reader);
        reader.close();

        Assert.assertEquals(1, followerHandler.dbIdToBackupOrRestoreJob.size());

        UtFrameUtils.tearDownForPersisTest();
    }
}
