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

package com.starrocks.backup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.backup.BackupJob.BackupJobState;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.common.conf.Config;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UnitTestUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.SnapshotTask;
import com.starrocks.task.UploadTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTaskType;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BackupJobMaterializedViewTest {

    private BackupJob job;
    private Database db;

    private long dbId = 11;
    private long tblId = 12;
    private long partId = 13;
    private long idxId = 14;
    private long tabletId = 15;
    private long backendId = 10000;
    private long version = 16;

    private long repoId = 30000;
    private AtomicLong id = new AtomicLong(50000);
    private static final String MV_LABEL = "mv_label";

    private static List<Path> pathsNeedToBeDeleted = Lists.newArrayList();

    @Mocked
    private GlobalStateMgr globalStateMgr;

    private MockBackupHandler backupHandler;

    private MockRepositoryMgr repoMgr;

    // Thread is not mockable in Jmockit, use subclass instead
    private final class MockBackupHandler extends BackupHandler {
        public MockBackupHandler(GlobalStateMgr globalStateMgr) {
            super(globalStateMgr);
        }

        @Override
        public RepositoryMgr getRepoMgr() {
            return repoMgr;
        }
    }

    // Thread is not mockable in Jmockit, use subclass instead
    private final class MockRepositoryMgr extends RepositoryMgr {
        public MockRepositoryMgr() {
            super();
        }

        @Override
        public Repository getRepo(long repoId) {
            return repo;
        }
    }

    @Mocked
    private EditLog editLog;

    private Repository repo = new Repository(repoId, "repo", false, "my_repo",
            new BlobStorage("broker", Maps.newHashMap()));

    @BeforeAll
    public static void start() {
        Config.tmp_dir = "./";
        File backupDir = new File(BackupHandler.TEST_BACKUP_ROOT_DIR.toString());
        if (!backupDir.exists()) {
            backupDir.mkdirs();
        }

        MetricRepo.init();
    }

    @AfterAll
    public static void end() throws IOException {
        for (Path path : pathsNeedToBeDeleted) {
            File backupDir = new File(path.toString());
            if (backupDir.exists()) {
                Files.walk(path, FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }

    @BeforeEach
    public void setUp() {

        repoMgr = new MockRepositoryMgr();
        backupHandler = new MockBackupHandler(globalStateMgr);

        // Thread is unmockable after Jmockit version 1.48, so use reflection to set field instead.
        Deencapsulation.setField(globalStateMgr, "backupHandler", backupHandler);

        db = UnitTestUtil.createDbWithMaterializedView(dbId, tblId, partId, idxId, tabletId,
                backendId, version, KeysType.DUP_KEYS);

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = db;

                globalStateMgr.getNextId();
                minTimes = 0;
                result = id.getAndIncrement();

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        new Expectations() {
            {
                editLog.logBackupJob((BackupJob) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logBackupJob(BackupJob job) {
                        System.out.println("log backup job: " + job);
                    }
                };
            }
        };

        new MockUp<AgentTaskExecutor>() {
            @Mock
            public void submit(AgentBatchTask task) {

            }
        };

        new MockUp<Repository>() {
            @Mock
            Status upload(String localFilePath, String remoteFilePath) {
                return Status.OK;
            }

            @Mock
            Status getBrokerAddress(Long beId, GlobalStateMgr globalStateMgr, List<FsBroker> brokerAddrs) {
                brokerAddrs.add(new FsBroker());
                return Status.OK;
            }
        };

        List<TableRef> tableRefs = Lists.newArrayList();
        // disorder
        tableRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, UnitTestUtil.MATERIALIZED_VIEW_NAME), null));
        tableRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME), null));

        job = new BackupJob(MV_LABEL, dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, globalStateMgr, repo.getId());
    }

    @Test
    @Order(1)
    public void testRunNormal() {
        // 1.pending
        Assert.assertEquals(BackupJobState.PENDING, job.getState());
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        BackupMeta backupMeta = job.getBackupMeta();
        Assert.assertEquals(2, backupMeta.getTables().size());

        // test backup meta
        {
            {
                OlapTable backupTbl = (OlapTable) backupMeta.getTable(UnitTestUtil.TABLE_NAME);
                List<String> partNames = Lists.newArrayList(backupTbl.getPartitionNames());
                Assert.assertNotNull(backupTbl);
                Assert.assertEquals(backupTbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true),
                        ((OlapTable) db.getTable(tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true));
            }
            {
                OlapTable backupTbl = (OlapTable) backupMeta.getTable(UnitTestUtil.MATERIALIZED_VIEW_NAME);
                List<String> partNames = Lists.newArrayList(backupTbl.getPartitionNames());
                Assert.assertNotNull(backupTbl);
                Assert.assertEquals(backupTbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true),
                        ((OlapTable) db.getTable(tblId + 1)).getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true));
            }
        }

        Assert.assertEquals(2, AgentTaskQueue.getTaskNum());

        // 2. snapshoting
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        // 3. snapshot finished
        String snapshotPath = "/path/to/snapshot";
        List<String> snapshotFiles = Lists.newArrayList();
        snapshotFiles.add("1.dat");
        snapshotFiles.add("meta");
        TStatus taskStatus = new TStatus(TStatusCode.OK);
        TBackend tBackend = new TBackend("", 0, 1);

        // run task 1
        {
            AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, tabletId);
            Assert.assertTrue(task instanceof SnapshotTask);
            SnapshotTask snapshotTask = (SnapshotTask) task;
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    snapshotTask.getSignature(), taskStatus);
            request.setSnapshot_files(snapshotFiles);
            request.setSnapshot_path(snapshotPath);
            Assert.assertTrue(job.finishTabletSnapshotTask(snapshotTask, request));
            job.run();
            Assert.assertEquals(Status.OK, job.getStatus());
        }
        // run task 2
        {
            AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, tabletId + 1);
            Assert.assertTrue(task instanceof SnapshotTask);
            SnapshotTask snapshotTask = (SnapshotTask) task;
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    snapshotTask.getSignature(), taskStatus);
            request.setSnapshot_files(snapshotFiles);
            request.setSnapshot_path(snapshotPath);
            Assert.assertTrue(job.finishTabletSnapshotTask(snapshotTask, request));
            job.run();
            Assert.assertEquals(Status.OK, job.getStatus());
        }
        Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());

        // 4. upload snapshots
        AgentTaskQueue.clearAllTasks();
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum());

        AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
        Assert.assertTrue(task instanceof UploadTask);
        UploadTask upTask = (UploadTask) task;

        Assert.assertEquals(job.getJobId(), upTask.getJobId());
        Map<String, String> srcToDest = upTask.getSrcToDestPath();
        Assert.assertEquals(1, srcToDest.size());
        String dest = srcToDest.get(snapshotPath + "/" + (tabletId + 1) + "/" + 0);
        Assert.assertNotNull(dest);

        // 5. uploading
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
        TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.UPLOAD,
                upTask.getSignature(), taskStatus);
        request.setTablet_files(tabletFileMap);

        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        List<String> tabletFiles = Lists.newArrayList();
        tabletFileMap.put(tabletId, tabletFiles);
        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("wrong_files.4f158689243a3d6030352fec3cfd3798");
        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        tabletFiles.clear();
        tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("meta.4f158689243a3d6030352fec3cfd3798");
        Assert.assertTrue(job.finishSnapshotUploadTask(upTask, request));
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SAVE_META, job.getState());

        // 6. save meta
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOAD_INFO, job.getState());
        File metaInfo = new File(job.getLocalMetaInfoFilePath());
        Assert.assertTrue(metaInfo.exists());
        File jobInfo = new File(job.getLocalJobInfoFilePath());
        Assert.assertTrue(jobInfo.exists());

        BackupMeta restoreMetaInfo = null;
        BackupJobInfo restoreJobInfo = null;
        try {
            restoreMetaInfo = BackupMeta.fromFile(job.getLocalMetaInfoFilePath(), FeConstants.STARROCKS_META_VERSION);
            Assert.assertEquals(2, restoreMetaInfo.getTables().size());

            {
                OlapTable olapTable = (OlapTable) restoreMetaInfo.getTable(tblId);
                Assert.assertNotNull(olapTable);
                Assert.assertNotNull(restoreMetaInfo.getTable(UnitTestUtil.TABLE_NAME));
                List<String> names = Lists.newArrayList(olapTable.getPartitionNames());
                Assert.assertEquals(((OlapTable) db.getTable(tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, names, true),
                        olapTable.getSignature(BackupHandler.SIGNATURE_VERSION, names, true));
            }
            {
                MaterializedView mv = (MaterializedView) restoreMetaInfo.getTable(tblId + 1);
                Assert.assertNotNull(mv);
                Assert.assertNotNull(restoreMetaInfo.getTable(UnitTestUtil.MATERIALIZED_VIEW_NAME));
                List<String> names = Lists.newArrayList(mv.getPartitionNames());
                Assert.assertEquals(((OlapTable) db.getTable(tblId + 1)).getSignature(BackupHandler.SIGNATURE_VERSION, names,
                                true), mv.getSignature(BackupHandler.SIGNATURE_VERSION, names, true));
            }

            restoreJobInfo = BackupJobInfo.fromFile(job.getLocalJobInfoFilePath());
            Assert.assertEquals(UnitTestUtil.DB_NAME, restoreJobInfo.dbName);
            Assert.assertEquals(job.getLabel(), restoreJobInfo.name);
            Assert.assertEquals(2, restoreJobInfo.tables.size());

            // base table
            BackupJobInfo.BackupTableInfo baseTableBackupInfo = restoreJobInfo.getTableInfo(UnitTestUtil.TABLE_NAME);
            Assert.assertTrue(baseTableBackupInfo != null);
            Table remoteBaseTable = backupMeta.getTable(UnitTestUtil.TABLE_NAME);
            Assert.assertTrue(remoteBaseTable != null);

            // mv
            BackupJobInfo.BackupTableInfo mvBackupInfo = restoreJobInfo.getTableInfo(UnitTestUtil.TABLE_NAME);
            Assert.assertTrue(mvBackupInfo != null);
            Table mvTable = backupMeta.getTable(UnitTestUtil.MATERIALIZED_VIEW_NAME);
            Assert.assertTrue(mvTable != null);
            Assert.assertTrue(mvTable instanceof MaterializedView);
            MaterializedView mv = (MaterializedView) mvTable;
            Assert.assertTrue(mv != null);
            Assert.assertTrue(!mv.isActive());
            Assert.assertTrue(mv.getInactiveReason().contains(String.format("Set the materialized view %s inactive in backup",
                    UnitTestUtil.MATERIALIZED_VIEW_NAME)));
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }

        Assert.assertNull(job.getBackupMeta());
        Assert.assertNull(job.getJobInfo());

        // 7. upload_info
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.FINISHED, job.getState());

        if (job.getLocalJobDirPath() != null) {
            pathsNeedToBeDeleted.add(job.getLocalJobDirPath());
        }
    }

    @Test
    @Order(2)
    public void testRunAbnormal() {
        // 1.pending
        AgentTaskQueue.clearAllTasks();

        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, "unknown_tbl"), null));
        tableRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, "unknown_mv"), null));

        job = new BackupJob(MV_LABEL, dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, globalStateMgr, repo.getId());
        job.run();
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, job.getStatus().getErrCode());
        Assert.assertEquals(BackupJobState.CANCELLED, job.getState());

        if (job.getLocalJobDirPath() != null) {
            pathsNeedToBeDeleted.add(job.getLocalJobDirPath());
        }
    }
}
