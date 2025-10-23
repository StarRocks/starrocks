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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/backup/BackupJobTest.java

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
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TableRef;
import com.starrocks.backup.BackupJob.BackupJobState;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FsBroker;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.View;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UnitTestUtil;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.NodeMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
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
import com.starrocks.transaction.GtidGenerator;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class BackupJobTest {

    private BackupJob job;
    private BackupJob jobView;
    private Database db;

    private long dbId = 1;
    private long tblId = 2;
    private long partId = 3;
    private long idxId = 4;
    private long tabletId = 5;
    private long backendId = 10000;
    private long version = 6;
    private long viewId = 10;

    private long repoId = 20000;
    private AtomicLong id = new AtomicLong(50000);

    @Mocked
    private GlobalStateMgr globalStateMgr;

    @Mocked
    private LocalMetastore localMetastore;

    private MockBackupHandler backupHandler;

    private MockRepositoryMgr repoMgr;

    private int schemaHash = 1;

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

    @BeforeClass
    public static void start() {
        Config.tmp_dir = "./";
        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        backupDir.mkdirs();

        MetricRepo.init();
    }

    @AfterClass
    public static void end() throws IOException {
        Config.tmp_dir = "./";
        File backupDir = new File(BackupHandler.BACKUP_ROOT_DIR.toString());
        if (backupDir.exists()) {
            Files.walk(BackupHandler.BACKUP_ROOT_DIR,
                                    FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).map(Path::toFile)
                        .forEach(File::delete);
        }
    }

    @Before
    public void setUp() {
        globalStateMgr = GlobalStateMgr.getCurrentState();
        repoMgr = new MockRepositoryMgr();
        backupHandler = new MockBackupHandler(globalStateMgr);

        // Thread is unmockable after Jmockit version 1.48, so use reflection to set field instead.
        Deencapsulation.setField(globalStateMgr, "backupHandler", backupHandler);

        db = UnitTestUtil.createDb(dbId, tblId, partId, idxId, tabletId, backendId, version, KeysType.AGG_KEYS);
        View view = UnitTestUtil.createTestView(viewId);
        db.registerTableUnlocked(view);

        LockManager lockManager = new LockManager();

        // Setup default NodeMgr with SystemInfoService
        SystemInfoService infoService = new SystemInfoService();
        Backend backend = new Backend(backendId, "127.0.0.1", 9050);
        backend.setAlive(true);
        infoService.addBackend(backend);
        
        NodeMgr nodeMgr = new NodeMgr();
        Deencapsulation.setField(nodeMgr, "systemInfo", infoService);

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getLockManager();
                minTimes = 0;
                result = lockManager;

                globalStateMgr.getGtidGenerator();
                minTimes = 0;
                result = new GtidGenerator();

                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;

                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                globalStateMgr.getNextId();
                minTimes = 0;
                result = id.getAndIncrement();

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        new Expectations(localMetastore) {
            {
                localMetastore.getDb(anyLong);
                minTimes = 0;
                result = db;

                localMetastore.getTable("testDb", "testTable");
                minTimes = 0;
                result = db.getTable(tblId);

                localMetastore.getTable("testDb", UnitTestUtil.VIEW_NAME);
                minTimes = 0;
                result = db.getTable(viewId);

                localMetastore.getTable("testDb", "unknown_tbl");
                minTimes = 0;
                result = null;
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
        tableRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME), null));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, globalStateMgr, repo.getId());

        List<TableRef> viewRefs = Lists.newArrayList();
        viewRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, UnitTestUtil.VIEW_NAME), null));
        jobView = new BackupJob("label-view", dbId, UnitTestUtil.DB_NAME, viewRefs, 13600 * 1000, globalStateMgr, repo.getId());
    }

    @Test
    public void testRunNormal() {
        // 1.pending
        Assert.assertEquals(BackupJobState.PENDING, job.getState());
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        BackupMeta backupMeta = job.getBackupMeta();
        Assert.assertEquals(1, backupMeta.getTables().size());
        OlapTable backupTbl = (OlapTable) backupMeta.getTable(UnitTestUtil.TABLE_NAME);
        List<String> partNames = Lists.newArrayList(backupTbl.getPartitionNames());
        Assert.assertNotNull(backupTbl);
        Assert.assertEquals(backupTbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true),
                ((OlapTable) db.getTable(tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true));
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
        AgentTask task = AgentTaskQueue.getTask(backendId, TTaskType.MAKE_SNAPSHOT, tabletId);
        Assert.assertTrue(task instanceof SnapshotTask);
        SnapshotTask snapshotTask = (SnapshotTask) task;

        // 2. snapshoting
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.SNAPSHOTING, job.getState());

        // 3. snapshot finished
        String snapshotPath = "/path/to/snapshot";
        List<String> snapshotFiles = Lists.newArrayList();
        snapshotFiles.add("1.dat");
        snapshotFiles.add("1.idx");
        snapshotFiles.add("1.hdr");
        TStatus taskStatus = new TStatus(TStatusCode.OK);
        TBackend tBackend = new TBackend("", 0, 1);
        TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    snapshotTask.getSignature(), taskStatus);
        request.setSnapshot_files(snapshotFiles);
        request.setSnapshot_path(snapshotPath);
        Assert.assertTrue(job.finishTabletSnapshotTask(snapshotTask, request));
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, job.getState());

        // 4. upload snapshots
        AgentTaskQueue.clearAllTasks();
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        Assert.assertEquals(1, AgentTaskQueue.getTaskNum());
        task = AgentTaskQueue.getTask(backendId, TTaskType.UPLOAD, id.get() - 1);
        Assert.assertTrue(task instanceof UploadTask);
        UploadTask upTask = (UploadTask) task;

        Assert.assertEquals(job.getJobId(), upTask.getJobId());
        Map<String, String> srcToDest = upTask.getSrcToDestPath();
        Assert.assertEquals(1, srcToDest.size());
        System.out.println(srcToDest);
        String dest = srcToDest.get(snapshotPath + "/" + tabletId + "/" + 0);
        Assert.assertNotNull(dest);

        // 5. uploading
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(BackupJobState.UPLOADING, job.getState());
        Map<Long, List<String>> tabletFileMap = Maps.newHashMap();
        request = new TFinishTaskRequest(tBackend, TTaskType.UPLOAD,
                    upTask.getSignature(), taskStatus);
        request.setTablet_files(tabletFileMap);

        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        List<String> tabletFiles = Lists.newArrayList();
        tabletFileMap.put(tabletId, tabletFiles);
        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("wrong_files.idx.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("wrong_files.hdr.4f158689243a3d6030352fec3cfd3798");
        Assert.assertFalse(job.finishSnapshotUploadTask(upTask, request));
        tabletFiles.clear();
        tabletFiles.add("1.dat.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("1.idx.4f158689243a3d6030352fec3cfd3798");
        tabletFiles.add("1.hdr.4f158689243a3d6030352fec3cfd3798");
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
            Assert.assertEquals(1, restoreMetaInfo.getTables().size());
            OlapTable olapTable = (OlapTable) restoreMetaInfo.getTable(tblId);
            Assert.assertNotNull(olapTable);
            Assert.assertNotNull(restoreMetaInfo.getTable(UnitTestUtil.TABLE_NAME));
            List<String> names = Lists.newArrayList(olapTable.getPartitionNames());
            Assert.assertEquals(((OlapTable) db.getTable(tblId)).getSignature(BackupHandler.SIGNATURE_VERSION, names, true),
                    olapTable.getSignature(BackupHandler.SIGNATURE_VERSION, names, true));

            restoreJobInfo = BackupJobInfo.fromFile(job.getLocalJobInfoFilePath());
            Assert.assertEquals(UnitTestUtil.DB_NAME, restoreJobInfo.dbName);
            Assert.assertEquals(job.getLabel(), restoreJobInfo.name);
            Assert.assertEquals(1, restoreJobInfo.tables.size());
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

        try {
            // test get backup info
            job.getInfo();
        } catch (Exception ignore) {
        }
    }

    @Test
    public void testRunAbnormal() {
        // 1.pending
        AgentTaskQueue.clearAllTasks();

        List<TableRef> tableRefs = Lists.newArrayList();
        tableRefs.add(new TableRef(new TableName(UnitTestUtil.DB_NAME, "unknown_tbl"), null));
        job = new BackupJob("label", dbId, UnitTestUtil.DB_NAME, tableRefs, 13600 * 1000, globalStateMgr, repo.getId());
        job.run();
        Assert.assertEquals(Status.ErrCode.NOT_FOUND, job.getStatus().getErrCode());
        Assert.assertEquals(BackupJobState.CANCELLED, job.getState());
    }

    @Test
    public void testRunViewNormal() {
        // 1.pending
        Assert.assertEquals(BackupJobState.PENDING, jobView.getState());
        jobView.run();
        Assert.assertEquals(Status.OK, jobView.getStatus());
        Assert.assertEquals(BackupJobState.SNAPSHOTING, jobView.getState());

        BackupMeta backupMeta = jobView.getBackupMeta();
        Assert.assertEquals(1, backupMeta.getTables().size());
        View backupView = (View) backupMeta.getTable(UnitTestUtil.VIEW_NAME);
        Assert.assertTrue(backupView != null);
        Assert.assertTrue(backupView.getPartitions().isEmpty());

        // 2. snapshoting finished, not snapshot needed
        jobView.run();
        Assert.assertEquals(Status.OK, jobView.getStatus());
        Assert.assertEquals(BackupJobState.UPLOAD_SNAPSHOT, jobView.getState());

        // 3. upload snapshots
        jobView.run();
        Assert.assertEquals(Status.OK, jobView.getStatus());
        Assert.assertEquals(BackupJobState.UPLOADING, jobView.getState());

        // 4. uploading
        jobView.run();
        Assert.assertEquals(Status.OK, jobView.getStatus());
        Assert.assertEquals(BackupJobState.SAVE_META, jobView.getState());

        // 5. save meta
        jobView.run();
        Assert.assertEquals(Status.OK, jobView.getStatus());
        Assert.assertEquals(BackupJobState.UPLOAD_INFO, jobView.getState());
        File metaInfo = new File(jobView.getLocalMetaInfoFilePath());
        Assert.assertTrue(metaInfo.exists());
        File jobInfo = new File(jobView.getLocalJobInfoFilePath());
        Assert.assertTrue(jobInfo.exists());

        BackupMeta restoreMetaInfo = null;
        BackupJobInfo restoreJobInfo = null;
        try {
            restoreMetaInfo = BackupMeta.fromFile(jobView.getLocalMetaInfoFilePath(), FeConstants.STARROCKS_META_VERSION);
            Assert.assertEquals(1, restoreMetaInfo.getTables().size());
            View view = (View) restoreMetaInfo.getTable(viewId);
            Assert.assertNotNull(view);
            Assert.assertNotNull(restoreMetaInfo.getTable(UnitTestUtil.VIEW_NAME));
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }

        Assert.assertNull(jobView.getBackupMeta());
        Assert.assertNull(jobView.getJobInfo());

        // 6. upload_info
        jobView.run();
        Assert.assertEquals(Status.OK, jobView.getStatus());
        Assert.assertEquals(BackupJobState.FINISHED, jobView.getState());

        try {
            // test get backup info
            jobView.getInfo();
        } catch (Exception ignore) {
        }
    }

    /**
     * Test Replica.computeReplicaStatus() returns OK status when:
     * - Backend is available
     * - Replica version >= visible version
     * - No failed versions
     * - Schema hash matches
     */
    @Test
    public void testComputeReplicaStatusOk() {
        SystemInfoService infoService = new SystemInfoService();
        Backend backend = new Backend(backendId, "127.0.0.1", 9050);
        backend.setAlive(true);
        infoService.addBackend(backend);

        Replica replica = new Replica(1L, backendId, 10L, schemaHash, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);

        Replica.ReplicaStatus status = replica.computeReplicaStatus(infoService, 10L, schemaHash);
        Assertions.assertEquals(Replica.ReplicaStatus.OK, status);

        // Test with higher replica version
        status = replica.computeReplicaStatus(infoService, 8L, schemaHash);
        Assertions.assertEquals(Replica.ReplicaStatus.OK, status);
    }

    /**
     * Test Replica.computeReplicaStatus() returns DEAD status when:
     * - Backend is null
     * - Backend is not available
     * - Replica is marked as bad
     */
    @Test
    public void testComputeReplicaStatusDead() {
        SystemInfoService infoService = new SystemInfoService();

        // Case 1: Backend is null
        Replica replica = new Replica(1L, backendId, 10L, schemaHash, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);
        Replica.ReplicaStatus status = replica.computeReplicaStatus(infoService, 10L, schemaHash);
        Assertions.assertEquals(Replica.ReplicaStatus.DEAD, status);

        // Case 2: Backend is not available (not alive)
        Backend backend = new Backend(backendId, "127.0.0.1", 9050);
        backend.setAlive(false);
        infoService.addBackend(backend);
        status = replica.computeReplicaStatus(infoService, 10L, schemaHash);
        Assertions.assertEquals(Replica.ReplicaStatus.DEAD, status);

        // Case 3: Backend is available but replica is bad
        backend.setAlive(true);
        replica.setBad(true);
        status = replica.computeReplicaStatus(infoService, 10L, schemaHash);
        Assertions.assertEquals(Replica.ReplicaStatus.DEAD, status);
    }

    /**
     * Test Replica.computeReplicaStatus() returns VERSION_ERROR status when:
     * - Replica version < visible version
     * - Replica has failed version (lastFailedVersion > 0)
     */
    @Test
    public void testComputeReplicaStatusVersionError() {
        SystemInfoService infoService = new SystemInfoService();
        Backend backend = new Backend(backendId, "127.0.0.1", 9050);
        backend.setAlive(true);
        infoService.addBackend(backend);

        // Case 1: Replica version < visible version
        Replica replica = new Replica(1L, backendId, 5L, schemaHash, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);
        Replica.ReplicaStatus status = replica.computeReplicaStatus(infoService, 10L, schemaHash);
        Assertions.assertEquals(Replica.ReplicaStatus.VERSION_ERROR, status);

        // Case 2: Replica has failed version
        Replica replica2 = new Replica(2L, backendId, 10L, schemaHash, 0L, 0L,
                Replica.ReplicaState.NORMAL, 8L, -1L);
        status = replica2.computeReplicaStatus(infoService, 10L, schemaHash);
        Assertions.assertEquals(Replica.ReplicaStatus.VERSION_ERROR, status);
    }

    /**
     * Test Replica.computeReplicaStatus() returns SCHEMA_ERROR status when:
     * - Schema hash does not match (and schema hash is not -1)
     */
    @Test
    public void testComputeReplicaStatusSchemaError() {
        SystemInfoService infoService = new SystemInfoService();
        Backend backend = new Backend(backendId, "127.0.0.1", 9050);
        backend.setAlive(true);
        infoService.addBackend(backend);

        Replica replica = new Replica(1L, backendId, 10L, 12345, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);

        Replica.ReplicaStatus status = replica.computeReplicaStatus(infoService, 10L, 67890);
        Assertions.assertEquals(Replica.ReplicaStatus.SCHEMA_ERROR, status);

        // Test that -1 schema hash is treated as OK (not checked)
        Replica replica2 = new Replica(2L, backendId, 10L, -1, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);
        status = replica2.computeReplicaStatus(infoService, 10L, 67890);
        Assertions.assertEquals(Replica.ReplicaStatus.OK, status);
    }

    /**
     * Test that chooseReplica correctly chooses a healthy replica and skips bad replicas.
     * This test creates a tablet with multiple replicas in different states and verifies
     * that BackupJob.prepareSnapshotTask() chooses only the healthy one.
     */
    @Test
    public void testChooseReplicaSkipsBadReplicas() {
        // Get the existing NodeMgr and modify its SystemInfoService
        NodeMgr existingNodeMgr = globalStateMgr.getNodeMgr();
        SystemInfoService infoService = Deencapsulation.getField(existingNodeMgr, "systemInfo");
        
        // Clear existing backends and add test-specific ones
        Map<Long, Backend> backends = Deencapsulation.getField(infoService, "idToBackendRef");
        backends.clear();
        
        // Setup multiple backends
        long backendId1 = 10001;
        long backendId2 = 10002;
        long backendId3 = 10003;
        long backendId4 = 10004;
        
        // Backend 1: not available (DEAD replica)
        Backend backend1 = new Backend(backendId1, "127.0.0.1", 9050);
        backend1.setAlive(false);
        infoService.addBackend(backend1);
        
        // Backend 2: available but replica has version error
        Backend backend2 = new Backend(backendId2, "127.0.0.2", 9050);
        backend2.setAlive(true);
        infoService.addBackend(backend2);
        
        // Backend 3: available and healthy (should be chosen)
        Backend backend3 = new Backend(backendId3, "127.0.0.3", 9050);
        backend3.setAlive(true);
        infoService.addBackend(backend3);
        
        // Backend 4: available but replica is marked as bad
        Backend backend4 = new Backend(backendId4, "127.0.0.4", 9050);
        backend4.setAlive(true);
        infoService.addBackend(backend4);

        // Create a tablet with multiple replicas
        LocalTablet tablet = new LocalTablet(tabletId);
        
        // Replica 1: DEAD (backend not available)
        Replica replica1 = new Replica(1L, backendId1, 10L, schemaHash, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);
        
        // Replica 2: VERSION_ERROR (version too old)
        Replica replica2 = new Replica(2L, backendId2, 5L, schemaHash, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);
        
        // Replica 3: OK (should be chosen)
        Replica replica3 = new Replica(3L, backendId3, 10L, schemaHash, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);
        
        // Replica 4: DEAD (marked as bad)
        Replica replica4 = new Replica(4L, backendId4, 10L, schemaHash, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);
        replica4.setBad(true);
        
        // Use reflection to add replicas to avoid TabletInvertedIndex issues
        List<Replica> replicas = Lists.newArrayList(replica1, replica2, replica3, replica4);
        Deencapsulation.setField(tablet, "replicas", replicas);
        Deencapsulation.setField(tablet, "immutableReplicas", Collections.unmodifiableList(replicas));

        // Test chooseReplica through reflection since it's private
        try {
            Method chooseReplicaMethod = BackupJob.class.getDeclaredMethod("chooseReplica",
                    LocalTablet.class, long.class, int.class);
            chooseReplicaMethod.setAccessible(true);
            
            Replica chosenReplica = (Replica) chooseReplicaMethod.invoke(job, tablet, 10L, schemaHash);
            
            // Should choose replica3 which is healthy
            Assertions.assertNotNull(chosenReplica);
            Assertions.assertEquals(3L, chosenReplica.getId());
            Assertions.assertEquals(backendId3, chosenReplica.getBackendId());
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("Failed to test chooseReplica: " + e.getMessage());
        }
    }

    /**
     * Test that chooseReplica returns null when no healthy replica exists.
     */
    @Test
    public void testChooseReplicaReturnsNullWhenNoHealthyReplica() {
        // Get the existing NodeMgr and modify its SystemInfoService
        NodeMgr existingNodeMgr = globalStateMgr.getNodeMgr();
        SystemInfoService infoService = Deencapsulation.getField(existingNodeMgr, "systemInfo");
        
        // Clear existing backends and add test-specific one
        Map<Long, Backend> backends = Deencapsulation.getField(infoService, "idToBackendRef");
        backends.clear();
        
        // Use a different backend ID to avoid conflicts
        long testBackendId = 20001;
        
        // Setup backend that is not available
        Backend backend = new Backend(testBackendId, "127.0.0.1", 9050);
        backend.setAlive(false);
        infoService.addBackend(backend);

        // Create a tablet with only bad replicas
        LocalTablet tablet = new LocalTablet(tabletId);
        
        // All replicas are DEAD
        Replica replica1 = new Replica(1L, testBackendId, 10L, schemaHash, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);
        
        // Use reflection to add replicas to avoid TabletInvertedIndex issues
        List<Replica> replicas = Lists.newArrayList(replica1);
        Deencapsulation.setField(tablet, "replicas", replicas);
        Deencapsulation.setField(tablet, "immutableReplicas", Collections.unmodifiableList(replicas));

        // Test chooseReplica through reflection
        try {
            Method chooseReplicaMethod = BackupJob.class.getDeclaredMethod("chooseReplica",
                    LocalTablet.class, long.class, int.class);
            chooseReplicaMethod.setAccessible(true);
            
            Replica chosenReplica = (Replica) chooseReplicaMethod.invoke(job, tablet, 10L, schemaHash);
            
            // Should return null when no healthy replica exists
            Assertions.assertNull(chosenReplica);
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("Failed to test chooseReplica: " + e.getMessage());
        }
    }

    /**
     * Test that prepareSnapshotTask fails with proper error when no healthy replica exists.
     * This ensures the bug fix properly handles the case where all replicas are bad.
     */
    @Test
    public void testPrepareSnapshotTaskFailsWhenNoHealthyReplica() {
        // Get the existing NodeMgr and modify its SystemInfoService
        NodeMgr existingNodeMgr = globalStateMgr.getNodeMgr();
        SystemInfoService infoService = Deencapsulation.getField(existingNodeMgr, "systemInfo");
        
        // Clear existing backends and add test-specific one
        Map<Long, Backend> backends = Deencapsulation.getField(infoService, "idToBackendRef");
        backends.clear();
        
        // Setup backend that is not available
        Backend backend = new Backend(backendId, "127.0.0.1", 9050);
        backend.setAlive(false);
        infoService.addBackend(backend);

        // Get the table and its components
        // Note: UnitTestUtil creates partition with physical partition ID = partId + 100
        OlapTable table = (OlapTable) db.getTable(tblId);
        PhysicalPartition partition = table.getPhysicalPartition(partId + 100);
        MaterializedIndex index = partition.getIndex(idxId);
        LocalTablet tablet = (LocalTablet) index.getTablet(tabletId);

        // Test prepareSnapshotTask through reflection
        try {
            Method prepareSnapshotTaskMethod = BackupJob.class.getDeclaredMethod("prepareSnapshotTask",
                    PhysicalPartition.class, com.starrocks.catalog.Table.class, 
                    com.starrocks.catalog.Tablet.class, MaterializedIndex.class,
                    long.class, int.class);
            prepareSnapshotTaskMethod.setAccessible(true);
            
            // Before calling prepareSnapshotTask, job should be OK
            Assertions.assertEquals(Status.OK, job.getStatus());
            
            prepareSnapshotTaskMethod.invoke(job, partition, table, tablet, index, version, schemaHash);
            
            // After calling with no healthy replica, job status should be error
            Assertions.assertNotEquals(Status.OK, job.getStatus());
            Assertions.assertTrue(job.getStatus().getErrMsg().contains("failed to choose replica"));
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("Failed to test prepareSnapshotTask: " + e.getMessage());
        }
    }

    /**
     * Test that chooseReplica chooses replicas in order by replica ID.
     * This ensures deterministic behavior in backup job.
     */
    @Test
    public void testChooseReplicaOrderByReplicaId() {
        // Get the existing NodeMgr and modify its SystemInfoService
        NodeMgr existingNodeMgr = globalStateMgr.getNodeMgr();
        SystemInfoService infoService = Deencapsulation.getField(existingNodeMgr, "systemInfo");
        
        // Clear existing backends and add test-specific ones
        Map<Long, Backend> backends = Deencapsulation.getField(infoService, "idToBackendRef");
        backends.clear();
        
        // Setup multiple healthy backends
        long backendId1 = 10001;
        long backendId2 = 10002;
        long backendId3 = 10003;
        
        Backend backend1 = new Backend(backendId1, "127.0.0.1", 9050);
        backend1.setAlive(true);
        infoService.addBackend(backend1);
        
        Backend backend2 = new Backend(backendId2, "127.0.0.2", 9050);
        backend2.setAlive(true);
        infoService.addBackend(backend2);
        
        Backend backend3 = new Backend(backendId3, "127.0.0.3", 9050);
        backend3.setAlive(true);
        infoService.addBackend(backend3);

        // Create a tablet with multiple healthy replicas (add in non-sorted order)
        LocalTablet tablet = new LocalTablet(tabletId);
        
        // Create replicas in non-sorted order: 3, 1, 2
        Replica replica3 = new Replica(30L, backendId3, 10L, schemaHash, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);
        
        Replica replica1 = new Replica(10L, backendId1, 10L, schemaHash, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);
        
        Replica replica2 = new Replica(20L, backendId2, 10L, schemaHash, 0L, 0L,
                Replica.ReplicaState.NORMAL, -1L, -1L);
        
        // Use reflection to add replicas to avoid TabletInvertedIndex issues
        List<Replica> replicas = Lists.newArrayList(replica3, replica1, replica2);
        Deencapsulation.setField(tablet, "replicas", replicas);
        Deencapsulation.setField(tablet, "immutableReplicas", Collections.unmodifiableList(replicas));

        // Test chooseReplica through reflection
        try {
            Method chooseReplicaMethod = BackupJob.class.getDeclaredMethod("chooseReplica",
                    LocalTablet.class, long.class, int.class);
            chooseReplicaMethod.setAccessible(true);
            
            Replica chosenReplica = (Replica) chooseReplicaMethod.invoke(job, tablet, 10L, schemaHash);
            
            // Should choose replica with smallest ID (replica1 with ID 10)
            Assertions.assertNotNull(chosenReplica);
            Assertions.assertEquals(10L, chosenReplica.getId());
            Assertions.assertEquals(backendId1, chosenReplica.getBackendId());
        } catch (Exception e) {
            e.printStackTrace();
            Assertions.fail("Failed to test chooseReplica: " + e.getMessage());
        }
    }
}
