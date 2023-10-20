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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/backup/RestoreJobTest.java

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
import com.google.common.collect.Multimap;
import com.starrocks.backup.BackupJobInfo.BackupIndexInfo;
import com.starrocks.backup.BackupJobInfo.BackupPartitionInfo;
import com.starrocks.backup.BackupJobInfo.BackupPhysicalPartitionInfo;
import com.starrocks.backup.BackupJobInfo.BackupTableInfo;
import com.starrocks.backup.BackupJobInfo.BackupTabletInfo;
import com.starrocks.backup.RestoreJob.RestoreJobState;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.SnapshotTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTaskType;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Adler32;

public class RestoreJobTest {

    private Database db;
    private BackupJobInfo jobInfo;
    private RestoreJob job;
    private String label = "test_label";

    private AtomicLong id = new AtomicLong(50000);

    private OlapTable expectedRestoreTbl;

    private long repoId = 20000;

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
    @Mocked
    private SystemInfoService systemInfoService;

    @Injectable
    private Repository repo = new Repository(repoId, "repo", false, "bos://my_repo",
            new BlobStorage("broker", Maps.newHashMap()));

    private BackupMeta backupMeta;

    @Before
    public void setUp() throws AnalysisException {
        db = CatalogMocker.mockDb();
        backupHandler = new MockBackupHandler(globalStateMgr);
        repoMgr = new MockRepositoryMgr();
        Deencapsulation.setField(globalStateMgr, "backupHandler", backupHandler);
        MetricRepo.init();
    }

    public void testResetPartitionForRestore() {
        expectedRestoreTbl = (OlapTable) db.getTable(CatalogMocker.TEST_TBL4_ID);
        OlapTable localTbl = new OlapTable(expectedRestoreTbl.getId(), expectedRestoreTbl.getName(),
                expectedRestoreTbl.getBaseSchema(), KeysType.DUP_KEYS, expectedRestoreTbl.getPartitionInfo(),
                expectedRestoreTbl.getDefaultDistributionInfo());

        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, 3, 100000,
                globalStateMgr, repo.getId(), backupMeta);

        job.resetPartitionForRestore(localTbl, expectedRestoreTbl, CatalogMocker.TEST_PARTITION1_NAME, 3);
    }

    @Test
    public void testRunBackupMultiSubPartitionTable() {
        new Expectations() {
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

                GlobalStateMgr.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        List<Long> beIds = Lists.newArrayList();
        beIds.add(CatalogMocker.BACKEND1_ID);
        beIds.add(CatalogMocker.BACKEND2_ID);
        beIds.add(CatalogMocker.BACKEND3_ID);
        new Expectations() {
            {
                systemInfoService.seqChooseBackendIds(anyInt, anyBoolean, anyBoolean);
                minTimes = 0;
                result = beIds;

                systemInfoService.checkExceedDiskCapacityLimit((Multimap<Long, Long>) any, anyBoolean);
                minTimes = 0;
                result = com.starrocks.common.Status.OK;
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

        new Expectations() {
            {
                repo.upload(anyString, anyString);
                result = Status.OK;
                minTimes = 0;

                List<BackupMeta> backupMetas = Lists.newArrayList();
                repo.getSnapshotMetaFile(label, backupMetas, -1, -1);
                minTimes = 0;
                result = new Delegate() {
                    public Status getSnapshotMetaFile(String label, List<BackupMeta> backupMetas) {
                        backupMetas.add(backupMeta);
                        return Status.OK;
                    }
                };
            }
        };

        new MockUp<MarkedCountDownLatch>() {
            @Mock
            boolean await(long timeout, TimeUnit unit) {
                return true;
            }
        };

        // gen BackupJobInfo
        jobInfo = new BackupJobInfo();
        jobInfo.backupTime = System.currentTimeMillis();
        jobInfo.dbId = CatalogMocker.TEST_DB_ID;
        jobInfo.dbName = CatalogMocker.TEST_DB_NAME;
        jobInfo.name = label;
        jobInfo.success = true;

        expectedRestoreTbl = (OlapTable) db.getTable(CatalogMocker.TEST_TBL4_ID);
        BackupTableInfo tblInfo = new BackupTableInfo();
        tblInfo.id = CatalogMocker.TEST_TBL4_ID;
        tblInfo.name = CatalogMocker.TEST_TBL4_NAME;
        jobInfo.tables.put(tblInfo.name, tblInfo);

        for (Partition partition : expectedRestoreTbl.getPartitions()) {
            BackupPartitionInfo partInfo = new BackupPartitionInfo();
            partInfo.id = partition.getId();
            partInfo.name = partition.getName();
            tblInfo.partitions.put(partInfo.name, partInfo);

            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                BackupPhysicalPartitionInfo physicalPartInfo = new BackupPhysicalPartitionInfo();
                physicalPartInfo.id = physicalPartition.getId();
                partInfo.subPartitions.put(physicalPartInfo.id, physicalPartInfo);

                for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    BackupIndexInfo idxInfo = new BackupIndexInfo();
                    idxInfo.id = index.getId();
                    idxInfo.name = expectedRestoreTbl.getIndexNameById(index.getId());
                    idxInfo.schemaHash = expectedRestoreTbl.getSchemaHashByIndexId(index.getId());
                    physicalPartInfo.indexes.put(idxInfo.name, idxInfo);

                    for (Tablet tablet : index.getTablets()) {
                        BackupTabletInfo tabletInfo = new BackupTabletInfo();
                        tabletInfo.id = tablet.getId();
                        tabletInfo.files.add(tabletInfo.id + ".dat");
                        tabletInfo.files.add(tabletInfo.id + ".idx");
                        tabletInfo.files.add(tabletInfo.id + ".hdr");
                        idxInfo.tablets.add(tabletInfo);
                    }
                }
            }

        }

        // drop this table, cause we want to try restoring this table
        db.dropTable(expectedRestoreTbl.getName());

        List<Table> tbls = Lists.newArrayList();
        tbls.add(expectedRestoreTbl);
        backupMeta = new BackupMeta(tbls);
        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, 3, 100000,
                globalStateMgr, repo.getId(), backupMeta);
        job.setRepo(repo);
        // pending
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assert.assertEquals(1, job.getFileMapping().getMapping().size());

        // 2. snapshoting
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assert.assertEquals(4, AgentTaskQueue.getTaskNum());

        // 3. snapshot finished
        List<AgentTask> agentTasks = Lists.newArrayList();
        Map<TTaskType, Set<Long>> runningTasks = Maps.newHashMap();
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND1_ID, runningTasks));
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND2_ID, runningTasks));
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND3_ID, runningTasks));
        Assert.assertEquals(4, agentTasks.size());

        for (AgentTask agentTask : agentTasks) {
            if (agentTask.getTaskType() != TTaskType.MAKE_SNAPSHOT) {
                continue;
            }

            SnapshotTask task = (SnapshotTask) agentTask;
            String snapshotPath = "/path/to/snapshot/" + System.currentTimeMillis();
            TStatus taskStatus = new TStatus(TStatusCode.OK);
            TBackend tBackend = new TBackend("", 0, 1);
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    task.getSignature(), taskStatus);
            request.setSnapshot_path(snapshotPath);
            Assert.assertTrue(job.finishTabletSnapshotTask(task, request));
        }

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.DOWNLOAD, job.getState());
    }

    @Test
    public void testRunBackupRangeTable() {
        new Expectations() {
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

                GlobalStateMgr.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        List<Long> beIds = Lists.newArrayList();
        beIds.add(CatalogMocker.BACKEND1_ID);
        beIds.add(CatalogMocker.BACKEND2_ID);
        beIds.add(CatalogMocker.BACKEND3_ID);
        new Expectations() {
            {
                systemInfoService.seqChooseBackendIds(anyInt, anyBoolean, anyBoolean);
                minTimes = 0;
                result = beIds;

                systemInfoService.checkExceedDiskCapacityLimit((Multimap<Long, Long>) any, anyBoolean);
                minTimes = 0;
                result = com.starrocks.common.Status.OK;
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

        new Expectations() {
            {
                repo.upload(anyString, anyString);
                result = Status.OK;
                minTimes = 0;

                List<BackupMeta> backupMetas = Lists.newArrayList();
                repo.getSnapshotMetaFile(label, backupMetas, -1, -1);
                minTimes = 0;
                result = new Delegate() {
                    public Status getSnapshotMetaFile(String label, List<BackupMeta> backupMetas) {
                        backupMetas.add(backupMeta);
                        return Status.OK;
                    }
                };
            }
        };

        new MockUp<MarkedCountDownLatch>() {
            @Mock
            boolean await(long timeout, TimeUnit unit) {
                return true;
            }
        };

        // gen BackupJobInfo
        jobInfo = new BackupJobInfo();
        jobInfo.backupTime = System.currentTimeMillis();
        jobInfo.dbId = CatalogMocker.TEST_DB_ID;
        jobInfo.dbName = CatalogMocker.TEST_DB_NAME;
        jobInfo.name = label;
        jobInfo.success = true;

        expectedRestoreTbl = (OlapTable) db.getTable(CatalogMocker.TEST_TBL2_ID);
        BackupTableInfo tblInfo = new BackupTableInfo();
        tblInfo.id = CatalogMocker.TEST_TBL2_ID;
        tblInfo.name = CatalogMocker.TEST_TBL2_NAME;
        jobInfo.tables.put(tblInfo.name, tblInfo);

        for (Partition partition : expectedRestoreTbl.getPartitions()) {
            BackupPartitionInfo partInfo = new BackupPartitionInfo();
            partInfo.id = partition.getId();
            partInfo.name = partition.getName();
            tblInfo.partitions.put(partInfo.name, partInfo);

            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                BackupIndexInfo idxInfo = new BackupIndexInfo();
                idxInfo.id = index.getId();
                idxInfo.name = expectedRestoreTbl.getIndexNameById(index.getId());
                idxInfo.schemaHash = expectedRestoreTbl.getSchemaHashByIndexId(index.getId());
                partInfo.indexes.put(idxInfo.name, idxInfo);

                for (Tablet tablet : index.getTablets()) {
                    BackupTabletInfo tabletInfo = new BackupTabletInfo();
                    tabletInfo.id = tablet.getId();
                    tabletInfo.files.add(tabletInfo.id + ".dat");
                    tabletInfo.files.add(tabletInfo.id + ".idx");
                    tabletInfo.files.add(tabletInfo.id + ".hdr");
                    idxInfo.tablets.add(tabletInfo);
                }
            }
        }

        // drop this table, cause we want to try restoring this table
        db.dropTable(expectedRestoreTbl.getName());

        List<Table> tbls = Lists.newArrayList();
        tbls.add(expectedRestoreTbl);
        backupMeta = new BackupMeta(tbls);
        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, 3, 100000,
                globalStateMgr, repo.getId(), backupMeta);
        job.setRepo(repo);
        // pending
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assert.assertEquals(1, job.getFileMapping().getMapping().size());

        // 2. snapshoting
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assert.assertEquals(4, AgentTaskQueue.getTaskNum());

        // 3. snapshot finished
        List<AgentTask> agentTasks = Lists.newArrayList();
        Map<TTaskType, Set<Long>> runningTasks = Maps.newHashMap();
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND1_ID, runningTasks));
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND2_ID, runningTasks));
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND3_ID, runningTasks));
        Assert.assertEquals(4, agentTasks.size());

        for (AgentTask agentTask : agentTasks) {
            if (agentTask.getTaskType() != TTaskType.MAKE_SNAPSHOT) {
                continue;
            }

            SnapshotTask task = (SnapshotTask) agentTask;
            String snapshotPath = "/path/to/snapshot/" + System.currentTimeMillis();
            TStatus taskStatus = new TStatus(TStatusCode.OK);
            TBackend tBackend = new TBackend("", 0, 1);
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    task.getSignature(), taskStatus);
            request.setSnapshot_path(snapshotPath);
            Assert.assertTrue(job.finishTabletSnapshotTask(task, request));
        }

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.DOWNLOAD, job.getState());
    }

    @Test
    public void testSignature() {
        Adler32 sig1 = new Adler32();
        sig1.update("name1".getBytes());
        sig1.update("name2".getBytes());
        System.out.println("sig1: " + Math.abs((int) sig1.getValue()));

        Adler32 sig2 = new Adler32();
        sig2.update("name2".getBytes());
        sig2.update("name1".getBytes());
        System.out.println("sig2: " + Math.abs((int) sig2.getValue()));

        OlapTable tbl = (OlapTable) db.getTable(CatalogMocker.TEST_TBL_NAME);
        List<String> partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println(partNames);
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true));
        tbl.setName("newName");
        partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true));
    }

}

