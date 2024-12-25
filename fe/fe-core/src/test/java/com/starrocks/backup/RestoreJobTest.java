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
import com.starrocks.analysis.FunctionName;
import com.starrocks.backup.BackupJobInfo.BackupIndexInfo;
import com.starrocks.backup.BackupJobInfo.BackupPartitionInfo;
import com.starrocks.backup.BackupJobInfo.BackupPhysicalPartitionInfo;
import com.starrocks.backup.BackupJobInfo.BackupTableInfo;
import com.starrocks.backup.BackupJobInfo.BackupTabletInfo;
import com.starrocks.backup.RestoreJob.RestoreJobState;
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FakeEditLog;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.QueryStatement;
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

    private GlobalStateMgr globalStateMgr;

    private MockBackupHandler backupHandler;

    private MockRepositoryMgr repoMgr;

    @Mocked
    private EditLog editLog;

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
    private SystemInfoService systemInfoService;

    @Injectable
    private Repository repo = new Repository(repoId, "repo", false, "bos://my_repo",
            new BlobStorage("broker", Maps.newHashMap()));

    private BackupMeta backupMeta;

    @Before
    public void setUp() throws AnalysisException {
        globalStateMgr = GlobalStateMgr.getCurrentState();
        new FakeEditLog();

        db = CatalogMocker.mockDb();
        backupHandler = new MockBackupHandler(globalStateMgr);
        repoMgr = new MockRepositoryMgr();
        Deencapsulation.setField(globalStateMgr, "backupHandler", backupHandler);
        MetricRepo.init();

        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;
            }
        };

        AgentTaskQueue.clearAllTasks();
    }

    public void testResetPartitionForRestore() {
        expectedRestoreTbl = (OlapTable) db.getTable(CatalogMocker.TEST_TBL4_ID);

        OlapTable localTbl = new OlapTable(expectedRestoreTbl.getId(), expectedRestoreTbl.getName(),
                expectedRestoreTbl.getBaseSchema(), KeysType.DUP_KEYS, expectedRestoreTbl.getPartitionInfo(),
                expectedRestoreTbl.getDefaultDistributionInfo());

        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, 3, 100000,
                globalStateMgr, repo.getId(), backupMeta, new MvRestoreContext());

        job.resetPartitionForRestore(localTbl, expectedRestoreTbl, CatalogMocker.TEST_PARTITION1_NAME, 3);
    }

    @Test
    public void testRunBackupMultiSubPartitionTable() {
        new Expectations() {
            {

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = db;

                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
            }
        };

        FakeEditLog fakeEditLog = new FakeEditLog();

        List<Long> beIds = Lists.newArrayList();
        beIds.add(CatalogMocker.BACKEND1_ID);
        beIds.add(CatalogMocker.BACKEND2_ID);
        beIds.add(CatalogMocker.BACKEND3_ID);
        new Expectations() {
            {
                systemInfoService.getNodeSelector().seqChooseBackendIds(anyInt, anyBoolean, anyBoolean, null);
                minTimes = 0;
                result = beIds;

                systemInfoService.getBackend(anyLong);
                minTimes = 0;
                result = null;

                systemInfoService.getComputeNode(anyLong);
                minTimes = 0;
                result = null;

                systemInfoService.checkExceedDiskCapacityLimit((Multimap<Long, Long>) any, anyBoolean);
                minTimes = 0;
                result = com.starrocks.common.Status.OK;
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

        Locker locker = new Locker();

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
                        idxInfo.tablets.add(tabletInfo);
                    }
                }
            }

        }

        // drop this table, cause we want to try restoring this table
        db.dropTable(expectedRestoreTbl.getName());

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db;
            }

            @Mock
            public Table getTable(String dbName, String tblName) {
                return db.getTable(tblName);
            }

            @Mock
            public Table getTable(Long dbId, Long tableId) {
                return db.getTable(tableId);
            }
        };

        List<Table> tbls = Lists.newArrayList();
        tbls.add(expectedRestoreTbl);
        backupMeta = new BackupMeta(tbls);
        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, 3, 100000,
                globalStateMgr, repo.getId(), backupMeta, new MvRestoreContext());
        job.setRepo(repo);
        // pending
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assert.assertEquals(6, job.getFileMapping().getMapping().size());

        // 2. snapshoting
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assert.assertEquals(12, AgentTaskQueue.getTaskNum());

        // 3. snapshot finished
        List<AgentTask> agentTasks = Lists.newArrayList();
        Map<TTaskType, Set<Long>> runningTasks = Maps.newHashMap();
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND1_ID, runningTasks));
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND2_ID, runningTasks));
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND3_ID, runningTasks));
        Assert.assertEquals(12, agentTasks.size());

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

        // test get restore info
        try {
            job.getInfo();
        } catch (Exception ignore) {
        }
    }

    @Test
    public void testRunBackupRangeTable() {
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = db;

                globalStateMgr.getNextId();
                minTimes = 0;
                result = id.incrementAndGet();

                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
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
                systemInfoService.getNodeSelector().seqChooseBackendIds(anyInt, anyBoolean, anyBoolean, null);
                minTimes = 0;
                result = beIds;

                systemInfoService.getBackend(anyLong);
                minTimes = 0;
                result = null;

                systemInfoService.getComputeNode(anyLong);
                minTimes = 0;
                result = null;

                systemInfoService.checkExceedDiskCapacityLimit((Multimap<Long, Long>) any, anyBoolean);
                minTimes = 0;
                result = com.starrocks.common.Status.OK;
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
        Locker locker = new Locker();

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

            for (MaterializedIndex index : partition.getDefaultPhysicalPartition()
                    .getMaterializedIndices(IndexExtState.VISIBLE)) {
                BackupIndexInfo idxInfo = new BackupIndexInfo();
                idxInfo.id = index.getId();
                idxInfo.name = expectedRestoreTbl.getIndexNameById(index.getId());
                idxInfo.schemaHash = expectedRestoreTbl.getSchemaHashByIndexId(index.getId());
                partInfo.indexes.put(idxInfo.name, idxInfo);

                for (Tablet tablet : index.getTablets()) {
                    BackupTabletInfo tabletInfo = new BackupTabletInfo();
                    tabletInfo.id = tablet.getId();
                    idxInfo.tablets.add(tabletInfo);
                }
            }
        }

        // drop this table, cause we want to try restoring this table
        db.dropTable(expectedRestoreTbl.getName());

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db;
            }

            @Mock
            public Table getTable(String dbName, String tblName) {
                return db.getTable(tblName);
            }

            @Mock
            public Table getTable(Long dbId, Long tableId) {
                return db.getTable(tableId);
            }
        };

        List<Table> tbls = Lists.newArrayList();
        tbls.add(expectedRestoreTbl);
        backupMeta = new BackupMeta(tbls);
        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, 3, 100000,
                globalStateMgr, repo.getId(), backupMeta, new MvRestoreContext());
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
    public void testRunBackupListTable() {
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = db;

                globalStateMgr.getNextId();
                minTimes = 0;
                result = id.incrementAndGet();

                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
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
                systemInfoService.getNodeSelector().seqChooseBackendIds(anyInt, anyBoolean, anyBoolean, null);
                minTimes = 0;
                result = beIds;

                systemInfoService.getBackend(anyLong);
                minTimes = 0;
                result = null;

                systemInfoService.getComputeNode(anyLong);
                minTimes = 0;
                result = null;

                systemInfoService.checkExceedDiskCapacityLimit((Multimap<Long, Long>) any, anyBoolean);
                minTimes = 0;
                result = com.starrocks.common.Status.OK;
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
        Locker locker = new Locker();

        // gen BackupJobInfo
        jobInfo = new BackupJobInfo();
        jobInfo.backupTime = System.currentTimeMillis();
        jobInfo.dbId = CatalogMocker.TEST_DB_ID;
        jobInfo.dbName = CatalogMocker.TEST_DB_NAME;
        jobInfo.name = label;
        jobInfo.success = true;

        expectedRestoreTbl = (OlapTable) db.getTable(CatalogMocker.TEST_TBL5_ID);
        BackupTableInfo tblInfo = new BackupTableInfo();
        tblInfo.id = CatalogMocker.TEST_TBL5_ID;
        tblInfo.name = CatalogMocker.TEST_TBL5_NAME;
        jobInfo.tables.put(tblInfo.name, tblInfo);

        for (Partition partition : expectedRestoreTbl.getPartitions()) {
            BackupPartitionInfo partInfo = new BackupPartitionInfo();
            partInfo.id = partition.getId();
            partInfo.name = partition.getName();
            tblInfo.partitions.put(partInfo.name, partInfo);

            for (MaterializedIndex index : partition.getDefaultPhysicalPartition()
                    .getMaterializedIndices(IndexExtState.VISIBLE)) {
                BackupIndexInfo idxInfo = new BackupIndexInfo();
                idxInfo.id = index.getId();
                idxInfo.name = expectedRestoreTbl.getIndexNameById(index.getId());
                idxInfo.schemaHash = expectedRestoreTbl.getSchemaHashByIndexId(index.getId());
                partInfo.indexes.put(idxInfo.name, idxInfo);

                for (Tablet tablet : index.getTablets()) {
                    BackupTabletInfo tabletInfo = new BackupTabletInfo();
                    tabletInfo.id = tablet.getId();
                    idxInfo.tablets.add(tabletInfo);
                }
            }
        }

        // drop this table, cause we want to try restoring this table
        db.dropTable(expectedRestoreTbl.getName());

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db;
            }

            @Mock
            public Table getTable(String dbName, String tblName) {
                return db.getTable(tblName);
            }

            @Mock
            public Table getTable(Long dbId, Long tableId) {
                return db.getTable(tableId);
            }
        };

        List<Table> tbls = Lists.newArrayList();
        tbls.add(expectedRestoreTbl);
        backupMeta = new BackupMeta(tbls);
        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, 3, 100000,
                globalStateMgr, repo.getId(), backupMeta, new MvRestoreContext());
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

    public void testSignature() {
        Adler32 sig1 = new Adler32();
        sig1.update("name1".getBytes());
        sig1.update("name2".getBytes());
        System.out.println("sig1: " + Math.abs((int) sig1.getValue()));

        Adler32 sig2 = new Adler32();
        sig2.update("name2".getBytes());
        sig2.update("name1".getBytes());
        System.out.println("sig2: " + Math.abs((int) sig2.getValue()));

        Locker locker = new Locker();

        OlapTable tbl = (OlapTable) db.getTable(CatalogMocker.TEST_TBL_NAME);
        List<String> partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println(partNames);
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true));
        tbl.setName("newName");
        partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true));
    }

    @Test
    public void testColocateRestore() {
        Config.enable_colocate_restore = true;

        expectedRestoreTbl = (OlapTable) db.getTable(CatalogMocker.TEST_TBL4_ID);

        expectedRestoreTbl.resetIdsForRestore(globalStateMgr, db, 3, null);

        new Expectations() {
            {
                try {
                    GlobalStateMgr.getCurrentState().getColocateTableIndex()
                            .addTableToGroup((Database) any, (OlapTable) any, (String) any, false);
                } catch (Exception e) {
                }
                result = true;
            }
        };
        expectedRestoreTbl.setColocateGroup("test_group");
        expectedRestoreTbl.resetIdsForRestore(globalStateMgr, db, 3, null);
        expectedRestoreTbl.resetIdsForRestore(globalStateMgr, db, 3, null);

        Config.enable_colocate_restore = false;
    }

    @Test
    public void testRestoreView() {
        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = db;

                globalStateMgr.getNextId();
                minTimes = 0;
                result = id.incrementAndGet();

                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
                minTimes = 0;
                result = systemInfoService;
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
        Locker locker = new Locker();

        // gen BackupJobInfo
        jobInfo = new BackupJobInfo();
        jobInfo.backupTime = System.currentTimeMillis();
        jobInfo.dbId = CatalogMocker.TEST_DB_ID;
        jobInfo.dbName = CatalogMocker.TEST_DB_NAME;
        jobInfo.name = label;
        jobInfo.success = true;

        View restoredView = (View) db.getTable(CatalogMocker.TEST_TBL6_ID);

        BackupTableInfo tblInfo = new BackupTableInfo();
        tblInfo.id = CatalogMocker.TEST_TBL6_ID;
        tblInfo.name = CatalogMocker.TEST_TBL6_NAME;
        jobInfo.tables.put(tblInfo.name, tblInfo);

        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                return db;
            }

            @Mock
            public Table getTable(String dbName, String tblName) {
                return db.getTable(tblName);
            }

            @Mock
            public Table getTable(Long dbId, Long tableId) {
                return db.getTable(tableId);
            }
        };

        new MockUp<View>() {
            @Mock
            public synchronized QueryStatement getQueryStatement() throws UserException {
                return null;
            }
        };

        new Expectations() {
            {
                systemInfoService.checkExceedDiskCapacityLimit((Multimap<Long, Long>) any, anyBoolean);
                minTimes = 0;
                result = com.starrocks.common.Status.OK;
            }
        };

        List<Table> tbls = Lists.newArrayList();
        tbls.add(restoredView);
        backupMeta = new BackupMeta(tbls);

        db.dropTable(restoredView.getName());
        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, 3, 100000,
                globalStateMgr, repo.getId(), backupMeta, new MvRestoreContext());
        job.setRepo(repo);
        Assert.assertEquals(RestoreJobState.PENDING, job.getState());
        {
            new MockUp<View>() {
                @Mock
                public synchronized QueryStatement init() throws UserException {
                    return null;
                }
            };
            job.run();
        }
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assert.assertEquals(0, job.getFileMapping().getMapping().size());

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.DOWNLOAD, job.getState());

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.DOWNLOADING, job.getState());

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.COMMIT, job.getState());

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.COMMITTING, job.getState());

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.FINISHED, job.getState());

        // restore when the view already existed
        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, 3, 100000,
                globalStateMgr, repo.getId(), backupMeta, new MvRestoreContext());
        job.setRepo(repo);
        Assert.assertEquals(RestoreJobState.PENDING, job.getState());

        {
            new MockUp<View>() {
                @Mock
                public synchronized QueryStatement init() throws UserException {
                    return null;
                }
            };
            job.run();
        }
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assert.assertEquals(0, job.getFileMapping().getMapping().size());

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.DOWNLOAD, job.getState());

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.DOWNLOADING, job.getState());

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.COMMIT, job.getState());

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.COMMITTING, job.getState());

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.FINISHED, job.getState());
    }

    @Test
    public void testRestoreAddFunction() {
        backupMeta = new BackupMeta(Lists.newArrayList());
        Function f1 = new Function(new FunctionName(db.getFullName(), "test_function"),
                new Type[] {Type.INT}, new String[] {"argName"}, Type.INT, false);

        backupMeta.setFunctions(Lists.newArrayList(f1));
        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                new BackupJobInfo(), false, 3, 100000,
                globalStateMgr, repo.getId(), backupMeta, new MvRestoreContext());

        job.addRestoredFunctions(db);
    }

    @Test
    public void testRestoreAddCatalog() {
        backupMeta = new BackupMeta(Lists.newArrayList());
        Catalog catalog = new Catalog(1111111, "test_catalog", Maps.newHashMap(), "");

        backupMeta.setCatalogs(Lists.newArrayList(catalog));
        job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                new BackupJobInfo(), false, 3, 100000,
                globalStateMgr, repo.getId(), backupMeta, new MvRestoreContext());
        job.setRepo(repo);
        job.addRestoredFunctions(db);
        job.run();
        job.run();
    }
}