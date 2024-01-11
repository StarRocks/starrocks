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
// limitations under the License

package com.starrocks.backup;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.backup.BackupJobInfo.BackupIndexInfo;
import com.starrocks.backup.BackupJobInfo.BackupPartitionInfo;
import com.starrocks.backup.BackupJobInfo.BackupTableInfo;
import com.starrocks.backup.BackupJobInfo.BackupTabletInfo;
import com.starrocks.backup.RestoreJob.RestoreJobState;
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.util.UnitTestUtil;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.DirMoveTask;
import com.starrocks.task.DownloadTask;
import com.starrocks.task.SnapshotTask;
import com.starrocks.thrift.TBackend;
import com.starrocks.thrift.TFinishTaskRequest;
import com.starrocks.thrift.THdfsProperties;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.zip.Adler32;

import static com.starrocks.common.util.UnitTestUtil.DB_NAME;
import static com.starrocks.common.util.UnitTestUtil.MATERIALIZED_VIEW_NAME;
import static com.starrocks.common.util.UnitTestUtil.TABLE_NAME;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RestoreJobMaterializedViewTest {

    private Database db;

    private String label = "test_mv_label";

    private static final int ID_SIZE = 10000;

    private AtomicLong id = new AtomicLong(50000);

    private long repoId = 30000;

    @Mocked
    private GlobalStateMgr globalStateMgr;

    private MockBackupHandler backupHandler;

    private MockRepositoryMgr repoMgr;

    private MvRestoreContext mvRestoreContext;


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

    private long dbId = 11;
    private long tblId = 12;
    private long partId = 13;
    private long idxId = 14;
    private long tabletId = 15;
    private long backendId = 10000;
    private long version = 16;

    private Object[] arrayIds;
    private void setUpMocker() {
        MetricRepo.init();

        List<Long> beIds = Lists.newArrayList();
        beIds.add(CatalogMocker.BACKEND1_ID);
        beIds.add(CatalogMocker.BACKEND2_ID);
        beIds.add(CatalogMocker.BACKEND3_ID);
        arrayIds = new Object[ID_SIZE];
        IntStream.range(0, ID_SIZE).forEach(i -> arrayIds[i] = id.getAndIncrement());

        new Expectations() {
            {
                globalStateMgr.getDb(anyLong);
                minTimes = 0;
                result = db;

                globalStateMgr.getEditLog();
                minTimes = 0;
                result = editLog;

                GlobalStateMgr.getCurrentSystemInfo();
                minTimes = 0;
                result = systemInfoService;

                globalStateMgr.mayGetDb(anyLong);
                minTimes = 0;
                result = Optional.of(db);

                globalStateMgr.getNextId();
                minTimes = 0;
                returns(arrayIds[0], arrayIds[1], Arrays.copyOfRange(arrayIds, 2, ID_SIZE));
            }

            {
                systemInfoService.seqChooseBackendIds(anyInt, anyBoolean, anyBoolean);
                minTimes = 0;
                result = beIds;

                systemInfoService.checkExceedDiskCapacityLimit((Multimap<Long, Long>) any, anyBoolean);
                minTimes = 0;
                result = com.starrocks.common.Status.OK;
            }

            {
                editLog.logBackupJob((BackupJob) any);
                minTimes = 0;
                result = new Delegate() {
                    public void logBackupJob(BackupJob job) {
                        System.out.println("log backup job: " + job);
                    }
                };
            }

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

        new MockUp<ConnectContext>() {
                @Mock
                GlobalStateMgr getGlobalStateMgr() {
                    return globalStateMgr;
                }
            };
        new MockUp<GlobalStateMgr>() {
                @Mock
                BackupHandler getBackupHandler() {
                    return backupHandler;
                }
            };

        new MockUp<HdfsUtil>() {
            @Mock
            public void getTProperties(String path, BrokerDesc brokerDesc, THdfsProperties tProperties) throws UserException {
            }
        };
    }

    @BeforeEach
    public void setUp() throws AnalysisException {
        repoMgr = new MockRepositoryMgr();
        backupHandler = new MockBackupHandler(globalStateMgr);

        Deencapsulation.setField(globalStateMgr, "backupHandler", backupHandler);
        systemInfoService = new SystemInfoService();
        db = UnitTestUtil.createDbWithMaterializedView(dbId, tblId, partId, idxId, tabletId,
                backendId, version, KeysType.DUP_KEYS);
        mvRestoreContext = new MvRestoreContext();
        setUpMocker();
    }

    private BackupTableInfo mockBackupTableInfo(OlapTable olapTable) {
        Assert.assertTrue(olapTable != null);
        BackupTableInfo tblInfo = new BackupTableInfo();
        tblInfo.id = olapTable.getId();
        tblInfo.name = olapTable.getName();
        for (Partition partition : olapTable.getPartitions()) {
            BackupPartitionInfo partInfo = new BackupPartitionInfo();
            partInfo.id = partition.getId();
            partInfo.name = partition.getName();
            tblInfo.partitions.put(partInfo.name, partInfo);

            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                BackupIndexInfo idxInfo = new BackupIndexInfo();
                idxInfo.id = index.getId();
                idxInfo.name = olapTable.getIndexNameById(index.getId());
                idxInfo.schemaHash = olapTable.getSchemaHashByIndexId(index.getId());
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
        return tblInfo;
    }

    public RestoreJob createRestoreJob(List<String> restoreTbls) {
        BackupJobInfo jobInfo = new BackupJobInfo();
        jobInfo.backupTime = System.currentTimeMillis();
        jobInfo.dbId = dbId;
        jobInfo.dbName = DB_NAME;
        jobInfo.name = label;
        jobInfo.success = true;

        List<Table> tbls = Lists.newArrayList();
        for (String tbl : restoreTbls) {
            OlapTable baseTable = (OlapTable) db.getTable(tbl);
            BackupTableInfo baseTblInfo = mockBackupTableInfo(baseTable);
            jobInfo.tables.put(baseTblInfo.name, baseTblInfo);
            tbls.add(baseTable);
        }
        backupMeta = new BackupMeta(tbls);

        // drop table to test restore
        for (String tbl : restoreTbls) {
            db.dropTable(tbl);
        }

        RestoreJob job = new RestoreJob(label, "2018-01-01 01:01:01", db.getId(), db.getFullName(),
                jobInfo, false, 3, 100000,
                globalStateMgr, repo.getId(), backupMeta, mvRestoreContext);
        job.setRepo(repo);

        // add job into mvRestoreContext
        mvRestoreContext.addIntoMvBaseTableBackupInfo(job);

        return job;
    }

    private void checkJobRun(RestoreJob job) {
        globalStateMgr.setNextId(id.getAndDecrement());

        int tblNums = job.getJobInfo().tables.size();
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assert.assertEquals(3 * tblNums, job.getFileMapping().getMapping().size());

        // 2. snapshoting
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.SNAPSHOTING, job.getState());
        Assert.assertEquals(6 * tblNums, AgentTaskQueue.getTaskNum());

        // 3. snapshot finished
        List<AgentTask> agentTasks = Lists.newArrayList();
        Map<TTaskType, Set<Long>> runningTasks = Maps.newHashMap();
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND1_ID, runningTasks));
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND2_ID, runningTasks));
        agentTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND3_ID, runningTasks));
        Assert.assertEquals(6 * tblNums, agentTasks.size());

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

        // download
        AgentTaskQueue.clearAllTasks();
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.DOWNLOADING, job.getState());
        Assert.assertEquals(3 * tblNums, AgentTaskQueue.getTaskNum());

        // downloading
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.DOWNLOADING, job.getState());

        List<AgentTask> downloadTasks = Lists.newArrayList();
        runningTasks = Maps.newHashMap();
        downloadTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND1_ID, runningTasks));
        downloadTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND2_ID, runningTasks));
        downloadTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND3_ID, runningTasks));
        Assert.assertEquals(3 * tblNums, downloadTasks.size());

        List<Long> downloadedTabletIds = Lists.newArrayList();
        for (AgentTask agentTask : downloadTasks) {
            TStatus taskStatus = new TStatus(TStatusCode.OK);
            TBackend tBackend = new TBackend("", 0, 1);
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    agentTask.getSignature(), taskStatus);
            request.setDownloaded_tablet_ids(downloadedTabletIds);
            Assert.assertTrue(job.finishTabletDownloadTask((DownloadTask) agentTask, request));
        }

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.COMMIT, job.getState());

        // commit
        AgentTaskQueue.clearAllTasks();
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.COMMITTING, job.getState());
        Assert.assertEquals(3 * tblNums, AgentTaskQueue.getTaskNum());

        // committing
        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.COMMITTING, job.getState());

        List<AgentTask> dirMoveTasks = Lists.newArrayList();
        runningTasks = Maps.newHashMap();
        dirMoveTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND1_ID, runningTasks));
        dirMoveTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND2_ID, runningTasks));
        dirMoveTasks.addAll(AgentTaskQueue.getDiffTasks(CatalogMocker.BACKEND3_ID, runningTasks));
        Assert.assertEquals(3 * tblNums, dirMoveTasks.size());

        for (AgentTask agentTask : dirMoveTasks) {
            TStatus taskStatus = new TStatus(TStatusCode.OK);
            TBackend tBackend = new TBackend("", 0, 1);
            TFinishTaskRequest request = new TFinishTaskRequest(tBackend, TTaskType.MAKE_SNAPSHOT,
                    agentTask.getSignature(), taskStatus);
            job.finishDirMoveTask((DirMoveTask) agentTask, request);
        }

        job.run();
        Assert.assertEquals(Status.OK, job.getStatus());
        Assert.assertEquals(RestoreJobState.FINISHED, job.getState());

        // clear
        AgentTaskQueue.clearAllTasks();
    }

    @Test
    @Order(1)
    public void testMVRestore_TestOneTable1() {
        RestoreJob job = createRestoreJob(ImmutableList.of(UnitTestUtil.MATERIALIZED_VIEW_NAME));
        checkJobRun(job);
    }

    @Test
    @Order(2)
    public void testMVRestore_TestOneTable2() {
        RestoreJob job = createRestoreJob(ImmutableList.of(UnitTestUtil.TABLE_NAME));
        checkJobRun(job);
    }

    @Test
    @Order(3)
    public void testMVRestore_TestMVWithBaseTable1() {
        new Expectations() {
            {
                globalStateMgr.getCurrentState().getCatalogMgr().catalogExists("default_catalog");
                result = true;

                globalStateMgr.getCurrentState().getMetadataMgr().getDb("default_catalog", DB_NAME);
                minTimes = 0;
                result = db;
            }
        };

        // gen BackupJobInfo
        RestoreJob job = createRestoreJob(ImmutableList.of(TABLE_NAME, MATERIALIZED_VIEW_NAME));
        // backup & restore
        checkJobRun(job);

        {
            Table mvTable = db.getTable(UnitTestUtil.MATERIALIZED_VIEW_NAME);
            Assert.assertTrue(mvTable != null);
            Assert.assertTrue(mvTable.isMaterializedView());
            MaterializedView mv = (MaterializedView) mvTable;
            Assert.assertTrue(mv.isActive());
        }
    }

    @Test
    @Order(4)
    public void testMVRestore_TestMVWithBaseTable2() {
        new Expectations() {
            {
                globalStateMgr.getCurrentState().getCatalogMgr().catalogExists("default_catalog");
                result = true;

                globalStateMgr.getCurrentState().getMetadataMgr().getDb("default_catalog", DB_NAME);
                minTimes = 0;
                result = db;
            }
        };

        // gen BackupJobInfo
        RestoreJob job = createRestoreJob(ImmutableList.of(MATERIALIZED_VIEW_NAME, TABLE_NAME));
        // backup & restore
        checkJobRun(job);

        {
            Table mvTable = db.getTable(UnitTestUtil.MATERIALIZED_VIEW_NAME);
            Assert.assertTrue(mvTable != null);
            Assert.assertTrue(mvTable.isMaterializedView());
            MaterializedView mv = (MaterializedView) mvTable;
            Assert.assertTrue(mv.isActive());
        }
    }

    @Test
    @Order(5)
    public void testMVRestore_TestMVWithBaseTable3() {
        new Expectations() {
            {
                globalStateMgr.getCurrentState().getCatalogMgr().catalogExists("default_catalog");
                result = true;

                globalStateMgr.getCurrentState().getMetadataMgr().getDb("default_catalog", DB_NAME);
                minTimes = 0;
                result = db;
            }
        };

        // gen BackupJobInfo
        RestoreJob job1 = createRestoreJob(ImmutableList.of(TABLE_NAME));
        // backup & restore
        checkJobRun(job1);

        RestoreJob job2 = createRestoreJob(ImmutableList.of(MATERIALIZED_VIEW_NAME));
        checkJobRun(job2);

        {
            Table mvTable = db.getTable(UnitTestUtil.MATERIALIZED_VIEW_NAME);
            Assert.assertTrue(mvTable != null);
            Assert.assertTrue(mvTable.isMaterializedView());
            MaterializedView mv = (MaterializedView) mvTable;
            Assert.assertTrue(mv.isActive());
        }
    }

    @Test
    @Order(6)
    public void testMVRestore_TestMVWithBaseTable4() {
        new Expectations() {
            {
                globalStateMgr.getCurrentState().getCatalogMgr().catalogExists("default_catalog");
                result = true;

                globalStateMgr.getCurrentState().getMetadataMgr().getDb("default_catalog", DB_NAME);
                minTimes = 0;
                result = db;
            }
        };

        new MockUp<MetadataMgr>() {
            @Mock
            public Table getTable(String catalogName, String dbName, String tblName) {
                return db.getTable(tblName);
            }
        };

        // gen BackupJobInfo
        RestoreJob job1 = createRestoreJob(ImmutableList.of(MATERIALIZED_VIEW_NAME));
        // backup & restore
        checkJobRun(job1);

        RestoreJob job2 = createRestoreJob(ImmutableList.of(TABLE_NAME));
        checkJobRun(job2);

        {
            Table mvTable = db.getTable(UnitTestUtil.MATERIALIZED_VIEW_NAME);
            Assert.assertTrue(mvTable != null);
            Assert.assertTrue(mvTable.isMaterializedView());
            MaterializedView mv = (MaterializedView) mvTable;
            Assert.assertTrue(mv.isActive());
        }
    }

    @Test
    @Order(7)
    public void testSignature() {
        Adler32 sig1 = new Adler32();
        sig1.update("name1".getBytes());
        sig1.update("name2".getBytes());
        System.out.println("sig1: " + Math.abs((int) sig1.getValue()));

        Adler32 sig2 = new Adler32();
        sig2.update("name2".getBytes());
        sig2.update("name1".getBytes());
        System.out.println("sig2: " + Math.abs((int) sig2.getValue()));

        OlapTable tbl = (OlapTable) db.getTable(UnitTestUtil.MATERIALIZED_VIEW_NAME);
        List<String> partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println(partNames);
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true));
        tbl.setName("newName");
        partNames = Lists.newArrayList(tbl.getPartitionNames());
        System.out.println("tbl signature: " + tbl.getSignature(BackupHandler.SIGNATURE_VERSION, partNames, true));
    }
}

