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

package com.starrocks.alter;

import com.google.common.collect.Table;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.lake.DataCacheInfo;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.Utils;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.task.UpdateTabletMetaInfoTask;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletMetaType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TUpdateTabletMetaInfoReq;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import javax.validation.constraints.NotNull;

public class LakeTableAlterMetaJobTest {
    private static final int NUM_BUCKETS = 4;
    private ConnectContext connectContext;
    private LakeTableAlterMetaJob alterMetaJob;
    private Database db;
    private LakeTable table;
    private List<Long> shadowTabletIds = new ArrayList<>();

    public LakeTableAlterMetaJobTest() {
        connectContext = new ConnectContext(null);
        connectContext.setStartTime();
        connectContext.setThreadLocalInfo();
    }

    @Before
    public void before() throws Exception {
        FeConstants.runningUnitTest = true;
        new MockUp<StarOSAgent>() {
            @Mock
            public List<Long> createShards(int shardCount, FilePathInfo path, FileCacheInfo cache, long groupId,
                                           List<Long> matchShardIds, Map<String, String> properties)
                    throws DdlException {
                for (int i = 0; i < shardCount; i++) {
                    shadowTabletIds.add(GlobalStateMgr.getCurrentState().getNextId());
                }
                return shadowTabletIds;
            }
        };

        new MockUp<EditLog>() {
            @Mock
            public void logSaveNextId(long nextId) {

            }

            @Mock
            public void logAlterJob(AlterJobV2 alterJob) {

            }

            @Mock
            public void logSaveTransactionId(long transactionId) {

            }

            @Mock
            public void logModifyEnablePersistentIndex(ModifyTablePropertyOperationLog info) {

            }
        };

        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));
        final long dbId = GlobalStateMgr.getCurrentState().getNextId();
        final long partitionId = GlobalStateMgr.getCurrentState().getNextId();
        final long tableId = GlobalStateMgr.getCurrentState().getNextId();
        final long indexId = GlobalStateMgr.getCurrentState().getNextId();

        GlobalStateMgr.getCurrentState().setStarOSAgent(new StarOSAgent());

        KeysType keysType = KeysType.DUP_KEYS;
        db = new Database(dbId, "db0");

        Database oldDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().putIfAbsent(db.getId(), db);
        Assert.assertNull(oldDb);

        Column c0 = new Column("c0", Type.INT, true, AggregateType.NONE, false, null, null);
        DistributionInfo dist = new HashDistributionInfo(NUM_BUCKETS, Collections.singletonList(c0));
        PartitionInfo partitionInfo = new RangePartitionInfo(Collections.singletonList(c0));
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);

        table = new LakeTable(tableId, "t0", Collections.singletonList(c0), keysType, partitionInfo, dist);
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(partitionId, "t0", index, dist);
        TStorageMedium storage = TStorageMedium.HDD;
        TabletMeta tabletMeta =
                new TabletMeta(db.getId(), table.getId(), partition.getId(), index.getId(), 0, storage, true);
        for (int i = 0; i < NUM_BUCKETS; i++) {
            Tablet tablet = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId());
            index.addTablet(tablet, tabletMeta);
        }
        table.addPartition(partition);

        table.setIndexMeta(index.getId(), "t0", Collections.singletonList(c0), 0, 0, (short) 1, TStorageType.COLUMN,
                keysType);
        table.setBaseIndexId(index.getId());

        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();

        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("test-bucket");
        s3FsBuilder.setRegion("test-region");
        S3FileStoreInfo s3FsInfo = s3FsBuilder.build();

        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("test-bucket");
        fsBuilder.setS3FsInfo(s3FsInfo);
        FileStoreInfo fsInfo = fsBuilder.build();

        builder.setFsInfo(fsInfo);
        builder.setFullPath("s3://test-bucket/object-1");
        FilePathInfo pathInfo = builder.build();

        table.setStorageInfo(pathInfo, new DataCacheInfo(false, false));
        DataCacheInfo dataCacheInfo = new DataCacheInfo(false, false);
        partitionInfo.setDataCacheInfo(partitionId, dataCacheInfo);

        db.registerTableUnlocked(table);

        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, "true");
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();

        List<AlterClause> alterList = Collections.singletonList(modify);
        alterMetaJob = (LakeTableAlterMetaJob) schemaChangeHandler.createAlterMetaJob(alterList, db, table);
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
    }

    @After
    public void after() throws Exception {
        db.dropTable(table.getName());
    }

    @Test
    public void testRunPendingJob() throws AlterCancelException {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };
        Assert.assertEquals(alterMetaJob.jobState, AlterJobV2.JobState.PENDING);
        alterMetaJob.runPendingJob();
        Assert.assertEquals(alterMetaJob.jobState, AlterJobV2.JobState.RUNNING);
        Assert.assertNotEquals(alterMetaJob.getTransactionId().get().longValue(), -1L);

    }

    @Test
    public void testUpdatePartitonMetaFailed() throws AlterCancelException {
        FeConstants.runningUnitTest = false;
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };

        Assert.assertEquals(alterMetaJob.jobState, AlterJobV2.JobState.PENDING);
        Exception exception = Assert.assertThrows(AlterCancelException.class, () -> {
            alterMetaJob.runPendingJob();
        });

        Assert.assertTrue(exception.getMessage().contains("Failed to update partition"));
        Assert.assertEquals(alterMetaJob.jobState, AlterJobV2.JobState.PENDING);
    }

    @Test
    public void testCancelPendingJob() {
        alterMetaJob.cancel("cancel test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, alterMetaJob.getJobState());

        // test cancel again
        Assert.assertFalse(alterMetaJob.cancel("test"));
    }

    @Test
    public void testDropTableBeforeCancel() {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };

        db.dropTable(table.getName());
        Assert.assertTrue(alterMetaJob.cancel("test"));
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, alterMetaJob.getJobState());
    }

    @Test
    public void testRunningJob() throws AlterCancelException {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };

        alterMetaJob.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, alterMetaJob.getJobState());

        Table<Long, Long, MaterializedIndex> partitionIndexMap = alterMetaJob.getPartitionIndexMap();
        Map<Long, Long> commitVersionMap = alterMetaJob.getCommitVersionMap();
        Assert.assertEquals(1, partitionIndexMap.size());
        Assert.assertEquals(0, commitVersionMap.size());

        alterMetaJob.runRunningJob();
        for (long partitionId : partitionIndexMap.rowKeySet()) {
            Partition partition = table.getPartition(partitionId);
            Assert.assertEquals(commitVersionMap.get(partitionId).longValue(), partition.getCommittedVersion());
        }
        Assert.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, alterMetaJob.getJobState());
        Assert.assertTrue(alterMetaJob.getFinishedTimeMs() > System.currentTimeMillis() - 10_000L);
    }

    @Test
    public void testFinishedRewritingJob() throws AlterCancelException {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }

            @Mock
            public void publishVersion(@NotNull List<Tablet> tablets, long txnId, long baseVersion, long newVersion,
                                       long commitTime) throws
                    RpcException {
            }
        };

        alterMetaJob.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, alterMetaJob.getJobState());

        alterMetaJob.runRunningJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, alterMetaJob.getJobState());
        Assert.assertTrue(alterMetaJob.getFinishedTimeMs() > System.currentTimeMillis() - 10_000L);

        alterMetaJob.runFinishedRewritingJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterMetaJob.getJobState());
        Assert.assertTrue(alterMetaJob.getFinishedTimeMs() > System.currentTimeMillis() - 10_000L);

        Table<Long, Long, MaterializedIndex> partitionIndexMap = alterMetaJob.getPartitionIndexMap();
        Map<Long, Long> commitVersionMap = alterMetaJob.getCommitVersionMap();
        for (long partitionId : partitionIndexMap.rowKeySet()) {
            Partition partition = table.getPartition(partitionId);
            Assert.assertEquals(commitVersionMap.get(partitionId).longValue(), partition.getVisibleVersion());
        }

        Assert.assertTrue(table.enablePersistentIndex());
    }

    @Test
    public void testReplay() throws AlterCancelException {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }

            @Mock
            public void publishVersion(@NotNull List<Tablet> tablets, long txnId, long baseVersion, long newVersion,
                                       long commitTime) throws
                    RpcException {
            }
        };

        LakeTableAlterMetaJob replayAlterMetaJob = new LakeTableAlterMetaJob(alterMetaJob.jobId,
                alterMetaJob.dbId, alterMetaJob.tableId, alterMetaJob.tableName,
                alterMetaJob.timeoutMs, TTabletMetaType.ENABLE_PERSISTENT_INDEX, true);

        alterMetaJob.runPendingJob();
        alterMetaJob.runRunningJob();
        alterMetaJob.runFinishedRewritingJob();

        Database db = GlobalStateMgr.getCurrentState().getDb(alterMetaJob.getDbId());
        LakeTable table = (LakeTable) db.getTable(alterMetaJob.getTableId());

        Table<Long, Long, MaterializedIndex> partitionIndexMap = alterMetaJob.getPartitionIndexMap();
        Map<Long, Long> commitVersionMap = alterMetaJob.getCommitVersionMap();

        // for replay will check partition.getVisibleVersion()
        // here we reduce the visibleVersion for test
        for (long partitionId : partitionIndexMap.rowKeySet()) {
            Partition partition = table.getPartition(partitionId);
            long commitVersion = commitVersionMap.get(partitionId);
            Assert.assertEquals(partition.getVisibleVersion(), commitVersion);
            partition.updateVisibleVersion(commitVersion - 1);
        }

        replayAlterMetaJob.replay(alterMetaJob);

        Assert.assertEquals(AlterJobV2.JobState.FINISHED, replayAlterMetaJob.getJobState());
        Assert.assertEquals(alterMetaJob.getFinishedTimeMs(), replayAlterMetaJob.getFinishedTimeMs());
        Assert.assertEquals(alterMetaJob.getTransactionId(), replayAlterMetaJob.getTransactionId());
        Assert.assertEquals(alterMetaJob.getJobId(), replayAlterMetaJob.getJobId());
        Assert.assertEquals(alterMetaJob.getTableId(), replayAlterMetaJob.getTableId());
        Assert.assertEquals(alterMetaJob.getDbId(), replayAlterMetaJob.getDbId());
        Assert.assertEquals(alterMetaJob.getCommitVersionMap(), replayAlterMetaJob.getCommitVersionMap());
        Assert.assertEquals(alterMetaJob.getPartitionIndexMap(), replayAlterMetaJob.getPartitionIndexMap());

        for (long partitionId : partitionIndexMap.rowKeySet()) {
            Partition partition = table.getPartition(partitionId);
            long commitVersion = commitVersionMap.get(partitionId);
            Assert.assertEquals(partition.getVisibleVersion(), commitVersion);
        }
    }

    @Test
    public void testUpdateTabletMetaInfoTaskToThrift() throws AlterCancelException {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
        long backend = 1L;
        long txnId = 1L;
        Set<Long> tabletSet = new HashSet<>();
        tabletSet.add(1L);
        MarkedCountDownLatch<Long, Set<Long>> latch = new MarkedCountDownLatch<>(1);
        UpdateTabletMetaInfoTask task = UpdateTabletMetaInfoTask.updateEnablePersistentIndex(backend, tabletSet, true);
        task.setLatch(latch);
        task.setTxnId(txnId);
        TUpdateTabletMetaInfoReq result = task.toThrift();
        Assert.assertEquals(result.txn_id, txnId);
        Assert.assertEquals(result.tablet_type, TTabletType.TABLET_TYPE_LAKE);
    }

    @Test
    public void testSetPropertyNotSupport() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_WRITE_QUORUM, "all");
        ModifyTablePropertiesClause modify = new ModifyTablePropertiesClause(properties);
        List<AlterClause> alterList = Collections.singletonList(modify);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        Assertions.assertThrows(DdlException.class,
                () -> schemaChangeHandler.createAlterMetaJob(alterList, db, table));
    }
}
