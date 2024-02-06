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

import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.TypeDef;
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
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.journal.JournalTask;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.ShardDeleter;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.lake.StorageCacheInfo;
import com.starrocks.lake.Utils;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

import static com.starrocks.catalog.TabletInvertedIndex.NOT_EXIST_TABLET_META;

public class LakeTableSchemaChangeJobTest {
    private static final int NUM_BUCKETS = 4;
    private ConnectContext connectContext;
    private LakeTableSchemaChangeJob schemaChangeJob;
    private Database db;
    private LakeTable table;
    private List<Long> shadowTabletIds = new ArrayList<>();

    public LakeTableSchemaChangeJobTest() {
        connectContext = new ConnectContext(null);
        connectContext.setStartTime();
        connectContext.setThreadLocalInfo();
    }

    @Before
    public void before() throws Exception {
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
        };

        GlobalStateMgr.getCurrentState().setEditLog(new EditLog(new ArrayBlockingQueue<>(100)));

        final long dbId = GlobalStateMgr.getCurrentState().getNextId();
        final long partitionId = GlobalStateMgr.getCurrentState().getNextId();
        final long tableId = GlobalStateMgr.getCurrentState().getNextId();
        final long indexId = GlobalStateMgr.getCurrentState().getNextId();

        GlobalStateMgr.getCurrentState().setStarOSAgent(new StarOSAgent());

        KeysType keysType = KeysType.DUP_KEYS;
        db = new Database(dbId, "db0");

        Database oldDb = GlobalStateMgr.getCurrentState().getIdToDb().putIfAbsent(db.getId(), db);
        Assert.assertNull(oldDb);

        Column c0 = new Column("c0", Type.INT, true, AggregateType.NONE, false, null, null);
        DistributionInfo dist = new HashDistributionInfo(NUM_BUCKETS, Collections.singletonList(c0));
        PartitionInfo partitionInfo = new RangePartitionInfo(Collections.singletonList(c0));
        partitionInfo.setDataProperty(partitionId, DataProperty.DEFAULT_DATA_PROPERTY);

        table = new LakeTable(tableId, "t0", Collections.singletonList(c0), keysType, partitionInfo, dist);
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(partitionId, "t0", index, dist);
        TStorageMedium storage = TStorageMedium.HDD;
        TabletMeta tabletMeta = new TabletMeta(db.getId(), table.getId(), partition.getId(), index.getId(), 0, storage, true);
        for (int i = 0; i < NUM_BUCKETS; i++) {
            Tablet tablet = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId());
            index.addTablet(tablet, tabletMeta);
        }
        table.addPartition(partition);

        table.setIndexMeta(index.getId(), "t0", Collections.singletonList(c0), 0, 0, (short) 1, TStorageType.COLUMN, keysType);
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

        table.setStorageInfo(pathInfo, false, 0, false);
        StorageCacheInfo storageCacheInfo = new StorageCacheInfo(false, 0, false);
        partitionInfo.setStorageCacheInfo(partitionId, storageCacheInfo);

        db.createTable(table);

        ColumnDef c1 = new ColumnDef("c1", TypeDef.create(PrimitiveType.DOUBLE));
        AddColumnClause alter = new AddColumnClause(c1, null, null, null);
        alter.setColumn(new Column("c1", Type.DOUBLE));
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        List<AlterClause> alterList = Collections.singletonList(alter);
        schemaChangeJob = (LakeTableSchemaChangeJob) schemaChangeHandler.analyzeAndCreateJob(alterList, db, table);
        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
    }

    @After
    public void after() throws Exception {
        db.dropTable(table.getName());
    }

    @Test
    public void testCancelPendingJob() throws IOException {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }
        };

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (Long tabletId : shadowTabletIds) {
            Assert.assertNotNull(invertedIndex.getTabletMeta(tabletId));
        }

        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        for (Long tabletId : shadowTabletIds) {
            Assert.assertNull(invertedIndex.getTabletMeta(tabletId));
        }

        // test cancel again
        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testDropTableBeforeCancel() {
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }
        };

        db.dropTable(table.getName());

        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testPendingJobNoAliveBackend() {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return null;
            }
        };
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }
        };

        Exception exception = Assert.assertThrows(AlterCancelException.class, () -> {
            schemaChangeJob.runPendingJob();
        });
        Assert.assertTrue(exception.getMessage().contains("No alive backend"));
        Assert.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assert.assertEquals(-1, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testTableDroppedInPending() {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };
        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }
        };

        db.dropTable(table.getName());

        Exception exception = Assert.assertThrows(AlterCancelException.class, () -> {
            schemaChangeJob.runPendingJob();
        });
        Assert.assertTrue(exception.getMessage().contains("Table does not exist"));
        Assert.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assert.assertEquals(-1, schemaChangeJob.getWatershedTxnId());

        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().remove(db.getId());
        exception = Assert.assertThrows(AlterCancelException.class, () -> {
            schemaChangeJob.runPendingJob();
        });
        Assert.assertTrue(exception.getMessage().contains("Database does not exist"));
        Assert.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assert.assertEquals(-1, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testCreateTabletFailed() {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };

        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                             long timeoutSeconds) throws AlterCancelException {
                throw new AlterCancelException("Create tablet failed");
            }

            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }
        };

        Exception exception = Assert.assertThrows(AlterCancelException.class, () -> {
            schemaChangeJob.runPendingJob();
        });
        Assert.assertTrue(exception.getMessage().contains("Create tablet failed"));
        Assert.assertEquals(AlterJobV2.JobState.PENDING, schemaChangeJob.getJobState());
        Assert.assertEquals(-1, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testCreateTabletSuccess() throws AlterCancelException {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };

        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                             long timeoutSeconds) throws AlterCancelException {
                Assert.assertEquals(NUM_BUCKETS, countDownLatch.getCount());
            }

            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }

            @Mock
            public long getNextTransactionId() {
                return 10101L;
            }
        };

        schemaChangeJob.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assert.assertEquals(10101L, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assert.assertEquals(0, partition.getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testPreviousTxnNotFinished() throws AlterCancelException {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };

        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                             long timeoutSeconds) throws AlterCancelException {
                // nothing to do.
            }

            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }

            @Mock
            public long getNextTransactionId() {
                return 10101L;
            }

            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) {
                return false;
            }
        };

        schemaChangeJob.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assert.assertEquals(10101L, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.runWaitingTxnJob();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assert.assertEquals(0, partition.getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testThrowAnalysisExceptiondWhileWaitingTxn() throws AlterCancelException {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };

        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                             long timeoutSeconds) throws AlterCancelException {
                // nothing to do.
            }

            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }

            @Mock
            public long getNextTransactionId() {
                return 10101L;
            }

            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
                throw new AnalysisException("isPreviousLoadFinished exception");
            }
        };

        schemaChangeJob.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assert.assertEquals(10101L, schemaChangeJob.getWatershedTxnId());

        Exception exception = Assert.assertThrows(AlterCancelException.class, () -> {
            schemaChangeJob.runWaitingTxnJob();
        });
        Assert.assertTrue(exception.getMessage().contains("sPreviousLoadFinished exception"));
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assert.assertEquals(0, partition.getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testTableNotExistWhileWaitingTxn() throws AlterCancelException {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };

        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                             long timeoutSeconds) throws AlterCancelException {
                // nothing to do.
            }

            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }

            @Mock
            public long getNextTransactionId() {
                return 10101L;
            }

            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
                return true;
            }
        };

        schemaChangeJob.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assert.assertEquals(10101L, schemaChangeJob.getWatershedTxnId());

        db.dropTable(table.getName());

        Exception exception = Assert.assertThrows(AlterCancelException.class, () -> {
            schemaChangeJob.runWaitingTxnJob();
        });
        Assert.assertTrue(exception.getMessage().contains("Table does not exist."));
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().remove(db.getId());
        exception = Assert.assertThrows(AlterCancelException.class, () -> {
            schemaChangeJob.runWaitingTxnJob();
        });
        Assert.assertTrue(exception.getMessage().contains("Database does not exist"));
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());

        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().put(db.getId(), db);
        db.createTable(table);
        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assert.assertEquals(0, partition.getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testTableDroppedBeforeRewriting() throws AlterCancelException {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };

        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                             long timeoutSeconds) throws AlterCancelException {
                // nothing to do.
            }

            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                // nothing to do
            }

            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }

            @Mock
            public long getNextTransactionId() {
                return 10101L;
            }

            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
                return true;
            }
        };

        schemaChangeJob.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assert.assertEquals(10101L, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.runWaitingTxnJob();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        db.dropTable(table.getName());
        Exception exception = Assert.assertThrows(AlterCancelException.class, () -> {
            schemaChangeJob.runRunningJob();
        });
        Assert.assertTrue(exception.getMessage().contains("Table or database does not exist"));

        db.createTable(table);
        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().remove(db.getId());

        exception = Assert.assertThrows(AlterCancelException.class, () -> {
            schemaChangeJob.runRunningJob();
        });
        Assert.assertTrue(exception.getMessage().contains("Table or database does not exist"));

        GlobalStateMgr.getCurrentState().getLocalMetastore().getIdToDb().put(db.getId(), db);
        db.createTable(table);
        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assert.assertEquals(0, partition.getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testAlterTabletFailed() throws AlterCancelException {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };

        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                             long timeoutSeconds) throws AlterCancelException {
                // nothing to do.
            }

            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().stream().findFirst().get().failed();
                batchTask.getAllTasks().stream().findFirst().get().failed();
                batchTask.getAllTasks().stream().findFirst().get().failed();
            }

            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }

            @Mock
            public long getNextTransactionId() {
                return 10101L;
            }

            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
                return true;
            }
        };

        schemaChangeJob.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assert.assertEquals(10101L, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.runWaitingTxnJob();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        Exception exception = Assert.assertThrows(AlterCancelException.class, () -> {
            schemaChangeJob.runRunningJob();
        });
        Assert.assertTrue(exception.getMessage().contains("schema change task failed after try three times"));

        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());

        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, table.getState());

        Partition partition = table.getPartitions().stream().findFirst().get();
        Assert.assertEquals(0, partition.getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW).size());
    }

    @Test
    public void testAlterTabletSuccess() throws AlterCancelException {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }
        };

        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                             long timeoutSeconds) throws AlterCancelException {
                // nothing to do.
            }

            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }

            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }

            @Mock
            public long getNextTransactionId() {
                return 10101L;
            }

            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
                return true;
            }
        };

        schemaChangeJob.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assert.assertEquals(10101L, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.runWaitingTxnJob();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        schemaChangeJob.runRunningJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());
        Collection<Partition> partitions = table.getPartitions();
        Assert.assertEquals(1, partitions.size());
        Partition partition = partitions.stream().findFirst().orElse(null);
        Assert.assertNotNull(partition);
        Assert.assertEquals(3, partition.getNextVersion());
        List<MaterializedIndex> shadowIndexes = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        Assert.assertEquals(1, shadowIndexes.size());
        MaterializedIndex shadowIndex = shadowIndexes.get(0);
        Assert.assertEquals(shadowTabletIds, shadowIndex.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()));

        // Does not support cancel job in FINISHED_REWRITING state.
        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        // LakeTablet alter job will not mark tablet force delete into TabletInvertedIndex
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getForceDeleteTablets().isEmpty());

        // Drop the table, now it's ok to cancel the job
        db.dropTable(table.getName());
        schemaChangeJob.cancel("table does not exist anymore");
        Assert.assertEquals(AlterJobV2.JobState.CANCELLED, schemaChangeJob.getJobState());
    }

    @Test
    public void testPublishVersion() throws AlterCancelException {
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }

            @Mock
            public void publishVersion(@NotNull List<Tablet> tablets, long txnId, long baseVersion, long newVersion) throws
                    RpcException {
                throw new RpcException("publish version failed", "127.0.0.1");
            }
        };

        new MockUp<LakeTableSchemaChangeJob>() {
            @Mock
            public void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                             long timeoutSeconds) throws AlterCancelException {
                // nothing to do.
            }

            @Mock
            public void sendAgentTask(AgentBatchTask batchTask) {
                batchTask.getAllTasks().forEach(t -> t.setFinished(true));
            }

            @Mock
            public void writeEditLog(LakeTableSchemaChangeJob job) {
                // nothing to do.
            }

            @Mock
            public JournalTask writeEditLogAsync(LakeTableSchemaChangeJob job) {
                JournalTask journalTask = new JournalTask(System.nanoTime(), null, -1);
                journalTask.markSucceed();
                return journalTask;
            }

            @Mock
            public long getNextTransactionId() {
                return 10101L;
            }

            @Mock
            public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
                return true;
            }
        };

        schemaChangeJob.runPendingJob();
        Assert.assertEquals(AlterJobV2.JobState.WAITING_TXN, schemaChangeJob.getJobState());
        Assert.assertEquals(10101L, schemaChangeJob.getWatershedTxnId());

        schemaChangeJob.runWaitingTxnJob();
        Assert.assertEquals(AlterJobV2.JobState.RUNNING, schemaChangeJob.getJobState());

        Collection<Partition> partitions = table.getPartitions();
        Assert.assertEquals(1, partitions.size());
        Partition partition = partitions.stream().findFirst().orElse(null);
        Assert.assertNotNull(partition);

        Assert.assertEquals(1, partition.getVisibleVersion());
        Assert.assertEquals(2, partition.getNextVersion());
        // Disable send publish version
        partition.setNextVersion(3);

        schemaChangeJob.runRunningJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        List<MaterializedIndex> shadowIndexes = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        Assert.assertEquals(1, shadowIndexes.size());
        MaterializedIndex shadowIndex = shadowIndexes.get(0);
        Assert.assertEquals(shadowTabletIds, shadowIndex.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()));

        // The partition's visible version has not catch up with the commit version of this schema change job now.
        schemaChangeJob.runFinishedRewritingJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        // Reset partition's next version
        partition.setVisibleVersion(2, System.currentTimeMillis());

        // Drop table
        db.dropTable(table.getName());

        Exception exception = Assert.assertThrows(AlterCancelException.class, () -> {
            schemaChangeJob.runFinishedRewritingJob();
        });
        Assert.assertTrue(exception.getMessage().contains("Table does not exist"));
        Assert.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        // Add table back to database
        db.createTable(table);

        // We've mocked ColumnTypeConverter.publishVersion to throw RpcException, should this runFinishedRewritingJob will fail but
        // should not throw any exception.
        schemaChangeJob.runFinishedRewritingJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED_REWRITING, schemaChangeJob.getJobState());

        // Make publish version success
        new MockUp<Utils>() {
            @Mock
            public Long chooseBackend(LakeTablet tablet) {
                return 1L;
            }

            @Mock
            public void publishVersion(@NotNull List<Tablet> tablets, long txnId, long baseVersion, long newVersion) {
                // nothing to do
            }
        };

        new MockUp<ShardDeleter>() {
            @Mock
            public void dropTabletAndDeleteShard(List<Long> shardIds, StarOSAgent starOSAgent) {
                // nothing to do
            }
        };

        schemaChangeJob.runFinishedRewritingJob();
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, schemaChangeJob.getJobState());

        Assert.assertEquals(2, table.getBaseSchema().size());
        Assert.assertEquals("c0", table.getBaseSchema().get(0).getName());
        Assert.assertEquals("c1", table.getBaseSchema().get(1).getName());

        Assert.assertSame(partition, table.getPartitions().stream().findFirst().get());
        Assert.assertEquals(3, partition.getVisibleVersion());
        Assert.assertEquals(4, partition.getNextVersion());

        shadowIndexes = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.SHADOW);
        Assert.assertEquals(0, shadowIndexes.size());

        List<MaterializedIndex> normalIndexes = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE);
        Assert.assertEquals(1, normalIndexes.size());
        MaterializedIndex normalIndex = normalIndexes.get(0);
        Assert.assertEquals(shadowTabletIds, normalIndex.getTablets().stream().map(Tablet::getId).collect(Collectors.toList()));

        for (Long tabletId : shadowTabletIds) {
            TabletMeta tabletMeta = GlobalStateMgr.getCurrentState().getTabletInvertedIndex().getTabletMeta(tabletId);
            Assert.assertNotSame(NOT_EXIST_TABLET_META, tabletMeta);
            Assert.assertTrue(tabletMeta.isLakeTablet());
            Assert.assertEquals(db.getId(), tabletMeta.getDbId());
            Assert.assertEquals(table.getId(), tabletMeta.getTableId());
            Assert.assertEquals(partition.getId(), tabletMeta.getPartitionId());
            Assert.assertEquals(normalIndex.getId(), tabletMeta.getIndexId());
        }

        // Does not support cancel job in FINISHED state.
        schemaChangeJob.cancel("test");
        Assert.assertEquals(AlterJobV2.JobState.FINISHED, schemaChangeJob.getJobState());
    }
}
