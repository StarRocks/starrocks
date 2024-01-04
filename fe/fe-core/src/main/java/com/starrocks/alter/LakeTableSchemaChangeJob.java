// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.alter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.ShardDeleter;
import com.starrocks.lake.Utils;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.AlterReplicaTask;
import com.starrocks.task.CreateReplicaTask;
import com.starrocks.thrift.TStorageFormat;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.thrift.TTabletType;
import com.starrocks.thrift.TTaskType;
import com.starrocks.transaction.GlobalTransactionMgr;
import io.opentelemetry.api.trace.StatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class LakeTableSchemaChangeJob extends AlterJobV2 {
    private static final Logger LOG = LogManager.getLogger(LakeTableSchemaChangeJob.class);

    // partition id -> (shadow index id -> (shadow tablet id -> origin tablet id))
    @SerializedName(value = "partitionIndexTabletMap")
    private Table<Long, Long, Map<Long, Long>> partitionIndexTabletMap = HashBasedTable.create();
    // partition id -> (shadow index id -> shadow index))
    @SerializedName(value = "partitionIndexMap")
    private Table<Long, Long, MaterializedIndex> partitionIndexMap = HashBasedTable.create();
    // shadow index id -> origin index id
    @SerializedName(value = "indexIdMap")
    private Map<Long, Long> indexIdMap = Maps.newHashMap();
    // shadow index id -> shadow index name(__starrocks_shadow_xxx)
    @SerializedName(value = "indexIdToName")
    private Map<Long, String> indexIdToName = Maps.newHashMap();
    // shadow index id -> index schema
    @SerializedName(value = "indexSchemaMap")
    private Map<Long, List<Column>> indexSchemaMap = Maps.newHashMap();
    // shadow index id -> shadow index short key count
    @SerializedName(value = "indexShortKeyMap")
    private Map<Long, Short> indexShortKeyMap = Maps.newHashMap();

    // bloom filter info
    @SerializedName(value = "hasBfChange")
    private boolean hasBfChange;
    @SerializedName(value = "bfColumns")
    private Set<String> bfColumns = null;
    @SerializedName(value = "bfFpp")
    private double bfFpp = 0;

    // alter index info
    @SerializedName(value = "indexChange")
    private boolean indexChange = false;
    @SerializedName(value = "indexes")
    private List<Index> indexes = null;

    // The schema change job will wait all transactions before this txn id finished, then send the schema change tasks.
    @SerializedName(value = "watershedTxnId")
    protected long watershedTxnId = -1;
    @SerializedName(value = "storageFormat")
    private TStorageFormat storageFormat = TStorageFormat.DEFAULT;
    @SerializedName(value = "startTime")
    private long startTime;

    @SerializedName(value = "commitVersionMap")
    // Mapping from partition id to commit version
    private Map<Long, Long> commitVersionMap;

    // save all schema change tasks
    private AgentBatchTask schemaChangeBatchTask = new AgentBatchTask();

    public LakeTableSchemaChangeJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
    }

    void setBloomFilterInfo(boolean hasBfChange, Set<String> bfColumns, double bfFpp) {
        this.hasBfChange = hasBfChange;
        this.bfColumns = bfColumns;
        this.bfFpp = bfFpp;
    }

    void setAlterIndexInfo(boolean indexChange, List<Index> indexes) {
        this.indexChange = indexChange;
        this.indexes = indexes;
    }

    void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    void setStorageFormat(TStorageFormat storageFormat) {
        this.storageFormat = storageFormat;
    }

    void addTabletIdMap(long partitionId, long shadowIdxId, long shadowTabletId, long originTabletId) {
        Map<Long, Long> tabletMap = partitionIndexTabletMap.get(partitionId, shadowIdxId);
        if (tabletMap == null) {
            tabletMap = Maps.newHashMap();
            partitionIndexTabletMap.put(partitionId, shadowIdxId, tabletMap);
        }
        tabletMap.put(shadowTabletId, originTabletId);
    }

    void addPartitionShadowIndex(long partitionId, long shadowIdxId, MaterializedIndex shadowIdx) {
        partitionIndexMap.put(partitionId, shadowIdxId, shadowIdx);
    }

    void addIndexSchema(long shadowIdxId, long originIdxId, @NotNull String shadowIndexName, short shadowIdxShortKeyCount,
                        @NotNull List<Column> shadowIdxSchema) {
        indexIdMap.put(shadowIdxId, originIdxId);
        indexIdToName.put(shadowIdxId, shadowIndexName);
        indexShortKeyMap.put(shadowIdxId, shadowIdxShortKeyCount);
        indexSchemaMap.put(shadowIdxId, shadowIdxSchema);
    }

    // REQUIRE: has acquired the exclusive lock of database
    void addShadowIndexToCatalog(@NotNull LakeTable table) {
        Preconditions.checkState(watershedTxnId != -1);
        for (long partitionId : partitionIndexMap.rowKeySet()) {
            Partition partition = table.getPartition(partitionId);
            Preconditions.checkState(partition != null);
            Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
            for (MaterializedIndex shadowIndex : shadowIndexMap.values()) {
                shadowIndex.setVisibleTxnId(watershedTxnId);
                Preconditions.checkState(shadowIndex.getState() == MaterializedIndex.IndexState.SHADOW, shadowIndex.getState());
                partition.createRollupIndex(shadowIndex);
            }
        }

        for (long shadowIdxId : indexIdMap.keySet()) {
            long orgIndexId = indexIdMap.get(shadowIdxId);
            table.setIndexMeta(shadowIdxId, indexIdToName.get(shadowIdxId), indexSchemaMap.get(shadowIdxId), 0, 0,
                    indexShortKeyMap.get(shadowIdxId), TStorageType.COLUMN,
                    table.getKeysTypeByIndexId(indexIdMap.get(shadowIdxId)));
            MaterializedIndexMeta orgIndexMeta = table.getIndexMetaByIndexId(orgIndexId);
            Preconditions.checkNotNull(orgIndexMeta);
            MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(shadowIdxId);
            Preconditions.checkNotNull(indexMeta);
            rebuildMaterializedIndexMeta(orgIndexMeta, indexMeta);
        }

        table.rebuildFullSchema();
    }

    @VisibleForTesting
    public long getWatershedTxnId() {
        return watershedTxnId;
    }

    @VisibleForTesting
    public static void sendAgentTask(AgentBatchTask batchTask) {
        AgentTaskQueue.addBatchTask(batchTask);
        AgentTaskExecutor.submit(batchTask);
    }

    @VisibleForTesting
    public static void sendAgentTaskAndWait(AgentBatchTask batchTask, MarkedCountDownLatch<Long, Long> countDownLatch,
                                            long timeoutSeconds) throws AlterCancelException {
        AgentTaskQueue.addBatchTask(batchTask);
        AgentTaskExecutor.submit(batchTask);
        long timeout = 1000L * Math.min(timeoutSeconds, Config.max_create_table_timeout_second);
        boolean ok = false;
        try {
            ok = countDownLatch.await(timeout, TimeUnit.MILLISECONDS) && countDownLatch.getStatus().ok();
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException: ", e);
        }

        if (!ok) {
            AgentTaskQueue.removeBatchTask(batchTask, TTaskType.CREATE);
            String errMsg;
            if (!countDownLatch.getStatus().ok()) {
                errMsg = countDownLatch.getStatus().getErrorMsg();
            } else {
                // only show at most 3 results
                List<Map.Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                List<Map.Entry<Long, Long>> subList = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 3));
                errMsg = "Error tablets:" + Joiner.on(", ").join(subList);
            }
            throw new AlterCancelException("Create tablet failed. Error: " + errMsg);
        }
    }

    @VisibleForTesting
    public static void writeEditLog(LakeTableSchemaChangeJob job) {
        GlobalStateMgr.getCurrentState().getEditLog().logAlterJob(job);
    }

    @VisibleForTesting
    public static long getNextTransactionId() {
        return GlobalStateMgr.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId();
    }

    @Override
    protected void runPendingJob() throws AlterCancelException {
        long numTablets = 0;
        AgentBatchTask batchTask = new AgentBatchTask();
        MarkedCountDownLatch<Long, Long> countDownLatch;
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            LakeTable table = getTableOrThrow(db, tableId);
            Preconditions.checkState(table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE);
            MaterializedIndexMeta indexMeta = table.getIndexMetaByIndexId(table.getBaseIndexId());
            numTablets = partitionIndexMap.values().stream().map(MaterializedIndex::getTablets).count();
            countDownLatch = new MarkedCountDownLatch<>((int) numTablets);

            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = table.getPartition(partitionId);
                Preconditions.checkState(partition != null);
                TStorageMedium storageMedium = table.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();

                Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();

                    short shadowShortKeyColumnCount = indexShortKeyMap.get(shadowIdxId);
                    List<Column> shadowSchema = indexSchemaMap.get(shadowIdxId);
                    long originIndexId = indexIdMap.get(shadowIdxId);
                    KeysType originKeysType = table.getKeysTypeByIndexId(originIndexId);

                    // copy for generate some const default value
                    List<Column> copiedShadowSchema = Lists.newArrayList();
                    for (Column column : shadowSchema) {
                        Column.DefaultValueType defaultValueType = column.getDefaultValueType();
                        if (defaultValueType == Column.DefaultValueType.CONST) {
                            Column copiedColumn = new Column(column);
                            copiedColumn.setDefaultValue(column.calculatedDefaultValueWithTime(startTime));
                            copiedShadowSchema.add(copiedColumn);
                        } else {
                            copiedShadowSchema.add(column);
                        }
                    }

                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        long shadowTabletId = shadowTablet.getId();
                        LakeTablet lakeTablet = ((LakeTablet) shadowTablet);
                        Long backendId = Utils.chooseBackend(lakeTablet);
                        if (backendId == null) {
                            throw new AlterCancelException("No alive backend");
                        }
                        countDownLatch.addMark(backendId, shadowTabletId);
                        CreateReplicaTask createReplicaTask = new CreateReplicaTask(backendId, dbId, tableId, partitionId,
                                shadowIdxId, shadowTabletId, shadowShortKeyColumnCount, 0, Partition.PARTITION_INIT_VERSION,
                                originKeysType, TStorageType.COLUMN, storageMedium, copiedShadowSchema, bfColumns, bfFpp,
                                countDownLatch, indexes, table.isInMemory(), table.enablePersistentIndex(),
                                TTabletType.TABLET_TYPE_LAKE, table.getCompressionType(), indexMeta.getSortKeyIdxes());

                        Long baseTabletId = partitionIndexTabletMap.row(partitionId).get(shadowIdxId).get(shadowTabletId);
                        assert baseTabletId != null;
                        createReplicaTask.setBaseTablet(baseTabletId, 0/*unused*/);
                        createReplicaTask.setStorageFormat(this.storageFormat);
                        batchTask.addTask(createReplicaTask);
                    }
                }
            }
        } catch (Exception e) {
            throw new AlterCancelException(e.getMessage());
        }

        sendAgentTaskAndWait(batchTask, countDownLatch, Config.tablet_create_timeout_second * numTablets);

        // Add shadow indexes to table.
        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            LakeTable table = getTableOrThrow(db, tableId);
            Preconditions.checkState(table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE);
            watershedTxnId = getNextTransactionId();
            addShadowIndexToCatalog(table);
        }

        jobState = JobState.WAITING_TXN;
        if (span != null) {
            span.setAttribute("watershedTxnId", this.watershedTxnId);
            span.addEvent("setWaitingTxn");
        }

        writeEditLog(this);

        LOG.info("transfer schema change job {} state to {}, watershed txn_id: {}", jobId, this.jobState, watershedTxnId);
    }

    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.WAITING_TXN, jobState);

        try {
            if (!isPreviousLoadFinished(dbId, tableId, watershedTxnId)) {
                LOG.info("wait transactions before {} to be finished, schema change job: {}", watershedTxnId, jobId);
                return;
            }
        } catch (AnalysisException e) {
            throw new AlterCancelException(e.getMessage());
        }

        LOG.info("previous transactions are all finished, begin to send schema change tasks. job: {}", jobId);

        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            LakeTable table = getTableOrThrow(db, tableId);
            Preconditions.checkState(table.getState() == OlapTable.OlapTableState.SCHEMA_CHANGE);
            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = table.getPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);

                // the schema change task will transform the data before visible version(included).
                long visibleVersion = partition.getVisibleVersion();

                Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                for (Map.Entry<Long, MaterializedIndex> entry : shadowIndexMap.entrySet()) {
                    long shadowIdxId = entry.getKey();
                    MaterializedIndex shadowIdx = entry.getValue();
                    for (Tablet shadowTablet : shadowIdx.getTablets()) {
                        Long backendId = Utils.chooseBackend((LakeTablet) shadowTablet);
                        if (backendId == null) {
                            throw new AlterCancelException("No alive backend");
                        }
                        long shadowTabletId = shadowTablet.getId();
                        long originTabletId = partitionIndexTabletMap.row(partitionId).get(shadowIdxId).get(shadowTabletId);
                        AlterReplicaTask alterTask = AlterReplicaTask.alterLakeTablet(backendId, dbId, tableId, partitionId,
                                shadowIdxId, shadowTabletId, originTabletId, visibleVersion, jobId, watershedTxnId);
                        getOrCreateSchemaChangeBatchTask().addTask(alterTask);
                    }
                }
            } // end for partitions
        }

        sendAgentTask(getOrCreateSchemaChangeBatchTask());

        this.jobState = JobState.RUNNING;
        if (span != null) {
            span.addEvent("setRunning");
        }

        // DO NOT write edit log here, will send AlterReplicaTask again if FE restarted or master changed.
        LOG.info("transfer schema change job {} state to {}", jobId, this.jobState);
    }

    @Override
    protected void runRunningJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.RUNNING, jobState);

        // must check if db or table still exist first.
        // or if table is dropped, the tasks will never be finished,
        // and the job will be in RUNNING state forever.
        if (tableHasBeenDropped()) {
            throw new AlterCancelException("Table or database does not exist");
        }

        if (!getOrCreateSchemaChangeBatchTask().isFinished()) {
            LOG.info("schema change tasks not finished. job: {}", jobId);
            List<AgentTask> tasks = getOrCreateSchemaChangeBatchTask().getUnfinishedTasks(2000);
            AgentTask task = tasks.stream().filter(t -> t.getFailedTimes() >= 3).findAny().orElse(null);
            if (task != null) {
                throw new AlterCancelException("schema change task failed after try three times: " + task.getErrorMsg());
            } else {
                return;
            }
        }

        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            LakeTable table = getTableOrThrow(db, tableId);
            commitVersionMap = new HashMap<>();
            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = table.getPartition(partitionId);
                Preconditions.checkNotNull(partition, partitionId);
                long commitVersion = partition.getNextVersion();
                commitVersionMap.put(partitionId, commitVersion);
                LOG.debug("commit version of partition {} is {}. jobId={}", partitionId, commitVersion, jobId);
            }
            this.jobState = JobState.FINISHED_REWRITING;

            writeEditLog(this);

            // NOTE: !!! below this point, this schema change job must success unless the database or table been dropped. !!!
            updateNextVersion(table);
        }

        if (span != null) {
            span.addEvent("finishedRewriting");
        }
        LOG.info("schema change job finished rewriting historical data: {}", jobId);
    }

    // Note: The only allowed situation to cancel the schema change job is the table has been dropped.
    @Override
    protected void runFinishedRewritingJob() throws AlterCancelException {
        Preconditions.checkState(jobState == JobState.FINISHED_REWRITING);
        // If the table or database has been dropped, `readyToPublishVersion()` will throw AlterCancelException and
        // this schema change job will be cancelled.
        if (!readyToPublishVersion()) {
            return;
        }

        if (!publishVersion()) {
            LOG.info("publish version failed, will retry later. jobId={}", jobId);
            return;
        }

        this.jobState = JobState.FINISHED;
        this.finishedTimeMs = System.currentTimeMillis();

        writeEditLog(this);

        // Replace the current index with shadow index.
        List<MaterializedIndex> droppedIndexes;
        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            LakeTable table = (db != null) ? db.getTable(tableId) : null;
            if (table == null) {
                LOG.info("database or table been dropped while doing schema change job {}", jobId);
                return;
            }
            // Below this point, all query and load jobs will use the new schema.
            droppedIndexes = visualiseShadowIndex(table);
        }

        // Delete shards from StarOS
        ShardDeleter shardDeleter = GlobalStateMgr.getCurrentState().getShardManager().getShardDeleter();
        for (MaterializedIndex droppedIndex : droppedIndexes) {
            Set<Long> shards = droppedIndex.getTablets().stream().map(Tablet::getId).collect(Collectors.toSet());
            shardDeleter.addUnusedShardId(shards);
        }

        if (span != null) {
            span.end();
        }
        LOG.info("schema change job finished: {}", jobId);
    }

    // Note: throws AlterCancelException iff the database or table has been dropped.
    boolean readyToPublishVersion() throws AlterCancelException {
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            LakeTable table = getTableOrThrow(db, tableId);
            for (long partitionId : partitionIndexMap.rowKeySet()) {
                Partition partition = table.getPartition(partitionId);
                Preconditions.checkState(partition != null, partitionId);
                long commitVersion = commitVersionMap.get(partitionId);
                if (commitVersion != partition.getVisibleVersion() + 1) {
                    Preconditions.checkState(partition.getVisibleVersion() < commitVersion,
                            "partition=" + partitionId + " visibleVersion=" + partition.getVisibleVersion() +
                                    " commitVersion=" + commitVersion);
                    return false;
                }
            }
        }
        return true;
    }

    boolean publishVersion() {
        try {
            for (long partitionId : partitionIndexMap.rowKeySet()) {
                long commitVersion = commitVersionMap.get(partitionId);
                Map<Long, MaterializedIndex> shadowIndexMap = partitionIndexMap.row(partitionId);
                for (MaterializedIndex shadowIndex : shadowIndexMap.values()) {
                    Utils.publishVersion(shadowIndex.getTablets(), watershedTxnId, 1, commitVersion);
                }
            }
            return true;
        } catch (Exception e) {
            LOG.error("Fail to publish version for schema change job {}: {}", jobId, e.getMessage());
            return false;
        }
    }

<<<<<<< HEAD
=======
    private Set<String> collectModifiedColumnsForRelatedMVs(@NotNull LakeTable tbl) {
        if (tbl.getRelatedMaterializedViews().isEmpty()) {
            return Sets.newHashSet();
        }
        Set<String> modifiedColumns = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

        for (Map.Entry<Long, List<Column>> entry : indexSchemaMap.entrySet()) {
            Long shadowIdxId = entry.getKey();
            long originIndexId = indexIdMap.get(shadowIdxId);
            List<Column> shadowSchema = entry.getValue();
            List<Column> originSchema = tbl.getSchemaByIndexId(originIndexId);
            if (shadowSchema.size() == originSchema.size()) {
                // modify column
                for (Column col : shadowSchema) {
                    if (col.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX)) {
                        modifiedColumns.add(col.getNameWithoutPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX));
                    }
                }
            } else if (shadowSchema.size() < originSchema.size()) {
                // drop column
                List<Column> differences = originSchema.stream().filter(element ->
                        !shadowSchema.contains(element)).collect(Collectors.toList());
                // can just drop one column one time, so just one element in differences
                Integer dropIdx = new Integer(originSchema.indexOf(differences.get(0)));
                modifiedColumns.add(originSchema.get(dropIdx).getName());
            } else {
                // add column should not affect old mv, just ignore.
            }
        }
        return modifiedColumns;
    }

    private void inactiveRelatedMv(Set<String> modifiedColumns, @NotNull LakeTable tbl) {
        if (modifiedColumns.isEmpty()) {
            return;
        }
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        for (MvId mvId : tbl.getRelatedMaterializedViews()) {
            MaterializedView mv = (MaterializedView) db.getTable(mvId.getId());
            if (mv == null) {
                LOG.warn("Ignore materialized view {} does not exists", mvId);
                continue;
            }
            for (Column mvColumn : mv.getColumns()) {
                if (modifiedColumns.contains(mvColumn.getName())) {
                    LOG.warn("Setting the materialized view {}({}) to invalid because " +
                                    "the column {} of the table {} was modified.", mv.getName(), mv.getId(),
                            mvColumn.getName(), tbl.getName());
                    mv.setInactiveAndReason(
                            "base table schema changed for columns: " + StringUtils.join(modifiedColumns, ","));
                    return;
                }
            }
        }
    }

>>>>>>> 8fd6a085bf ([BugFix] Add more checks when schema changing has referred materialized views (backport #37388) (#38436))
    boolean tableHasBeenDropped() {
        try (ReadLockedDatabase db = getReadLockedDatabase(dbId)) {
            return db == null || db.getTable(tableId) == null;
        }
    }

    // Update each Partition's nextVersion.
    // The caller must have acquired the database's exclusive lock.
    void updateNextVersion(@NotNull LakeTable table) {
        for (long partitionId : partitionIndexMap.rowKeySet()) {
            Partition partition = table.getPartition(partitionId);
            long commitVersion = commitVersionMap.get(partitionId);
            Preconditions.checkState(partition.getNextVersion() == commitVersion,
                    "partitionNextVersion=" + partition.getNextVersion() + " commitVersion=" + commitVersion);
            partition.setNextVersion(commitVersion + 1);
        }
    }

    @NotNull
    LakeTable getTableOrThrow(@Nullable LockedDatabase db, long tableId) throws AlterCancelException {
        if (db == null) {
            throw new AlterCancelException("Database does not exist");
        }
        LakeTable table = db.getTable(tableId);
        if (table == null) {
            throw new AlterCancelException("Table does not exist. tableId=" + tableId);
        }
        return table;
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        LakeTableSchemaChangeJob other = (LakeTableSchemaChangeJob) replayedJob;

        LOG.info("Replaying lake table schema change job. state={} jobId={}", replayedJob.jobState, replayedJob.jobId);

        if (this != other) {
            Preconditions.checkState(this.type.equals(other.type));
            Preconditions.checkState(this.jobId == other.jobId);
            Preconditions.checkState(this.dbId == other.dbId);
            Preconditions.checkState(this.tableId == other.tableId);

            this.jobState = other.jobState;
            this.createTimeMs = other.createTimeMs;
            this.finishedTimeMs = other.finishedTimeMs;
            this.errMsg = other.errMsg;
            this.timeoutMs = other.timeoutMs;

            this.partitionIndexTabletMap = other.partitionIndexTabletMap;
            this.partitionIndexMap = other.partitionIndexMap;
            this.indexIdMap = other.indexIdMap;
            this.indexIdToName = other.indexIdToName;
            this.indexSchemaMap = other.indexSchemaMap;
            this.indexShortKeyMap = other.indexShortKeyMap;
            this.hasBfChange = other.hasBfChange;
            this.bfColumns = other.bfColumns;
            this.bfFpp = other.bfFpp;
            this.indexChange = other.indexChange;
            this.indexes = other.indexes;
            this.watershedTxnId = other.watershedTxnId;
            this.storageFormat = other.storageFormat;
            this.startTime = other.startTime;
            this.commitVersionMap = other.commitVersionMap;
            // this.schemaChangeBatchTask = other.schemaChangeBatchTask;
        }

        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            LakeTable table = (db != null) ? db.getTable(tableId) : null;
            if (table == null) {
                return; // do nothing if the table has been dropped.
            }

            if (jobState == JobState.PENDING) {
                addTabletToTabletInvertedIndex(table);
                table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
            } else if (jobState == JobState.WAITING_TXN) {
                addShadowIndexToCatalog(table);
            } else if (jobState == JobState.FINISHED_REWRITING) {
                updateNextVersion(table);
            } else if (jobState == JobState.FINISHED) {
                visualiseShadowIndex(table);
            } else if (jobState == JobState.CANCELLED) {
                removeShadowIndex(table);
            } else {
                throw new RuntimeException("unknown job state '{}'" + jobState.name());
            }
        }
    }

    void addTabletToTabletInvertedIndex(@NotNull LakeTable table) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (Table.Cell<Long, Long, MaterializedIndex> cell : partitionIndexMap.cellSet()) {
            Long partitionId = cell.getRowKey();
            Long shadowIndexId = cell.getColumnKey();
            MaterializedIndex shadowIndex = cell.getValue();
            assert partitionId != null;
            assert shadowIndexId != null;
            assert shadowIndex != null;
            TStorageMedium medium = table.getPartitionInfo().getDataProperty(partitionId).getStorageMedium();
            TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, partitionId, shadowIndexId, 0, medium, true);
            for (Tablet shadowTablet : shadowIndex.getTablets()) {
                invertedIndex.addTablet(shadowTablet.getId(), shadowTabletMeta);
            }
        }
    }

    void removeShadowIndex(@NotNull LakeTable table) {
        for (long partitionId : partitionIndexMap.rowKeySet()) {
            Partition partition = table.getPartition(partitionId);
            Preconditions.checkNotNull(partition, partitionId);
            for (MaterializedIndex shadowIdx : partitionIndexMap.row(partitionId).values()) {
                partition.deleteRollupIndex(shadowIdx.getId());
            }
        }
        for (String shadowIndexName : indexIdToName.values()) {
            table.deleteIndexInfo(shadowIndexName);
        }
        // Delete tablet from TabletInvertedIndex
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();
        for (long partitionId : partitionIndexMap.rowKeySet()) {
            for (MaterializedIndex shadowIdx : partitionIndexMap.row(partitionId).values()) {
                for (Tablet tablet : shadowIdx.getTablets()) {
                    invertedIndex.deleteTablet(tablet.getId());
                }
            }
        }
        table.setState(OlapTable.OlapTableState.NORMAL);
    }

    @NotNull
    List<MaterializedIndex> visualiseShadowIndex(@NotNull LakeTable table) {
        List<MaterializedIndex> droppedIndexes = new ArrayList<>();
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentInvertedIndex();

        for (Column column : table.getColumns()) {
            if (Type.VARCHAR.equals(column.getType())) {
                IDictManager.getInstance().removeGlobalDict(table.getId(), column.getName());
            }
        }

        // replace the origin index with shadow index, set index state as NORMAL
        for (Partition partition : table.getPartitions()) {
            Preconditions.checkState(commitVersionMap.containsKey(partition.getId()));
            long commitVersion = commitVersionMap.get(partition.getId());

            LOG.debug("update partition visible version. partition=" + partition.getId() + " commitVersion=" + commitVersion);
            // Update Partition's visible version
            Preconditions.checkState(commitVersion == partition.getVisibleVersion() + 1,
                    commitVersion + " vs " + partition.getVisibleVersion());
            partition.setVisibleVersion(commitVersion, finishedTimeMs);
            LOG.debug("update visible version of partition {} to {}. jobId={}", partition.getId(),
                    commitVersion, jobId);
            TStorageMedium medium = table.getPartitionInfo().getDataProperty(partition.getId()).getStorageMedium();
            // drop the origin index from partitions
            for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
                long shadowIdxId = entry.getKey();
                long originIdxId = entry.getValue();
                // get index from globalStateMgr, not from 'partitionIdToRollupIndex'.
                // because if this alter job is recovered from edit log, index in 'partitionIndexMap'
                // is not the same object in globalStateMgr. So modification on that index can not reflect to the index
                // in globalStateMgr.
                MaterializedIndex shadowIdx = partition.getIndex(shadowIdxId);
                Preconditions.checkNotNull(shadowIdx, shadowIdxId);
                MaterializedIndex droppedIdx;
                if (originIdxId == partition.getBaseIndex().getId()) {
                    droppedIdx = partition.getBaseIndex();
                } else {
                    droppedIdx = partition.deleteRollupIndex(originIdxId);
                }
                Preconditions.checkNotNull(droppedIdx, originIdxId + " vs. " + shadowIdxId);

                // Add Tablet to TabletInvertedIndex.
                TabletMeta shadowTabletMeta = new TabletMeta(dbId, tableId, partition.getId(), shadowIdxId, 0, medium, true);
                for (Tablet tablet : shadowIdx.getTablets()) {
                    invertedIndex.addTablet(tablet.getId(), shadowTabletMeta);
                }

                partition.visualiseShadowIndex(shadowIdxId, originIdxId == partition.getBaseIndex().getId());

                // the origin tablet created by old schema can be deleted from FE metadata
                for (Tablet originTablet : droppedIdx.getTablets()) {
                    invertedIndex.deleteTablet(originTablet.getId());
                }

                droppedIndexes.add(droppedIdx);
            }
        }

        // update index schema info of each index
        for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
            long shadowIdxId = entry.getKey();
            long originIdxId = entry.getValue();
            String shadowIdxName = table.getIndexNameById(shadowIdxId);
            String originIdxName = table.getIndexNameById(originIdxId);
            table.deleteIndexInfo(originIdxName);
            // the shadow index name is '__starrocks_shadow_xxx', rename it to origin name 'xxx'
            // this will also remove the prefix of columns
            table.renameIndexForSchemaChange(shadowIdxName, originIdxName);
            table.renameColumnNamePrefix(shadowIdxId);

            if (originIdxId == table.getBaseIndexId()) {
                table.setBaseIndexId(shadowIdxId);
            }
        }
        // rebuild table's full schema
        table.rebuildFullSchema();

        // update bloom filter
        if (hasBfChange) {
            table.setBloomFilterInfo(bfColumns, bfFpp);
        }
        // update index
        if (indexChange) {
            table.setIndexes(indexes);
        }

        // set storage format of table, only set if format is v2
        if (storageFormat == TStorageFormat.V2) {
            table.setStorageFormat(storageFormat);
        }

        table.setState(OlapTable.OlapTableState.NORMAL);

        return droppedIndexes;
    }

    @Override
    protected boolean cancelImpl(String errMsg) {
        if (jobState == JobState.CANCELLED || jobState == JobState.FINISHED_REWRITING || jobState == JobState.FINISHED) {
            return false;
        }

        if (schemaChangeBatchTask != null) {
            AgentTaskQueue.removeBatchTask(schemaChangeBatchTask, TTaskType.ALTER);
        }

        try (WriteLockedDatabase db = getWriteLockedDatabase(dbId)) {
            LakeTable table = (db != null) ? db.getTable(tableId) : null;
            if (table != null) {
                removeShadowIndex(table);
            }
        }

        this.jobState = JobState.CANCELLED;
        this.errMsg = errMsg;
        this.finishedTimeMs = System.currentTimeMillis();
        if (span != null) {
            span.setStatus(StatusCode.ERROR, errMsg);
            span.end();
        }

        writeEditLog(this);

        return true;
    }

    AgentBatchTask getOrCreateSchemaChangeBatchTask() {
        if (schemaChangeBatchTask == null) { // This would happen after FE restarted and this object was deserialized from Json.
            schemaChangeBatchTask = new AgentBatchTask();
        }
        return schemaChangeBatchTask;
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        // calc progress first. all index share the same process
        String progress = FeConstants.null_string;
        if (jobState == JobState.RUNNING && schemaChangeBatchTask.getTaskNum() > 0) {
            progress = schemaChangeBatchTask.getFinishedTaskNum() + "/" + schemaChangeBatchTask.getTaskNum();
        }

        // one line for one shadow index
        for (Map.Entry<Long, Long> entry : indexIdMap.entrySet()) {
            long shadowIndexId = entry.getKey();
            List<Comparable> info = Lists.newArrayList();
            info.add(jobId);
            info.add(tableName);
            info.add(TimeUtils.longToTimeString(createTimeMs));
            info.add(TimeUtils.longToTimeString(finishedTimeMs));
            // only show the origin index name
            info.add(Column.removeNamePrefix(indexIdToName.get(shadowIndexId)));
            info.add(shadowIndexId);
            info.add(entry.getValue());
            info.add("0:0"); // schema version and schema hash
            info.add(watershedTxnId);
            info.add(jobState.name());
            info.add(errMsg);
            info.add(progress);
            info.add(timeoutMs / 1000);
            infos.add(info);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this, AlterJobV2.class);
        Text.writeString(out, json);
    }

    @Nullable
    ReadLockedDatabase getReadLockedDatabase(long dbId) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        return db != null ? new ReadLockedDatabase(db) : null;
    }

    @Nullable
    WriteLockedDatabase getWriteLockedDatabase(long dbId) {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        return db != null ? new WriteLockedDatabase(db) : null;
    }

    // Check whether transactions of the given database which txnId is less than 'watershedTxnId' are finished.
    @VisibleForTesting
    public boolean isPreviousLoadFinished(long dbId, long tableId, long txnId) throws AnalysisException {
        GlobalTransactionMgr globalTxnMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        return globalTxnMgr.isPreviousTransactionsFinished(txnId, dbId, Lists.newArrayList(tableId));
    }

    private abstract static class LockedDatabase implements AutoCloseable {
        private final Database db;

        LockedDatabase(@NotNull Database db) {
            lock(db);
            this.db = db;
        }

        abstract void lock(Database db);

        abstract void unlock(Database db);

        @Nullable
        LakeTable getTable(long tableId) {
            return (LakeTable) db.getTable(tableId);
        }

        @Override
        public void close() {
            unlock(db);
        }
    }

    private static class ReadLockedDatabase extends LockedDatabase {
        ReadLockedDatabase(@NotNull Database db) {
            super(db);
        }

        @Override
        void lock(Database db) {
            db.readLock();
        }

        @Override
        void unlock(Database db) {
            db.readUnlock();
        }
    }

    private static class WriteLockedDatabase extends LockedDatabase {
        WriteLockedDatabase(@NotNull Database db) {
            super(db);
        }

        @Override
        void lock(Database db) {
            db.writeLock();
        }

        @Override
        void unlock(Database db) {
            db.writeUnlock();
        }
    }

    @Override
    public Optional<Long> getTransactionId() {
        return watershedTxnId < 0 ? Optional.empty() : Optional.of(watershedTxnId);
    }
}
