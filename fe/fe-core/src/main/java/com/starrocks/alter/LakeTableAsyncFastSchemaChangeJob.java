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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.lake.LakeTable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.task.TabletMetadataUpdateAgentTask;
import com.starrocks.task.TabletMetadataUpdateAgentTaskFactory;
import com.starrocks.warehouse.Warehouse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * The overall workflow to modify the table schema is as follows:
 * 1. Begin a new transaction.
 * 2. Send {@link com.starrocks.thrift.TUpdateTabletMetaInfoReq} requests to all tablet in the table.
 * 3. Get the new tablet schema from the TUpdateTabletMetaInfoReq and writes it to the txn log for each tablet.
 * 4. Commit transaction.
 * 5. Send {@link com.starrocks.proto.PublishVersionRequest} requests to all tablets.
 * 6. Apply txn log on each tablet to create a new version of the tablet metadata with the new tablet schema.
 * 7. Modify the table schema and visible version in the FE catalog
 * 8. Finish the transaction
 */
public class LakeTableAsyncFastSchemaChangeJob extends LakeTableAlterMetaJobBase implements GsonPostProcessable {

    private static final Logger LOG = LoggerFactory.getLogger(LakeTableAsyncFastSchemaChangeJob.class);

    // shadow index id -> index schema
    @SerializedName(value = "schemaInfos")
    private List<IndexSchemaInfo> schemaInfos;

    /**
     * Whether this job is used to disable fast schema evolution v2. When this flag is true,
     * this job will not perform a regular schema change, but will instead update tablet metadata
     * to the latest schema version, and set the table property to false.
     */
    @SerializedName(value = "disableFseV2")
    private boolean disableFastSchemaEvolutionV2 = false;

    @SerializedName(value = "historySchema")
    private OlapTableHistorySchema historySchema;

    /**
     * Per-tablet TARGET (N+1) range for an arity-changing trailing sort-key ADD on a
     * range-distribution table. Built once (before the first job WAL) and used for BE task
     * construction, the leader catalog flip, and replay. When null, this is an ordinary fast
     * schema-evolution job and every range-specific behavior is skipped.
     */
    @SerializedName(value = "targetRanges")
    private Map<Long, TabletRange> targetRanges;

    private Set<String> partitionsWithSchemaFile = new HashSet<>();

    // for deserialization
    public LakeTableAsyncFastSchemaChangeJob() {
        super(JobType.SCHEMA_CHANGE);
    }

    LakeTableAsyncFastSchemaChangeJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
        schemaInfos = new ArrayList<>();
    }

    LakeTableAsyncFastSchemaChangeJob(LakeTableAsyncFastSchemaChangeJob other) {
        this(other.getJobId(), other.getDbId(), other.getTableId(), other.getTableName(), other.getTimeoutMs());
        for (IndexSchemaInfo indexSchemaInfo : other.schemaInfos) {
            setIndexTabletSchema(indexSchemaInfo.getIndexMetaId(), indexSchemaInfo.getIndexName(),
                    indexSchemaInfo.getSchemaInfo());
        }
        this.disableFastSchemaEvolutionV2 = other.disableFastSchemaEvolutionV2;
        this.historySchema = other.historySchema;
        this.targetRanges = other.targetRanges == null ? null : new HashMap<>(other.targetRanges);
        partitionsWithSchemaFile.addAll(other.partitionsWithSchemaFile);
    }

    private LakeTableAsyncFastSchemaChangeJob(LakeTableAsyncFastSchemaChangeJob job, boolean copyForPersist) {
        super(job);
        this.schemaInfos = job.schemaInfos == null ? null : new ArrayList<>(job.schemaInfos);
        this.disableFastSchemaEvolutionV2 = job.disableFastSchemaEvolutionV2;
        this.historySchema = job.historySchema;
        this.targetRanges = job.targetRanges == null ? null : new HashMap<>(job.targetRanges);
    }

    public void setIndexTabletSchema(long indexMetaId, String indexName, SchemaInfo schemaInfo) {
        schemaInfos.add(new IndexSchemaInfo(indexMetaId, indexName, schemaInfo));
    }

    /**
     * Installs the per-tablet target range map. Package-private so the schema-change routing (and
     * tests) can set it when constructing the job for a metadata-only trailing sort-key ADD.
     */
    void setTargetRanges(Map<Long, TabletRange> targetRanges) {
        if (targetRanges == null) {
            this.targetRanges = null;
            return;
        }
        // Defensively copy and freeze so the must-not-fail catalog callback cannot observe a
        // caller-mutated map or a null key/value (which would throw mid-flip after the FINISHED
        // record is journaled).
        Map<Long, TabletRange> copy = new HashMap<>();
        for (Map.Entry<Long, TabletRange> entry : targetRanges.entrySet()) {
            Preconditions.checkArgument(entry.getKey() != null && entry.getValue() != null,
                    "target range map must not contain null keys or values");
            copy.put(entry.getKey(), entry.getValue());
        }
        this.targetRanges = Collections.unmodifiableMap(copy);
    }

    Map<Long, TabletRange> getTargetRanges() {
        return targetRanges;
    }

    @Override
    protected TabletMetadataUpdateAgentTask createTask(PhysicalPartition partition, MaterializedIndex index, long nodeId,
                                                       Set<Long> tablets) {
        String tag = String.format("%d_%d", partition.getId(), index.getMetaId());
        TabletMetadataUpdateAgentTask task = null;
        boolean needUpdateSchema = false;
        for (IndexSchemaInfo info : schemaInfos) {
            if (info.getIndexMetaId() == index.getMetaId()) {
                needUpdateSchema = true;
                // `Set.add()` returns true means this set did not already contain the specified element
                boolean createSchemaFile = partitionsWithSchemaFile.add(tag);
                task = TabletMetadataUpdateAgentTaskFactory.createTabletSchemaUpdateTask(nodeId,
                        new ArrayList<>(tablets), info.getSchemaInfo().toTabletSchema(), createSchemaFile, targetRanges);
                break;
            }
        }

        // if the index is not in schemaInfos, it means the schema of index are not needed to be modified,
        // but we still need to update the tablet meta to improve the meta version
        if (!needUpdateSchema) {
            task = TabletMetadataUpdateAgentTaskFactory.createTabletSchemaUpdateTask(nodeId,
                    new ArrayList<>(tablets), null, false);
        }

        return task;
    }

    @Override
    protected void updateCatalog(Database db, OlapTable table, boolean isReplay) {
        updateCatalogUnprotected(db, table, isReplay);
    }

    @Override
    protected void prepareForPersist(Database db, OlapTable table) {
        if (disableFastSchemaEvolutionV2) {
            return;
        }
        // Create historySchema before persistStateChange so it can be included in copyForPersist
        OlapTableHistorySchema.Builder historySchemaBuilder = OlapTableHistorySchema.newBuilder();
        for (IndexSchemaInfo indexSchemaInfo : schemaInfos) {
            long indexMetaId = indexSchemaInfo.getIndexMetaId();
            MaterializedIndexMeta indexMeta = requireNonNull(table.getIndexMetaByMetaId(indexMetaId)).shallowCopy();
            SchemaInfo oldSchemaInfo = SchemaInfo.fromMaterializedIndex(table, indexMetaId, indexMeta);
            historySchemaBuilder.addIndexSchema(
                    new IndexSchemaInfo(indexMetaId, table.getIndexNameByMetaId(indexMetaId), oldSchemaInfo));
        }
        long txnId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionIDGenerator()
                .getNextTransactionId();
        historySchemaBuilder.setHistoryTxnIdThreshold(txnId);
        this.historySchema = historySchemaBuilder.build();
    }

    private void updateCatalogUnprotected(Database db, OlapTable table, boolean isReplay) {
        if (disableFastSchemaEvolutionV2) {
            // only update the property, no need to update schema which is actually not changed
            setFastSchemaEvolutionV2(table, false);
            LOG.info("Schema change job finish to disable {}, job_id: {}, table: {}",
                    PropertyAnalyzer.PROPERTIES_CLOUD_NATIVE_FAST_SCHEMA_EVOLUTION_V2, getJobId(), table.getName());
            return;
        }

        Set<String> droppedOrModifiedColumns = Sets.newHashSet();
        boolean hasMv = !table.getRelatedMaterializedViews().isEmpty();
        for (IndexSchemaInfo indexSchemaInfo : schemaInfos) {
            SchemaInfo schemaInfo = indexSchemaInfo.getSchemaInfo();
            long indexMetaId = indexSchemaInfo.getIndexMetaId();
            MaterializedIndexMeta indexMeta = requireNonNull(table.getIndexMetaByMetaId(indexMetaId)).shallowCopy();
            List<Column> oldColumns = indexMeta.getSchema();

            Preconditions.checkState(Objects.equals(indexMeta.getKeysType(), schemaInfo.getKeysType()));
            if (targetRanges != null) {
                // Range path: the short-key count may change, so the equal-assertion is relaxed and
                // the version relation is handled explicitly for exact-state replay idempotency.
                // The exact-state-replay `return` below exits the whole loop, which is only correct
                // because the metadata-only range route guarantees exactly one index; a future
                // multi-index extension must revisit this instead of silently skipping other indexes.
                Preconditions.checkState(schemaInfos.size() == 1,
                        "range flip target only supports a single index, got " + schemaInfos.size());
                if (schemaInfo.getVersion() == indexMeta.getSchemaVersion()) {
                    // Exact-state replay: the flip already happened. Verify the live ranges already
                    // equal the targets and return WITHOUT re-appending (no double-append).
                    verifyLiveRangesMatchTargets(table, indexMetaId);
                    return;
                }
                Preconditions.checkState(schemaInfo.getVersion() > indexMeta.getSchemaVersion(),
                        "range flip target schema version " + schemaInfo.getVersion()
                                + " is older than current " + indexMeta.getSchemaVersion());
            } else {
                Preconditions.checkState(schemaInfo.getVersion() > indexMeta.getSchemaVersion());
                Preconditions.checkState(
                        Objects.equals(indexMeta.getShortKeyColumnCount(), schemaInfo.getShortKeyColumnCount()));
            }

            if (hasMv) {
                droppedOrModifiedColumns.addAll(AlterHelper.collectDroppedOrModifiedColumns(oldColumns, schemaInfo.getColumns()));
            }

            indexMeta.setSchema(schemaInfo.getColumns());
            indexMeta.setSchemaVersion(schemaInfo.getVersion());
            indexMeta.setSchemaId(schemaInfo.getId());
            indexMeta.setSortKeyIdxes(schemaInfo.getSortKeyIndexes());
            indexMeta.setSortKeyUniqueIds(schemaInfo.getSortKeyUniqueIds());
            if (targetRanges != null) {
                indexMeta.setShortKeyColumnCount(schemaInfo.getShortKeyColumnCount());
            }

            // update the indexIdToMeta
            table.getIndexMetaIdToMeta().put(indexMetaId, indexMeta);
            table.setIndexes(schemaInfo.getIndexes());
            table.renameColumnNamePrefix(indexMetaId);

            if (targetRanges != null) {
                flipTabletRangesInPlace(table, indexMetaId);
            }
        }
        if (targetRanges != null) {
            // Must use the same clock as OptimisticVersion.generate() (System.nanoTime), which the
            // lock-free planner compares against; mixing it with epoch millis would break stale-plan
            // invalidation after the in-place range flip. Mirrors SplitTabletJob's reshard flip.
            table.lastSchemaUpdateTime.set(System.nanoTime());
        }
        table.rebuildFullSchema();

        // If modified columns are already done, inactive related mv
        AlterMVJobExecutor.inactiveRelatedMaterializedViewsRecursive(table, droppedOrModifiedColumns);
    }

    /**
     * In-place reference swap of each live tablet's range to its persisted target, over every tablet
     * of every visible physical partition. In-place (not copy-and-swap) so background lake-stat and
     * autovacuum work that holds the same Tablet object is not orphaned.
     */
    private void flipTabletRangesInPlace(OlapTable table, long indexMetaId) {
        for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
            MaterializedIndex index = physicalPartition.getLatestIndex(indexMetaId);
            if (index == null) {
                continue;
            }
            for (Tablet tablet : index.getTablets()) {
                TabletRange target = targetRanges.get(tablet.getId());
                Preconditions.checkState(target != null, "no target range for tablet " + tablet.getId());
                tablet.setRange(target);
            }
        }
    }

    /**
     * Exact-state replay guard: every live tablet's range must already equal its persisted target
     * (value comparison via {@link TabletRange#equals}), proving the flip was applied exactly once.
     */
    private void verifyLiveRangesMatchTargets(OlapTable table, long indexMetaId) {
        for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
            MaterializedIndex index = physicalPartition.getLatestIndex(indexMetaId);
            if (index == null) {
                continue;
            }
            for (Tablet tablet : index.getTablets()) {
                TabletRange target = targetRanges.get(tablet.getId());
                Preconditions.checkState(target != null && Objects.equals(target, tablet.getRange()),
                        "exact-state replay range mismatch for tablet " + tablet.getId());
            }
        }
    }

    @Override
    protected void validateBeforeFinishUnprotected(Database db, OlapTable table) throws AlterCancelException {
        // Runs after publishVersion() (see LakeTableAlterMetaJobBase#runFinishedRewritingJob) but before
        // the catalog flip; that ordering is safe only because (a) visibility is gated on this
        // validation succeeding and (b) this job inherits allowConcurrentPartitionCreation() == false,
        // so the live tablet set cannot change during the SCHEMA_CHANGE window. If this job ever opts
        // into concurrent partition creation, the coverage validation below must move before publish.
        if (targetRanges == null) {
            return;
        }
        // Everything the must-not-fail catalog callback (updateCatalogUnprotected range path)
        // depends on is validated HERE, before the FINISHED record is journaled, so the callback
        // cannot throw mid-flip and leave a durable FINISHED record with a partial catalog update.
        if (schemaInfos.size() != 1) {
            throw new AlterCancelException("range flip target only supports a single index, got " + schemaInfos.size());
        }
        Set<Long> liveTabletIds = new HashSet<>();
        for (IndexSchemaInfo indexSchemaInfo : schemaInfos) {
            long indexMetaId = indexSchemaInfo.getIndexMetaId();
            MaterializedIndexMeta indexMeta = table.getIndexMetaByMetaId(indexMetaId);
            if (indexMeta == null) {
                throw new AlterCancelException("index meta not found for range flip, indexMetaId: " + indexMetaId);
            }
            // The target must not be older than the current schema (corruption / stale replay).
            if (indexSchemaInfo.getSchemaInfo().getVersion() < indexMeta.getSchemaVersion()) {
                throw new AlterCancelException("range flip target schema version "
                        + indexSchemaInfo.getSchemaInfo().getVersion() + " is older than current "
                        + indexMeta.getSchemaVersion());
            }
            // Mirror the callback's keysType precondition so it cannot throw post-journal.
            if (!Objects.equals(indexMeta.getKeysType(), indexSchemaInfo.getSchemaInfo().getKeysType())) {
                throw new AlterCancelException("range flip keysType mismatch for indexMetaId " + indexMetaId);
            }
            for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
                MaterializedIndex index = physicalPartition.getLatestIndex(indexMetaId);
                if (index == null) {
                    continue;
                }
                for (Tablet tablet : index.getTablets()) {
                    // Non-null value check (not just containsKey) — the callback dereferences the target.
                    if (targetRanges.get(tablet.getId()) == null) {
                        throw new AlterCancelException("missing target range for tablet " + tablet.getId());
                    }
                    liveTabletIds.add(tablet.getId());
                }
            }
        }
        // Reject stale coverage: the target map must cover exactly the live tablet set, no extras.
        if (!targetRanges.keySet().equals(liveTabletIds)) {
            throw new AlterCancelException("target range coverage mismatch: targets=" + targetRanges.keySet()
                    + " liveTablets=" + liveTabletIds);
        }
    }

    private void setFastSchemaEvolutionV2(OlapTable table, boolean enabled) {
        if (table instanceof LakeTable) {
            ((LakeTable) table).setFastSchemaEvolutionV2(enabled);
        } else if (table instanceof LakeMaterializedView) {
            ((LakeMaterializedView) table).setFastSchemaEvolutionV2(enabled);
        }
    }

    @Override
    protected void restoreState(LakeTableAlterMetaJobBase job) {
        // This PR(#55282) only writes the schemaInfo once in the entire schema change job process, 
        // but it has compatibility issues with previous versions, so it was reverted.
        // However, since some versions include this PR, the schemaInfo may be null when upgrading from these versions.
        LakeTableAsyncFastSchemaChangeJob schemaChangeJob = (LakeTableAsyncFastSchemaChangeJob) job;
        List<IndexSchemaInfo> jobSchemaInfos = schemaChangeJob.schemaInfos;
        if (jobSchemaInfos != null && !jobSchemaInfos.isEmpty()) {
            this.schemaInfos = new ArrayList<>(jobSchemaInfos);
        }
        this.disableFastSchemaEvolutionV2 = schemaChangeJob.disableFastSchemaEvolutionV2;
        this.historySchema = ((LakeTableAsyncFastSchemaChangeJob) job).historySchema;
        // Replay reprojects from the identical persisted targets, so the flip is reproduced exactly.
        this.targetRanges = schemaChangeJob.targetRanges;
    }

    @Override
    protected boolean enableFileBundling() {
        return false;
    }

    @Override
    protected boolean disableFileBundling() {
        return false;
    }

    @Override
    protected void runFinishedRewritingJob() throws AlterCancelException {
        super.runFinishedRewritingJob();
        if (jobState == JobState.FINISHED) {
            AlterMetricRegistry.getInstance().updateAlterDuration(
                    AlterMetricRegistry.AlterExecutionMode.LEGACY_FAST_SCHEMA_EVOLUTION,
                    finishedTimeMs - createTimeMs);
        }
    }

    @Override
    public boolean isExpire() {
        boolean expiredByTime = super.isExpire();
        boolean expiredByHistorySchema = true;
        if (historySchema != null && !historySchema.isExpired()) {
            try {
                expiredByHistorySchema = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                    .isPreviousTransactionsFinished(historySchema.getHistoryTxnIdThreshold(), dbId, Lists.newArrayList(tableId));
            } catch (Exception e) {
                // As isPreviousTransactionsFinished said, exception happens only when db does not exist,
                // so could clean the history schema safely
            }
            if (expiredByHistorySchema) {
                historySchema.setExpire();
                LOG.info("Expire the history schema, jobId: {}, tableName: {}, expireTxnIdThreshold: {}",
                        jobId, tableName, historySchema.getHistoryTxnIdThreshold());
            }
        }
        return expiredByTime && expiredByHistorySchema;
    }

    List<SchemaInfo> getSchemaInfoList() {
        return schemaInfos.stream().map(IndexSchemaInfo::getSchemaInfo).collect(Collectors.toList());
    }

    public Optional<OlapTableHistorySchema> getHistorySchema() {
        return Optional.ofNullable(historySchema);
    }

    public void setDisableFastSchemaEvolutionV2() {
        this.disableFastSchemaEvolutionV2 = true;
    }

    boolean isDisableFastSchemaEvolutionV2() {
        return disableFastSchemaEvolutionV2;
    }

    @Override
    public AlterJobV2 copyForPersist() {
        return new LakeTableAsyncFastSchemaChangeJob(this, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected void getInfo(List<List<Comparable>> infos) {
        String progress = FeConstants.NULL_STRING;
        if (jobState == JobState.RUNNING && getBatchTask() != null) {
            progress = getBatchTask().getFinishedTaskNum() + "/" + getBatchTask().getTaskNum();
        }

        for (IndexSchemaInfo schemaInfo : schemaInfos) {
            List<Comparable> info = Lists.newArrayList();
            info.add(jobId);
            info.add(tableName);
            info.add(TimeUtils.longToTimeString(createTimeMs));
            info.add(TimeUtils.longToTimeString(finishedTimeMs));
            info.add(schemaInfo.getIndexName());
            info.add(schemaInfo.getIndexMetaId());
            info.add(schemaInfo.getIndexMetaId());
            info.add(String.format("%d:0", schemaInfo.getSchemaInfo().getVersion())); // schema version and schema hash
            info.add(getWatershedTxnId());
            info.add(jobState.name());
            info.add(errMsg);
            info.add(progress);
            info.add(timeoutMs / 1000);
            Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouseAllowNull(warehouseId);
            if (warehouse == null) {
                info.add("null");
            } else {
                info.add(warehouse.getName());
            }
            infos.add(info);
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        partitionsWithSchemaFile = new HashSet<>();
        if (targetRanges != null) {
            targetRanges = Collections.unmodifiableMap(targetRanges);
        }
    }
}
