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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.alter.reshard.TabletReshardUtils;
import com.starrocks.alter.reshard.presplit.BoundaryPlanner;
import com.starrocks.alter.reshard.presplit.BoundaryPlannerResult;
import com.starrocks.alter.reshard.presplit.DefaultPreSplitPipeline;
import com.starrocks.alter.reshard.presplit.Estimates;
import com.starrocks.alter.reshard.presplit.InternalPartitionScanContext;
import com.starrocks.alter.reshard.presplit.ReservoirSampler;
import com.starrocks.alter.reshard.presplit.SampleRequest;
import com.starrocks.alter.reshard.presplit.SampleSet;
import com.starrocks.alter.reshard.presplit.Sampler;
import com.starrocks.alter.reshard.presplit.TabletPreSplitCoordinator;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

/**
 * Schema-change job for shared-data (lake) range-distribution OLAP tables whose key change
 * shifts the range sort key (sort-key reorder or keyness flip) with the column set unchanged.
 *
 * <p>Unlike the 1:1 shadow-index rewrite of {@link LakeTableSchemaChangeJob}, this job re-routes
 * each partition's data into a fresh K-tablet shadow index whose tablet boundaries are sampled by
 * the <em>new</em> sort key, then materializes the shadow via an internal INSERT and an atomic flip.
 *
 * <p>The generic shared-data online double-write rewrite machinery (the
 * {@code PENDING -> WAITING_TXN -> RUNNING -> FINISHED_REWRITING -> FINISHED} state machine, the
 * watershed allocation and shadow exposure, the resumable per-partition rewrite, the reserve /
 * publish / flip chain, force-cancel handling and journal replay) lives in
 * {@link LakeOnlineRewriteJobBase}. This subclass supplies the range-rewrite specifics: the new key
 * configuration applied at the flip, the boundary sampling by the new sort key, and the abstract
 * hooks that plan the K-tablet shadow layout, register the shadow index meta, perform the catalog
 * flip, choose the columns that inactivate dependent MVs, and project the rewrite INSERT.
 */
public class LakeRangeRewriteSchemaChangeJob extends LakeOnlineRewriteJobBase {
    private static final Logger LOG = LogManager.getLogger(LakeRangeRewriteSchemaChangeJob.class);

    // The rewritten base-index schema and key configuration applied at flip. These describe the
    // new key space the shadow is sorted/distributed by; the column set itself is unchanged.
    @SerializedName(value = "newSchema")
    private List<Column> newSchema;
    @SerializedName(value = "newKeysType")
    private KeysType newKeysType;
    @SerializedName(value = "newSortKeyIdxes")
    private List<Integer> newSortKeyIdxes;
    @SerializedName(value = "newSortKeyUniqueIds")
    private List<Integer> newSortKeyUniqueIds;

    // The new sort-key columns (a projection of newSchema) used to sample and to plan the K-tablet
    // boundaries. Kept separately because the boundary planner and the sampler take a Column list.
    @SerializedName(value = "newSortKeyColumns")
    private List<Column> newSortKeyColumns;

    // The shadow index short-key column count.
    @SerializedName(value = "shadowShortKeyColumnCount")
    private short shadowShortKeyColumnCount;

    // Sampling seam. Production samples a partition's base by the new sort key via a SQL sub-query
    // (needs a backend); tests inject a stub returning a canned SampleSet. Not serialized.
    private transient Sampler sampler;

    // for deserialization
    public LakeRangeRewriteSchemaChangeJob() {
        super(JobType.SCHEMA_CHANGE);
    }

    public LakeRangeRewriteSchemaChangeJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, JobType.SCHEMA_CHANGE, dbId, tableId, tableName, timeoutMs);
    }

    // Deep-copy constructor used by copyForPersist(): the WAL must record a snapshot, not a live
    // reference, so journaling never mutates the running job's state. Chains through
    // super(other) for the base + watershed + shadow-identity + partitionStates fields and
    // deep-copies this job's own collections. The transient sampler is intentionally NOT copied.
    protected LakeRangeRewriteSchemaChangeJob(LakeRangeRewriteSchemaChangeJob other) {
        super(other);
        this.newSchema = other.newSchema == null ? null : new ArrayList<>(other.newSchema);
        this.newKeysType = other.newKeysType;
        this.newSortKeyIdxes = other.newSortKeyIdxes == null ? null : new ArrayList<>(other.newSortKeyIdxes);
        this.newSortKeyUniqueIds = other.newSortKeyUniqueIds == null ? null : new ArrayList<>(other.newSortKeyUniqueIds);
        this.newSortKeyColumns = other.newSortKeyColumns == null ? null : new ArrayList<>(other.newSortKeyColumns);
        this.shadowShortKeyColumnCount = other.shadowShortKeyColumnCount;
    }

    public void setNewSchema(List<Column> newSchema) {
        this.newSchema = newSchema;
    }

    @VisibleForTesting
    public List<Column> getNewSchema() {
        return newSchema;
    }

    public void setNewKeysType(KeysType newKeysType) {
        this.newKeysType = newKeysType;
    }

    public void setNewSortKeyIdxes(List<Integer> newSortKeyIdxes) {
        this.newSortKeyIdxes = newSortKeyIdxes;
    }

    public void setNewSortKeyUniqueIds(List<Integer> newSortKeyUniqueIds) {
        this.newSortKeyUniqueIds = newSortKeyUniqueIds;
    }

    public void setNewSortKeyColumns(List<Column> newSortKeyColumns) {
        this.newSortKeyColumns = newSortKeyColumns;
    }

    public void setShadowIndex(long shadowIndexMetaId, long originIndexMetaId, String shadowIndexName,
                               short shadowShortKeyColumnCount) {
        this.shadowIndexMetaId = shadowIndexMetaId;
        this.originIndexMetaId = originIndexMetaId;
        this.shadowIndexName = shadowIndexName;
        this.shadowShortKeyColumnCount = shadowShortKeyColumnCount;
    }

    /**
     * Inject the sampler used by {@link #planPartitionShadow}. Tests use this to bypass the real SQL
     * sample sub-query (which needs a backend); production leaves it null so {@link #getSampler}
     * lazily builds the real internal-partition sampler.
     */
    @VisibleForTesting
    public void setSampler(Sampler sampler) {
        this.sampler = sampler;
    }

    private Sampler getSampler() {
        if (sampler == null) {
            sampler = ReservoirSampler.forInternalPartition();
        }
        return sampler;
    }

    @Override
    protected OlapTable.OlapTableState jobTableState() {
        return OlapTable.OlapTableState.SCHEMA_CHANGE;
    }

    @Override
    protected boolean flipNotYetApplied(@NotNull OlapTable table) {
        // Replace flip drops the origin index meta; if it is gone, the flip has already run.
        return table.getIndexMetaByMetaId(originIndexMetaId) != null;
    }

    @Override
    protected void validateRewriteConfig() throws AlterCancelException {
        Preconditions.checkNotNull(newSchema, "new schema is not set");
        Preconditions.checkNotNull(newKeysType, "new keys type is not set");
        Preconditions.checkState(newSortKeyColumns != null && !newSortKeyColumns.isEmpty(),
                "new sort key columns are not set");
    }

    /**
     * Plan one partition's K-tablet shadow layout lock-free: choose {@code K} from the base data size
     * and the active compute-node count, sample by the new sort key, plan the boundaries, and build the
     * shadow {@link MaterializedIndex} via the StarOS createShards RPC. NO db lock is held here.
     */
    @Override
    protected void planPartitionShadow(PendingPartitionPlan plan, OlapTable table, String dbName)
            throws AlterCancelException {
        int activeComputeNodeCount = TabletReshardUtils.computeNodeCount(computeResource);
        int requestedTabletCount = chooseTabletCount(plan.partitionDataSize, activeComputeNodeCount);
        List<Tuple> boundaries = planBoundaries(dbName, plan.tableName, plan.partitionName,
                plan.physicalPartitionId, plan.partitionDataSize, requestedTabletCount);
        // K may collapse to 1 (sampling failure / no-distinction): a single full-range tablet.
        int shadowTabletCount = boundaries.size() + 1;
        List<TabletRange> ranges = buildTabletRanges(boundaries);
        MaterializedIndex shadowIndex;
        try {
            shadowIndex = LakeTableAlterJobV2Builder.buildRangeShadowIndex(
                    dbId, table, plan.physicalPartition, shadowIndexMetaId, shadowTabletCount, ranges,
                    plan.shardGroupId, computeResource);
        } catch (DdlException e) {
            throw new AlterCancelException("failed to build shadow index for partition "
                    + plan.physicalPartitionId + ": " + e.getMessage());
        }
        plan.boundaries = boundaries;
        plan.shadowTabletCount = shadowTabletCount;
        plan.shadowIndex = shadowIndex;
    }

    /**
     * Idempotently register the shadow index's {@link MaterializedIndexMeta} (schema, keysType,
     * sort-key indexes) on the table. Re-runnable on replay: {@code setIndexMeta} overwrites the
     * meta for {@code shadowIndexMetaId}, so a second call is a no-op-equivalent. The caller holds
     * the database write lock.
     */
    @Override
    protected void registerShadowIndexMeta(@NotNull OlapTable table) {
        table.setIndexMeta(shadowIndexMetaId, shadowIndexName, newSchema, 0, 0, shadowShortKeyColumnCount,
                TStorageType.COLUMN, newKeysType, null, newSortKeyIdxes, newSortKeyUniqueIds);
        MaterializedIndexMeta originIndexMeta = table.getIndexMetaByMetaId(originIndexMetaId);
        MaterializedIndexMeta shadowIndexMeta = table.getIndexMetaByMetaId(shadowIndexMetaId);
        Preconditions.checkNotNull(originIndexMeta);
        Preconditions.checkNotNull(shadowIndexMeta);
        rebuildMaterializedIndexMeta(originIndexMeta, shadowIndexMeta);
    }

    /**
     * Choose {@code K} from the partition's base data size and the active compute-node count.
     * {@link TabletPreSplitCoordinator#selectTabletCount} clamps to {@code [2, maxSplitCount]}, so the
     * requested count is always {@code >= 2}; the actual effective K may still collapse to 1 in
     * {@link #planBoundaries} when the sample shows no useful distinction.
     */
    private int chooseTabletCount(long partitionDataSize, int activeComputeNodeCount) {
        Estimates estimates = new Estimates(partitionDataSize, 0L);
        return TabletPreSplitCoordinator.selectTabletCount(estimates, activeComputeNodeCount);
    }

    /**
     * Sample the partition by the new sort key and plan {@code K-1} boundaries. A sampling failure or
     * a no-distinction sample (empty sample / all-equal keys → planner NO_SPLIT) degrades to an empty
     * boundary list = a single full-range tablet (K=1 fallback). All inputs are captured snapshots so
     * this runs lock-free (the sampler itself re-acquires a read lock for its SQL round-trip).
     */
    private List<Tuple> planBoundaries(String dbName, String tableName, String partitionName,
                                       long physicalPartitionId, long partitionDataSize,
                                       int requestedTabletCount) {
        try {
            SampleSet sampleSet = sampleByNewSortKey(dbName, tableName, partitionName, partitionDataSize);
            if (sampleSet.isEmpty()) {
                return List.of();
            }
            BoundaryPlannerResult planResult =
                    BoundaryPlanner.planRowQuantileBoundaries(sampleSet, requestedTabletCount, newSortKeyColumns);
            if (planResult.isNoSplit()) {
                return List.of();
            }
            return planResult.getBoundaries();
        } catch (StarRocksException | RuntimeException e) {
            // Sampling / planning failure must not fail the alter: fall back to a single full-range
            // shadow tablet (the rewrite still re-sorts by the new key; it just isn't split).
            LOG.warn("range-rewrite schema change job {}: sampling/planning failed for partition {}, "
                    + "falling back to a single full-range shadow tablet", jobId, physicalPartitionId, e);
            return List.of();
        }
    }

    private SampleSet sampleByNewSortKey(String dbName, String tableName, String partitionName,
                                         long partitionDataSize) throws StarRocksException {
        List<String> sortKeySourceColumnNames =
                newSortKeyColumns.stream().map(Column::getName).collect(Collectors.toList());
        // The sample sub-query addresses the logical partition by name; physical partitions are not
        // directly addressable in SQL. Boundaries only need to be representative, so sampling the
        // parent partition is sufficient.
        InternalPartitionScanContext scanContext = new InternalPartitionScanContext(
                dbName,
                tableName,
                partitionName,
                sortKeySourceColumnNames,
                List.of(),
                partitionDataSize,
                computeResource);
        SampleRequest request = new SampleRequest(
                scanContext, newSortKeyColumns, Config.tablet_pre_split_sample_byte_limit, jobId);
        return getSampler().sample(request);
    }

    /**
     * Tile {@code (-inf, +inf)} with one {@link TabletRange} per tablet given the {@code K-1}
     * sorted boundaries: {@code (-inf, c1), [c1, c2), ..., [c_{K-1}, +inf)}. An empty boundary list
     * yields a single full-range tablet (K=1 case).
     */
    @VisibleForTesting
    static List<TabletRange> buildTabletRanges(List<Tuple> boundaries) {
        if (boundaries.isEmpty()) {
            List<TabletRange> singleRange = new ArrayList<>(1);
            singleRange.add(new TabletRange(Range.all()));
            return singleRange;
        }
        return DefaultPreSplitPipeline.buildTabletRanges(boundaries);
    }

    /**
     * Non-generated column names the rewrite INSERT selects: the new-schema columns (the column set is
     * unchanged from the base, so these are the base column names re-projected in the new order).
     */
    @Override
    protected List<String> rewriteSelectColumnNames(@NotNull OlapTable table) {
        return newSchema.stream()
                .filter(column -> !column.isGeneratedColumn())
                .map(column -> ParseUtil.backquote(column.getName()))
                .collect(Collectors.toList());
    }

    /**
     * The atomic catalog flip, adapted from {@link LakeTableSchemaChangeJob#visualiseShadowIndex} for the
     * single base-index-&rarr;K-tablet-shadow swap. Runs inside the edit-log applier under the table write
     * lock. For each partition: advance the visible version to {@code commitVersion}, drop the origin
     * (base) index, and promote the shadow index to NORMAL as the new base. Then rename the shadow index
     * to the origin name, repoint {@code baseIndexMetaId} at the shadow meta (so the table-level schema /
     * keysType / sortKeyIdxes / range distribution all follow, since the range distribution columns are
     * derived from the base index sort key), rebuild the full schema, and reset the table to NORMAL.
     */
    @Override
    protected void visualiseShadowIndex(@NotNull OlapTable table) {
        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();

        for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
            PartitionRewriteState partitionState = partitionStates.get(physicalPartition.getId());
            Preconditions.checkState(partitionState != null && partitionState.commitVersion != null,
                    physicalPartition.getId());
            long commitVersion = partitionState.commitVersion;

            Preconditions.checkState(commitVersion == physicalPartition.getVisibleVersion() + 1,
                    commitVersion + " vs " + physicalPartition.getVisibleVersion());
            physicalPartition.setVisibleVersion(commitVersion, finishedTimeMs);

            TStorageMedium medium =
                    table.getPartitionInfo().getDataProperty(physicalPartition.getParentId()).getStorageMedium();

            // Look the shadow index up from the live catalog (not the journaled state): a job recovered
            // from the edit log holds a different MaterializedIndex object than the one in globalStateMgr,
            // so mutating the journaled object would not reflect into the catalog.
            MaterializedIndex shadowIndex = physicalPartition.getLatestIndex(shadowIndexMetaId);
            Preconditions.checkNotNull(shadowIndex, shadowIndexMetaId);
            List<MaterializedIndex> droppedOriginIndices =
                    physicalPartition.deleteMaterializedIndexByMetaId(originIndexMetaId);
            Preconditions.checkState(!droppedOriginIndices.isEmpty(),
                    originIndexMetaId + " vs. " + shadowIndexMetaId);

            TabletMeta shadowTabletMeta =
                    new TabletMeta(dbId, tableId, physicalPartition.getId(), shadowIndex.getId(), medium, true);
            for (Tablet tablet : shadowIndex.getTablets()) {
                invertedIndex.addTablet(tablet.getId(), shadowTabletMeta);
            }

            physicalPartition.visualiseShadowIndex(shadowIndex.getId(),
                    originIndexMetaId == table.getBaseIndexMetaId());

            // The origin tablets created under the old key can leave FE metadata.
            for (MaterializedIndex droppedIdx : droppedOriginIndices) {
                for (Tablet originTablet : droppedIdx.getTablets()) {
                    invertedIndex.deleteTablet(originTablet.getId());
                }
            }
        }

        // Flip the table-level index identity: rename the shadow index to the origin name (also strips the
        // shadow column-name prefix) and repoint the base index meta id at the shadow meta.
        String shadowIndexName = table.getIndexNameByMetaId(shadowIndexMetaId);
        String originIndexName = table.getIndexNameByMetaId(originIndexMetaId);
        table.deleteIndexInfo(originIndexName);
        table.renameIndexForSchemaChange(shadowIndexName, originIndexName);
        table.renameColumnNamePrefix(shadowIndexMetaId);
        if (originIndexMetaId == table.getBaseIndexMetaId()) {
            table.setBaseIndexMetaId(shadowIndexMetaId);
        }
        table.rebuildFullSchema();

        table.setState(OlapTable.OlapTableState.NORMAL);
    }

    /**
     * The columns whose meaning changed for this range-rewrite (column set unchanged), so dependent async
     * MVs built on the old schema get inactivated: every column whose key membership flipped between the
     * origin and new schema, plus the new sort-key columns (their ordering / role changed). Empty when
     * nothing relevant changed and there are no dependent MVs.
     */
    @Override
    protected Set<String> affectedColumnsForMvInactivation(@NotNull OlapTable table) {
        if (table.getRelatedMaterializedViews().isEmpty()) {
            return Sets.newHashSet();
        }
        Set<String> affected = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        Map<String, Boolean> originIsKey = new HashMap<>();
        for (Column column : table.getSchemaByIndexMetaId(originIndexMetaId)) {
            originIsKey.put(column.getName(), column.isKey());
        }
        for (Column column : newSchema) {
            Boolean wasKey = originIsKey.get(column.getName());
            if (wasKey != null && wasKey != column.isKey()) {
                affected.add(column.getName());
            }
        }
        for (Column sortKeyColumn : newSortKeyColumns) {
            affected.add(sortKeyColumn.getName());
        }
        return affected;
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        // Copy the subclass-specific journaled fields first (the shared base fields and the catalog
        // reconstruction are handled by super.replay), so the base's catalog reconstruction hooks
        // (registerShadowIndexMeta / visualiseShadowIndex) see this job's new schema/key config.
        if (this != replayedJob) {
            LakeRangeRewriteSchemaChangeJob other = (LakeRangeRewriteSchemaChangeJob) replayedJob;
            this.newSchema = other.newSchema;
            this.newKeysType = other.newKeysType;
            this.newSortKeyIdxes = other.newSortKeyIdxes;
            this.newSortKeyUniqueIds = other.newSortKeyUniqueIds;
            this.newSortKeyColumns = other.newSortKeyColumns;
            this.shadowShortKeyColumnCount = other.shadowShortKeyColumnCount;
        }
        super.replay(replayedJob);
    }

    @Override
    public AlterJobV2 copyForPersist() {
        // The WAL records a deep snapshot (not a live reference) so journaling never mutates the
        // running job. getTransactionId() (keyed off watershedTxnId) is inherited from the base.
        return new LakeRangeRewriteSchemaChangeJob(this);
    }
}
