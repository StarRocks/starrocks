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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

/**
 * Schema-change job for shared-data (lake) range-distribution OLAP tables whose key change
 * shifts the range sort key (sort-key reorder or keyness flip with the column set unchanged, or an
 * ADD/DROP of a key column that changes the column set).
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

    protected List<Column> getNewSortKeyColumns() {
        return newSortKeyColumns;
    }

    @VisibleForTesting
    List<Integer> getNewSortKeyUniqueIds() {
        return newSortKeyUniqueIds;
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
        List<Tuple> boundaries = planBoundaries(table, dbName, plan.tableName, plan.partitionName,
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

    @Override
    protected List<Column> getShadowSchema() {
        return newSchema;
    }

    @Override
    protected KeysType getShadowKeysType() {
        return newKeysType;
    }

    @Override
    protected List<Integer> getShadowSortKeyIdxes() {
        return newSortKeyIdxes;
    }

    @Override
    protected List<Integer> getShadowSortKeyUniqueIds() {
        return newSortKeyUniqueIds;
    }

    @Override
    protected short getShadowShortKeyColumnCount() {
        return shadowShortKeyColumnCount;
    }

    @Override
    protected String shadowKindLabel() {
        return "range-rewrite";
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
        // Expose the shadow index's columns in getFullSchema() during the rewrite window. For an added
        // (__starrocks_shadow_-prefixed) key column this makes InsertPlanner.fillShadowColumns materialize its
        // constant default for the rewrite INSERT and every concurrent double-write, while the prefix keeps it
        // out of the user namespace until the flip strips it. No-op for a same-column-set shadow (reorder /
        // keyness flip / drop). Mirrors addShadowIndexToCatalog.
        table.rebuildFullSchema();
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
    private List<Tuple> planBoundaries(OlapTable table, String dbName, String tableName, String partitionName,
                                       long physicalPartitionId, long partitionDataSize,
                                       int requestedTabletCount) {
        try {
            SampleSet sampleSet = sampleByNewSortKey(table, dbName, tableName, partitionName, partitionDataSize);
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

    @VisibleForTesting
    public List<Tuple> planBoundariesForTest(OlapTable table, String dbName, String tableName, String partitionName,
                                             long physicalPartitionId, long partitionDataSize,
                                             int requestedTabletCount) {
        return planBoundaries(table, dbName, tableName, partitionName, physicalPartitionId, partitionDataSize,
                requestedTabletCount);
    }

    private SampleSet sampleByNewSortKey(OlapTable table, String dbName, String tableName, String partitionName,
                                         long partitionDataSize) throws StarRocksException {
        Set<String> sourceColumnNames = table.getSchemaByIndexMetaId(originIndexMetaId).stream()
                .map(Column::getName)
                .collect(Collectors.toCollection(() -> Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER)));
        List<String> sortKeyProjection = buildSampleProjection(sourceColumnNames);
        // The sample sub-query addresses the logical partition by name; physical partitions are not
        // directly addressable in SQL. Boundaries only need to be representative, so sampling the
        // parent partition is sufficient.
        InternalPartitionScanContext scanContext = new InternalPartitionScanContext(
                dbName,
                tableName,
                partitionName,
                sortKeyProjection,
                List.of(),
                partitionDataSize,
                computeResource,
                /*sortKeyProjectionIsVerbatim=*/ true);
        SampleRequest request = new SampleRequest(
                scanContext, newSortKeyColumns, Config.tablet_pre_split_sample_byte_limit, jobId);
        return getSampler().sample(request);
    }

    /**
     * Builds the sampling sub-query projection for each {@link #newSortKeyColumns} entry: the
     * backquoted column name when it exists in the source (base) schema, or a raw
     * {@code CAST(<default> AS <type>) AS <alias>} literal when it does not (an added,
     * {@code __starrocks_shadow_}-prefixed key column that has no source value at PENDING time). A
     * constant projection contributes no distinction to {@link BoundaryPlanner}, so real boundaries
     * still come from the other (source-present) sort-key columns; only a sole-added-key ADD
     * collapses to K=1.
     */
    private List<String> buildSampleProjection(Set<String> sourceColumnNames) {
        return newSortKeyColumns.stream()
                .map(column -> projectSortKeyColumn(column, sourceColumnNames))
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    public List<String> buildSampleProjectionForTest(Set<String> sourceColumnNames) {
        return buildSampleProjection(sourceColumnNames);
    }

    /**
     * Projects a single new-sort-key column: the backquoted name when {@code column} exists in the
     * source (base) schema, else a raw {@code CAST(...) AS <alias>} literal built from the column's
     * constant default (a NULL default projects SQL {@code NULL}). The CAST must stay a raw
     * expression, not a backquoted identifier — backquoting it would send the backend a reference to
     * a (missing) column literally named "CAST(...) AS ..." instead of a literal.
     */
    private static String projectSortKeyColumn(Column column, Set<String> sourceColumnNames) {
        String backquoted = ParseUtil.backquote(column.getName());
        if (sourceColumnNames.contains(column.getName())) {
            return backquoted;
        }
        // A MODIFY-prefixed sort-key column -- e.g. an integer key widen -- has no source column under
        // its prefixed name, but its unprefixed origin is still present in the base. Sample the ACTUAL
        // source values cast to the new (wider) type, NOT a constant default: sampling the default would
        // make this column non-distinguishing and collapse the boundaries.
        String originName = Column.removeNamePrefix(column.getName());
        if (sourceColumnNames.contains(originName)) {
            return "CAST(" + ParseUtil.backquote(originName) + " AS " + column.getType().toSql()
                    + ") AS " + backquoted;
        }
        // A genuinely-added key column: no source value, so materialize its constant default.
        // Backslash must be escaped before the double quote, or a trailing backslash in the
        // default would swallow the closing quote and a mid-string backslash would be silently
        // reinterpreted by the StarRocks string-literal lexer.
        String literal = column.getDefaultValue() == null
                ? "NULL"
                : "\"" + column.getDefaultValue().replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        return "CAST(" + literal + " AS " + column.getType().toSql() + ") AS " + backquoted;
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
     * Non-generated column names the rewrite INSERT selects: the newSchema columns present in the base
     * (origin) index schema. For a sort-key reorder or keyness flip the column set is unchanged, so this
     * is a no-op re-projection in the new order; for a DROP it yields the surviving columns; for an ADD
     * it excludes the shadow-only added column, which has no source value in the base.
     */
    @Override
    protected List<String> rewriteSelectColumnNames(@NotNull OlapTable table) {
        // Select each surviving column by its UNPREFIXED name: an unchanged column selects itself; a
        // MODIFY-prefixed type change (e.g. a key widen) selects its unprefixed origin, which
        // InsertPlanner.fillShadowColumns then casts to the (wider) shadow-column type.
        return survivingColumns(table).stream()
                .map(column -> ParseUtil.backquote(Column.removeNamePrefix(column.getName())))
                .collect(Collectors.toList());
    }

    /**
     * Explicit target column list for the rewrite INSERT, needed only when newSchema has FEWER
     * non-generated columns than the base index (a DROP): the INSERT then writes a column subset, so
     * InsertAnalyzer needs an explicit target list naming the survivors (raw names; the base backquotes
     * them once when it builds the INSERT text). A reorder, keyness flip, or ADD keeps the base default
     * (empty = full base schema).
     */
    @Override
    protected List<String> rewriteTargetColumnNames(@NotNull OlapTable table) {
        long baseNonGenerated = table.getSchemaByIndexMetaId(originIndexMetaId).stream()
                .filter(column -> !column.isGeneratedColumn())
                .count();
        List<Column> survivors = survivingColumns(table);
        if (survivors.size() >= baseNonGenerated) {
            return Collections.emptyList();
        }
        return survivors.stream().map(Column::getName).collect(Collectors.toList());
    }

    /**
     * The non-generated newSchema columns whose value comes from the base (origin) index schema, in
     * newSchema order. A column qualifies when its name -- with any {@code __starrocks_shadow_} prefix
     * removed -- is present in the base schema: an unchanged column (no prefix), or a MODIFY-prefixed
     * type change (e.g. an integer key widen) whose unprefixed origin still exists in the base and whose
     * value is a cast of that origin. A genuinely-added ({@code __starrocks_shadow_}-prefixed) column has
     * no unprefixed origin in the base and is excluded (it is default-materialized, not selected).
     */
    private List<Column> survivingColumns(@NotNull OlapTable table) {
        Set<String> baseNames = table.getSchemaByIndexMetaId(originIndexMetaId).stream()
                .map(Column::getName)
                .collect(Collectors.toCollection(() -> Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER)));
        return newSchema.stream()
                .filter(column -> !column.isGeneratedColumn())
                .filter(column -> baseNames.contains(Column.removeNamePrefix(column.getName())))
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
     * The columns whose meaning changed for this range-rewrite, so dependent async MVs built on the old
     * schema get inactivated: every column whose key membership flipped between the origin and new schema,
     * the new sort-key columns (their ordering / role changed) unless this is a pure key ADD that leaves
     * all origin data unchanged, and every origin column dropped from the new schema. Empty when nothing
     * relevant changed and there are no dependent MVs.
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
        // A pure key ADD (newSchema contains a column absent from the origin schema, e.g. a
        // `__starrocks_shadow_`-prefixed added key column) leaves historical base data -- and thus
        // dependent MVs built on the pre-existing sort key -- unchanged, so the new sort-key columns must
        // not be reported as affected in that case. Reorder/keyness-flip (same column set) and DROP
        // (fewer columns; base data does change) still run this loop.
        Set<String> originNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        originNames.addAll(originIsKey.keySet());
        boolean hasAddedColumn = newSchema.stream()
                .anyMatch(column -> !originNames.contains(Column.removeNamePrefix(column.getName())));
        if (!hasAddedColumn) {
            for (Column sortKeyColumn : newSortKeyColumns) {
                affected.add(sortKeyColumn.getName());
            }
        }
        Set<String> newNames = newSchema.stream().map(Column::getName)
                .collect(Collectors.toCollection(() -> Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER)));
        for (Column column : table.getSchemaByIndexMetaId(originIndexMetaId)) {
            if (!newNames.contains(column.getName())) {
                affected.add(column.getName()); // a dropped column: dependent MVs referencing it must inactivate
            }
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
