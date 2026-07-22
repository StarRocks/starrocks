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

package com.starrocks.alter.reshard;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeMgr;
import com.starrocks.catalog.ColocateRangeUtils;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.TabletRange;
import com.starrocks.catalog.Tuple;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.SplitTabletClause;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/*
 * SplitTabletJobFactory is for creating TabletReshardJob for tablet splitting.
 */
public class SplitTabletJobFactory implements TabletReshardJobFactory {
    private static final Logger LOG = LogManager.getLogger(SplitTabletJobFactory.class);

    private final Database db;

    private final OlapTable table;

    private final SplitTabletClause splitTabletClause;

    public SplitTabletJobFactory(Database db, OlapTable table, SplitTabletClause splitTabletClause) {
        this.db = db;
        this.table = table;
        this.splitTabletClause = splitTabletClause;
    }

    /*
     * Create a tablet reshard job and return it.
     * New shareds are created for new tablets.
     */
    @Override
    public TabletReshardJob createTabletReshardJob() throws StarRocksException {
        validateTableLevel(db, table);

        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions = createReshardingPhysicalPartitions();
        if (reshardingPhysicalPartitions.isEmpty()) {
            throw new StarRocksException("No tablets need to split in table "
                    + db.getFullName() + '.' + table.getName());
        }

        return newSplitTabletJob(db, table, reshardingPhysicalPartitions);
    }

    /**
     * External-boundaries entry point. Builds ONE {@link SplitTabletJob} whose splittingTablets
     * span every entry in {@code oldTabletIdToRanges}, using FE-supplied K-1 boundaries instead
     * of letting BE compute boundaries from segment distribution. Handles one or many tablets;
     * a single-entry map is the single-tablet case.
     *
     * <p>The new-tablet count for each tablet is implied by its range-list size and must be
     * >= 2. FE owns range validity at this layer; BE re-validates structural and schema-aware
     * invariants and falls back to an identical tablet on any mismatch.
     *
     * <p>The job is metadata-only at this point; StarOS shard allocation happens later in
     * {@link SplitTabletJob#createShardsOnStarOS} after admission, which iterates
     * {@code reshardingPhysicalPartitions} and so handles multiple partitions with no further
     * substrate change.
     *
     * <p>No partial state is left behind on throw — the factory has no side effects (no shard
     * allocation, no catalog mutation).
     *
     * @throws IllegalArgumentException when {@code oldTabletIdToRanges} is empty or an entry's
     *         range count is outside {@code [2, tablet_reshard_max_split_count]}.
     * @throws StarRocksException when table-level preconditions fail (not cloud-native, not
     *         range-distribution, not in NORMAL state, or a colocate peer is unstable), or
     *         catalog lookup fails for any input old tablet id.
     */
    public static TabletReshardJob forExternalBoundaries(Database db, OlapTable table,
            Map<Long, List<TabletRange>> oldTabletIdToRanges) throws StarRocksException {
        Preconditions.checkArgument(oldTabletIdToRanges != null && !oldTabletIdToRanges.isEmpty(),
                "External-boundaries split requires a non-empty oldTabletIdToRanges map");

        validateTableLevel(db, table);

        // Per-entry shape check. Run BEFORE the catalog lookup loop so a bad
        // caller fails fast without grabbing a table lock.
        for (Map.Entry<Long, List<TabletRange>> entry : oldTabletIdToRanges.entrySet()) {
            validateRangeListShape(entry.getValue());
        }

        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions = new HashMap<>();

        try (AutoCloseableLock lock = new AutoCloseableLock(db.getId(), table.getId(), LockType.READ)) {
            checkTableNormalState(db, table);

            // Group input entries by (physicalPartition, materializedIndex) so each
            // ReshardingMaterializedIndex carries all of its targets in one pass.
            // physicalPartitionId -> indexId -> oldTabletId -> ranges
            Map<Long, Map<Long, Map<Long, List<TabletRange>>>> groupedByPartitionAndIndex = new LinkedHashMap<>();

            for (Map.Entry<Long, List<TabletRange>> entry : oldTabletIdToRanges.entrySet()) {
                long oldTabletId = entry.getKey();
                TabletMeta tabletMeta = GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                        .getTabletMeta(oldTabletId);
                ResolvedTablet resolved = resolveTabletMeta(db, table, oldTabletId, tabletMeta);

                groupedByPartitionAndIndex
                        .computeIfAbsent(resolved.physicalPartition().getId(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(resolved.index().getId(), k -> new LinkedHashMap<>())
                        .put(oldTabletId, entry.getValue());
            }

            for (Map.Entry<Long, Map<Long, Map<Long, List<TabletRange>>>> partitionEntry
                    : groupedByPartitionAndIndex.entrySet()) {
                long physicalPartitionId = partitionEntry.getKey();
                // Re-fetch the index per physical partition. A flat indexById map keyed by
                // index.getId() is unsafe here: a freshly-created partition's base
                // MaterializedIndex id can equal the index meta id, which is shared across
                // every physical partition of a table, so a later partition's entry would
                // overwrite an earlier partition's and the rebuild loop would enumerate the
                // wrong partition's tablets.
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                if (physicalPartition == null) {
                    throw new StarRocksException("Cannot find physical partition " + physicalPartitionId
                            + " in table " + db.getFullName() + '.' + table.getName());
                }
                Map<Long, ReshardingMaterializedIndex> reshardingIndexes = new HashMap<>();
                for (Map.Entry<Long, Map<Long, List<TabletRange>>> indexEntry
                        : partitionEntry.getValue().entrySet()) {
                    long indexId = indexEntry.getKey();
                    MaterializedIndex index = physicalPartition.getIndex(indexId);
                    if (index == null) {
                        throw new StarRocksException("Cannot find materialized index " + indexId
                                + " in physical partition " + physicalPartitionId
                                + " in table " + db.getFullName() + '.' + table.getName());
                    }
                    Map<Long, List<TabletRange>> targetTablets = indexEntry.getValue();

                    // Build the full resharding-tablet list: external boundaries for every
                    // chosen old tablet, IdenticalTablet for siblings in the same index.
                    List<ReshardingTablet> reshardingTablets = new ArrayList<>();
                    for (Tablet sibling : index.getTablets()) {
                        List<TabletRange> ranges = targetTablets.get(sibling.getId());
                        if (ranges != null) {
                            reshardingTablets.add(createSplittingTablet(sibling.getId(), ranges));
                        } else {
                            reshardingTablets.add(createIdenticalTablet(sibling.getId()));
                        }
                    }

                    reshardingIndexes.put(indexId, newReshardingIndex(index, reshardingTablets));
                }
                reshardingPhysicalPartitions.put(physicalPartitionId,
                        new ReshardingPhysicalPartition(physicalPartitionId, reshardingIndexes));
            }
        }

        // Surface bad caller-supplied ranges synchronously, before the job is journaled
        // and mutates table state. SplitTabletJob.runPendingJob would otherwise hit a
        // Preconditions failure in createShardsOnStarOS, abort the job, and force the
        // operator to fish the error out of SHOW TABLET RESHARD JOBS instead of seeing
        // it on the DDL statement itself.
        verifyNewTabletRanges(table, reshardingPhysicalPartitions);

        // Pre-split: spread the new shards across compute nodes rather than PACKing them onto the
        // (empty) source tablet's single worker, so the load that follows parallelizes across BEs.
        // The triggering load's warehouse is applied by the caller via TabletReshardJob#setWarehouseId.
        return newSplitTabletJob(db, table, reshardingPhysicalPartitions);
    }

    /**
     * Table-type + distribution preconditions shared by every factory entry:
     * cloud-native and range-distribution. Excludes the unstable-group guard so
     * {@link #forColocateAlignment} — which is the resolver of unstable state —
     * can reuse this without the guard.
     */
    private static void validateTableDistribution(Database db, OlapTable table) throws StarRocksException {
        if (!table.isCloudNativeTableOrMaterializedView()) {
            throw new StarRocksException("Unsupported table type " + table.getType()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }
        if (!table.isRangeDistribution()) {
            throw new StarRocksException("Unsupported distribution type "
                    + table.getDefaultDistributionInfo().getType()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }
    }

    /**
     * Shared table-level preconditions for the user-facing and external-boundaries
     * entries: {@link #validateTableDistribution} plus the range-colocate
     * unstable-group guard. Run BEFORE the table lock is acquired so a bad
     * caller fails fast.
     */
    private static void validateTableLevel(Database db, OlapTable table) throws StarRocksException {
        validateTableDistribution(db, table);
        // Refuse to start a split while any peer GroupId is unstable: range-colocate group
        // membership is shared across DBs, so a still-unaligned split compounds the unaligned state.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId myGroupId = colocateTableIndex.getRangeColocateGroupId(table.getId());
        if (myGroupId != null && colocateTableIndex.isAnyGroupWithSameColocateGroupIdUnstable(myGroupId.grpId)) {
            throw new StarRocksException("Cannot split tablets for range-colocate group "
                    + myGroupId.grpId + ": group is unstable; wait for alignment to complete before retrying");
        }
    }

    /**
     * In-lock guard: the table must be in NORMAL state to build a reshard job.
     */
    private static void checkTableNormalState(Database db, OlapTable table) throws StarRocksException {
        if (table.getState() != OlapTable.OlapTableState.NORMAL) {
            throw new StarRocksException("Unexpected table state " + table.getState()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }
    }

    private record ResolvedTablet(PhysicalPartition physicalPartition, MaterializedIndex index, Tablet tablet) {
    }

    /**
     * Resolves an old tablet id to its (physical partition, materialized index, tablet) triple,
     * validating each level with the errors the factory entries share. Accepts a tablet meta
     * fetched by either the single {@code getTabletMeta} (null on a raw-map miss) or the batch
     * {@code getTabletMetaList} (NOT_EXIST sentinel) — both are treated as "not found".
     */
    private static ResolvedTablet resolveTabletMeta(Database db, OlapTable table, long tabletId,
            TabletMeta tabletMeta) throws StarRocksException {
        if (tabletMeta == null || tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META
                || tabletMeta.getTableId() != table.getId()) {
            throw new StarRocksException("Cannot find tablet " + tabletId
                    + " in inverted index in table " + db.getFullName() + '.' + table.getName());
        }

        PhysicalPartition physicalPartition = table.getPhysicalPartition(tabletMeta.getPhysicalPartitionId());
        if (physicalPartition == null) {
            throw new StarRocksException("Cannot find physical partition "
                    + tabletMeta.getPhysicalPartitionId()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }

        MaterializedIndex index = physicalPartition.getIndex(tabletMeta.getIndexId());
        if (index == null) {
            throw new StarRocksException("Cannot find materialized index " + tabletMeta.getIndexId()
                    + " in physical partition " + physicalPartition.getId()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }
        if (index.getState() != IndexState.NORMAL) {
            throw new StarRocksException("Not a normal state materialized index " + tabletMeta.getIndexId()
                    + " in physical partition " + physicalPartition.getId()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }
        Tablet tablet = index.getTablet(tabletId);
        if (tablet == null) {
            throw new StarRocksException("Cannot find tablet " + tabletId
                    + " in materialized index " + tabletMeta.getIndexId()
                    + " in physical partition " + physicalPartition.getId()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }
        return new ResolvedTablet(physicalPartition, index, tablet);
    }

    /**
     * Shape validation for a single external-boundaries range list: size >= 2
     * and <= {@code tablet_reshard_max_split_count}. Mirrors the upper bound
     * that {@code TabletReshardUtils.calcSplitCount} applies on the
     * data-driven path; external boundaries bypass that helper so the cap is
     * enforced here.
     */
    private static void validateRangeListShape(List<TabletRange> newTabletRanges) {
        Preconditions.checkArgument(newTabletRanges != null && newTabletRanges.size() >= 2,
                "External-boundaries split requires at least 2 new-tablet ranges (got %s)",
                newTabletRanges == null ? "null" : newTabletRanges.size());
        Preconditions.checkArgument(newTabletRanges.size() <= Config.tablet_reshard_max_split_count,
                "external new-tablet count %s exceeds tablet_reshard_max_split_count %s",
                newTabletRanges.size(), Config.tablet_reshard_max_split_count);
    }

    /**
     * For range-colocate tables, verify every FE-supplied new-tablet range has a covering
     * {@link ColocateRange} so the PACK lookup in {@link SplitTabletJob#createShardsOnStarOS}
     * will succeed. No-op when the table is not range-colocate or when no per-new-tablet
     * ranges were supplied (user-facing path).
     */
    private static void verifyNewTabletRanges(OlapTable table,
            Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions) throws StarRocksException {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getRangeColocateGroupId(table.getId());
        if (groupId == null) {
            return;
        }
        List<ColocateRange> colocateRanges = colocateTableIndex.getColocateRanges(groupId.grpId);
        int colocateColumnCount = colocateTableIndex.getGroupSchema(groupId).getColocateColumnCount();
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            for (ReshardingMaterializedIndex reshardingIndex
                    : reshardingPhysicalPartition.getReshardingIndexes().values()) {
                for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                    SplittingTablet splittingTablet = reshardingTablet.getSplittingTablet();
                    if (splittingTablet == null) {
                        continue;
                    }
                    for (TabletRange newTabletRange : splittingTablet.getNewTabletRanges()) {
                        Tuple prefix = ColocateRangeUtils.extractColocatePrefix(
                                newTabletRange.getRange(), colocateColumnCount);
                        if (ColocateRangeMgr.indexOf(colocateRanges, prefix) < 0) {
                            throw new StarRocksException(
                                    "Tablet " + reshardingTablet.getFirstOldTabletId()
                                            + " split: new tablet range has no covering ColocateRange"
                                            + " for colocate prefix " + prefix
                                            + " in range-colocate group " + groupId.grpId);
                        }
                    }
                }
            }
        }
    }

    /*
     * Create physical partition contexts for all tablets that need to split
     */
    private Map<Long, ReshardingPhysicalPartition> createReshardingPhysicalPartitions() throws StarRocksException {
        Preconditions.checkState(splitTabletClause.getPartitionNames() == null ||
                splitTabletClause.getTabletList() == null);

        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions = new HashMap<>();

        try (AutoCloseableLock lock = new AutoCloseableLock(db.getId(), table.getId(), LockType.READ)) {
            checkTableNormalState(db, table);

            if (splitTabletClause.getTabletList() != null) {
                Map<PhysicalPartition, Map<MaterializedIndex, Collection<Tablet>>> tablets = getTabletsByTabletIds(
                        splitTabletClause.getTabletList().getTabletIds());

                for (var physicalPartitionEntry : tablets.entrySet()) {
                    PhysicalPartition physicalPartition = physicalPartitionEntry.getKey();
                    Map<Long, ReshardingMaterializedIndex> reshardingIndexes = new HashMap<>();
                    for (var indexEntry : physicalPartitionEntry.getValue().entrySet()) {
                        MaterializedIndex oldIndex = indexEntry.getKey();

                        Map<Long, SplittingTablet> splittingTablets = new HashMap<>();
                        for (Tablet tablet : indexEntry.getValue()) {
                            int newTabletCount = TabletReshardUtils.calcSplitCount(tablet.getDataSize(true),
                                    splitTabletClause.getTabletReshardTargetSize());

                            if (newTabletCount <= 0) {
                                throw new StarRocksException("Invalid tablet_reshard_target_size: "
                                        + splitTabletClause.getTabletReshardTargetSize());
                            }

                            if (newTabletCount <= 1) {
                                continue;
                            }

                            splittingTablets.put(tablet.getId(), createSplittingTablet(tablet.getId(), newTabletCount));
                        }

                        if (splittingTablets.isEmpty()) {
                            continue;
                        }

                        List<ReshardingTablet> reshardingTablets = createReshardingTablets(oldIndex, splittingTablets);
                        reshardingIndexes.put(oldIndex.getId(), newReshardingIndex(oldIndex, reshardingTablets));
                    }

                    if (reshardingIndexes.isEmpty()) {
                        continue;
                    }

                    reshardingPhysicalPartitions.put(physicalPartition.getId(),
                            new ReshardingPhysicalPartition(physicalPartition.getId(), reshardingIndexes));
                }
            } else {
                Collection<PhysicalPartition> physicalPartitions = null;
                if (splitTabletClause.getPartitionNames() == null) {
                    physicalPartitions = table.getPhysicalPartitions();
                } else {
                    physicalPartitions = new ArrayList<>();
                    for (String partitonName : splitTabletClause.getPartitionNames().getPartitionNames()) {
                        Partition partition = table.getPartition(partitonName);
                        if (partition == null) {
                            throw new StarRocksException("Cannot find partition " + partitonName
                                    + " in table " + db.getFullName() + '.' + table.getName());
                        }
                        physicalPartitions.addAll(partition.getSubPartitions());
                    }
                }

                for (PhysicalPartition physicalPartition : physicalPartitions) {
                    Map<Long, ReshardingMaterializedIndex> reshardingIndexes = new HashMap<>();
                    for (MaterializedIndex oldIndex : physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {

                        Map<Long, SplittingTablet> splittingTablets = new HashMap<>();
                        for (Tablet tablet : oldIndex.getTablets()) {
                            // When not specifying which tablets to split,
                            // tablet_reshard_target_size must be greater than 0
                            Preconditions.checkState(splitTabletClause.getTabletReshardTargetSize() > 0,
                                    "Invalid tablet_reshard_target_size: "
                                            + splitTabletClause.getTabletReshardTargetSize());

                            int newTabletCount = TabletReshardUtils.calcSplitCount(tablet.getDataSize(true),
                                    splitTabletClause.getTabletReshardTargetSize());

                            if (newTabletCount <= 1) {
                                continue;
                            }

                            splittingTablets.put(tablet.getId(), createSplittingTablet(tablet.getId(), newTabletCount));
                        }

                        if (splittingTablets.isEmpty()) {
                            continue;
                        }

                        List<ReshardingTablet> reshardingTablets = createReshardingTablets(oldIndex, splittingTablets);
                        reshardingIndexes.put(oldIndex.getId(), newReshardingIndex(oldIndex, reshardingTablets));
                    }

                    if (reshardingIndexes.isEmpty()) {
                        continue;
                    }

                    reshardingPhysicalPartitions.put(physicalPartition.getId(),
                            new ReshardingPhysicalPartition(physicalPartition.getId(), reshardingIndexes));
                }
            }
        }

        return reshardingPhysicalPartitions;
    }

    private Map<PhysicalPartition, Map<MaterializedIndex, Collection<Tablet>>> getTabletsByTabletIds(
            List<Long> tabletIds) throws StarRocksException {
        Map<PhysicalPartition, Map<MaterializedIndex, Collection<Tablet>>> tablets = new HashMap<>();

        List<TabletMeta> tabletMetas = GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                .getTabletMetaList(tabletIds);
        for (int i = 0; i < tabletMetas.size(); ++i) {
            long tabletId = tabletIds.get(i);
            ResolvedTablet resolved = resolveTabletMeta(db, table, tabletId, tabletMetas.get(i));

            tablets.computeIfAbsent(resolved.physicalPartition(), k -> new HashMap<>())
                    .computeIfAbsent(resolved.index(), k -> new HashSet<>())
                    .add(resolved.tablet());
        }

        return tablets;
    }

    private List<ReshardingTablet> createReshardingTablets(MaterializedIndex index,
            Map<Long, SplittingTablet> splittingTablets) {
        List<ReshardingTablet> reshardingTablets = new ArrayList<>();
        for (Tablet tablet : index.getTablets()) {
            SplittingTablet splittingTablet = splittingTablets.get(tablet.getId());
            if (splittingTablet != null) {
                reshardingTablets.add(splittingTablet);
            } else {
                reshardingTablets.add(createIdenticalTablet(tablet.getId()));
            }
        }

        return reshardingTablets;
    }

    private static MaterializedIndex createMaterializedIndex(MaterializedIndex oldIndex,
            List<ReshardingTablet> reshardingTablets) {
        MaterializedIndex newIndex = new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId(), oldIndex.getMetaId(),
                IndexState.NORMAL, oldIndex.getShardGroupId());

        for (ReshardingTablet reshardingTablet : reshardingTablets) {
            Tablet oldTablet = oldIndex.getTablet(reshardingTablet.getFirstOldTabletId());
            Preconditions.checkNotNull(oldTablet, "Not found tablet " + reshardingTablet.getFirstOldTabletId());
            // Carry the async vector-index build watermark forward onto each split child (its single
            // parent's watermark), mirroring the BE side. No-op for non-vector tables.
            long vibv = TabletReshardUtils.minVectorIndexBuiltVersion(oldIndex, reshardingTablet.getOldTabletIds());
            for (long tabletId : reshardingTablet.getNewTabletIds()) {
                LakeTablet tablet = new LakeTablet(tabletId, oldTablet.getRange());
                tablet.setVectorIndexBuiltVersion(vibv);
                newIndex.addTablet(tablet, null, false);
            }
        }

        return newIndex;
    }

    private static ReshardingMaterializedIndex newReshardingIndex(MaterializedIndex oldIndex,
            List<ReshardingTablet> reshardingTablets) {
        return new ReshardingMaterializedIndex(oldIndex.getId(),
                createMaterializedIndex(oldIndex, reshardingTablets),
                reshardingTablets);
    }

    private static TabletReshardJob newSplitTabletJob(Database db, OlapTable table,
            Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions) {
        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        return new SplitTabletJob(jobId, db.getId(), table.getId(), reshardingPhysicalPartitions);
    }

    private static SplittingTablet createSplittingTablet(long oldTabletId, int newTabletCount) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<Long> newTabletIds = new ArrayList<>(newTabletCount);
        for (int j = 0; j < newTabletCount; ++j) {
            newTabletIds.add(globalStateMgr.getNextId());
        }
        return new SplittingTablet(oldTabletId, newTabletIds);
    }

    // external-boundaries overload: allocate K new tablet ids matching K FE-supplied ranges
    // and forward both into SplittingTablet so the BE dispatches to the
    // external-boundaries path on publish.
    private static SplittingTablet createSplittingTablet(long oldTabletId, List<TabletRange> newTabletRanges) {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<Long> newTabletIds = new ArrayList<>(newTabletRanges.size());
        for (int j = 0; j < newTabletRanges.size(); ++j) {
            newTabletIds.add(globalStateMgr.getNextId());
        }
        return new SplittingTablet(oldTabletId, newTabletIds, newTabletRanges);
    }

    private static IdenticalTablet createIdenticalTablet(long oldTabletId) {
        long newTabletId = GlobalStateMgr.getCurrentState().getNextId();
        return new IdenticalTablet(oldTabletId, newTabletId);
    }

    /**
     * Checker-only entry for {@link ColocateChecker}. Builds one
     * {@link SplitTabletJob} covering every misaligned (partition, visible index, tablet)
     * tuple in {@code alignmentMap}, using FE-supplied per-new-tablet ranges so the BE
     * dispatches to the external-boundaries path on publish.
     *
     * <p>Differs from the user-facing {@link #createTabletReshardJob} and the load-time
     * {@link #forExternalBoundaries} entries in one essential way: the
     * <b>unstable-guard is bypassed</b>. The checker IS the unstable-state resolver — if it
     * refused to act on unstable groups, the group would stay unstable forever.
     *
     * <p>Per-new-tablet PACK assignment is no longer a property of this entry. The unified
     * {@link SplitTabletJob#createShardsOnStarOS} method derives each new shard's PACK
     * group from its own FE-supplied range when one exists, falling back to the parent
     * old tablet's range otherwise; alignment is the case where per-new-tablet PACK groups
     * actually diverge from a per-parent lookup.
     *
     * @param alignmentMap {@code physicalPartitionId → indexId → oldTabletId → ranges that
     *                     tile the old tablet's range exactly}. Caller ensures every range
     *                     list has size ≥ 2 (otherwise no work is needed for that tablet)
     *                     and tiles the old tablet range exactly.
     */
    public static TabletReshardJob forColocateAlignment(Database db, OlapTable table,
            Map<Long, Map<Long, Map<Long, List<TabletRange>>>> alignmentMap) throws StarRocksException {
        Preconditions.checkArgument(alignmentMap != null && !alignmentMap.isEmpty(),
                "alignmentMap must be non-empty");
        // Deliberately NOT validateTableLevel: the checker is the resolver of unstable state, so
        // it must act even while the group is unstable (see method javadoc). Distribution only.
        validateTableDistribution(db, table);

        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions = new HashMap<>();
        try (AutoCloseableLock lock = new AutoCloseableLock(db.getId(), table.getId(), LockType.READ)) {
            checkTableNormalState(db, table);
            for (Map.Entry<Long, Map<Long, Map<Long, List<TabletRange>>>> partitionEntry : alignmentMap.entrySet()) {
                long physicalPartitionId = partitionEntry.getKey();
                PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
                if (physicalPartition == null) {
                    // Partition dropped between checker snapshot and factory build; skip.
                    continue;
                }
                Map<Long, ReshardingMaterializedIndex> reshardingIndexes = new HashMap<>();
                for (Map.Entry<Long, Map<Long, List<TabletRange>>> indexEntry : partitionEntry.getValue().entrySet()) {
                    long indexId = indexEntry.getKey();
                    MaterializedIndex oldIndex = physicalPartition.getIndex(indexId);
                    if (oldIndex == null || oldIndex.getState() != IndexState.NORMAL) {
                        continue;
                    }
                    Map<Long, List<TabletRange>> tabletToRanges = indexEntry.getValue();
                    if (tabletToRanges.isEmpty()) {
                        continue;
                    }
                    List<ReshardingTablet> reshardingTablets = new ArrayList<>();
                    for (Tablet sibling : oldIndex.getTablets()) {
                        List<TabletRange> newRanges = tabletToRanges.get(sibling.getId());
                        if (newRanges != null && newRanges.size() >= 2) {
                            reshardingTablets.add(createSplittingTablet(sibling.getId(), newRanges));
                        } else {
                            reshardingTablets.add(createIdenticalTablet(sibling.getId()));
                        }
                    }
                    reshardingIndexes.put(oldIndex.getId(), newReshardingIndex(oldIndex, reshardingTablets));
                }
                if (reshardingIndexes.isEmpty()) {
                    continue;
                }
                reshardingPhysicalPartitions.put(physicalPartitionId,
                        new ReshardingPhysicalPartition(physicalPartitionId, reshardingIndexes));
            }
        }

        if (reshardingPhysicalPartitions.isEmpty()) {
            throw new StarRocksException("No tablets need alignment in table "
                    + db.getFullName() + '.' + table.getName());
        }

        return newSplitTabletJob(db, table, reshardingPhysicalPartitions);
    }

}
