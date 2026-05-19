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
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.SplitTabletClause;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
        if (!table.isCloudNativeTableOrMaterializedView()) {
            throw new StarRocksException("Unsupported table type " + table.getType()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }

        if (!table.isRangeDistribution()) {
            throw new StarRocksException("Unsupported distribution type " + table.getDefaultDistributionInfo().getType()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }

        // Block re-split while any peer GroupId is unstable: range-colocate group membership is
        // shared across DBs, so a still-unaligned split would compound the unaligned state.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId myGroupId = colocateTableIndex.getRangeColocateGroupId(table.getId());
        if (myGroupId != null && colocateTableIndex.isAnyGroupWithSameColocateGroupIdUnstable(myGroupId.grpId)) {
            throw new StarRocksException("Cannot split tablets for range-colocate group "
                    + myGroupId.grpId + ": group is unstable; wait for alignment to complete before retrying");
        }

        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions = createReshardingPhysicalPartitions();
        if (reshardingPhysicalPartitions.isEmpty()) {
            throw new StarRocksException("No tablets need to split in table "
                    + db.getFullName() + '.' + table.getName());
        }

        createNewShards(table, reshardingPhysicalPartitions);

        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        return new SplitTabletJob(jobId, db.getId(), table.getId(), reshardingPhysicalPartitions);
    }

    /*
     * external boundaries entry point. Build a TabletReshardJob
     * that splits exactly one tablet using FE-supplied K-1 boundaries
     * instead of letting BE compute boundaries from segment distribution.
     *
     * The new-tablet count is implied by {@code newTabletRanges.size()} and
     * must be >= 2. FE owns range validity at this layer; BE re-validates
     * structural and schema-aware invariants and falls back to an identical
     * tablet on any mismatch.
     */
    public static TabletReshardJob forExternalBoundaries(Database db, OlapTable table, long oldTabletId,
            List<TabletRange> newTabletRanges) throws StarRocksException {
        if (!table.isCloudNativeTableOrMaterializedView()) {
            throw new StarRocksException("Unsupported table type " + table.getType()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }
        if (!table.isRangeDistribution()) {
            throw new StarRocksException("Unsupported distribution type "
                    + table.getDefaultDistributionInfo().getType()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }
        Preconditions.checkArgument(newTabletRanges != null && newTabletRanges.size() >= 2,
                "External-boundaries split requires at least 2 new-tablet ranges (got %s)",
                newTabletRanges == null ? "null" : newTabletRanges.size());
        // Mirror the upper bound that TabletReshardUtils.calcSplitCount applies on the
        // data-driven path. External boundaries bypass calcSplitCount, so enforce the cap here so
        // a bad caller cannot allocate an unbounded number of new tablets/shards.
        Preconditions.checkArgument(newTabletRanges.size() <= Config.tablet_reshard_max_split_count,
                "external new-tablet count %s exceeds tablet_reshard_max_split_count %s",
                newTabletRanges.size(), Config.tablet_reshard_max_split_count);

        // Mirror the data-driven path's range-colocate unstable-group guard:
        // refuse to start an external-boundaries split while any peer GroupId is unstable.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId myGroupId = colocateTableIndex.getRangeColocateGroupId(table.getId());
        if (myGroupId != null && colocateTableIndex.isAnyGroupWithSameColocateGroupIdUnstable(myGroupId.grpId)) {
            throw new StarRocksException("Cannot split tablets for range-colocate group "
                    + myGroupId.grpId + ": group is unstable; wait for alignment to complete before retrying");
        }

        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions = new HashMap<>();

        try (AutoCloseableLock lock = new AutoCloseableLock(db.getId(), table.getId(), LockType.READ)) {
            if (table.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new StarRocksException("Unexpected table state " + table.getState()
                        + " in table " + db.getFullName() + '.' + table.getName());
            }

            TabletMeta tabletMeta = GlobalStateMgr.getCurrentState().getTabletInvertedIndex()
                    .getTabletMeta(oldTabletId);
            // getTabletMeta returns null (raw map miss) when the tablet is
            // unknown — distinct from getTabletMetaList's NOT_EXIST sentinel.
            if (tabletMeta == null || tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META
                    || tabletMeta.getTableId() != table.getId()) {
                throw new StarRocksException("Cannot find tablet " + oldTabletId
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
            if (index.getTablet(oldTabletId) == null) {
                throw new StarRocksException("Cannot find tablet " + oldTabletId
                        + " in materialized index " + tabletMeta.getIndexId()
                        + " in physical partition " + physicalPartition.getId()
                        + " in table " + db.getFullName() + '.' + table.getName());
            }

            SplittingTablet splittingTablet = createSplittingTablet(oldTabletId, newTabletRanges);

            // Build the full resharding-tablet list: external boundaries for the chosen
            // tablet, IdenticalTablet for siblings in the same index.
            List<ReshardingTablet> reshardingTablets = new ArrayList<>();
            for (Tablet sibling : index.getTablets()) {
                if (sibling.getId() == oldTabletId) {
                    reshardingTablets.add(splittingTablet);
                } else {
                    reshardingTablets.add(createIdenticalTablet(sibling.getId()));
                }
            }

            Map<Long, ReshardingMaterializedIndex> reshardingIndexes = new HashMap<>();
            reshardingIndexes.put(index.getId(),
                    new ReshardingMaterializedIndex(index.getId(),
                            createMaterializedIndex(index, reshardingTablets),
                            reshardingTablets));

            reshardingPhysicalPartitions.put(physicalPartition.getId(),
                    new ReshardingPhysicalPartition(physicalPartition.getId(), reshardingIndexes));
        }

        createNewShards(table, reshardingPhysicalPartitions);

        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        return new SplitTabletJob(jobId, db.getId(), table.getId(), reshardingPhysicalPartitions);
    }

    /*
     * Create physical partition contexts for all tablets that need to split
     */
    private Map<Long, ReshardingPhysicalPartition> createReshardingPhysicalPartitions() throws StarRocksException {
        Preconditions.checkState(splitTabletClause.getPartitionNames() == null ||
                splitTabletClause.getTabletList() == null);

        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions = new HashMap<>();

        try (AutoCloseableLock lock = new AutoCloseableLock(db.getId(), table.getId(), LockType.READ)) {
            if (table.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new StarRocksException("Unexpected table state " + table.getState()
                        + " in table " + db.getFullName() + '.' + table.getName());
            }

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
                        reshardingIndexes.put(oldIndex.getId(),
                                new ReshardingMaterializedIndex(oldIndex.getId(),
                                        createMaterializedIndex(oldIndex, reshardingTablets),
                                        reshardingTablets));
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
                        reshardingIndexes.put(oldIndex.getId(),
                                new ReshardingMaterializedIndex(oldIndex.getId(),
                                        createMaterializedIndex(oldIndex, reshardingTablets),
                                        reshardingTablets));
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
            TabletMeta tabletMeta = tabletMetas.get(i);
            if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META ||
                    tabletMeta.getTableId() != table.getId()) {
                throw new StarRocksException("Cannot find tablet " + tabletId
                        + " in inverted index in table " + db.getFullName() + '.' + table.getName());
            }

            PhysicalPartition physicalPartition = table.getPhysicalPartition(
                    tabletMeta.getPhysicalPartitionId());
            if (physicalPartition == null) {
                throw new StarRocksException(
                        "Cannot find physical partition " + tabletMeta.getPhysicalPartitionId()
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

            tablets.computeIfAbsent(physicalPartition, k -> new HashMap<>())
                    .computeIfAbsent(index, k -> new HashSet<>())
                    .add(tablet);
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
            for (long tabletId : reshardingTablet.getNewTabletIds()) {
                Tablet tablet = new LakeTablet(tabletId, oldTablet.getRange());
                newIndex.addTablet(tablet, null, false);
            }
        }

        return newIndex;
    }

    private static void createNewShards(OlapTable table,
            Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions) throws StarRocksException {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getRangeColocateGroupId(table.getId());
        // Snapshot colocate ranges once: createShards inside the loop only reads the same
        // grpId, and the ranges list is stable for the duration of this DDL (the unstable
        // guard above blocks concurrent splits).
        List<ColocateRange> colocateRanges = groupId == null ? null
                : colocateTableIndex.getColocateRangeMgr().getColocateRanges(groupId.grpId);
        int colocateColumnCount = groupId == null ? 0
                : colocateTableIndex.getGroupSchema(groupId).getColocateColumnCount();

        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            long physicalPartitionId = reshardingPhysicalPartition.getPhysicalPartitionId();
            PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                    .getReshardingIndexes().values()) {
                MaterializedIndex newIndex = reshardingIndex.getMaterializedIndex();
                MaterializedIndex oldIndex = physicalPartition == null
                        ? null
                        : physicalPartition.getIndex(reshardingIndex.getMaterializedIndexId());
                Map<Long, List<Long>> oldToNewTabletIds = new HashMap<>();
                Map<Long, List<Long>> oldShardIdToGroupIds = new HashMap<>();
                for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                    long oldTabletId = reshardingTablet.getFirstOldTabletId();
                    oldToNewTabletIds.put(oldTabletId, reshardingTablet.getNewTabletIds());
                    List<Long> groupIds = new ArrayList<>(2);
                    groupIds.add(newIndex.getShardGroupId()); // SPREAD group is unchanged
                    if (colocateRanges != null) {
                        // Range-colocate invariant (P1): every tablet of a range-colocate table
                        // has a TabletRange. Fail fast rather than fall back to prefix=null,
                        // which would silently route new shards into the first ColocateRange's
                        // PACK group — wrong once the group has multiple ranges.
                        Preconditions.checkState(oldIndex != null,
                                "Missing old MaterializedIndex for range-colocate split");
                        Tablet oldTablet = oldIndex.getTablet(oldTabletId);
                        Preconditions.checkState(oldTablet != null && oldTablet.getRange() != null,
                                "Old tablet %s in range-colocate group has no TabletRange", oldTabletId);
                        Tuple prefix = ColocateRangeUtils.extractColocatePrefix(
                                oldTablet.getRange().getRange(), colocateColumnCount);
                        int idx = ColocateRangeMgr.indexOf(colocateRanges, prefix);
                        Preconditions.checkState(idx >= 0,
                                "Old tablet %s has no covering ColocateRange", oldTabletId);
                        groupIds.add(colocateRanges.get(idx).getShardGroupId());
                    }
                    oldShardIdToGroupIds.put(oldTabletId, groupIds);
                }

                Map<String, String> properties = new HashMap<>();
                properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
                properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartitionId));
                properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(newIndex.getId()));

                GlobalStateMgr.getCurrentState().getStarOSAgent().createShardsForSplit(
                        oldShardIdToGroupIds,
                        oldToNewTabletIds,
                        table.getPartitionFilePathInfo(physicalPartitionId),
                        table.getPartitionFileCacheInfo(physicalPartitionId),
                        properties, WarehouseManager.DEFAULT_RESOURCE);
            }
        }
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
     * dispatches to the external boundaries external-boundaries path on publish.
     *
     * <p>Differs from the user-facing {@link #createTabletReshardJob} and the load-time
     * external boundaries {@link #forExternalBoundaries} entries in two ways:
     * <ul>
     * <li><b>Unstable-guard bypass.</b> The checker IS the unstable-state resolver — if it
     *     refused to act on unstable groups, the group would stay unstable forever.</li>
     * <li><b>Per-new-tablet PACK assignment.</b> Each child of an alignment split lands in
     *     a different {@link ColocateRange} (that's the whole point of the alignment), so
     *     its PACK shard group differs from its siblings'. Uses the per-new-shard StarOS
     *     overload {@link com.starrocks.lake.StarOSAgent#createShardsForSplitPerNewShard}
     *     instead of the per-old-shard {@code createShardsForSplit} that the user-facing
     *     and load-time paths use.</li>
     * </ul>
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
        if (!table.isCloudNativeTableOrMaterializedView()) {
            throw new StarRocksException("Unsupported table type " + table.getType()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }
        if (!table.isRangeDistribution()) {
            throw new StarRocksException("Unsupported distribution type "
                    + table.getDefaultDistributionInfo().getType()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }

        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions = new HashMap<>();
        try (AutoCloseableLock lock = new AutoCloseableLock(db.getId(), table.getId(), LockType.READ)) {
            if (table.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new StarRocksException("Unexpected table state " + table.getState()
                        + " in table " + db.getFullName() + '.' + table.getName());
            }
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
                    reshardingIndexes.put(oldIndex.getId(),
                            new ReshardingMaterializedIndex(oldIndex.getId(),
                                    createMaterializedIndex(oldIndex, reshardingTablets),
                                    reshardingTablets));
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

        createNewShardsForColocateAlignment(table, reshardingPhysicalPartitions);

        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        return new SplitTabletJob(jobId, db.getId(), table.getId(), reshardingPhysicalPartitions);
    }

    /**
     * Alignment-path variant of {@link #createNewShards}: each new tablet's PACK shard
     * group is derived from the lower bound of its assigned range (looked up in
     * {@link ColocateRangeMgr}) — sibling children of the same old tablet can land in
     * different PACK groups when the alignment split crosses a colocate boundary.
     *
     * <p>Identical tablets inherit the old tablet's PACK group via the same prefix lookup,
     * matching how user-driven splits keep identical tablets in their existing groups.
     */
    private static void createNewShardsForColocateAlignment(OlapTable table,
            Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions) throws StarRocksException {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getRangeColocateGroupId(table.getId());
        Preconditions.checkState(groupId != null,
                "alignment split called for non-range-colocate table %s", table.getName());
        List<ColocateRange> colocateRanges =
                colocateTableIndex.getColocateRangeMgr().getColocateRanges(groupId.grpId);
        int colocateColumnCount = colocateTableIndex.getGroupSchema(groupId).getColocateColumnCount();

        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            long physicalPartitionId = reshardingPhysicalPartition.getPhysicalPartitionId();
            PhysicalPartition physicalPartition = table.getPhysicalPartition(physicalPartitionId);
            if (physicalPartition == null) {
                continue;
            }
            for (ReshardingMaterializedIndex reshardingIndex :
                    reshardingPhysicalPartition.getReshardingIndexes().values()) {
                MaterializedIndex newIndex = reshardingIndex.getMaterializedIndex();
                MaterializedIndex oldIndex = physicalPartition.getIndex(reshardingIndex.getMaterializedIndexId());
                if (oldIndex == null) {
                    continue;
                }
                Map<Long, Long> newToOldShardId = new HashMap<>();
                Map<Long, List<Long>> newShardIdToGroupIds = new HashMap<>();
                for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                    long firstOldTabletId = reshardingTablet.getFirstOldTabletId();
                    Tablet oldTablet = oldIndex.getTablet(firstOldTabletId);
                    Preconditions.checkState(oldTablet != null && oldTablet.getRange() != null,
                            "old tablet %s missing range during alignment", firstOldTabletId);
                    SplittingTablet splittingTablet = reshardingTablet.getSplittingTablet();
                    if (splittingTablet == null) {
                        // IdenticalTablet — single new shard, inherits old tablet's PACK group.
                        long newTabletId = reshardingTablet.getFirstNewTabletId();
                        List<Long> groupIds = findPlacementGroupIdsForRange(
                                colocateRanges, newIndex.getShardGroupId(),
                                oldTablet.getRange().getRange(), colocateColumnCount);
                        newToOldShardId.put(newTabletId, firstOldTabletId);
                        newShardIdToGroupIds.put(newTabletId, groupIds);
                        continue;
                    }
                    List<TabletRange> newRanges = splittingTablet.getNewTabletRanges();
                    List<Long> newTabletIds = splittingTablet.getNewTabletIds();
                    Preconditions.checkState(!newRanges.isEmpty()
                                    && newRanges.size() == newTabletIds.size(),
                            "alignment splitting tablet %s has %s ranges but %s ids",
                            firstOldTabletId, newRanges.size(), newTabletIds.size());
                    for (int i = 0; i < newTabletIds.size(); i++) {
                        long newTabletId = newTabletIds.get(i);
                        List<Long> groupIds = findPlacementGroupIdsForRange(
                                colocateRanges, newIndex.getShardGroupId(),
                                newRanges.get(i).getRange(), colocateColumnCount);
                        newToOldShardId.put(newTabletId, firstOldTabletId);
                        newShardIdToGroupIds.put(newTabletId, groupIds);
                    }
                }
                if (newToOldShardId.isEmpty()) {
                    continue;
                }
                // Defense-in-depth: re-validate leader admission immediately before each StarOS
                // RPC. The checker already gated at submit time, but the loop spans many
                // partitions / indexes and demotion can fire mid-iteration.
                if (!GlobalStateMgr.getCurrentState().isLeaderWorkAdmissionOpen()) {
                    throw new StarRocksException(
                            "leader work admission closed mid-alignment; aborting before StarOS shard creation");
                }
                Map<String, String> properties = new HashMap<>();
                properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
                properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartitionId));
                properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(newIndex.getId()));
                GlobalStateMgr.getCurrentState().getStarOSAgent().createShardsForSplitPerNewShard(
                        newToOldShardId,
                        newShardIdToGroupIds,
                        table.getPartitionFilePathInfo(physicalPartitionId),
                        table.getPartitionFileCacheInfo(physicalPartitionId),
                        properties, WarehouseManager.DEFAULT_RESOURCE);
            }
        }
    }

    /**
     * Returns the {@code [SPREAD, PACK]} placement group ids for the {@link ColocateRange}
     * that owns the colocate prefix of {@code range}'s lower bound. The SPREAD group is shared
     * across all new shards of an index; the PACK group is looked up from {@link ColocateRangeMgr}
     * so each new shard joins the PACK group of its target {@link ColocateRange}.
     */
    private static List<Long> findPlacementGroupIdsForRange(List<ColocateRange> colocateRanges, long spreadGroupId,
                                                     Range<Tuple> range,
                                                     int colocateColumnCount) {
        Tuple prefix = ColocateRangeUtils.extractColocatePrefix(range, colocateColumnCount);
        int rangeIndex = ColocateRangeMgr.indexOf(colocateRanges, prefix);
        Preconditions.checkState(rangeIndex >= 0,
                "alignment child has no covering ColocateRange for prefix %s", prefix);
        return List.of(spreadGroupId, colocateRanges.get(rangeIndex).getShardGroupId());
    }
}
