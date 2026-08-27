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
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.MergeTabletClause;
import com.starrocks.sql.ast.TabletGroupList;
import com.starrocks.sql.common.MetaUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/*
 * MergeTabletJobFactory is for creating TabletReshardJob for tablet merging.
 */
public class MergeTabletJobFactory implements TabletReshardJobFactory {
    private final Database db;
    private final OlapTable table;
    private final MergeTabletClause mergeTabletClause;

    // Set when a tablet was skipped only because its size statistics were older than the partition's
    // visible version. That makes an empty plan TRANSIENT rather than deterministic, which decides
    // which exception the empty case throws -- see createTabletReshardJob. A new factory is built per
    // invocation, so this is per-plan state, not shared.
    private boolean sawStaleTabletStats;

    public MergeTabletJobFactory(Database db, OlapTable table, MergeTabletClause mergeTabletClause) {
        this.db = db;
        this.table = table;
        this.mergeTabletClause = mergeTabletClause;
    }

    /*
     * Create a tablet reshard job and return it.
     * New shards are created for new tablets.
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

        // Refuse to start a merge while any peer GroupId is unstable: range-colocate group membership
        // is shared across DBs, so merging against ranges the colocate checker is still aligning would
        // plan on a snapshot that is about to change. Mirrors SplitTabletJobFactory#validateTableLevel.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId myGroupId = colocateTableIndex.getRangeColocateGroupId(table.getId());
        if (myGroupId != null && colocateTableIndex.isAnyGroupWithSameColocateGroupIdUnstable(myGroupId.grpId)) {
            throw new StarRocksException("Cannot merge tablets for range-colocate group "
                    + myGroupId.grpId + ": group is unstable; wait for alignment to complete before retrying");
        }
        // A registered range-colocate group whose range list is empty has a topology we cannot see --
        // its OP_COLOCATE_RANGE_UPDATE has not been replayed yet. Refuse rather than treat it as
        // not-colocate: merging without boundary knowledge would create SPREAD-only shards and, once
        // the topology appears, nothing repairs them -- ColocateChecker only visits UNSTABLE groups,
        // and a merge that never marked one leaves no trace. Merge is an optimization, so declining it
        // costs nothing.
        if (myGroupId != null && colocateTableIndex.getColocateRanges(myGroupId.grpId).isEmpty()) {
            throw new StarRocksException("Cannot merge tablets for range-colocate group "
                    + myGroupId.grpId + ": its colocate ranges are not available yet");
        }

        // Compute the parallelism floor before acquiring the table lock — it touches warehouse / node
        // state and must not be coupled to the table READ lock held inside createReshardingPhysicalPartitions.
        // Only the size-based auto-merge path is floor-gated; explicit tablet-group merges skip the lookup.
        int parallelismFloor = (mergeTabletClause.getTabletGroupList() == null)
                ? TabletReshardUtils.computeParallelismFloor(table.getId())
                : 0;
        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions =
                createReshardingPhysicalPartitions(parallelismFloor);
        if (reshardingPhysicalPartitions.isEmpty()) {
            if (sawStaleTabletStats) {
                // Transient, so it must stay retriable: a plain StarRocksException is not latched by
                // the caller. Signalling "deterministic" here would suppress this table until its
                // layout or configuration changed, which a statistics refresh does not do.
                throw new StarRocksException("No tablets need to merge in table "
                        + db.getFullName() + '.' + table.getName()
                        + " (tablet size statistics are stale)");
            }
            // Deterministic: the same layout and configuration produce the same empty plan, so the
            // caller may treat it as a normal outcome rather than a failure. That matters for a
            // range-colocate table, whose steady state is one tablet per ColocateRange -- every
            // adjacent pair then crosses a boundary, so the size-based signal keeps firing while this
            // plan stays legitimately empty. Mirrors SplitTabletJobFactory's empty-plan contract.
            throw new EmptyReshardPlanException("No tablets need to merge in table "
                    + db.getFullName() + '.' + table.getName());
        }

        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        return new MergeTabletJob(jobId, db.getId(), table.getId(), reshardingPhysicalPartitions);
    }

    /*
     * Create physical partition contexts for all tablets that need to merge.
     */
    private Map<Long, ReshardingPhysicalPartition> createReshardingPhysicalPartitions(
            int parallelismFloor) throws StarRocksException {
        Preconditions.checkState(mergeTabletClause.getPartitionNames() == null ||
                mergeTabletClause.getTabletGroupList() == null);

        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions = new HashMap<>();

        // Snapshot the colocate ranges once for the whole plan so every index is classified against
        // the same topology. Null means the table has no range-colocate group, and every tablet is then
        // treated as belonging to one implicit range (pre-colocate behavior); a registered group whose
        // ranges are not available was already refused by createTabletReshardJob.
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId colocateGroupId = colocateTableIndex.getRangeColocateGroupId(table.getId());
        List<ColocateRange> colocateRanges = colocateGroupId == null ? null
                : colocateTableIndex.getColocateRanges(colocateGroupId.grpId);
        int colocateColumnCount = colocateGroupId == null ? 0
                : colocateTableIndex.getGroupSchema(colocateGroupId).getColocateColumnCount();
        Map<Long, ColocateRangeUtils.Classifier> classifiers = new HashMap<>();

        try (AutoCloseableLock lock = new AutoCloseableLock(db.getId(), table.getId(), LockType.READ)) {
            if (table.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new StarRocksException("Unexpected table state " + table.getState()
                        + " in table " + db.getFullName() + '.' + table.getName());
            }

            TabletGroupList tabletGroupList = mergeTabletClause.getTabletGroupList();
            if (tabletGroupList != null) {
                Map<PhysicalPartition, Map<MaterializedIndex, List<List<Long>>>> mergeTabletGroups =
                        resolveMergeTabletGroups(tabletGroupList.getTabletIdGroups());
                for (var physicalPartitionEntry : mergeTabletGroups.entrySet()) {
                    PhysicalPartition physicalPartition = physicalPartitionEntry.getKey();
                    Map<Long, ReshardingMaterializedIndex> reshardingIndexes = new HashMap<>();
                    for (var indexEntry : physicalPartitionEntry.getValue().entrySet()) {
                        MaterializedIndex oldIndex = indexEntry.getKey();
                        List<List<Long>> mergeTabletGroupsForIndex = indexEntry.getValue();
                        if (mergeTabletGroupsForIndex.isEmpty()) {
                            continue;
                        }
                        List<ReshardingTablet> reshardingTablets = createReshardingTablets(oldIndex,
                                mergeTabletGroupsForIndex,
                                classifierFor(classifiers, oldIndex, colocateRanges, colocateColumnCount));
                        if (reshardingTablets.isEmpty()) {
                            continue;
                        }
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
                long targetSize = mergeTabletClause.getTabletReshardTargetSize();
                if (targetSize <= 0) {
                    throw new StarRocksException("Invalid tablet_reshard_target_size: " + targetSize);
                }

                Collection<PhysicalPartition> physicalPartitions;
                if (mergeTabletClause.getPartitionNames() == null) {
                    physicalPartitions = table.getPhysicalPartitions();
                } else {
                    physicalPartitions = new ArrayList<>();
                    for (String partitionName : mergeTabletClause.getPartitionNames().getPartitionNames()) {
                        Partition partition = table.getPartition(partitionName);
                        if (partition == null) {
                            throw new StarRocksException("Cannot find partition " + partitionName
                                    + " in table " + db.getFullName() + '.' + table.getName());
                        }
                        physicalPartitions.addAll(partition.getSubPartitions());
                    }
                }

                for (PhysicalPartition physicalPartition : physicalPartitions) {
                    Map<Long, ReshardingMaterializedIndex> reshardingIndexes = new HashMap<>();
                    for (MaterializedIndex oldIndex : physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                        ColocateRangeUtils.Classifier classifier =
                                classifierFor(classifiers, oldIndex, colocateRanges, colocateColumnCount);
                        List<List<Long>> mergeTabletGroupsForIndex = createMergeTabletGroups(
                                physicalPartition, oldIndex, targetSize, parallelismFloor, classifier);
                        if (mergeTabletGroupsForIndex.isEmpty()) {
                            continue;
                        }
                        List<ReshardingTablet> reshardingTablets = createReshardingTablets(oldIndex,
                                mergeTabletGroupsForIndex, classifier);
                        if (reshardingTablets.isEmpty()) {
                            continue;
                        }
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

    private Map<PhysicalPartition, Map<MaterializedIndex, List<List<Long>>>> resolveMergeTabletGroups(
            List<List<Long>> tabletIdGroups) throws StarRocksException {
        Map<PhysicalPartition, Map<MaterializedIndex, List<List<Long>>>> mergeTabletGroups = new HashMap<>();
        if (tabletIdGroups == null || tabletIdGroups.isEmpty()) {
            return mergeTabletGroups;
        }

        List<Long> allTabletIds = new ArrayList<>();
        Set<Long> seenTabletIds = new HashSet<>();
        for (List<Long> group : tabletIdGroups) {
            for (Long tabletId : group) {
                if (!seenTabletIds.add(tabletId)) {
                    throw new StarRocksException("Duplicate tablet " + tabletId + " in merge tablet groups");
                }
                allTabletIds.add(tabletId);
            }
        }

        TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
        List<TabletMeta> tabletMetas = invertedIndex.getTabletMetaList(allTabletIds);
        Map<Long, TabletMeta> tabletMetaMap = new HashMap<>();
        for (int i = 0; i < allTabletIds.size(); ++i) {
            long tabletId = allTabletIds.get(i);
            TabletMeta tabletMeta = tabletMetas.get(i);
            if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META ||
                    tabletMeta.getTableId() != table.getId()) {
                throw new StarRocksException("Cannot find tablet " + tabletId
                        + " in inverted index in table " + db.getFullName() + '.' + table.getName());
            }
            tabletMetaMap.put(tabletId, tabletMeta);
        }

        for (List<Long> group : tabletIdGroups) {
            long firstTabletId = group.get(0);
            TabletMeta firstMeta = tabletMetaMap.get(firstTabletId);
            PhysicalPartition physicalPartition = table.getPhysicalPartition(firstMeta.getPhysicalPartitionId());
            if (physicalPartition == null) {
                throw new StarRocksException("Cannot find physical partition " + firstMeta.getPhysicalPartitionId()
                        + " in table " + db.getFullName() + '.' + table.getName());
            }

            MaterializedIndex index = physicalPartition.getIndex(firstMeta.getIndexId());
            if (index == null) {
                throw new StarRocksException("Cannot find materialized index " + firstMeta.getIndexId()
                        + " in physical partition " + physicalPartition.getId()
                        + " in table " + db.getFullName() + '.' + table.getName());
            }
            if (index.getState() != IndexState.NORMAL) {
                throw new StarRocksException("Not a normal state materialized index " + firstMeta.getIndexId()
                        + " in physical partition " + physicalPartition.getId()
                        + " in table " + db.getFullName() + '.' + table.getName());
            }
            // A superseded index passes every check above (still reachable via getIndex, still NORMAL,
            // still owns the tablets, still mapped by TabletInvertedIndex), so it needs its own gate.
            TabletReshardUtils.checkIndexNotSuperseded(physicalPartition, index, firstTabletId,
                    db.getFullName(), table.getName());

            for (long tabletId : group) {
                TabletMeta meta = tabletMetaMap.get(tabletId);
                if (meta.getPhysicalPartitionId() != firstMeta.getPhysicalPartitionId()
                        || meta.getIndexId() != firstMeta.getIndexId()) {
                    throw new StarRocksException("Tablets in a group must be in the same index and partition");
                }
                Tablet tablet = index.getTablet(tabletId);
                if (tablet == null) {
                    throw new StarRocksException("Cannot find tablet " + tabletId
                            + " in materialized index " + firstMeta.getIndexId()
                            + " in physical partition " + physicalPartition.getId()
                            + " in table " + db.getFullName() + '.' + table.getName());
                }
            }

            mergeTabletGroups.computeIfAbsent(physicalPartition, k -> new HashMap<>())
                    .computeIfAbsent(index, k -> new ArrayList<>())
                    .add(group);
        }

        return mergeTabletGroups;
    }

    private List<List<Long>> createMergeTabletGroups(
            PhysicalPartition physicalPartition, MaterializedIndex oldIndex, long targetSize, int parallelismFloor,
            @Nullable ColocateRangeUtils.Classifier classifier) {
        // pairThresh: a single tablet at or above this is excluded from merging — aligned with
        //             TabletReshardUtils.needMerge() so a tablet that on its own already satisfies
        //             the new size band cannot be picked up as a merge candidate.
        // mergeCap:   maximum cumulative size of a merge group; merged output stays strictly below
        //             splitThreshold so it cannot turn around and trigger a split.
        long pairThresh = TabletReshardUtils.mergePairThreshold(targetSize);
        long mergeCap = TabletReshardUtils.mergeGroupCap(targetSize);

        // MaterializedIndex tablets are already ordered by range.
        List<Tablet> orderedTablets = oldIndex.getTablets();
        List<List<Long>> mergeTabletGroups = new ArrayList<>();

        // Never merge this index below the parallelism floor: keeping at least `parallelismFloor`
        // tablets preserves the count pre-split established for scan/write parallelism, otherwise
        // auto-merge would undo pre-split. Merging a group of k tablets removes k-1 tablets, so the
        // total reduction across all groups must not exceed (current tablet count - floor).
        int mergeBudget = orderedTablets.size() - parallelismFloor;
        if (mergeBudget <= 0) {
            return mergeTabletGroups;
        }

        List<Long> currentTabletGroup = new ArrayList<>();
        long currentSize = 0;
        long visibleVersionTime = physicalPartition.getVisibleVersionTime();
        // Index of the colocate range the current group lives in; -1 both as the initial value and as
        // "this tablet belongs to no single range", which is why an unmergeable tablet always flushes.
        int currentColocateRangeIndex = -1;
        for (Tablet tablet : orderedTablets) {
            if (!(tablet instanceof LakeTablet)) {
                flushMergeTabletGroup(mergeTabletGroups, currentTabletGroup);
                currentTabletGroup = new ArrayList<>();
                currentSize = 0;
                continue;
            }

            // A merge group must never span a colocate range: the merged tablet would sit in two
            // ColocateRanges at once, which de-aligns the group and makes every colocate plan fail
            // closed at RangeColocateScanDispatch#requireAligned -- and nothing marks the group
            // unstable, so the colocate checker would never repair it. A tablet that is already
            // spanning (index -1) is not a merge candidate at all and separates the groups around it.
            int colocateRangeIndex = classifier == null ? 0 : classifier.indexOf(tablet);
            if (colocateRangeIndex < 0 || colocateRangeIndex != currentColocateRangeIndex) {
                flushMergeTabletGroup(mergeTabletGroups, currentTabletGroup);
                currentTabletGroup = new ArrayList<>();
                currentSize = 0;
                currentColocateRangeIndex = colocateRangeIndex;
            }
            if (colocateRangeIndex < 0) {
                continue;
            }

            long dataSize = tablet.getDataSize(true);
            boolean staleStats = ((LakeTablet) tablet).getDataSizeUpdateTime() < visibleVersionTime;
            // A compaction publish advances visibleVersionTime without touching dataVersion, so a
            // tablet can be skipped here purely because its statistics have not caught up yet. Remember
            // it: the resulting empty plan is transient, and latching it would suppress a merge that
            // becomes valid as soon as the next statistics pass lands.
            sawStaleTabletStats |= staleStats;
            if (dataSize >= pairThresh || staleStats) {
                flushMergeTabletGroup(mergeTabletGroups, currentTabletGroup);
                currentTabletGroup = new ArrayList<>();
                currentSize = 0;
                continue;
            }

            // Step order below matters and must be preserved: (1) budget-exhaustion break, then
            // (2) mergeCap flush, then (3) budget decrement + add. Adding a second-or-later member
            // to the current group removes one more tablet, which consumes one unit of merge budget;
            // a fresh first member (empty group, e.g. right after a cap flush) is free. Once the
            // budget is exhausted, stop growing groups so the index settles exactly at the floor.
            // Tablets not visited after this break — and any not placed in a >=2 group — are emitted
            // as identical (unmerged) tablets by createReshardingTablets, so breaking here is safe.
            if (!currentTabletGroup.isEmpty() && mergeBudget <= 0) {
                flushMergeTabletGroup(mergeTabletGroups, currentTabletGroup);
                currentTabletGroup = new ArrayList<>();
                currentSize = 0;
                break;
            }

            if (currentSize + dataSize > mergeCap) {
                flushMergeTabletGroup(mergeTabletGroups, currentTabletGroup);
                currentTabletGroup = new ArrayList<>();
                currentSize = 0;
            }

            if (!currentTabletGroup.isEmpty()) {
                mergeBudget--;
            }
            currentTabletGroup.add(tablet.getId());
            currentSize += dataSize;
        }

        flushMergeTabletGroup(mergeTabletGroups, currentTabletGroup);
        return mergeTabletGroups;
    }

    /**
     * Binds the colocate ranges to {@code index}'s own sort key, or {@code null} when the table has no
     * range-colocate group. Resolved per index rather than from the base index because a rollup / MV
     * can have a shorter sort key, and classifying its tablets against the base arity would compare
     * bounds of different widths.
     *
     * <p>Memoized on {@code classifiers} by index meta id, since the expansion depends on nothing else:
     * this runs once per (physical partition, index) under the table READ lock, and on a table with
     * many partitions rebuilding it per partition dominates the walk's cost.
     */
    @Nullable
    private ColocateRangeUtils.Classifier classifierFor(Map<Long, ColocateRangeUtils.Classifier> classifiers,
            MaterializedIndex index, @Nullable List<ColocateRange> colocateRanges, int colocateColumnCount) {
        if (colocateRanges == null) {
            return null;
        }
        return classifiers.computeIfAbsent(index.getMetaId(), metaId -> ColocateRangeUtils.Classifier.of(
                colocateRanges, MetaUtils.getRangeDistributionColumns(table, metaId), colocateColumnCount));
    }

    private static void flushMergeTabletGroup(List<List<Long>> groups, List<Long> currentGroup) {
        if (currentGroup.size() >= 2) {
            groups.add(currentGroup);
        }
    }

    private List<ReshardingTablet> createReshardingTablets(MaterializedIndex index,
            List<List<Long>> mergeTabletGroups, @Nullable ColocateRangeUtils.Classifier classifier)
            throws StarRocksException {
        List<ReshardingTablet> reshardingTablets = new ArrayList<>();
        if (mergeTabletGroups == null || mergeTabletGroups.isEmpty()) {
            return reshardingTablets;
        }

        // MaterializedIndex tablets are already ordered by range.
        List<Tablet> orderedTablets = index.getTablets();
        Map<Long, Integer> tabletIdToPos = new HashMap<>();
        for (int i = 0; i < orderedTablets.size(); i++) {
            tabletIdToPos.put(orderedTablets.get(i).getId(), i);
        }

        Map<Integer, List<Tablet>> mergeTabletGroupsByStartPos = new HashMap<>();
        Set<Long> mergedTabletIds = new HashSet<>();
        for (List<Long> group : mergeTabletGroups) {
            if (group.size() < 2) {
                throw new StarRocksException("Tablet list must contain at least 2 tablets");
            }
            Set<Long> groupTabletIds = new HashSet<>(group);
            List<Tablet> groupTablets = new ArrayList<>(group.size());
            int minPos = Integer.MAX_VALUE;
            int maxPos = -1;
            for (long tabletId : group) {
                Integer pos = tabletIdToPos.get(tabletId);
                if (pos == null) {
                    throw new StarRocksException("Cannot find tablet " + tabletId + " in index " + index.getId());
                }
                if (!mergedTabletIds.add(tabletId)) {
                    throw new StarRocksException("Duplicate tablet " + tabletId + " in merge tablet groups");
                }
                minPos = Math.min(minPos, pos);
                maxPos = Math.max(maxPos, pos);
            }
            if (maxPos - minPos + 1 != groupTabletIds.size()) {
                throw new StarRocksException(
                        "Tablets in a merge tablet group must be contiguous in index " + index.getId());
            }
            // Group-shape checks, alongside contiguity: every member must also sit inside ONE colocate
            // range, or the merged tablet would span a boundary -- the group loses range alignment and
            // every colocate plan then fails closed at RangeColocateScanDispatch#requireAligned. This
            // is the funnel both producers pass through: an explicit ALTER ... MERGE TABLETS group is
            // rejected here, while createMergeTabletGroups has already broken its groups at boundaries,
            // so for the automatic path this is a fail-closed assertion on that grouper.
            int groupColocateRangeIndex = -1;
            for (int i = minPos; i <= maxPos; i++) {
                Tablet tablet = orderedTablets.get(i);
                if (!groupTabletIds.contains(tablet.getId())) {
                    throw new StarRocksException(
                            "Tablets in a merge tablet group must be contiguous in index " + index.getId());
                }
                if (classifier != null) {
                    int colocateRangeIndex = classifier.indexOf(tablet);
                    if (colocateRangeIndex < 0) {
                        throw new StarRocksException("Tablet " + tablet.getId()
                                + " is not contained in a single colocate range in table "
                                + db.getFullName() + '.' + table.getName() + "; it cannot be merged");
                    }
                    if (groupColocateRangeIndex < 0) {
                        groupColocateRangeIndex = colocateRangeIndex;
                    } else if (groupColocateRangeIndex != colocateRangeIndex) {
                        throw new StarRocksException("Tablets in a merge tablet group must be in the"
                                + " same colocate range; tablet " + tablet.getId() + " crosses a colocate"
                                + " range boundary in table " + db.getFullName() + '.' + table.getName());
                    }
                }
                groupTablets.add(tablet);
            }
            if (mergeTabletGroupsByStartPos.put(minPos, groupTablets) != null) {
                throw new StarRocksException("Duplicate merge tablet group start position in index " + index.getId());
            }
        }

        for (int i = 0; i < orderedTablets.size(); ) {
            List<Tablet> groupTablets = mergeTabletGroupsByStartPos.get(i);
            if (groupTablets != null) {
                List<Long> oldTabletIds = new ArrayList<>(groupTablets.size());
                for (Tablet tablet : groupTablets) {
                    oldTabletIds.add(tablet.getId());
                }
                reshardingTablets.add(createMergingTablet(oldTabletIds));
                i += groupTablets.size();
                continue;
            }

            Tablet tablet = orderedTablets.get(i);
            reshardingTablets.add(createIdenticalTablet(tablet.getId()));
            i++;
        }

        return reshardingTablets;
    }

    private MaterializedIndex createMaterializedIndex(MaterializedIndex oldIndex,
            List<ReshardingTablet> reshardingTablets) {
        MaterializedIndex newIndex = new MaterializedIndex(GlobalStateMgr.getCurrentState().getNextId(),
                oldIndex.getMetaId(), IndexState.NORMAL, oldIndex.getShardGroupId());

        for (ReshardingTablet reshardingTablet : reshardingTablets) {
            Tablet oldTablet = oldIndex.getTablet(reshardingTablet.getFirstOldTabletId());
            Preconditions.checkNotNull(oldTablet, "Not found tablet " + reshardingTablet.getFirstOldTabletId());
            // Carry the async vector-index build watermark forward: the merged tablet inherits the
            // MIN over its source tablets' watermarks (a rowset is only built if built in its own
            // source), mirroring the BE merge_tablet reconciliation. No-op for non-vector tables.
            long vibv = TabletReshardUtils.minVectorIndexBuiltVersion(oldIndex, reshardingTablet.getOldTabletIds());
            for (long tabletId : reshardingTablet.getNewTabletIds()) {
                LakeTablet tablet = new LakeTablet(tabletId, oldTablet.getRange());
                tablet.setVectorIndexBuiltVersion(vibv);
                newIndex.addTablet(tablet, null, false);
            }
        }

        return newIndex;
    }

    private static MergingTablet createMergingTablet(List<Long> oldTabletIds) {
        long newTabletId = GlobalStateMgr.getCurrentState().getNextId();
        return new MergingTablet(oldTabletIds, newTabletId);
    }

    private static IdenticalTablet createIdenticalTablet(long oldTabletId) {
        long newTabletId = GlobalStateMgr.getCurrentState().getNextId();
        return new IdenticalTablet(oldTabletId, newTabletId);
    }
}
