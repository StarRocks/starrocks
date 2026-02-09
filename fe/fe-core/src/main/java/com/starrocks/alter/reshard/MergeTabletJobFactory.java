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
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.MergeTabletClause;
import com.starrocks.sql.ast.TabletGroupList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * MergeTabletJobFactory is for creating TabletReshardJob for tablet merging.
 */
public class MergeTabletJobFactory implements TabletReshardJobFactory {
    private final Database db;
    private final OlapTable table;
    private final MergeTabletClause mergeTabletClause;

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

        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions = createReshardingPhysicalPartitions();
        if (reshardingPhysicalPartitions.isEmpty()) {
            throw new StarRocksException("No tablets need to merge in table "
                    + db.getFullName() + '.' + table.getName());
        }

        createNewShards(reshardingPhysicalPartitions);

        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        return new MergeTabletJob(jobId, db.getId(), table.getId(), reshardingPhysicalPartitions);
    }

    /*
     * Create physical partition contexts for all tablets that need to merge.
     */
    private Map<Long, ReshardingPhysicalPartition> createReshardingPhysicalPartitions() throws StarRocksException {
        Preconditions.checkState(mergeTabletClause.getPartitionNames() == null ||
                mergeTabletClause.getTabletGroupList() == null);

        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions = new HashMap<>();

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
                                mergeTabletGroupsForIndex);
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
                        List<List<Long>> mergeTabletGroupsForIndex = createMergeTabletGroups(oldIndex, targetSize);
                        if (mergeTabletGroupsForIndex.isEmpty()) {
                            continue;
                        }
                        List<ReshardingTablet> reshardingTablets = createReshardingTablets(oldIndex,
                                mergeTabletGroupsForIndex);
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

    private List<List<Long>> createMergeTabletGroups(MaterializedIndex oldIndex, long targetSize) {
        // MaterializedIndex tablets are already ordered by range.
        List<Tablet> orderedTablets = oldIndex.getTablets();
        List<List<Long>> mergeTabletGroups = new ArrayList<>();

        List<Long> currentTabletGroup = new ArrayList<>();
        long currentSize = 0;
        for (Tablet tablet : orderedTablets) {
            long dataSize = tablet.getDataSize(true);
            if (dataSize >= targetSize) {
                if (currentTabletGroup.size() >= 2) {
                    mergeTabletGroups.add(currentTabletGroup);
                }
                currentTabletGroup = new ArrayList<>();
                currentSize = 0;
                continue;
            }

            currentTabletGroup.add(tablet.getId());
            currentSize += dataSize;
            if (currentSize >= targetSize && currentTabletGroup.size() >= 2) {
                mergeTabletGroups.add(currentTabletGroup);
                currentTabletGroup = new ArrayList<>();
                currentSize = 0;
            }
        }

        if (currentTabletGroup.size() >= 2) {
            mergeTabletGroups.add(currentTabletGroup);
        }

        return mergeTabletGroups;
    }

    private List<ReshardingTablet> createReshardingTablets(MaterializedIndex index,
            List<List<Long>> mergeTabletGroups) throws StarRocksException {
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
            for (int i = minPos; i <= maxPos; i++) {
                Tablet tablet = orderedTablets.get(i);
                if (!groupTabletIds.contains(tablet.getId())) {
                    throw new StarRocksException(
                            "Tablets in a merge tablet group must be contiguous in index " + index.getId());
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
            for (long tabletId : reshardingTablet.getNewTabletIds()) {
                Tablet tablet = new LakeTablet(tabletId, oldTablet.getRange());
                newIndex.addTablet(tablet, null, false);
            }
        }

        return newIndex;
    }

    private void createNewShards(Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions)
            throws StarRocksException {
        for (ReshardingPhysicalPartition reshardingPhysicalPartition : reshardingPhysicalPartitions.values()) {
            long physicalPartitionId = reshardingPhysicalPartition.getPhysicalPartitionId();
            for (ReshardingMaterializedIndex reshardingIndex : reshardingPhysicalPartition
                    .getReshardingIndexes().values()) {
                MaterializedIndex newIndex = reshardingIndex.getMaterializedIndex();
                Map<Long, List<Long>> newToOldTabletIds = new HashMap<>();
                for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                    List<Long> oldTabletIds = reshardingTablet.getOldTabletIds();
                    for (long newTabletId : reshardingTablet.getNewTabletIds()) {
                        newToOldTabletIds.put(newTabletId, oldTabletIds);
                    }
                }

                Map<String, String> properties = new HashMap<>();
                properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
                properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartitionId));
                properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(newIndex.getId()));

                GlobalStateMgr.getCurrentState().getStarOSAgent().createShardsForMerge(
                        newToOldTabletIds,
                        table.getPartitionFilePathInfo(physicalPartitionId),
                        table.getPartitionFileCacheInfo(physicalPartitionId),
                        newIndex.getShardGroupId(),
                        properties, WarehouseManager.DEFAULT_RESOURCE);
            }
        }
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
