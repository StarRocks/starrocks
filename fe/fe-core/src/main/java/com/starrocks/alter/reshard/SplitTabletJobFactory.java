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
import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.MaterializedIndex.IndexState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.Config;
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

        if (table.getDefaultDistributionInfo().getType() != DistributionInfoType.RANGE) {
            throw new StarRocksException("Unsupported distribution type " + table.getDefaultDistributionInfo().getType()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }

        Map<Long, ReshardingPhysicalPartition> reshardingPhysicalPartitions = createReshardingPhysicalPartitions();
        if (reshardingPhysicalPartitions.isEmpty()) {
            throw new StarRocksException("No tablets need to split in table "
                    + db.getFullName() + '.' + table.getName());
        }

        createNewShards(reshardingPhysicalPartitions);

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
                            int newTabletCount = 0;
                            if (splitTabletClause.getTabletReshardTargetSize() <= 0) {
                                long splitCount = -splitTabletClause.getTabletReshardTargetSize();
                                if (splitCount > Config.tablet_reshard_max_split_count) {
                                    throw new StarRocksException("Invalid tablet_reshard_target_size: "
                                            + splitTabletClause.getTabletReshardTargetSize());
                                }

                                newTabletCount = (int) splitCount;
                            } else {
                                newTabletCount = TabletReshardUtils.calcSplitCount(tablet.getDataSize(true),
                                        splitTabletClause.getTabletReshardTargetSize());
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
                    for (MaterializedIndex oldIndex : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {

                        Map<Long, SplittingTablet> splittingTablets = new HashMap<>();
                        for (Tablet tablet : oldIndex.getTablets()) {
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

    private MaterializedIndex createMaterializedIndex(MaterializedIndex oldIndex,
            List<ReshardingTablet> reshardingTablets) {
        // TODO: Use new id after multiple versions of MaterializedIndex is supported
        MaterializedIndex newIndex = new MaterializedIndex(oldIndex.getId(), IndexState.NORMAL,
                oldIndex.getShardGroupId());

        for (ReshardingTablet reshardingTablet : reshardingTablets) {
            for (long tabletId : reshardingTablet.getNewTabletIds()) {
                Tablet tablet = new LakeTablet(tabletId);
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
                Map<Long, List<Long>> oldToNewTabletIds = new HashMap<>();
                for (ReshardingTablet reshardingTablet : reshardingIndex.getReshardingTablets()) {
                    oldToNewTabletIds.put(reshardingTablet.getFirstOldTabletId(), reshardingTablet.getNewTabletIds());
                }

                Map<String, String> properties = new HashMap<>();
                properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
                properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartitionId));
                properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(newIndex.getId()));

                GlobalStateMgr.getCurrentState().getStarOSAgent().createShards(
                        oldToNewTabletIds,
                        table.getPartitionFilePathInfo(physicalPartitionId),
                        table.getPartitionFileCacheInfo(physicalPartitionId),
                        newIndex.getShardGroupId(),
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

    private static IdenticalTablet createIdenticalTablet(long oldTabletId) {
        long newTabletId = GlobalStateMgr.getCurrentState().getNextId();
        return new IdenticalTablet(oldTabletId, newTabletId);
    }
}
