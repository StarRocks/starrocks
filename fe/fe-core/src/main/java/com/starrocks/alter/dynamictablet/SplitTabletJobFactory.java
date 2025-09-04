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

package com.starrocks.alter.dynamictablet;

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
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.SplitTabletClause;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/*
 * SplitTabletJobFactory is for creating DynamicTabletJob for tablet splitting.
 */
public class SplitTabletJobFactory implements DynamicTabletJobFactory {
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
     * Create a dynamic tablet job and return it.
     * New shareds are created for new tablets.
     */
    @Override
    public DynamicTabletJob createDynamicTabletJob() throws StarRocksException {
        if (!table.isCloudNativeTableOrMaterializedView()) {
            throw new StarRocksException("Unsupported table type " + table.getType()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }

        if (table.getDefaultDistributionInfo().getType() != DistributionInfoType.HASH) {
            throw new StarRocksException("Unsupported distribution type " + table.getDefaultDistributionInfo().getType()
                    + " in table " + db.getFullName() + '.' + table.getName());
        }

        Map<Long, PhysicalPartitionContext> physicalPartitionContexts = createPhysicalPartitionContexts();
        if (physicalPartitionContexts.isEmpty()) {
            throw new StarRocksException("No tablets need to split in table "
                    + db.getFullName() + '.' + table.getName());
        }

        createNewShards(physicalPartitionContexts);

        long jobId = GlobalStateMgr.getCurrentState().getNextId();
        return new DynamicTabletJob(jobId, DynamicTabletJob.JobType.SPLIT_TABLET,
                db.getId(), table.getId(), physicalPartitionContexts);
    }

    /*
     * Create physical partition contexts for all tablets that need to split
     */
    private Map<Long, PhysicalPartitionContext> createPhysicalPartitionContexts() throws StarRocksException {
        Preconditions.checkState(splitTabletClause.getPartitionNames() == null ||
                splitTabletClause.getTabletList() == null);

        Map<Long, PhysicalPartitionContext> physicalPartitionContexts = new HashMap<>();

        Locker locker = new Locker();
        locker.lockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        try {
            if (table.getState() != OlapTable.OlapTableState.NORMAL) {
                throw new StarRocksException("Unexpected table state " + table.getState()
                        + " in table " + db.getFullName() + '.' + table.getName());
            }

            if (splitTabletClause.getTabletList() != null) {
                Map<PhysicalPartition, Map<MaterializedIndex, Collection<Tablet>>> tablets = getTabletsByTabletIds(
                        splitTabletClause.getTabletList().getTabletIds());

                for (var physicalPartitionEntry : tablets.entrySet()) {
                    PhysicalPartition physicalPartition = physicalPartitionEntry.getKey();
                    Map<Long, DynamicTablets> dynamicTabletses = new HashMap<>();
                    for (var indexEntry : physicalPartitionEntry.getValue().entrySet()) {
                        MaterializedIndex index = indexEntry.getKey();

                        List<SplittingTablet> splittingTablets = new ArrayList<>();
                        for (Tablet tablet : indexEntry.getValue()) {
                            int newTabletCount = 0;
                            if (splitTabletClause.getDynamicTabletSplitSize() <= 0) {
                                long splitCount = -splitTabletClause.getDynamicTabletSplitSize();
                                if (splitCount > Config.dynamic_tablet_max_split_count
                                        || !DynamicTabletUtils.isPowerOfTwo(splitCount)) {
                                    throw new StarRocksException("Invalid dynamic_tablet_split_size: "
                                            + splitTabletClause.getDynamicTabletSplitSize());
                                }

                                newTabletCount = (int) splitCount;
                            } else {
                                newTabletCount = DynamicTabletUtils.calcSplitCount(tablet.getDataSize(true),
                                        splitTabletClause.getDynamicTabletSplitSize());
                            }

                            if (newTabletCount <= 1) {
                                continue;
                            }

                            splittingTablets.add(createSplittingTablet(tablet.getId(), newTabletCount));
                        }

                        if (splittingTablets.isEmpty()) {
                            continue;
                        }

                        dynamicTabletses.put(index.getId(), createDynamicTablets(index, splittingTablets));
                    }

                    if (dynamicTabletses.isEmpty()) {
                        continue;
                    }

                    physicalPartitionContexts.put(physicalPartition.getId(),
                            new PhysicalPartitionContext(
                                    createPhysicalPartition(physicalPartition, dynamicTabletses),
                                    dynamicTabletses));
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
                    Map<Long, DynamicTablets> dynamicTabletses = new HashMap<>();
                    for (MaterializedIndex index : physicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {

                        List<SplittingTablet> splittingTablets = new ArrayList<>();
                        for (Tablet tablet : index.getTablets()) {
                            Preconditions.checkState(splitTabletClause.getDynamicTabletSplitSize() > 0,
                                    "Invalid dynamic_tablet_split_size: "
                                            + splitTabletClause.getDynamicTabletSplitSize());

                            int newTabletCount = DynamicTabletUtils.calcSplitCount(tablet.getDataSize(true),
                                    splitTabletClause.getDynamicTabletSplitSize());

                            if (newTabletCount <= 1) {
                                continue;
                            }

                            splittingTablets.add(createSplittingTablet(tablet.getId(), newTabletCount));
                        }

                        if (splittingTablets.isEmpty()) {
                            continue;
                        }

                        dynamicTabletses.put(index.getId(), createDynamicTablets(index, splittingTablets));
                    }

                    if (dynamicTabletses.isEmpty()) {
                        continue;
                    }

                    physicalPartitionContexts.put(physicalPartition.getId(),
                            new PhysicalPartitionContext(
                                    createPhysicalPartition(physicalPartition, dynamicTabletses),
                                    dynamicTabletses));
                }
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(db.getId(), table.getId(), LockType.READ);
        }

        return physicalPartitionContexts;
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

    private DynamicTablets createDynamicTablets(MaterializedIndex index, List<SplittingTablet> splittingTablets) {
        List<IdenticalTablet> identicalTablets = new ArrayList<>();
        TABLETS_LOOP: for (Tablet tablet : index.getTablets()) {
            for (SplittingTablet splittingTablet : splittingTablets) {
                if (splittingTablet.getOldTabletId() == tablet.getId()) {
                    continue TABLETS_LOOP;
                }
            }

            IdenticalTablet identicalTablet = createIdenticalTablet(tablet.getId());
            identicalTablets.add(identicalTablet);
        }

        return new DynamicTablets(splittingTablets, Collections.emptyList(), identicalTablets);
    }

    private MaterializedIndex createMaterializedIndex(MaterializedIndex oldIndex, DynamicTablets dynamicTablets) {
        MaterializedIndex newIndex = new MaterializedIndex(oldIndex.getId(), IndexState.NORMAL,
                oldIndex.getShardGroupId());

        for (long tabletId : dynamicTablets.getNewTabletIds()) {
            Tablet tablet = new LakeTablet(tabletId);
            newIndex.addTablet(tablet, null, false);
        }

        newIndex.setVirtualBuckets(dynamicTablets.calcNewVirtualBuckets(oldIndex.getVirtualBuckets()));
        return newIndex;
    }

    private PhysicalPartition createPhysicalPartition(PhysicalPartition oldPhysicalPartition,
            Map<Long, DynamicTablets> dynamicTabletses) {
        PhysicalPartition newPhysicalPartition = new PhysicalPartition(
                GlobalStateMgr.getCurrentState().getNextId(), null, oldPhysicalPartition);

        for (MaterializedIndex oldIndex : oldPhysicalPartition.getMaterializedIndices(IndexExtState.VISIBLE)) {
            DynamicTablets dynamicTablets = dynamicTabletses.get(oldIndex.getId());
            if (dynamicTablets == null) {
                dynamicTablets = createDynamicTablets(oldIndex, Collections.emptyList());
            }

            MaterializedIndex newIndex = createMaterializedIndex(oldIndex, dynamicTablets);
            if (oldIndex == oldPhysicalPartition.getBaseIndex()) {
                newPhysicalPartition.setBaseIndex(newIndex);
            } else {
                newPhysicalPartition.createRollupIndex(newIndex);
            }
        }

        return newPhysicalPartition;
    }

    private void createNewShards(Map<Long, PhysicalPartitionContext> physicalPartitionContexts)
            throws StarRocksException {
        for (var physicalPartitionEntry : physicalPartitionContexts.entrySet()) {
            long oldPhysicalPartitionId = physicalPartitionEntry.getKey();
            PhysicalPartitionContext physicalPartitionContext = physicalPartitionEntry.getValue();
            PhysicalPartition physicalPartition = physicalPartitionContext.getPhysicalPartition();

            for (var indexEntry : physicalPartitionContext.getDynamicTabletses().entrySet()) {
                long indexId = indexEntry.getKey();
                MaterializedIndex index = physicalPartition.getIndex(indexId);
                DynamicTablets dynamicTablets = indexEntry.getValue();

                Map<Long, List<Long>> oldToNewTabletIds = new HashMap<>();
                for (SplittingTablet splittingTablet : dynamicTablets.getSplittingTablets()) {
                    oldToNewTabletIds.put(splittingTablet.getOldTabletId(), splittingTablet.getNewTabletIds());
                }
                for (IdenticalTablet identicalTablet : dynamicTablets.getIdenticalTablets()) {
                    oldToNewTabletIds.put(identicalTablet.getOldTabletId(),
                            List.of(identicalTablet.getNewTabletId()));
                }

                Map<String, String> properties = new HashMap<>();
                properties.put(LakeTablet.PROPERTY_KEY_TABLE_ID, Long.toString(table.getId()));
                properties.put(LakeTablet.PROPERTY_KEY_PARTITION_ID, Long.toString(physicalPartition.getId()));
                properties.put(LakeTablet.PROPERTY_KEY_INDEX_ID, Long.toString(indexId));

                // Create shards with the path id of the old physical partition.
                // The old and new phsycal partition will share the same storage path.
                GlobalStateMgr.getCurrentState().getStarOSAgent().createShards(
                        oldToNewTabletIds,
                        table.getPartitionFilePathInfo(physicalPartition.getPathId()),
                        table.getPartitionFileCacheInfo(oldPhysicalPartitionId),
                        index.getShardGroupId(),
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
