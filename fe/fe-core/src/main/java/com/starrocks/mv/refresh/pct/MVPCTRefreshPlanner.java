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

package com.starrocks.mv.refresh.pct;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.scheduler.mv.BaseTableSnapshotInfo;
import com.starrocks.scheduler.mv.pct.MVPCTRefreshPartitioner;
import com.starrocks.scheduler.mv.pct.PCTPartitionTopology;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.SyncPartitionUtils;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Shared refresh-control flow for partitioned PCT refresh.
 *
 * <p>Shape-specific algorithms still live on {@link MVPCTRefreshPartitioner}'s subclasses via
 * abstract hooks. This helper only owns the common orchestration path.
 */
public final class MVPCTRefreshPlanner {
    private final MVPCTRefreshPartitioner partitioner;

    public MVPCTRefreshPlanner(MVPCTRefreshPartitioner partitioner) {
        this.partitioner = partitioner;
    }

    /**
     * Compute the materialized view partitions to refresh for the current task run.
     *
     * <p>This method first resolves the refresh candidate set under the MV read lock, including
     * potential-partition expansion. If {@code skipBatchFilter} is false, it then applies
     * property-based and batch-size filtering outside the lock so the lock scope stays aligned
     * with the original detect-only path.
     *
     * @param snapshotBaseTables snapshot base tables captured for this refresh attempt
     * @param skipBatchFilter whether to skip the batch-filter stage and return the full candidate set
     * @return partitions selected for this refresh, or the full candidate set when batch filtering is skipped
     */
    public PCellSortedSet getMVToRefreshedPartitions(Map<Long, BaseTableSnapshotInfo> snapshotBaseTables,
                                                     boolean skipBatchFilter)
            throws AnalysisException, LockTimeoutException {
        PCellSortedSet mvToRefreshedPartitions;
        Locker locker = new Locker();
        if (!locker.tryLockTableWithIntensiveDbLock(partitioner.getDb().getId(), partitioner.getMv().getId(),
                LockType.READ, Config.mv_refresh_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            partitioner.getLogger().warn("failed to lock database: {} in detectMVPartitionsToRefresh",
                    partitioner.getDb().getFullName());
            throw new LockTimeoutException(String.format("Materialized view %s.%s refresh failed: "
                            + "failed to acquire read lock on database %s within %d ms "
                            + "when detecting partitions to refresh",
                    partitioner.getDb().getFullName(), partitioner.getMv().getName(),
                    partitioner.getDb().getFullName(), Config.mv_refresh_try_lock_timeout_ms));
        }
        try {
            if (partitioner.getMvRefreshParams().isForce()) {
                mvToRefreshedPartitions = partitioner.getMVPartitionsToRefreshWithForce();
            } else {
                mvToRefreshedPartitions = partitioner.getMVPartitionsToRefreshWithCheck(snapshotBaseTables);
            }
            if (mvToRefreshedPartitions == null || mvToRefreshedPartitions.isEmpty()) {
                partitioner.getLogger().info("no partitions to refresh for materialized view");
                return mvToRefreshedPartitions;
            }

            Map<Table, PCellSortedSet> baseChangedPartitionNames =
                    partitioner.getBasePartitionNamesByMVPartitionNames(mvToRefreshedPartitions);
            if (baseChangedPartitionNames.isEmpty()) {
                partitioner.getLogger().info(
                        "Cannot get associated base table change partitions from mv's refresh partitions {}",
                        mvToRefreshedPartitions);
                return mvToRefreshedPartitions;
            }

            Map<Table, PCellSortedSet> baseChangedPCellsSortedSet =
                    partitioner.toBaseTableWithSortedSet(baseChangedPartitionNames);
            if (partitioner.isCalcPotentialRefreshPartition(baseChangedPCellsSortedSet, mvToRefreshedPartitions)) {
                partitioner.getLogger().info("Start calcPotentialRefreshPartition, needRefreshMvPartitionNames: {},"
                                + " baseChangedPartitionNames: {}",
                        mvToRefreshedPartitions, baseChangedPCellsSortedSet);
                PCTPartitionTopology partitionTopology = partitioner.getMvContext().getPartitionTopology();
                PCellSortedSet potentialPartitions = PCellSortedSet.of();
                SyncPartitionUtils.calcPotentialRefreshPartition(mvToRefreshedPartitions,
                        baseChangedPartitionNames,
                        partitionTopology.getRefBaseTableMVIntersectedPartitions(),
                        partitionTopology.getMvRefBaseTableIntersectedPartitions(),
                        potentialPartitions);
                partitioner.addMVToRefreshPotentialPartitions(potentialPartitions);
                mvToRefreshedPartitions.addAll(partitioner.getMVToRefreshPotentialPartitions());
                partitioner.getLogger().info("Finish calcPotentialRefreshPartition, needRefreshMvPartitionNames: {},"
                                + " baseChangedPartitionNames: {}",
                        mvToRefreshedPartitions, baseChangedPartitionNames);
            }
        } finally {
            locker.unLockTableWithIntensiveDbLock(partitioner.getDb().getId(), partitioner.getMv().getId(), LockType.READ);
        }
        if (!skipBatchFilter) {
            filterMVToRefreshPartitions(mvToRefreshedPartitions);

            int partitionRefreshNumber = partitioner.getMv().getTableProperty().getPartitionRefreshNumber();
            partitioner.getLogger().info("filter partitions to refresh partitionRefreshNumber={}, "
                            + "partitionsToRefresh:{}, mvPotentialPartitionNames:{}, next start:{}, "
                            + "next end:{}, next list values:{}",
                    partitionRefreshNumber, mvToRefreshedPartitions, partitioner.getMVToRefreshPotentialPartitions(),
                    partitioner.getMvContext().getNextPartitionStart(), partitioner.getMvContext().getNextPartitionEnd(),
                    partitioner.getMvContext().getNextPartitionValues());
        }
        return mvToRefreshedPartitions;
    }

    private void filterMVToRefreshPartitions(PCellSortedSet mvToRefreshedPartitions) {
        if (mvToRefreshedPartitions == null || mvToRefreshedPartitions.isEmpty()) {
            return;
        }

        partitioner.filterMVToRefreshPartitionsByProperty(mvToRefreshedPartitions);
        partitioner.getLogger().info("after filter with auto_refresh_partition_number, partitionsToRefresh: {}",
                mvToRefreshedPartitions);
        if (mvToRefreshedPartitions.isEmpty() || mvToRefreshedPartitions.size() <= 1) {
            return;
        }
        if (!partitioner.getMvRefreshParams().isCanGenerateNextTaskRun() || !partitioner.isGenerateNextTaskRun()) {
            return;
        }
        boolean hasUnsupportedTableTypeForAdaptiveRefresh = partitioner.getMv().getBaseTableTypes().stream()
                .anyMatch(type -> !MVPCTRefreshPartitioner.isAdaptiveRefreshSupported(type));
        if (hasUnsupportedTableTypeForAdaptiveRefresh) {
            partitioner.filterPartitionByRefreshNumber(
                    mvToRefreshedPartitions, MaterializedView.PartitionRefreshStrategy.STRICT);
        } else {
            partitioner.filterPartitionByRefreshNumber(
                    mvToRefreshedPartitions, partitioner.getMv().getPartitionRefreshStrategy());
        }
        partitioner.getLogger().info("after filterPartitionByAdaptive, partitionsToRefresh: {}",
                mvToRefreshedPartitions);
    }
}
