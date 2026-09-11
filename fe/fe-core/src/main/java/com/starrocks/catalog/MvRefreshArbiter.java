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

package com.starrocks.catalog;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.catalog.mv.MVTimelinessListPartitionArbiter;
import com.starrocks.catalog.mv.MVTimelinessNonPartitionArbiter;
import com.starrocks.catalog.mv.MVTimelinessRangePartitionArbiter;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.ConnectorPartitionTraits;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellUtils;
import com.starrocks.sql.common.UnsupportedException;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

/**
* The arbiter of materialized view refresh. All implementations of refresh strategies should be here.
*/
public class MvRefreshArbiter {
    private static final Logger LOG = LogManager.getLogger(MvRefreshArbiter.class);

    public static boolean needsToRefreshTable(MaterializedView mv, BaseTableInfo baseTableInfo, Table table,
                                              MVTimelinessArbiter.QueryRewriteParams queryRewriteParams) {
        Optional<Boolean> needsToRefresh = needsToRefreshTable(mv, baseTableInfo, table, true, queryRewriteParams);
        if (needsToRefresh.isPresent()) {
            return needsToRefresh.get();
        }
        return true;
    }

    /**
     * Once materialized view's base tables have updated, we need to check correspond materialized views' partitions
     * to be refreshed.
     * @param mv The materialized view to check
     * @param queryRewriteParams Mark whether this caller is query rewrite or not, when it's true we can use staleness to shortcut
     * @return mv timeliness update info which contains all need refreshed partitions of materialized view and partition name
     * to partition values.
     */
    public static MvUpdateInfo getMVTimelinessUpdateInfo(MaterializedView mv,
                                                         MVTimelinessArbiter.QueryRewriteParams queryRewriteParams) {
        // Skip check for sync materialized view.
        if (mv.getRefreshScheme().isSync()) {
            return MvUpdateInfo.noRefresh(mv);
        }

        // check mv's query rewrite consistency mode property only in query rewrite.
        TableProperty tableProperty = mv.getTableProperty();
        TableProperty.QueryRewriteConsistencyMode mvConsistencyRewriteMode = tableProperty.getQueryRewriteConsistencyMode();
        if (queryRewriteParams.isQueryRewrite()) {
            switch (mvConsistencyRewriteMode) {
                case DISABLE:
                    return MvUpdateInfo.fullRefresh(mv);
                case NOCHECK:
                    return MvUpdateInfo.noRefresh(mv);
                case LOOSE:
                case CHECKED:
                default:
                    break;
            }
        }

        logMVPrepare(mv, "MV refresh arbiter start to get partition names to refresh, query rewrite mode: {}",
                mvConsistencyRewriteMode);
        MVTimelinessArbiter timelinessArbiter = buildMVTimelinessArbiter(mv, queryRewriteParams);
        try (Timer ignored = Tracers.watchScope("MVTimelinessUpdateInfo")) {
            return timelinessArbiter.getMVTimelinessUpdateInfo(mvConsistencyRewriteMode);
        } catch (AnalysisException e) {
            logMVPrepare(mv, "Failed to get mv timeliness info: {}", DebugUtil.getStackTrace(e));
            return MvUpdateInfo.unknown(mv);
        }
    }

    /**
     * Create the MVTimelinessArbiter instance according to the partition info of the materialized view.
     * @param mv the materialized view to get the timeliness arbiter
     * @param queryRewriteParams whether this caller is query rewrite or mv refresh
     * @return MVTimelinessArbiter instance according to the partition info of the materialized view
     */
    public static MVTimelinessArbiter buildMVTimelinessArbiter(MaterializedView mv,
                                                               MVTimelinessArbiter.QueryRewriteParams queryRewriteParams) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo.isUnPartitioned()) {
            return new MVTimelinessNonPartitionArbiter(mv, queryRewriteParams);
        } else if (partitionInfo.isRangePartition()) {
            return new MVTimelinessRangePartitionArbiter(mv, queryRewriteParams);
        } else if (partitionInfo.isListPartition()) {
            return new MVTimelinessListPartitionArbiter(mv, queryRewriteParams);
        } else {
            throw UnsupportedException.unsupportedException("unsupported partition info type:" +
                    partitionInfo.getClass().getName());
        }
    }

    /**
     * Check whether mv needs to refresh based on the ref base table. It's a shortcut version of getMvBaseTableUpdateInfo.
     * @return Optional<Boolean> : true if needs to refresh, false if not, empty if there are some unkown results.
     */
    private static Optional<Boolean> needsToRefreshTable(MaterializedView mv,
                                                         BaseTableInfo baseTableInfo,
                                                         Table baseTable,
                                                         boolean withMv,
                                                         MVTimelinessArbiter.QueryRewriteParams queryRewriteParams) {
        if (baseTable.isView()) {
            // do nothing
            return Optional.of(false);
        } else if (baseTable.isNativeTableOrMaterializedView()) {
            OlapTable olapBaseTable = (OlapTable) baseTable;

            if (!mv.shouldRefreshTable(baseTableInfo.getDbName(), baseTable.name)) {
                return Optional.of(false);
            }

            Set<String> baseUpdatedPartitionNames = mv.getUpdatedPartitionNamesOfOlapTable(olapBaseTable,
                    queryRewriteParams.isQueryRewrite());
            if (!baseUpdatedPartitionNames.isEmpty()) {
                return Optional.of(true);
            }

            // recursive check its children
            if (withMv && baseTable.isMaterializedView()) {
                MvUpdateInfo mvUpdateInfo = getMVTimelinessUpdateInfo((MaterializedView) baseTable, queryRewriteParams);
                if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
                    return Optional.empty();
                }
                // NOTE: if base table is mv, check to refresh partition names as the base table's update info.
                return Optional.of(!mvUpdateInfo.getMVToRefreshPCells().isEmpty());
            }
            return Optional.of(false);
        } else {
            Set<String> baseUpdatedPartitionNames = mv.getUpdatedPartitionNamesOfExternalTable(baseTable,
                    queryRewriteParams.isQueryRewrite());
            if (baseUpdatedPartitionNames == null) {
                return Optional.empty();
            }
            return Optional.of(!baseUpdatedPartitionNames.isEmpty());
        }
    }

    /**
     * Get to refresh partition info of the specific table.
     * @param baseTable: the table to check
     * @param withMv: whether to check the materialized view if it's a materialized view
     * @param queryRewriteParams: whether this caller is query rewrite or not
     * @return MvBaseTableUpdateInfo: the update info of the base table
     */
    public static MvBaseTableUpdateInfo getMvBaseTableUpdateInfo(MaterializedView mv,
                                                                 Table baseTable,
                                                                 boolean withMv,
                                                                 MVTimelinessArbiter.QueryRewriteParams queryRewriteParams) {
        MvBaseTableUpdateInfo baseTableUpdateInfo = new MvBaseTableUpdateInfo();
        if (baseTable.isView()) {
            // do nothing
            return baseTableUpdateInfo;
        } else if (baseTable.isNativeTableOrMaterializedView()) {
            OlapTable olapBaseTable = (OlapTable) baseTable;
            Set<String> baseUpdatedPartitionNames = mv.getUpdatedPartitionNamesOfOlapTable(olapBaseTable,
                    queryRewriteParams.isQueryRewrite());
            if (baseUpdatedPartitionNames == null) {
                return null;
            }
            PCellSortedSet updatedPCellSet = PCellUtils.ofOlapTable(olapBaseTable, baseUpdatedPartitionNames);
            // recursive check its children
            if (withMv && baseTable.isMaterializedView()) {
                MvUpdateInfo mvUpdateInfo = getMVTimelinessUpdateInfo((MaterializedView) baseTable, queryRewriteParams);
                if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
                    return null;
                }
                // NOTE: if base table is mv, check to refresh partition names as the base table's update info.
                updatedPCellSet.addAll(mvUpdateInfo.getMVToRefreshPCells());
                baseTableUpdateInfo.addMVPartitionNameToCellMap(mvUpdateInfo.getRefBaseNestedMVPCells());
            }
            // update base table's partition info
            baseTableUpdateInfo.addToRefreshPartitionNames(updatedPCellSet);
        } else {
            Set<String> baseUpdatedPartitionNames = mv.getUpdatedPartitionNamesOfExternalTable(baseTable,
                    queryRewriteParams.isQueryRewrite());
            if (baseUpdatedPartitionNames == null) {
                return null;
            }
            PCellSortedSet updatedPCellSet = PCellUtils.ofTable(mv, baseTable, baseUpdatedPartitionNames);
            baseTableUpdateInfo.addToRefreshPartitionNames(updatedPCellSet);
        }
        return baseTableUpdateInfo;
    }

    public static boolean hasDeletedPartitions(MaterializedView mv, Table table, TvrVersionRange pinnedVersionRange) {
        return !getDroppedTrackedPartitions(mv, table, pinnedVersionRange).isEmpty();
    }

    /**
     * The base table partitions the mv still tracks a refreshed version for, but which no longer exist in the base
     * table.
     *
<<<<<<< HEAD
     * @param mv the materialized view
     * @param baseTableInfo the base table info
     * @param table the base table
     * @return true if partitions have been deleted from the base table, false otherwise
     */
    public static boolean hasDeletedPartitions(MaterializedView mv, BaseTableInfo baseTableInfo, Table table) {
        // Only check for external tables (Iceberg, Hive, etc.)
        if (table.isNativeTableOrMaterializedView()) {
            return false;
=======
     * @param pinnedVersionRange the frozen snapshot to answer from, null to answer from the live table
     */
    public static Set<String> getDroppedTrackedPartitions(MaterializedView mv, Table table,
                                                          TvrVersionRange pinnedVersionRange) {
        Map<String, MaterializedView.BasePartitionInfo> versionMap = getTrackedPartitionVersions(mv, table);
        if (MapUtils.isEmpty(versionMap)) {
            return Sets.newHashSet();
>>>>>>> 8600f8bc580 ([BugFix] Refresh mv partitions that outlived a dropped base partition (#62450))
        }

        Set<String> dropped;
        try {
<<<<<<< HEAD
            // Get current partitions from the base table
            ConnectorPartitionTraits traits = ConnectorPartitionTraits.build(mv, table);
            Map<String, com.starrocks.connector.PartitionInfo> latestPartitionInfo =
                    traits.getPartitionNameWithPartitionInfo();

            // Get the partitions that were previously refreshed
            Map<String, MaterializedView.BasePartitionInfo> versionMap =
                    mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableRefreshInfo(baseTableInfo);

            if (MapUtils.isEmpty(versionMap)) {
                return false;
            }

            // Check if any previously refreshed partitions no longer exist
            Set<String> currentPartitions = latestPartitionInfo.keySet();
            for (String refreshedPartition : versionMap.keySet()) {
                if (!currentPartitions.contains(refreshedPartition)) {
                    logMVPrepare(mv, String.format(
                            "Base table partition %s has been deleted, need refresh totally.", refreshedPartition));
                    return true;
                }
=======
            if (table instanceof OlapTable olapTable) {
                dropped = versionMap.keySet().stream()
                        .filter(name -> olapTable.getPartition(name) == null)
                        .collect(Collectors.toSet());
            } else {
                ConnectorPartitionTraits traits = ConnectorPartitionTraits.build(mv, table, pinnedVersionRange);
                Set<String> livePartitions = traits.getPartitionNameWithPartitionInfo().keySet();
                dropped = Sets.difference(versionMap.keySet(), livePartitions).immutableCopy();
>>>>>>> 8600f8bc580 ([BugFix] Refresh mv partitions that outlived a dropped base partition (#62450))
            }
        } catch (Exception e) {
            // A connector that cannot enumerate partitions would otherwise report every tracked partition as dropped
            // and pin the mv to a permanent full refresh.
            LOG.debug("Cannot check for deleted partitions for table {}, skipping check: {}",
                    table.getName(), e.getMessage());
            return Sets.newHashSet();
        }

        if (!dropped.isEmpty()) {
            logMVPrepare(mv, String.format("Base table %s partitions %s have been dropped", table.getName(), dropped));
        }
        return dropped;
    }

    /** Olap base tables are tracked in a table-id keyed map, external ones in a {@link BaseTableInfo} keyed map. */
    private static Map<String, MaterializedView.BasePartitionInfo> getTrackedPartitionVersions(
            MaterializedView mv, Table table) {
        MaterializedView.AsyncRefreshContext context = mv.getRefreshScheme().getAsyncRefreshContext();
        if (table.isNativeTableOrMaterializedView()) {
            return context.getBaseTableVisibleVersionMap().getOrDefault(table.getId(), Maps.newHashMap());
        }
        // matchTable compares the whole identity; an identifier alone repeats across catalogs and databases and would
        // hand back another table's version map.
        return mv.getBaseTableInfos().stream()
                .filter(info -> info.matchTable(table))
                .findFirst()
                .map(context::getBaseTableRefreshInfo)
                .orElseGet(Maps::newHashMap);
    }

    /**
     * The live mv partitions that may still hold rows from {@code droppedBaseNames}.
     * <p>
     * A partition's association entry is the set of base partitions it absorbed at its last refresh, so one that is
     * mapped and disjoint from the dropped names provably holds none of their rows. An entry only appears once the
     * partition has been refreshed, so an unmapped partition cannot be ruled out and is included; an mv carried over
     * from a version that did not maintain the mapping therefore degrades to a full refresh.
     * <p>
     * Empty for the 1:1 and finer-grained shapes, where the mv partition fed by the dropped base partition was itself
     * dropped as an orphan by partition sync, taking the stale rows with it.
     */
    public static Set<String> getMvPartitionsAffectedByDrops(MaterializedView mv, Table table,
                                                             Set<String> droppedBaseNames) {
        if (droppedBaseNames.isEmpty()) {
            return Sets.newHashSet();
        }
        Map<String, Set<String>> mvToBasePartitions =
                mv.getRefreshScheme().getAsyncRefreshContext().getMvPartitionNameRefBaseTablePartitionMap();
        // The mapping records partition names without the table they came from and each ref base table overwrites the
        // previous one, so an entry may hold another table's names. Keep only the names this table tracks, the same
        // attribution filter dropRefBaseTableFromVersionMap applies; an entry left with nothing was another table's
        // and rules nothing out. Ref base tables sharing a partition namespace still cannot be told apart, which
        // leaves those mvs where they were before drops were detected at all rather than making them worse.
        Set<String> trackedByThisTable = getTrackedPartitionVersions(mv, table).keySet();
        Set<String> affected = mv.getVisiblePartitionNames().stream()
                .filter(p -> {
                    Set<String> absorbed = mvToBasePartitions.get(p);
                    if (absorbed == null) {
                        return true;
                    }
                    Set<String> absorbedFromThisTable = Sets.intersection(absorbed, trackedByThisTable);
                    return absorbedFromThisTable.isEmpty()
                            || !Collections.disjoint(absorbedFromThisTable, droppedBaseNames);
                })
                .collect(Collectors.toSet());
        if (!affected.isEmpty()) {
            logMVPrepare(mv, String.format("Base table %s dropped partitions %s, refreshing mv partitions %s",
                    table.getName(), droppedBaseNames, affected));
        }
        return affected;
    }
}
