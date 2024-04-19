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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.scheduler.TableWithPartitions;
import com.starrocks.sql.common.PartitionDiffer;
import com.starrocks.sql.common.RangePartitionDiff;
import com.starrocks.sql.common.SyncPartitionUtils;
import com.starrocks.sql.common.UnsupportedException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.getMVPartitionNameWithRange;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

/**
* The arbiter of materialized view refresh. All implementations of refresh strategies should be here.
*/
public class MvRefreshArbiter {
    private static final Logger LOG = LogManager.getLogger(MvRefreshArbiter.class);

    public static boolean needToRefreshTable(MaterializedView mv, Table table) {
        MvBaseTableUpdateInfo mvBaseTableUpdateInfo = getMvBaseTableUpdateInfo(mv, table, true, false);
        if (mvBaseTableUpdateInfo == null) {
            return true;
        }
        return CollectionUtils.isNotEmpty(mvBaseTableUpdateInfo.getToRefreshPartitionNames());
    }

    /**
     * Get to refresh partition info of the specific table.
     * @param baseTable: the table to check
     * @param withMv: whether to check the materialized view if it's a materialized view
     * @param isQueryRewrite: whether this caller is query rewrite or not
     * @return MvBaseTableUpdateInfo: the update info of the base table
     */
    public static MvBaseTableUpdateInfo getMvBaseTableUpdateInfo(MaterializedView mv,
                                                                 Table baseTable,
                                                                 boolean withMv,
                                                                 boolean isQueryRewrite) {
        MvBaseTableUpdateInfo mvBaseTableUpdateInfo = new MvBaseTableUpdateInfo();
        if (baseTable.isView()) {
            // do nothing
            return mvBaseTableUpdateInfo;
        } else if (baseTable.isNativeTableOrMaterializedView()) {
            OlapTable olapBaseTable = (OlapTable) baseTable;
            Set<String> baseTableUpdatedPartitionNames = mv.getUpdatedPartitionNamesOfOlapTable(olapBaseTable, isQueryRewrite);

            // recursive check its children
            if (withMv && baseTable.isMaterializedView()) {
                MvUpdateInfo mvUpdateInfo = getPartitionNamesToRefreshForMv((MaterializedView) baseTable, isQueryRewrite);
                if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
                    return null;
                }
                baseTableUpdatedPartitionNames.addAll(mvUpdateInfo.getMvToRefreshPartitionNames());
            }
            // update base table's partition info
            mvBaseTableUpdateInfo.getToRefreshPartitionNames().addAll(baseTableUpdatedPartitionNames);
        } else {
            Set<String> updatePartitionNames = mv.getUpdatedPartitionNamesOfExternalTable(baseTable, isQueryRewrite);
            if (updatePartitionNames == null) {
                return null;
            }
            Map<Table, Column> partitionTableAndColumns = mv.getRelatedPartitionTableAndColumn();
            if (!partitionTableAndColumns.containsKey(baseTable)) {
                // ATTENTION: This partition value is not formatted to mv partition type.
                mvBaseTableUpdateInfo.getToRefreshPartitionNames().addAll(updatePartitionNames);
                return mvBaseTableUpdateInfo;
            }

            try {
                List<String> updatedPartitionNamesList = Lists.newArrayList(updatePartitionNames);
                Column partitionColumn = partitionTableAndColumns.get(baseTable);
                Expr partitionExpr = MaterializedView.getPartitionExpr(mv);
                Map<String, Range<PartitionKey>> partitionNameWithRange = getMVPartitionNameWithRange(baseTable,
                        partitionColumn, updatedPartitionNamesList, partitionExpr);
                mvBaseTableUpdateInfo.getPartitionNameWithRanges().putAll(partitionNameWithRange);
                mvBaseTableUpdateInfo.getToRefreshPartitionNames().addAll(partitionNameWithRange.keySet());
            } catch (AnalysisException e) {
                LOG.warn("Mv {}'s base table {} get partition name fail", mv.getName(), baseTable.name, e);
                return null;
            }
        }
        return mvBaseTableUpdateInfo;
    }


    /**
     * Once the materialized view's base tables have updated, we need to check correspond materialized views' partitions
     * to be refreshed.
     *
     * @return : Collect all need refreshed partitions of materialized view.
     * @isQueryRewrite : Mark whether this caller is query rewrite or not, when it's true we can use staleness to shortcut
     * the update check.
     */
    public static MvUpdateInfo getPartitionNamesToRefreshForMv(MaterializedView mv,
                                                               boolean isQueryRewrite) {
        // Skip check for sync materialized view.
        if (mv.getRefreshScheme().isSync()) {
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.NO_REFRESH);
        }

        // check mv's query rewrite consistency mode property only in query rewrite.
        TableProperty tableProperty = mv.getTableProperty();
        TableProperty.QueryRewriteConsistencyMode mvConsistencyRewriteMode = tableProperty.getQueryRewriteConsistencyMode();
        if (isQueryRewrite) {
            switch (mvConsistencyRewriteMode) {
                case DISABLE:
                    return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
                case NOCHECK:
                    return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.NO_REFRESH);
                case LOOSE:
                case CHECKED:
                default:
                    break;
            }
        }

        logMVPrepare(mv, "MV refresh arbiter start to get partition names to refresh, query rewrite mode: {}",
                mvConsistencyRewriteMode);
        if (mvConsistencyRewriteMode == TableProperty.QueryRewriteConsistencyMode.LOOSE) {
            return getPartitionNamesToRefreshForMvInLooseMode(mv, isQueryRewrite);
        } else {
            PartitionInfo partitionInfo = mv.getPartitionInfo();
            if (partitionInfo instanceof SinglePartitionInfo) {
                return getNonPartitionedMVRefreshPartitions(mv, isQueryRewrite);
            } else if (partitionInfo instanceof ExpressionRangePartitionInfo) {
                // partitions to refresh
                // 1. dropped partitions
                // 2. newly added partitions
                // 3. partitions loaded with new data
                return getPartitionedMVRefreshPartitions(mv, isQueryRewrite);
            } else {
                throw UnsupportedException.unsupportedException("unsupported partition info type:"
                        + partitionInfo.getClass().getName());
            }
        }
    }

    // In Loose mode, do not need to check mv partition's data is consistent with base table's partition's data.
    // Only need to check the mv partition existence.
    private static MvUpdateInfo getPartitionNamesToRefreshForMvInLooseMode(MaterializedView mv,
                                                                           boolean isQueryRewrite) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo instanceof SinglePartitionInfo) {
            List<Partition> partitions = Lists.newArrayList(mv.getPartitions());
            if (partitions.size() > 0 && partitions.get(0).getVisibleVersion() <= 1) {
                // the mv is newly created, can not use it to rewrite query.
                return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
            }
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.NO_REFRESH);
        }

        MvUpdateInfo mvUpdateInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.PARTIAL,
                TableProperty.QueryRewriteConsistencyMode.LOOSE);
        Expr partitionExpr = mv.getFirstPartitionRefTableExpr();
        Map<Table, Column> partitionTableAndColumn = mv.getRelatedPartitionTableAndColumn();
        Map<String, Range<PartitionKey>> mvRangePartitionMap = mv.getRangePartitionMap();
        RangePartitionDiff rangePartitionDiff = null;
        try {
            if (!collectBaseTablePartitionInfos(mv, partitionTableAndColumn, partitionExpr, isQueryRewrite, mvUpdateInfo)) {
                return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
            }
            Map<Table, MvBaseTableUpdateInfo> baseTableUpdateInfos = mvUpdateInfo.getBaseTableUpdateInfos();
            Map<Table, Map<String, Range<PartitionKey>>> refBaseTablePartitionMap = baseTableUpdateInfos.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPartitionNameWithRanges()));
            Table partitionTable = mv.getDirectTableAndPartitionColumn().first;
            Column partitionColumn = mv.getPartitionInfo().getPartitionColumns().get(0);
            PartitionDiffer differ = PartitionDiffer.build(mv, Pair.create(null, null));
            rangePartitionDiff = PartitionUtil.getPartitionDiff(partitionExpr, partitionColumn,
                    refBaseTablePartitionMap.get(partitionTable), mvRangePartitionMap, differ);
        } catch (Exception e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return null;
        }

        if (rangePartitionDiff == null) {
            LOG.warn("Materialized view compute partition difference with base table failed, the diff of range partition" +
                    " is null.");
            return null;
        }
        Map<String, Range<PartitionKey>> adds = rangePartitionDiff.getAdds();
        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            mvUpdateInfo.getMvToRefreshPartitionNames().add(mvPartitionName);
        }
        return mvUpdateInfo;
    }

    /**
     * For non-partitioned materialized view, once its base table have updated, we need refresh the
     * materialized view's totally.
     *
     * @return : non-partitioned materialized view's all need updated partition names.
     */
    private static MvUpdateInfo getNonPartitionedMVRefreshPartitions(MaterializedView mv,
                                                                     boolean isQueryRewrite) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Preconditions.checkState(partitionInfo instanceof SinglePartitionInfo);
        List<BaseTableInfo> baseTableInfos = mv.getBaseTableInfos();
        TableProperty tableProperty = mv.getTableProperty();
        for (BaseTableInfo tableInfo : baseTableInfos) {
            Table table = tableInfo.getTableChecked();
            // skip check freshness of view
            if (table.isView()) {
                continue;
            }

            // skip check external table if the external does not support rewrite.
            if (!table.isNativeTableOrMaterializedView()) {
                if (tableProperty.getForceExternalTableQueryRewrite() ==
                        TableProperty.QueryRewriteConsistencyMode.DISABLE) {
                    logMVPrepare(mv, "Non-partitioned contains external table, and it's disabled query rewrite");
                    return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
                }
            }

            // once mv's base table has updated, refresh the materialized view totally.
            MvBaseTableUpdateInfo mvBaseTableUpdateInfo = getMvBaseTableUpdateInfo(mv, table, true, isQueryRewrite);
            // TODO: fixme if mvBaseTableUpdateInfo is null, should return full refresh?
            if (mvBaseTableUpdateInfo != null &&
                    CollectionUtils.isNotEmpty(mvBaseTableUpdateInfo.getToRefreshPartitionNames())) {
                logMVPrepare(mv, "Non-partitioned base table has updated, need refresh totally.");
                return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
            }
        }
        return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.NO_REFRESH);
    }

    /**
     * Materialized Views' base tables have two kinds: ref base table and non-ref base table.
     * - If non ref base tables updated, need refresh all mv partitions.
     * - If ref base table updated, need refresh the ref base table's updated partitions.
     * <p>
     * eg:
     * CREATE MATERIALIZED VIEW mv1
     * PARTITION BY k1
     * DISTRIBUTED BY HASH(k1) BUCKETS 10
     * AS
     * SELECT k1, v1 as k2, v2 as k3
     * from t1 join t2
     * on t1.k1 and t2.kk1;
     * <p>
     * - t1 is mv1's ref base table because mv1's partition column k1 is deduced from t1
     * - t2 is mv1's non ref base table because mv1's partition column k1 is not associated with t2.
     *
     * @return : partitioned materialized view's all need updated partition names.
     */
    private static MvUpdateInfo getPartitionedMVRefreshPartitions(MaterializedView mv,
                                                                  boolean isQueryRewrite) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Preconditions.checkState(partitionInfo instanceof ExpressionRangePartitionInfo);
        // If non-partition-by table has changed, should refresh all mv partitions
        Expr partitionExpr = mv.getFirstPartitionRefTableExpr();
        Map<Table, Column> partitionInfos = mv.getRelatedPartitionTableAndColumn();
        if (partitionInfos.isEmpty()) {
            mv.setInactiveAndReason("partition configuration changed");
            LOG.warn("mark mv:{} inactive for get partition info failed", mv.getName());
            throw new RuntimeException(String.format("getting partition info failed for mv: %s", mv.getName()));
        }

        MvUpdateInfo.MvToRefreshType refreshType = determineRefreshType(mv, partitionInfos, isQueryRewrite);
        logMVPrepare(mv, "Partitioned mv to refresh type:{}", refreshType);
        if (refreshType == MvUpdateInfo.MvToRefreshType.FULL) {
            return new MvUpdateInfo(refreshType);
        }

        MvUpdateInfo mvRefreshInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.PARTIAL);
        if (!collectBaseTablePartitionInfos(mv, partitionInfos, partitionExpr,  isQueryRewrite, mvRefreshInfo)) {
            logMVPrepare(mv, "Partitioned mv collect base table infos failed");
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
        }

        Map<Table, MvBaseTableUpdateInfo> baseTableUpdateInfos = mvRefreshInfo.getBaseTableUpdateInfos();
        Map<Table, Map<String, Range<PartitionKey>>> basePartitionNameToRangeMap = baseTableUpdateInfos.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getPartitionNameWithRanges()));
        // TODO: prune the partitions based on ttl
        Pair<Table, Column> directTableAndPartitionColumn = mv.getDirectTableAndPartitionColumn();
        Table refBaseTable = directTableAndPartitionColumn.first;
        Column refBaseTablePartitionColumn = directTableAndPartitionColumn.second;

        Map<String, Range<PartitionKey>> refTablePartitionMap = basePartitionNameToRangeMap.get(refBaseTable);
        Map<String, Range<PartitionKey>> mvPartitionNameToRangeMap = mv.getRangePartitionMap();
        RangePartitionDiff rangePartitionDiff = PartitionUtil.getPartitionDiff(partitionExpr,
                refBaseTablePartitionColumn, refTablePartitionMap, mvPartitionNameToRangeMap, null);

        Set<String> needRefreshMvPartitionNames = Sets.newHashSet();

        // TODO: no needs to refresh the deleted partitions, because the deleted partitions are not in the mv's partition map.
        needRefreshMvPartitionNames.addAll(rangePartitionDiff.getDeletes().keySet());
        // remove ref base table's deleted partitions from `mvPartitionMap`
        for (String deleted : rangePartitionDiff.getDeletes().keySet()) {
            mvPartitionNameToRangeMap.remove(deleted);
        }

        // step2: refresh ref base table's new added partitions
        needRefreshMvPartitionNames.addAll(rangePartitionDiff.getAdds().keySet());
        mvPartitionNameToRangeMap.putAll(rangePartitionDiff.getAdds());

        Map<Table, Expr> tableToPartitionExprMap = mv.getTableToPartitionExprMap();
        Map<Table, Map<String, Set<String>>> baseToMvNameRef = SyncPartitionUtils
                .generateBaseRefMap(basePartitionNameToRangeMap, tableToPartitionExprMap, mvPartitionNameToRangeMap);
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef = SyncPartitionUtils
                .generateMvRefMap(mvPartitionNameToRangeMap, tableToPartitionExprMap, basePartitionNameToRangeMap);

        mvRefreshInfo.getBasePartToMvPartNames().putAll(baseToMvNameRef);
        mvRefreshInfo.getMvPartToBasePartNames().putAll(mvToBaseNameRef);

        // Step1: collect updated partitions by partition name to range name:
        // - deleted partitions.
        // - added partitions.
        Map<Table, Set<String>> baseChangedPartitionNames = mvRefreshInfo.getBaseTableUpdateInfos().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getToRefreshPartitionNames()));
        for (Map.Entry<Table, Set<String>> entry : baseChangedPartitionNames.entrySet()) {
            entry.getValue().stream().forEach(x ->
                    needRefreshMvPartitionNames.addAll(baseToMvNameRef.get(entry.getKey()).get(x))
            );
        }

        if (partitionExpr instanceof FunctionCallExpr) {
            List<TableWithPartitions> baseTableWithPartitions = baseChangedPartitionNames.keySet().stream()
                    .map(x -> new TableWithPartitions(x, baseChangedPartitionNames.get(x)))
                    .collect(Collectors.toList());
            if (mv.isCalcPotentialRefreshPartition(baseTableWithPartitions,
                    basePartitionNameToRangeMap, needRefreshMvPartitionNames, mvPartitionNameToRangeMap)) {
                // because the relation of partitions between materialized view and base partition table is n : m,
                // should calculate the candidate partitions recursively.
                SyncPartitionUtils.calcPotentialRefreshPartition(needRefreshMvPartitionNames, baseChangedPartitionNames,
                        baseToMvNameRef, mvToBaseNameRef, Sets.newHashSet());
            }
        }

        // update mv's to refresh partitions
        mvRefreshInfo.getMvToRefreshPartitionNames().addAll(needRefreshMvPartitionNames);
        return mvRefreshInfo;
    }

    private static boolean collectBaseTablePartitionInfos(MaterializedView mv,
                                                          Map<Table, Column> partitionInfos,
                                                          Expr partitionExpr,
                                                          boolean isQueryRewrite,
                                                          MvUpdateInfo mvUpdateInfo) {
        for (Map.Entry<Table, Column> entry : partitionInfos.entrySet()) {
            Table table = entry.getKey();

            // TODO: merge getPartitionKeyRange into mvBaseTableUpdateInfo
            // step1.2: check ref base table's updated partition names by checking its ref tables recursively.
            MvBaseTableUpdateInfo mvBaseTableUpdateInfo  =
                    getMvBaseTableUpdateInfo(mv, table, true, isQueryRewrite);
            if (mvBaseTableUpdateInfo == null) {
                return false;
            }

            // TODO: no need to list all partitions, just need to list the changed partitions?
            try {
                Map<String, Range<PartitionKey>> partitionKeyRanges =
                        PartitionUtil.getPartitionKeyRange(table, entry.getValue(), partitionExpr);
                mvBaseTableUpdateInfo.getPartitionNameWithRanges().putAll(partitionKeyRanges);
            } catch (UserException e) {
                LOG.warn("Materialized view compute partition difference with base table failed.", e);
                return false;
            }
            mvUpdateInfo.getBaseTableUpdateInfos().put(table, mvBaseTableUpdateInfo);
        }
        return true;
    }

    private static MvUpdateInfo.MvToRefreshType determineRefreshType(MaterializedView mv,
                                                                     Map<Table, Column> partitionInfos,
                                                                     boolean isQueryRewrite) {
        TableProperty tableProperty = mv.getTableProperty();
        for (BaseTableInfo tableInfo : mv.getBaseTableInfos()) {
            Table baseTable = tableInfo.getTableChecked();
            // skip view
            if (baseTable.isView()) {
                continue;
            }
            // skip external table that is not supported for query rewrite, return all partition ?
            // skip check external table if the external does not support rewrite.
            if (!baseTable.isNativeTableOrMaterializedView()) {
                if (tableProperty.getForceExternalTableQueryRewrite() ==
                        TableProperty.QueryRewriteConsistencyMode.DISABLE) {
                    return MvUpdateInfo.MvToRefreshType.FULL;
                }
            }
            if (partitionInfos.containsKey(baseTable)) {
                continue;
            }
            // If the non ref table has already changed, need refresh all materialized views' partitions.
            MvBaseTableUpdateInfo mvBaseTableUpdateInfo =
                    getMvBaseTableUpdateInfo(mv, baseTable, true, isQueryRewrite);
            if (mvBaseTableUpdateInfo == null || CollectionUtils.isNotEmpty(mvBaseTableUpdateInfo.getToRefreshPartitionNames())) {
                return MvUpdateInfo.MvToRefreshType.FULL;
            }
        }
        return MvUpdateInfo.MvToRefreshType.PARTIAL;
    }
}
